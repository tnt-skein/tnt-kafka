--- Сообщение: конверт, настройки отправки, тело — и то, как всё это
--- ложится в запись Kafka.
---
--- **Опознаватель ставит отправитель**, а не брокер: смещение записи
--- у копии в теме повтора другое, и по нему повтор не отсечь. ULID едет
--- заголовком `x-tnt-id`, несёт время в старших битах, и по нему
--- получатель узнаёт повтор.
---
--- **Ключ записи — ключ порядка.** Записи одного ключа ложатся в один
--- раздел и выдаются по порядку; без ключа раздел выбирает производитель.
---
--- **Контекст едет заголовками записи**: `context.export()` в миг
--- отправки, поверх — заголовки вызывающего, они главнее.
---
--- **Своё — с приставкой `x-tnt-`**: опознаватель, номер выдачи
--- (`x-tnt-attempt`), точное время отправки (`x-tnt-created`), причина
--- у зарытого (`x-tnt-reason`) и срок, раньше которого копию в теме
--- повтора не обрабатывают (`x-tnt-due`). Обработчику они не
--- показываются, как и `content-type`: это знание пакета, а не
--- отправителя.
---
--- **Тело — простые данные** и уходит JSON с `content-type:
--- application/json`: так его прочтёт и чужой получатель. cdata
--- (`int64`, `decimal`, `uuid`, `datetime`) в JSON теряет род — это
--- исключение при отправке, как и функция, бесконечность и кольцо.

local json = require('json')

local clock = require('tnt.clock')
local context = require('tnt.context')
local id = require('tnt.id')
local must = require('tnt.must')
local names = require('tnt.context.key')

local Module = {}

--- Потолок сроков в секундах — сто лет по 365 суток: срок длиннее
--- читается как «никогда».
Module.MAX_SECONDS = 3153600000

--- Заголовок и вид тела.
Module.CONTENT_TYPE, Module.JSON = 'content-type', 'application/json'

--- Свои заголовки.
Module.ID = 'x-tnt-id'
Module.ATTEMPT = 'x-tnt-attempt'
Module.CREATED = 'x-tnt-created'
Module.REASON = 'x-tnt-reason'
Module.DUE = 'x-tnt-due'

--- Как глубоко тело вкладывает таблицы: глубже — исключение, а не
--- переполнение стека на проверке.
Module.MAX_DEPTH = 100

--- Что пакет умеет: у Kafka есть порядок по ключу, и только он.
Module.FEATURES = {
    atomic = false,
    delay = false,
    key = true,
    priority = false,
    touch = false,
    transactional = false,
    ttl = false,
    ttr = false,
}

--- Почему брокер не умеет названное.
local UNSUPPORTED = {
    delay = 'отсрочки у Kafka нет: запись выдаётся, как только легла',
    ttl = 'срок жизни у Kafka один на тему — её хранение, а не у сообщения',
    priority = 'приоритетов у Kafka нет: раздел выдаётся по порядку смещений',
}

--- Настройки отправки. Те, которых брокер не умеет, знакомы: о них
--- говорит свой отказ, а не «такого ключа нет».
local SEND = { headers = '?table', id = '?not_empty', key = '?not_empty', timeout = '?number' }

for name in pairs(UNSUPPORTED) do
    SEND[name] = '?'
end

---@class TntKafkaMessage Конверт сообщения: то, что видит обработчик
---@field id string ULID, его ставит отправитель
---@field name string Назначение: имя темы
---@field body any Тело — простые данные
---@field headers table<string, string> Заголовки: контекст отправителя и свои
---@field key string|nil Ключ порядка
---@field attempt integer Номер выдачи: 0 до первой, 1 на первой
---@field created number Стенное время отправки, секунды
---@field foreign boolean|nil Чужое содержимое темы: тело не JSON

---@class TntKafkaOutgoing Запись для производителя
---@field key string|nil
---@field value string|nil
---@field headers { [1]: string, [2]: string }[]
---@field timestamp integer Отметка времени, мс

--- Проверяет настройки отправки; негодные — исключение на строке того,
--- кто позвал отправку.
---@param opts any
---@param level integer Уровень вины в кадрах вызывающего эту функцию
---@return table
function Module.options(opts, level)
    local caller = must.at(level + 1)
    local given = caller.optional.options(opts, 'настройки отправки', SEND) or {}

    for name, reason in pairs(UNSUPPORTED) do
        if given[name] ~= nil then
            error(('настройки отправки: «%s» — %s'):format(name, reason), level + 1)
        end
    end

    caller.optional.positive(given.timeout, 'настройки отправки.timeout')

    if given.headers ~= nil then
        for label, text in pairs(given.headers) do
            caller.matches(label, 'имя заголовка', names.HEADER_PATTERN)
            caller.string(text, 'заголовок ' .. label)
        end
    end

    return given
end

--- Почему значение — не простые данные; `nil` — простые.
---@param value any
---@param path string Где оно в теле
---@param trail table<table, boolean> Таблицы на пути от корня
---@param depth integer
---@return string|nil
local function complaint(value, path, trail, depth)
    local kind = type(value)

    if kind == 'number' then
        -- NaN не равен сам себе, а бесконечность JSON пишет словом `inf`,
        -- и прочесть его обратно уже нечем.
        if value ~= value or value == math.huge or value == -math.huge then
            return ('%s — конечное число, а не %s'):format(path, tostring(value))
        end

        return nil
    end

    if kind ~= 'table' then
        if kind == 'string' or kind == 'boolean' or kind == 'nil' then
            return nil
        end

        return ('%s — простые данные, а не %s'):format(path, kind)
    end

    if trail[value] then
        return ('%s — кольцо: таблица лежит сама в себе'):format(path)
    end

    if depth > Module.MAX_DEPTH then
        return ('%s — вложенность глубже %d'):format(path, Module.MAX_DEPTH)
    end

    trail[value] = true

    for name, item in pairs(value) do
        local named = type(name)

        if named ~= 'string' and named ~= 'number' then
            return ('%s: ключ — строка или число, а не %s'):format(path, named)
        end

        local deeper = complaint(item, ('%s.%s'):format(path, tostring(name)), trail, depth + 1)

        if deeper ~= nil then
            return deeper
        end
    end

    trail[value] = nil

    return nil
end

--- Проверяет тело: простые данные либо исключение.
---@param body any
---@param level integer Уровень вины в кадрах вызывающего эту функцию
function Module.body(body, level)
    local found = complaint(body, 'тело', {}, 1)

    if found == nil then
        return
    end

    error(found, level + 1)
end

--- Собирает конверт в миг отправки.
---@param name string Имя темы
---@param body any
---@param opts table Проверенные настройки
---@return TntKafkaMessage
function Module.envelope(name, body, opts)
    local headers = context.export()

    -- Заголовки вызывающего главнее перенесённых контекстом.
    if opts.headers ~= nil then
        for label, text in pairs(opts.headers) do
            headers[label] = text
        end
    end

    ---@type table
    local envelope = { name = name, body = body, key = opts.key, headers = headers }

    envelope.id = opts.id or id.ulid()
    envelope.attempt, envelope.created = 0, clock.realtime()

    return envelope --[[@as TntKafkaMessage]]
end

--- Заголовки записи по порядку имён: одинаковый порядок у одного
--- конверта — одинаковые байты у повторной отправки.
---@param headers table<string, string>
---@return { [1]: string, [2]: string }[]
local function listed(headers)
    local list = {}

    for name, value in pairs(headers) do
        list[#list + 1] = { name, value }
    end

    table.sort(list, function(left, right)
        return left[1] < right[1]
    end)

    return list
end

--- Свои заголовки конверта поверх его заголовков.
---@param envelope TntKafkaMessage
---@param extra table<string, string>|nil Причина, срок
---@return table<string, string>
local function stamped(envelope, extra)
    local headers = {}

    for name, value in pairs(envelope.headers) do
        headers[name] = value
    end

    for name, value in pairs(extra or {}) do
        headers[name] = value
    end

    headers[Module.CONTENT_TYPE] = Module.JSON
    headers[Module.ID] = envelope.id
    headers[Module.ATTEMPT] = tostring(envelope.attempt)
    headers[Module.CREATED] = ('%.6f'):format(envelope.created)

    return headers
end

--- Запись для отправки: ключ, тело JSON и заголовки.
---@param envelope TntKafkaMessage
---@return TntKafkaOutgoing
function Module.record(envelope)
    return {
        key = envelope.key,
        value = json.encode(envelope.body),
        headers = listed(stamped(envelope)),
        timestamp = math.floor(envelope.created * 1000),
    }
end

--- Копия доставленного для темы повтора либо темы мёртвых.
---
--- Тело копии — байты доставки как есть: разбирать и собирать его заново
--- значило бы потерять то, чего не знает JSON, у чужого отправителя.
---@param value string|nil Байты тела доставки
---@param envelope TntKafkaMessage Конверт доставки
---@param extra table<string, string> Причина либо срок
---@return TntKafkaOutgoing
function Module.forward(value, envelope, extra)
    return {
        key = envelope.key,
        value = value,
        headers = listed(stamped(envelope, extra)),
        timestamp = math.floor(clock.realtime() * 1000),
    }
end

--- Разбирает запись в конверт.
---
--- Чужая запись — та, чьё тело не JSON: её получатель зароет сразу, под
--- новым опознавателем. Запись чужого отправителя с телом JSON — не чужая:
--- опознаватель ей даётся свой, а номер выдачи начинается с первой.
---@param name string Имя темы, откуда выдана запись
---@param record TntKafkaRecord
---@param redelivered integer|nil Сколько раз её уже выдавали без итога; нет — ни разу
---@return TntKafkaMessage envelope
---@return table<string, string> own Свои заголовки записи
function Module.delivered(name, record, redelivered)
    local headers, own = {}, {}

    for _, header in ipairs(record.headers) do
        local label, value = header[1], header[2]

        if value ~= nil then
            if label == Module.CONTENT_TYPE or label:find('^x%-tnt%-') then
                own[label] = value
            else
                headers[label] = value
            end
        end
    end

    local envelope = {
        id = own[Module.ID] or id.ulid(),
        name = name,
        headers = headers,
        key = record.key,
        attempt = math.floor(tonumber(own[Module.ATTEMPT]) or 0) + 1 + (redelivered or 0),
        created = tonumber(own[Module.CREATED]) or record.timestamp / 1000,
    }
    local ok, decoded = false, nil

    if record.value ~= nil then
        ok, decoded = pcall(json.decode, record.value)
    end

    if not ok then
        envelope.foreign = true
    elseif decoded ~= nil then
        -- Пустое тело приходит из JSON как `box.NULL`: обработчику оно
        -- показывается настоящей пустотой.
        envelope.body = decoded
    end

    return envelope, own
end

--- Отказы формы одной строкой, по порядку мест: `место: текст; …`.
---@param errors table<string, string>
---@return string
function Module.shaped(errors)
    local places = {}

    for place in pairs(errors) do
        table.insert(places, place)
    end

    table.sort(places)

    for index, place in ipairs(places) do
        places[index] = place .. ': ' .. errors[place]
    end

    return table.concat(places, '; ')
end

return Module
