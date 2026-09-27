--- Настройки клиента Kafka: умолчания и проверка.
---
--- Проверяется всё и сразу, в `kafka.new`, а не там, где до настройки
--- впервые дошло дело: клиент заводят при подъёме узла, а первое
--- сообщение шлют через час под нагрузкой. Негодная настройка —
--- исключение на строке того, кто завёл клиент: это ошибка кода,
--- а не отказ службы. Незнакомый ключ — тоже: опечатка в имени
--- (`pasword`) иначе молча оставила бы клиент без пароля.
---
--- Сроки проверяет `tnt-storage` (`within.settings`): умолчание 5 с,
--- потолок `max_timeout` 60 с. Границы пула и повторов проверяют сами
--- `tnt-pool` и `tnt-retry`: здесь — только набор ключей и род значений,
--- чтобы опечатка показывала на вызывающего.

local must = require('tnt.must')
local within = require('tnt.storage.within')

local Module = {}

--- Узел по умолчанию: брокер на той же машине.
Module.DEFAULT_BROKER = '127.0.0.1:9092'

--- Предел длины сообщения по умолчанию: 1 МБ, столько же по умолчанию
--- принимает брокер (`message.max.bytes`).
---
--- Тело длиннее занимает память узла, в которой живут его данные. Кому
--- нужно больше, поднимает предел здесь и у брокера.
Module.DEFAULT_MAX_BYTES = 1024 * 1024

--- Имя клиента по умолчанию: в журнале, в имени пула и ведре повторов.
Module.DEFAULT_NAME = 'kafka'

--- Чем клиент представляется брокеру: имя видно в его журнале и квотах.
Module.DEFAULT_CLIENT_ID = 'tnt-kafka'

--- Сколько группа ждёт стука участника, секунд.
---
--- Десять: брокер по умолчанию принимает от шести, а участник стучит
--- втрое чаще срока — раз в три с небольшим секунды.
Module.DEFAULT_SESSION_TIMEOUT = 10

--- Сколько группа ждёт участников при раздаче разделов, секунд.
---
--- Участник замечает раздачу стуком и входит заново, дорабатывая взятое
--- сообщение: минута — срок обработчику, после которого брокер раздаёт
--- его разделы без него, а итог его опоздает.
Module.DEFAULT_REBALANCE_TIMEOUT = 60

--- Способ входа SASL, который пакет умеет.
Module.SASL_PLAIN = 'PLAIN'

--- Уровень вины: строка того, кто завёл клиент.
---
--- `check` зовёт `kafka.new` не хвостовым вызовом: уровень 2 — строка
--- в `kafka.lua`, 3 — строка вызывающего.
local OWNER = 3

--- Проверки с виной на строке того, кто завёл клиент.
local owner = must.at(OWNER)

--- Как настройки называются в отказе.
local TITLE = 'настройки kafka'

--- Адрес узла: имя либо адрес и порт через двоеточие; адрес IPv6 —
--- в квадратных скобках.
local ADDRESS = '^%[?([^%[%]]-)%]?:(%d+)$'

--- Настройки пула и повторов, которые клиент передаёт как есть.
local POOL = { size = '?integer' }
local RETRY = { attempts = '?integer', jitter = '?number|string' }

for _, name in ipairs({
    'wait_timeout',
    'idle_timeout',
    'max_lifetime',
    'open_cooldown',
    'leak_timeout',
    'sweep_interval',
}) do
    POOL[name] = '?number'
end

for _, name in ipairs({ 'base', 'factor', 'max' }) do
    RETRY[name] = '?number'
end

--- Шифрование: то, что уходит в `tnt-tls`.
local TLS = { verify = '?boolean', ca_file = '?not_empty', ca_path = '?not_empty', sni = '?not_empty' }

--- Вход по SASL.
local SASL = { mechanism = '?not_empty', username = 'not_empty', password = 'string' }

--- Все настройки клиента. Незнакомый ключ — отказ.
local OPTIONS = {
    brokers = '?table',
    tls = '?boolean|table',
    sasl = { '?options', SASL },
    timeout = '?number',
    max_timeout = '?number',
    max_bytes = '?integer',
    session_timeout = '?number',
    rebalance_timeout = '?number',
    client_id = '?not_empty',
    pool = { '?options', POOL },
    retry = { '?options', RETRY },
    name = '?not_empty',
}

---@class TntKafkaOptions
---@field brokers string[]|nil Узлы для первого знакомства: `host:port`; по умолчанию 127.0.0.1:9092
---@field tls boolean|table|nil Шифрование: true либо настройки `tnt-tls`
---@field sasl { mechanism: string|nil, username: string, password: string }|nil Вход по SASL PLAIN
---@field timeout number|nil Срок вызова по умолчанию, секунд; 5
---@field max_timeout number|nil Потолок срока вызова, секунд; 60
---@field max_bytes integer|nil Предел длины сообщения, байт; 1 МБ
---@field session_timeout number|nil Сколько группа ждёт стука участника, секунд; 10
---@field rebalance_timeout number|nil Сколько группа ждёт участников при раздаче, секунд; 60
---@field client_id string|nil Чем клиент представляется брокеру; tnt-kafka
---@field pool table|nil Настройки пула на каждый узел: size, wait_timeout и прочие `tnt-pool`
---@field retry table|nil Настройки повторов: attempts (по умолчанию — до срока вызова), base, factor, jitter, max
---@field name string|nil Имя клиента в журнале; kafka

---@class TntKafkaAddress
---@field host string
---@field port integer

---@class TntKafkaSettings
---@field name string
---@field brokers TntKafkaAddress[]
---@field tls table|nil
---@field sasl { mechanism: string, username: string, password: string }|nil
---@field max_bytes integer
---@field session_timeout number
---@field rebalance_timeout number
---@field client_id string
---@field limits TntStorageLimits Сроки вызова
---@field pool table Настройки пула как даны
---@field retry table Настройки повторов как даны

--- Узлы знакомства: список адресов, каждый — узел и порт.
---
--- Вина считается на кадр глубже, чем у соседних проверок: между этой
--- функцией и `check` лежит её собственный кадр.
---@param brokers any
---@return TntKafkaAddress[]
local function addresses(brokers)
    local caller = must.at(OWNER + 1)
    local given = brokers or { Module.DEFAULT_BROKER }
    local parsed = {}

    caller.array(given, TITLE .. '.brokers')
    caller.positive(#given, TITLE .. '.brokers: число узлов')

    for index, text in ipairs(given) do
        local name = ('%s.brokers[%d]'):format(TITLE, index)

        caller.matches(text, name, ADDRESS)

        local host, port = text:match(ADDRESS)

        caller.not_empty(host, name .. ': узел')
        caller.between(tonumber(port), name .. ': порт', 1, 65535)
        parsed[index] = { host = host, port = tonumber(port) }
    end

    return parsed
end

--- Вход по SASL: способ только PLAIN.
---@param sasl table|nil
---@return table|nil
local function login(sasl)
    if sasl == nil then
        return nil
    end

    local mechanism = sasl.mechanism or Module.SASL_PLAIN

    must.at(OWNER + 1).one_of(mechanism, TITLE .. '.sasl.mechanism', { Module.SASL_PLAIN })

    return { mechanism = mechanism, username = sasl.username, password = sasl.password }
end

--- Проверяет настройки и дополняет их умолчаниями.
---@param opts TntKafkaOptions|nil
---@return TntKafkaSettings
function Module.check(opts)
    owner.optional.options(opts, TITLE, OPTIONS)

    local given = opts or {}
    ---@type table|nil
    local tls = nil

    if type(given.tls) == 'table' then
        owner.options(given.tls, TITLE .. '.tls', TLS)
        tls = given.tls
    elseif given.tls == true then
        tls = {}
    end

    owner.optional.positive(given.max_bytes, TITLE .. '.max_bytes')
    owner.optional.positive(given.session_timeout, TITLE .. '.session_timeout')
    owner.optional.positive(given.rebalance_timeout, TITLE .. '.rebalance_timeout')

    return {
        name = given.name or Module.DEFAULT_NAME,
        brokers = addresses(given.brokers),
        tls = tls,
        sasl = login(given.sasl),
        max_bytes = given.max_bytes or Module.DEFAULT_MAX_BYTES,
        session_timeout = given.session_timeout or Module.DEFAULT_SESSION_TIMEOUT,
        rebalance_timeout = given.rebalance_timeout or Module.DEFAULT_REBALANCE_TIMEOUT,
        client_id = given.client_id or Module.DEFAULT_CLIENT_ID,
        limits = within.settings(given.timeout, given.max_timeout, OWNER),
        pool = given.pool or {},
        retry = given.retry or {},
    }
end

return Module
