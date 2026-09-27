--- Объявленная тема: отправка, получатель, возврат зарытых.
---
--- **Тема повтора и тема мёртвых принадлежат паре «тема и группа».**
--- Получатель — группа, и её неудачи никого больше не касаются: копия
--- в самой теме досталась бы всем группам, которые её читают, и чужой
--- получатель обработал бы сообщение ещё раз. Поэтому у группы по
--- умолчанию — её имя совпадает с именем темы — это `<тема>.retry`
--- и `<тема>.dead`, а у названной группы — `<тема>.<группа>.retry`
--- и `<тема>.<группа>.dead`.
---
--- **Темы заводятся при первом деле**: пока сообщений нет, соединение
--- не открывается вовсе. Заводится только недостающее
--- (`tnt.kafka.admin`), и учётке без права на заведение тем хватает
--- права на запись в готовые.
---
--- **Отправка в транзакции box — ошибка программиста**: она ждёт брокера,
--- а уступка порвала бы транзакцию, и сообщение ушло бы и при откате.
--- «Сохранили и отправили» — через `tnt-outbox` (`broker:sender()`).

local admin = require('tnt.kafka.admin')
local external = require('tnt.external')
local hook = require('tnt.kafka.hook')
local kick = require('tnt.kafka.kick')
local message = require('tnt.kafka.message')
local must = require('tnt.must')
local outcome = require('tnt.kafka.outcome')
local within = require('tnt.storage.within')
local worker = require('tnt.kafka.worker')

local Module = {}

--- Внешние средства: идёт ли транзакция box. До `box.cfg` транзакций
--- нет, и спрашивать о них нечего.
local source = external.install(Module, {
    in_txn = function()
        return type(box.cfg) ~= 'function' and box.is_in_txn()
    end,
})

--- Имя темы и группы: буквы, цифры, точка, тире и подчёркивание —
--- ровно то, что Kafka принимает в имени темы.
local NAME = '^[%w%._%-]+$'

--- Предел длины имени темы у Kafka.
Module.NAME_LIMIT = 249

--- Приставки темы повтора и темы мёртвых.
Module.RETRY, Module.DEAD = '.retry', '.dead'

--- Настройки объявления.
local DECLARE = { group = '?not_empty', partitions = '?integer', replication = '?integer', shape = '?table' }

---@class TntKafkaDeclareOptions
---@field group string|nil Группа получателей; по умолчанию — имя темы
---@field partitions integer|nil Число разделов заводимых тем; по умолчанию — брокера
---@field replication integer|nil Число копий заводимых тем; по умолчанию — брокера
---@field shape table|nil Форма тела `tnt-data`: проверяется при отправке и на входе получателя

---@class TntKafkaTopic Объявленная тема
---@field broker TntKafkaBroker
---@field name string Имя темы
---@field group string Группа получателей
---@field retry string Тема повтора
---@field dead string Тема мёртвых
---@field spec TntKafkaTopicSpec Чем заводятся темы
---@field shape table|nil Форма тела
---@field ready boolean Заведены ли темы
---@field counts table<string, integer> Счётчики
---@field consumers TntKafkaConsumer[] Объявленные получатели
---@field kicker { coordinator: TntKafkaAddress|nil } Ведущий группы возврата
local Topic = {}
Topic.__index = Topic

--- Объявляет тему. Темы на брокере заводятся при первом деле.
---@param broker TntKafkaBroker
---@param name string
---@param opts TntKafkaDeclareOptions|nil
---@param level integer Уровень вины в кадрах вызывающего эту функцию
---@return TntKafkaTopic
function Module.new(broker, name, opts, level)
    local caller = must.at(level + 1)

    caller.matches(name, 'имя темы', NAME)

    local given = caller.optional.options(opts, 'настройки темы', DECLARE) or {}
    local group = given.group or name

    caller.matches(group, 'настройки темы.group', NAME)
    caller.optional.positive(given.partitions, 'настройки темы.partitions')
    caller.optional.positive(given.replication, 'настройки темы.replication')

    if given.shape ~= nil then
        caller.callable(given.shape.from, 'настройки темы.shape.from')
    end

    local stem = group == name and name or ('%s.%s'):format(name, group)

    caller.between(#stem + #Module.RETRY, 'длина имени темы повтора', 1, Module.NAME_LIMIT)

    return setmetatable({
        broker = broker,
        name = name,
        group = group,
        retry = stem .. Module.RETRY,
        dead = stem .. Module.DEAD,
        spec = { partitions = given.partitions, replication = given.replication },
        shape = given.shape,
        ready = false,
        counts = { sent = 0, ack = 0, retry = 0, bury = 0, late = 0, failures = 0, skipped = 0, kicked = 0 },
        consumers = {},
        kicker = {},
    }, Topic)
end

--- Заводит тему, тему повтора и тему мёртвых, если их нет. Один раз
--- на объявление: дальше за темами следит описание кластера.
---@param deadline number
---@return boolean|nil ok
---@return TntStorageFailure|nil err
function Topic:prepare(deadline)
    if self.ready then
        return true
    end

    local ok, err = admin.ensure(self.broker.cluster, { self.name, self.retry, self.dead }, self.spec, deadline)

    self.ready = ok == true

    return ok, err
end

--- Пишет запись в тему одним вызовом с повторами.
---@param destination string Тема
---@param record TntKafkaOutgoing
---@param timeout number|nil Срок; по умолчанию — клиента
---@return { partition: integer, offset: number }|nil
---@return TntStorageFailure|nil err
function Topic:forward(destination, record, timeout)
    local state = {}

    return self.broker.cluster:call(function(deadline)
        local ready, err = self:prepare(deadline)

        if not ready then
            return nil, err
        end

        return self.broker.producer:attempt(destination, record, deadline, state)
    end, timeout or self.broker.limits.timeout)
end

--- Отправляет сообщение и отдаёт его опознаватель — после того, как
--- запись подтвердили все синхронные копии раздела.
---@param body any Тело — простые данные
---@param opts table|nil Настройки отправки
---@return string|nil id
---@return TntStorageFailure|nil err
function Topic:send(body, opts)
    local given = message.options(opts, 2)

    message.body(body, 2)

    if source().in_txn() then
        error(
            ('отправка в %s внутри транзакции box: она ждёт брокера, а уступка порвёт транзакцию — '):format(
                self.name
            ) .. 'отправляйте через tnt-outbox',
            2
        )
    end

    if self.shape ~= nil then
        local _, errors = self.shape.from(body)

        if errors ~= nil then
            error(('тело для темы %s не по форме: %s'):format(self.name, message.shaped(errors)), 2)
        end
    end

    local call = { kind = hook.SEND, name = self.name }
    local timeout = within.timeout(given.timeout, self.broker.limits, 2)

    -- Конверт собирается внутри крюков: контекст, который завёл крюк,
    -- уезжает заголовками вместе с сообщением.
    local function proceed()
        call.message = message.envelope(self.name, body, given)

        local written, err = self:forward(self.name, message.record(call.message), timeout)

        if not written then
            return nil, err
        end

        self.counts.sent = self.counts.sent + 1

        return call.message.id
    end

    -- Крюк отдаёт итог отправки как есть: пару «опознаватель, отказ».
    ---@diagnostic disable-next-line: redundant-return-value
    return hook.around(call, proceed)
end

--- Объявляет получателя: работники входят в группу и обрабатывают
--- тему и тему повтора.
---@param handler fun(message: TntKafkaMessage): any, any
---@param opts table|nil Настройки получателя
---@return TntKafkaConsumer
function Topic:consume(handler, opts)
    must.at(2).callable(handler, 'обработчик')

    local consumer = worker.start(self, handler, outcome.policy(opts, 2))

    self.consumers[#self.consumers + 1] = consumer

    return consumer
end

--- Возвращает до `count` зарытых на выдачу и отдаёт число возвращённых.
---
--- Возвращённое уходит в тему повтора со счётом выдач с нуля: основная
--- тема досталась бы всем её группам, а зарыла сообщение одна.
---@param count integer|nil Сколько вернуть; по умолчанию одно
---@return integer|nil moved
---@return TntStorageFailure|nil err
function Topic:kick(count)
    local caller = must.at(2)

    caller.optional.integer(count, 'сколько вернуть')

    local asked = count or 1

    caller.positive(asked, 'сколько вернуть')

    local moved, err = kick.revive(self, asked)

    self.counts.kicked = self.counts.kicked + (moved or 0)

    return moved, err
end

--- Что с темой сейчас: имена, счётчики, работники, крюки.
---@return table
function Topic:status()
    local workers = 0

    for _, consumer in ipairs(self.consumers) do
        workers = workers + consumer.workers
    end

    return {
        name = self.name,
        group = self.group,
        retry = self.retry,
        dead = self.dead,
        counts = table.copy(self.counts),
        workers = workers,
        hooks = hook.names(),
    }
end

return Module
