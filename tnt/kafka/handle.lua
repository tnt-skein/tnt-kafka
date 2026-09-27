--- Одна выданная запись: отметка выдачи, обработчик, итог и смещение.
---
--- **Порядок шагов держит «хотя бы раз».** Сначала отмечается выдача
--- (`tnt.kafka.offsets`), затем обработчик, затем движение итога — копия
--- в тему повтора либо мёртвых, — и только потом смещение следующей
--- записи. Обрыв между любыми двумя шагами оставляет смещение на этой
--- записи, и её выдадут снова; копия, ушедшая до обрыва, уйдёт второй раз
--- с тем же опознавателем, и получатель отсечёт её по `message.id`.
---
--- **Повтор — копией в тему повтора**, а не сном на месте: раздел идёт
--- дальше, и одно больное сообщение не держит все за собой. Копия несёт
--- номер выдачи и срок `x-tnt-due`, раньше которого её не обрабатывают.
--- Цена — порядок ключа: повторённое сообщение обработается позже
--- следующих за ним того же ключа.
---
--- **Зарытое — копией в тему мёртвых** с причиной заголовком
--- `x-tnt-reason`: человек видит тело вместе с объяснением.
---
--- **Итог мог не дойти.** Копия не ушла либо смещение не легло — запись
--- выдадут снова: работа сделана дважды, пишется `warn` со счётом `late`.
---
--- Обработчик — код приложения, и права у него — права фонового файбера,
--- то есть процесса, а не того, кто объявил получателя.

local clock = require('tnt.clock')
local codes = require('tnt.kafka.codes')
local context = require('tnt.context')
local hook = require('tnt.kafka.hook')
local id = require('tnt.id')
local message = require('tnt.kafka.message')
local offsets = require('tnt.kafka.offsets')
local outcome = require('tnt.kafka.outcome')

local log = require('tnt.log').new('tnt.kafka')

local Module = {}

---@class TntKafkaLane Раздел, который ведёт участник
---@field topic string Тема
---@field partition integer
---@field position number Смещение следующей выборки
---@field marked { offset: number, deliveries: integer|nil } Отметка выдачи с брокера; без отметки — без числа выдач
---@field buffer TntKafkaRecord[] Выбранное и ещё не обработанное
---@field resume number|nil Когда раздел снова можно обрабатывать, по монотонным часам

---@class TntKafkaStation Участник группы: место, ведущий, разделы
---@field seat TntKafkaSeat
---@field coordinator TntKafkaAddress|nil
---@field lanes TntKafkaLane[]
---@field rejoin boolean Войти в группу заново

--- Обработчик `xpcall`: стек берётся здесь — после раскрутки его нет.
---@param err any
---@return { err: any, traceback: string }
local function traced(err)
    return { err = err, traceback = debug.traceback() }
end

--- Фиксирует смещение раздела через ведущего группы.
---@param consumer TntKafkaConsumer
---@param station TntKafkaStation
---@param lane TntKafkaLane
---@param offset number
---@param deliveries integer|nil Отметка выдачи; `nil` — итог
---@return boolean|nil ok
---@return TntStorageFailure|nil err
function Module.commit(consumer, station, lane, offset, deliveries)
    local broker = consumer.topic.broker
    local seat = station.seat
    local member = { group = seat.group, generation = seat.generation, member = seat.member }

    return offsets.call(broker.cluster, station, seat.group, broker.limits.timeout, function(coordinator, deadline)
        return offsets.commit(
            broker.cluster,
            coordinator,
            member,
            lane.topic,
            lane.partition,
            offset,
            deliveries,
            deadline
        )
    end)
end

--- Сколько раздел отдыхает после отказа, секунд: беда, которая держится,
--- иначе крутила бы выборку и отметку без передышки.
Module.PAUSE = 0.1

--- Шаг не дошёл до брокера: запись выдадут снова. Выбранное
--- выбрасывается, раздел отдыхает и перечитает с зафиксированного,
--- а отказ «выпал из группы» зовёт вход заново.
---@param consumer TntKafkaConsumer
---@param station TntKafkaStation
---@param lane TntKafkaLane
---@param err TntStorageFailure
---@param counter string Какой счётчик растёт: `late` либо `failures`
---@param text string Запись в журнал
---@param fields table
function Module.lost(consumer, station, lane, err, counter, text, fields)
    local counts = consumer.topic.counts

    counts[counter] = counts[counter] + 1
    fields.err = tostring(err)
    log.warn(text, fields)

    -- Выборка начнётся с головы запаса: она и не дошла до итога.
    if lane.buffer[1] ~= nil then
        lane.position = lane.buffer[1].offset
    end

    lane.buffer = {}
    lane.resume = clock.monotonic() + Module.PAUSE

    if err.server_code ~= nil and codes.of(err.server_code).rejoin then
        station.rejoin = true
    end
end

--- Почему сообщение зарывается, не доходя до обработчика; `nil` — нет.
---@param consumer TntKafkaConsumer
---@param envelope TntKafkaMessage
---@return string|nil
local function unfit(consumer, envelope)
    local limit = consumer.policy.max_attempts
    local shape = consumer.topic.shape

    if envelope.foreign then
        return 'в теме не конверт tnt-kafka: тело не JSON'
    end

    if envelope.attempt > limit then
        return ('выдач %d при потолке %d: прежние выдачи итога не вернули'):format(
            envelope.attempt,
            limit
        )
    end

    -- Вход из очереди недоверенный: тело не по форме — зарыть сразу.
    if shape ~= nil then
        local _, errors = shape.from(envelope.body)

        if errors ~= nil then
            return 'тело не по форме: ' .. message.shaped(errors)
        end
    end

    return nil
end

--- Исполняет обработчик в области контекста отправителя и внутри крюков.
---@param consumer TntKafkaConsumer
---@param envelope TntKafkaMessage
---@return string verdict
---@return any extra
local function judged(consumer, envelope)
    -- Без опознавателя запроса — свой: запись обработчика без него
    -- в журнале не найти.
    local values = context.import(envelope.headers)

    values.request_id = values.request_id or id.ulid()

    local call = { kind = hook.HANDLE, name = consumer.topic.name, message = envelope }
    local ok, value, err = context.run(values, hook.around, call, function()
        return xpcall(consumer.handler, traced, envelope)
    end)

    if not ok then
        log.error('обработчик сообщения бросил', {
            destination = consumer.topic.name,
            id = envelope.id,
            attempt = envelope.attempt,
            err = tostring(value.err),
            traceback = value.traceback,
        })
        value = value.err
    end

    return outcome.decide(ok, value, err, envelope.attempt, consumer.policy)
end

--- Движение итога: копия в тему повтора либо мёртвых; у `ack` его нет.
---@param consumer TntKafkaConsumer
---@param record TntKafkaRecord
---@param envelope TntKafkaMessage
---@param verdict string
---@param extra any
---@return table|boolean|nil ok
---@return TntStorageFailure|nil err
local function moved(consumer, record, envelope, verdict, extra)
    local topic = consumer.topic

    if verdict == outcome.RETRY then
        local due = ('%.6f'):format(clock.realtime() + extra)

        return topic:forward(topic.retry, message.forward(record.value, envelope, { [message.DUE] = due }))
    end

    if verdict == outcome.BURY then
        return topic:forward(topic.dead, message.forward(record.value, envelope, { [message.REASON] = extra }))
    end

    return true
end

--- Пропускает сжатую пачку: прочесть её пакет не может, а стоять
--- на ней раздел не должен. Пропуск фиксируется смещением за пачкой
--- и пишется `error` с диапазоном — по нему пачку найдёт человек.
---@param consumer TntKafkaConsumer
---@param station TntKafkaStation
---@param lane TntKafkaLane
---@param record TntKafkaRecord Место пачки: `offset`, `last` и `codec`
local function skip(consumer, station, lane, record)
    local last = record.last --[[@as number]]
    local fields = {
        destination = lane.topic,
        partition = lane.partition,
        first = record.offset,
        last = last,
        codec = record.codec,
    }
    local done, err = Module.commit(consumer, station, lane, last + 1, nil)

    if not done then
        ---@cast err TntStorageFailure
        return Module.lost(
            consumer,
            station,
            lane,
            err,
            'failures',
            'пропуск сжатой пачки не зафиксирован',
            fields
        )
    end

    table.remove(lane.buffer, 1)
    consumer.topic.counts.skipped = consumer.topic.counts.skipped + 1
    log.error('пачка сжата, и прочесть её нечем: записи пропущены', fields)
end

--- Обрабатывает голову раздела.
---
--- Копия с неистёкшим сроком `x-tnt-due` не обрабатывается: раздел
--- откладывается до срока, а участник идёт к другим разделам.
---@param consumer TntKafkaConsumer
---@param station TntKafkaStation
---@param lane TntKafkaLane
function Module.process(consumer, station, lane)
    local record = lane.buffer[1] --[[@as TntKafkaRecord]]

    if record.codec ~= nil then
        return skip(consumer, station, lane, record)
    end

    local redelivered = record.offset == lane.marked.offset and lane.marked.deliveries or 0
    local envelope, own = message.delivered(consumer.topic.name, record, redelivered)
    local due = tonumber(own[message.DUE])
    local wait = due and due - clock.realtime()

    if wait ~= nil and wait > 0 then
        lane.resume = clock.monotonic() + wait

        return
    end

    local fields = { destination = consumer.topic.name, id = envelope.id, attempt = envelope.attempt }
    local marked, refused = Module.commit(consumer, station, lane, record.offset, redelivered + 1)

    if not marked then
        ---@cast refused TntStorageFailure
        return Module.lost(
            consumer,
            station,
            lane,
            refused,
            'failures',
            'выдача сообщения не отмечена',
            fields
        )
    end

    -- Отметка помнится и здесь: запись, которую выдадут снова после
    -- отказа итога, получит номер выдачи на единицу больше.
    lane.marked = { offset = record.offset, deliveries = redelivered + 1 }

    local reason = unfit(consumer, envelope)
    local verdict, extra = outcome.BURY, reason

    if reason == nil then
        verdict, extra = judged(consumer, envelope)
    end

    local done, err = moved(consumer, record, envelope, verdict, extra)

    if done then
        done, err = Module.commit(consumer, station, lane, record.offset + 1, nil)
    end

    fields.outcome = verdict

    if not done then
        ---@cast err TntStorageFailure
        return Module.lost(
            consumer,
            station,
            lane,
            err,
            'late',
            'итог обработчика не дошёл до брокера: сообщение выдадут снова',
            fields
        )
    end

    table.remove(lane.buffer, 1)

    local counts = consumer.topic.counts

    counts[verdict] = counts[verdict] + 1

    if verdict == outcome.RETRY then
        fields.delay = extra
        log.debug('сообщение выдадут снова', fields)
    elseif verdict == outcome.BURY then
        fields.err = extra
        log.error('сообщение зарыто', fields)
    end
end

return Module
