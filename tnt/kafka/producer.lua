--- Производитель: раздел по ключу, запись пачкой и подтверждение всех копий.
---
--- **Запись подтверждают все копии** (`acks = -1`): ответ приходит, когда
--- запись легла на все синхронные копии раздела, и опознаватель
--- отдаётся после этого.
---
--- **Производитель идемпотентный.** Брокер выдаёт номер производителя
--- (`InitProducerId`), и каждая запись в раздел несёт свой номер в его
--- счёте. Повтор после обрыва приходит с тем же номером, и брокер
--- узнаёт его и не пишет второй раз (`DUPLICATE_SEQUENCE_NUMBER`
--- или ответ первой записи). Поэтому отправка повторяется и после
--- `timeout` и `broken` — то, что у прочих драйверов повторять нельзя.
---
--- Цена — порядок номеров. Брокер ждёт номера раздела строго по одному,
--- и две записи в раздел не уходят разом: у раздела свой замок, и
--- следующая отправка в тот же раздел ждёт ответа на предыдущую.
--- Отправка, бросившая раздел с неизвестным итогом (срок вышел после
--- записи), оставляет счёт в сомнении, и следующая отправка в раздел
--- берёт новый номер производителя: иначе новая запись с тем же номером
--- в счёте была бы принята брокером за повтор и молча пропала.
---
--- **Раздел ключа — как у всех клиентов Kafka**: murmur2 от байтов ключа
--- без знака по модулю числа разделов. Тогда записи одного ключа от
--- этого пакета и от чужого производителя ложатся в один раздел, и
--- порядок по ключу держится между ними. Без ключа разделы идут по кругу.

local bit = require('bit')
local fiber = require('fiber')

local api = require('tnt.kafka.api')
local cluster_of = require('tnt.kafka.cluster')
local codes = require('tnt.kafka.codes')
local fail = require('tnt.must.fail')
local failure = require('tnt.storage.failure')
local records = require('tnt.kafka.records')
local within = require('tnt.storage.within')

local Module = {}

--- Затравка и множитель murmur2 у клиентов Kafka.
local SEED, MIX = 0x9747b28c, 0x5bd1e995

--- Счёт записей производителя — целое со знаком в четыре байта: после
--- наибольшего идёт ноль.
local SEQUENCE_LIMIT = 0x80000000

--- Все синхронные копии.
local ALL = -1

--- Произведение по модулю 2^32 без потери точности: множитель делится
--- на половины, и ни одно промежуточное число не выходит за 2^53.
---@param left integer
---@param right integer
---@return integer
local function multiply(left, right)
    local low, high = bit.band(right, 0xffff), bit.rshift(right, 16)

    return bit.tobit(left * low + (left * high % 0x10000) * 0x10000)
end

--- murmur2 от строки, как у клиентов Kafka: целое со знаком.
---@param data string
---@return integer
function Module.murmur2(data)
    local length = #data
    local hash = bit.bxor(SEED, length)
    local whole = length - length % 4

    for at = 1, whole, 4 do
        local word = bit.bor(
            data:byte(at),
            bit.lshift(data:byte(at + 1), 8),
            bit.lshift(data:byte(at + 2), 16),
            bit.lshift(data:byte(at + 3), 24)
        )

        word = multiply(word, MIX)
        word = bit.bxor(word, bit.rshift(word, 24))
        word = multiply(word, MIX)
        hash = bit.bxor(multiply(hash, MIX), word)
    end

    local tail = { data:byte(whole + 1, length) }

    if #tail > 0 then
        for index = #tail, 1, -1 do
            hash = bit.bxor(hash, bit.lshift(tail[index], 8 * (index - 1)))
        end

        hash = multiply(hash, MIX)
    end

    hash = bit.bxor(hash, bit.rshift(hash, 13))
    hash = multiply(hash, MIX)

    return bit.bxor(hash, bit.rshift(hash, 15))
end

--- Раздел для ключа.
---@param key string
---@param count integer Число разделов
---@return integer
function Module.partition_of(key, count)
    return bit.band(Module.murmur2(key), 0x7fffffff) % count
end

---@class TntKafkaProducerLane Раздел глазами производителя
---@field lock any Замок: канал на одно место
---@field owner number|nil Номер производителя, чьим счётом идут записи
---@field sequence integer Номер следующей записи
---@field doubt table|nil Отправка, бросившая раздел с неизвестным итогом

---@class TntKafkaProducer
---@field cluster TntKafkaCluster
---@field identity { id: integer, epoch: integer }|nil Номер производителя
---@field lanes table<string, TntKafkaProducerLane> Разделы по «тема/номер»
---@field turns table<string, integer> Круг разделов у записей без ключа
local Producer = {}
Producer.__index = Producer

--- Заводит производителя; номер он спросит при первой записи.
---@param cluster TntKafkaCluster
---@return TntKafkaProducer
function Module.new(cluster)
    return setmetatable({ cluster = cluster, lanes = {}, turns = {} }, Producer)
end

--- Раздел, в который уйдёт запись: по ключу либо следующий по кругу.
---@param meta TntKafkaTopicMeta
---@param key string|nil
---@return integer
function Producer:choose(meta, key)
    if key ~= nil then
        return Module.partition_of(key, meta.count)
    end

    local turn = (self.turns[meta.name] or -1) + 1

    self.turns[meta.name] = turn

    return turn % meta.count
end

--- Номер производителя: спрошенный раньше либо новый.
---@param address TntKafkaAddress Узел, у которого спросить
---@param deadline number
---@return { id: integer, epoch: integer }|nil
---@return TntStorageFailure|nil err
function Producer:identify(address, deadline)
    if self.identity ~= nil then
        return self.identity
    end

    local values = { transactional_id = nil, transaction_timeout_ms = 0 }
    local answer, err = self.cluster:exchange(address, api.INIT_PRODUCER_ID, values, deadline, true)

    if answer == nil then
        return nil, err
    end

    if answer.error_code ~= codes.NONE then
        return nil,
            codes.failure(
                answer.error_code,
                'kafka: брокер не выдал номер производителя'
            )
    end

    self.identity = { id = answer.producer_id, epoch = answer.producer_epoch }

    return self.identity
end

--- Раздел глазами производителя; заводится при первой записи.
---@param topic string
---@param partition integer
---@return TntKafkaProducerLane
function Producer:lane(topic, partition)
    local name = ('%s/%d'):format(topic, partition)
    local found = self.lanes[name]

    if found == nil then
        -- Счёт записей ставит первая запись: номера производителя ещё нет.
        ---@diagnostic disable-next-line: missing-fields
        found = { lock = fiber.channel(1) }
        self.lanes[name] = found
    end

    return found
end

--- Разбирает ответ на запись в раздел.
---@param producer TntKafkaProducer
---@param meta TntKafkaTopicMeta
---@param lane TntKafkaProducerLane
---@param answer table
---@return { partition: integer, offset: number }|nil
---@return TntStorageFailure|nil err
local function settled(producer, meta, lane, answer)
    local reply = answer.responses[1].partition_responses[1]
    local code = reply.error_code

    if code == codes.NONE or code == codes.DUPLICATE_SEQUENCE_NUMBER then
        lane.sequence = (lane.sequence + 1) % SEQUENCE_LIMIT
        lane.doubt = nil

        return { partition = reply.index, offset = reply.base_offset }
    end

    local meaning = codes.of(code)

    if codes.PRODUCER_RESET[code] then
        producer.identity = nil
    elseif meaning.stale then
        producer.cluster:forget(meta.name)
    end

    local told = reply.error_message and (' (%s)'):format(reply.error_message) or ''

    return nil,
        codes.failure(
            code,
            ('kafka: запись в %s/%d не принята%s'):format(meta.name, reply.index, told),
            {
                idempotent = true,
            }
        )
end

--- Пишет запись в раздел, держа его замок.
---@param producer TntKafkaProducer
---@param meta TntKafkaTopicMeta
---@param partition integer
---@param record TntKafkaOutgoing
---@param deadline number
---@param token table Отправка, которая пишет
---@return { partition: integer, offset: number }|nil
---@return TntStorageFailure|nil err
local function write(producer, meta, partition, record, deadline, token)
    local cluster = producer.cluster
    local leader = cluster:leader(meta, partition)

    if leader == nil then
        cluster:forget(meta.name)

        return nil,
            failure.new(
                failure.UNREACHABLE,
                ('kafka: у раздела %s/%d нет ведущего'):format(meta.name, partition)
            )
    end

    local lane = producer:lane(meta.name, partition)

    -- Сомнение чужой отправки: её запись могла лечь с тем номером, который
    -- взяла бы эта, — новый номер производителя начинает счёт заново.
    if lane.doubt ~= nil and lane.doubt ~= token then
        producer.identity, lane.doubt = nil, nil
    end

    local identity, refused = producer:identify(leader, deadline)

    if identity == nil then
        return nil, refused
    end

    if lane.owner ~= identity.id then
        lane.owner, lane.sequence = identity.id, 0
    end

    local batch = records.encode({
        producer_id = identity.id,
        producer_epoch = identity.epoch,
        base_sequence = lane.sequence,
        timestamp = record.timestamp,
        records = { record },
    })
    local values = {
        acks = ALL,
        timeout_ms = cluster_of.milliseconds(deadline),
        topic_data = { { name = meta.name, partition_data = { { index = partition, records = batch } } } },
    }
    local answer, err = cluster:exchange(leader, api.PRODUCE, values, deadline, true)

    if answer == nil then
        ---@cast err TntStorageFailure
        if err.sent then
            lane.doubt = token
        end

        return nil, err
    end

    return settled(producer, meta, lane, answer)
end

--- Одна попытка записи: раздел, замок, запись.
---
--- `state` живёт через все попытки одной отправки: в нём раздел, выбранный
--- первой попыткой (запись без ключа не должна скакать по кругу на каждом
--- повторе), и знак самой отправки для сомнения в счёте.
---@param topic string
---@param record TntKafkaOutgoing
---@param deadline number
---@param state table
---@return { partition: integer, offset: number }|nil
---@return TntStorageFailure|nil err
function Producer:attempt(topic, record, deadline, state)
    local meta, err = self.cluster:topic(topic, deadline)

    if meta == nil then
        return nil, err
    end

    state.partition = state.partition or self:choose(meta, record.key)

    local lane = self:lane(topic, state.partition)

    -- Отрицательного срока канал не принимает: вышедший срок — не ждать.
    if not lane.lock:put(state, math.max(0, within.left(deadline))) then
        return nil,
            failure.new(
                failure.BUSY,
                ('kafka: раздел %s/%d занят другой записью дольше срока'):format(
                    topic,
                    state.partition
                )
            )
    end

    local done, result, refusal = pcall(write, self, meta, state.partition, record, deadline, state)

    lane.lock:get()

    if not done then
        fail.raise(result)
    end

    return result, refusal
end

return Module
