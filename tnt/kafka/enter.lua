--- Вход участника в группу: ведущий группы, вход, раздача и смещения.
---
--- Вход идёт одним сроком — срок раздачи и стука вместе: брокер держит
--- вход, пока не соберёт всех участников, до `rebalance_timeout`.
--- Ведущий группы раздаёт разделы всех подписок, для этого он спрашивает
--- описание всех тем. Раздел без зафиксированного смещения начинается
--- с самого раннего: очередь не теряет того, что отправили до объявления
--- получателя.
---
--- Отказ входа — пауза и новая попытка на следующем шаге работника;
--- отказ «ведущий сменился» забывает ведущего, а «участник неизвестен» —
--- номер участника: брокер его забыл, и войти можно только новичком.

local fiber = require('fiber')

local codes = require('tnt.kafka.codes')
local external = require('tnt.external')
local group = require('tnt.kafka.group')
local offsets = require('tnt.kafka.offsets')
local within = require('tnt.storage.within')

local log = require('tnt.log').new('tnt.kafka')

--- Отказ входа пишется с подавлением повторов: беда, которая держится,
--- иначе писала бы строку на каждой попытке.
local steps = require('tnt.log').changes('tnt.kafka')

local Module = {}

--- Сколько спать после неудачного входа, секунд.
Module.PAUSE = 0.5

--- Код брокера «такого участника нет».
local UNKNOWN_MEMBER = 25

--- Внешние средства: сон.
local source = external.install(Module, { sleep = fiber.sleep })

--- Раздел, с которого участник начинает: зафиксированное смещение либо
--- начало раздела.
---@param consumer TntKafkaConsumer
---@param station TntKafkaStation
---@param topic string
---@param partitions integer[]
---@param deadline number
---@return boolean|nil ok
---@return TntStorageFailure|nil err
local function open_lanes(consumer, station, topic, partitions, deadline)
    local cluster = consumer.topic.broker.cluster
    local coordinator = station.coordinator --[[@as TntKafkaAddress]]
    local found, err = offsets.committed(cluster, coordinator, station.seat.group, topic, partitions, deadline)

    if found == nil then
        return nil, err
    end

    local meta, missing = cluster:topic(topic, deadline)

    if meta == nil then
        return nil, missing
    end

    for _, partition in ipairs(partitions) do
        -- Без отметки нет и числа выдач: записи раздела начинаются с нуля
        -- и со смещением `NONE` не совпадут никогда.
        local committed = found[partition] or { offset = offsets.NONE }
        local position = committed.offset

        if position == offsets.NONE then
            local earliest, cut = offsets.earliest(cluster, meta, partition, deadline)

            if earliest == nil then
                return nil, cut
            end

            position = earliest
        end

        table.insert(station.lanes, {
            topic = topic,
            partition = partition,
            position = position,
            marked = { offset = committed.offset, deliveries = committed.deliveries },
            buffer = {},
        })
    end

    return true
end

--- Раздача ведущего: число разделов по темам всех подписок и круг.
---@param consumer TntKafkaConsumer
---@param members { member_id: string, metadata: string }[]
---@param deadline number
---@return table|nil assignments
---@return TntStorageFailure|nil err
local function distribute(consumer, members, deadline)
    local cluster = consumer.topic.broker.cluster
    local answer, err = cluster:metadata(nil, deadline)

    if answer == nil then
        return nil, err
    end

    local counts = {}

    for _, topic in ipairs(answer.topics) do
        counts[topic.name] = #topic.partitions
    end

    return group.assign(members, counts)
end

--- Вход в группу: ведущий, вход, раздача, смещения.
---@param consumer TntKafkaConsumer
---@param station TntKafkaStation
---@param deadline number
---@return boolean|nil ok
---@return TntStorageFailure|nil err
local function enter(consumer, station, deadline)
    local topic = consumer.topic
    local cluster = topic.broker.cluster
    local ready, err = topic:prepare(deadline)

    if not ready then
        return nil, err
    end

    if station.coordinator == nil then
        station.coordinator, err = offsets.coordinator(cluster, station.seat.group, deadline)

        if station.coordinator == nil then
            return nil, err
        end
    end

    local joined, refused = group.join(cluster, station.coordinator, station.seat, deadline)

    if joined == nil then
        return nil, refused
    end

    local assignments

    if joined.leader then
        assignments, err = distribute(consumer, joined.members, deadline)

        if assignments == nil then
            return nil, err
        end
    end

    local owned, cut = group.sync(cluster, station.coordinator, station.seat, assignments, deadline)

    if owned == nil then
        return nil, cut
    end

    station.lanes = {}

    for name, partitions in pairs(owned) do
        local opened, lost = open_lanes(consumer, station, name, partitions, deadline)

        if not opened then
            return nil, lost
        end
    end

    return true
end

--- Входит в группу; отказ — пауза и новая попытка на следующем шаге.
---
--- Отказ «выпал из поколения» с незнакомым номером участника обнуляет
--- номер: брокер забыл участника, и войти можно только новичком.
---@param consumer TntKafkaConsumer
---@param station TntKafkaStation
function Module.rejoin(consumer, station)
    local settings = consumer.topic.broker.cluster.settings
    local deadline = within.deadline(settings.rebalance_timeout + settings.session_timeout)
    local ok, err = enter(consumer, station, deadline)

    if ok then
        station.rejoin = false
        log.info('участник вошёл в группу', {
            destination = consumer.topic.name,
            group = station.seat.group,
            generation = station.seat.generation,
            partitions = #station.lanes,
        })

        return
    end

    ---@cast err TntStorageFailure
    local meaning = err.server_code and codes.of(err.server_code)

    if meaning and meaning.coordinator then
        station.coordinator = nil
    end

    if err.server_code == UNKNOWN_MEMBER then
        station.seat.member = ''
    end

    consumer.topic.counts.failures = consumer.topic.counts.failures + 1
    steps.warn(
        'вход в группу не удался',
        { destination = consumer.topic.name, group = station.seat.group, err = tostring(err) }
    )
    source().sleep(Module.PAUSE)
end

return Module
