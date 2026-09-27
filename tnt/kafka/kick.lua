--- Возврат зарытых: из темы мёртвых в тему повтора.
---
--- Тема мёртвых читается с места, которое помнит своя группа — с именем
--- самой темы мёртвых. Своя, а не группа получателя: та занята
--- работниками, и фиксацию вне её поколения брокер отвергает. Группа
--- возврата участников не держит, смещения фиксирует вне поколения.
---
--- Возврат идёт по одному: взять, записать копию в тему повтора со счётом
--- выдач с нуля и без причины, зафиксировать смещение. Обрыв посреди
--- этого оставляет сообщение в теме мёртвых — его вернут снова, копией
--- с тем же опознавателем, и получатель отсечёт повтор по `message.id`.

local api = require('tnt.kafka.api')
local codes = require('tnt.kafka.codes')
local message = require('tnt.kafka.message')
local offsets = require('tnt.kafka.offsets')
local records = require('tnt.kafka.records')

local Module = {}

--- Участник вне группы: фиксация без поколения.
local OUTSIDER = { generation = -1, member = '' }

--- Записи раздела темы мёртвых с места; пусто — дочитано.
---@param topic TntKafkaTopic
---@param meta TntKafkaTopicMeta
---@param partition integer
---@param position number
---@param deadline number
---@return TntKafkaRecord[]|nil
---@return TntStorageFailure|nil err
local function read(topic, meta, partition, position, deadline)
    local cluster = topic.broker.cluster
    local leader = cluster:leader(meta, partition)

    if leader == nil then
        cluster:forget(meta.name)

        return nil,
            codes.failure(5, ('kafka: у раздела %s/%d нет ведущего'):format(meta.name, partition))
    end

    local answer, err = cluster:exchange(leader, api.FETCH, {
        replica_id = -1,
        max_wait_ms = 0,
        min_bytes = 0,
        max_bytes = cluster.settings.max_bytes,
        isolation_level = 1,
        topics = {
            {
                topic = meta.name,
                partitions = {
                    {
                        partition = partition,
                        fetch_offset = position,
                        partition_max_bytes = cluster.settings.max_bytes,
                    },
                },
            },
        },
    }, deadline, true)

    if answer == nil then
        return nil, err
    end

    local reply = answer.responses[1].partitions[1]

    if reply.error_code ~= codes.NONE then
        return nil,
            codes.failure(
                reply.error_code,
                ('kafka: зарытые %s/%d не прочитаны'):format(meta.name, partition)
            )
    end

    local found = {}

    for _, batch in ipairs(records.visible(records.decode(reply.records), reply.aborted_transactions)) do
        for _, record in ipairs(batch.records) do
            if record.offset >= position then
                found[#found + 1] = record
            end
        end
    end

    return found
end

--- Возвращает зарытые одного раздела, пока не наберётся `left`.
---@param topic TntKafkaTopic
---@param meta TntKafkaTopicMeta
---@param partition integer
---@param left integer Сколько ещё вернуть
---@param deadline number
---@return integer moved Сколько возвращено, и при отказе тоже: смещение за ними уже легло
---@return TntStorageFailure|nil err
local function drain(topic, meta, partition, left, deadline)
    local cluster = topic.broker.cluster
    local coordinator, missing = offsets.coordinator(cluster, topic.dead, deadline)

    if coordinator == nil then
        return 0, missing
    end

    local committed, refused = offsets.committed(cluster, coordinator, topic.dead, topic.dead, { partition }, deadline)

    if committed == nil then
        return 0, refused
    end

    local position = committed[partition] and committed[partition].offset or offsets.NONE

    if position == offsets.NONE then
        local earliest, cut = offsets.earliest(cluster, meta, partition, deadline)

        if earliest == nil then
            return 0, cut
        end

        position = earliest
    end

    local member = { group = topic.dead, generation = OUTSIDER.generation, member = OUTSIDER.member }
    local moved = 0
    local found, err = read(topic, meta, partition, position, deadline)

    while found ~= nil and #found > 0 and moved < left do
        for index = 1, math.min(#found, left - moved) do
            local record = found[index] --[[@as TntKafkaRecord]]
            local envelope = message.delivered(topic.name, record)

            envelope.attempt = 0

            local sent, lost =
                topic.broker.producer:attempt(topic.retry, message.forward(record.value, envelope, {}), deadline, {})

            if sent == nil then
                return moved, lost
            end

            local done, cut =
                offsets.commit(cluster, coordinator, member, topic.dead, partition, record.offset + 1, nil, deadline)

            if not done then
                return moved, cut
            end

            moved = moved + 1
            position = record.offset + 1
        end

        found, err = read(topic, meta, partition, position, deadline)
    end

    return moved, err
end

--- Возвращает до `asked` зарытых и отдаёт число возвращённых.
---@param topic TntKafkaTopic
---@param asked integer
---@return integer|nil moved
---@return TntStorageFailure|nil err
function Module.revive(topic, asked)
    local cluster = topic.broker.cluster
    local moved = 0

    local done, err = cluster:call(function(deadline)
        local ready, refused = topic:prepare(deadline)

        if not ready then
            return nil, refused
        end

        local meta, missing = cluster:topic(topic.dead, deadline)

        if meta == nil then
            return nil, missing
        end

        for partition = 0, meta.count - 1 do
            if moved >= asked then
                break
            end

            local got, cut = drain(topic, meta, partition, asked - moved, deadline)

            moved = moved + got

            if cut ~= nil then
                return nil, cut
            end
        end

        return true
    end, topic.broker.limits.timeout)

    if not done then
        return nil, err
    end

    return moved
end

return Module
