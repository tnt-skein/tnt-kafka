--- Смещения группы: у кого они, какие зафиксированы, откуда начать.
---
--- **Подтверждение у Kafka — фиксация смещения**, и она накопительная:
--- смещение раздела говорит «всё до него обработано». Поэтому раздел
--- ведёт ровно один участник группы и подтверждает сообщения строго
--- по порядку.
---
--- **Выдача отмечается до обработчика.** Сообщение, которое роняет
--- процесс, иначе выдавалось бы после перезапуска с тем же номером
--- без конца: Kafka выдач не считает, и ничего, кроме зафиксированного
--- смещения, от упавшего участника не остаётся. Поэтому перед обработчиком
--- фиксируется смещение самого сообщения с числом его выдач в поле
--- `metadata`, а после итога — смещение следующего. Новый владелец
--- раздела читает число выдач вместе со смещением и прибавляет его
--- к номеру выдачи. Цена — вторая фиксация на каждое сообщение.
---
--- **Смещения нет — начало раздела.** Группа, впервые пришедшая к теме,
--- начинает с самого раннего смещения: очередь не теряет того, что
--- отправили до объявления получателя. Смещение, которое хранение темы
--- уже стёрло, — тоже начало, с записью в журнал: стёртого не вернуть.

local api = require('tnt.kafka.api')
local codes = require('tnt.kafka.codes')
local coordination = require('tnt.kafka.coordination')

local Module = {}

--- Смещения нет.
Module.NONE = -1

--- Время для самого раннего смещения раздела.
local EARLIEST = -2

--- Род ключа: группа.
local GROUP = 0

---@class TntKafkaMember Участник группы в миг фиксации
---@field group string
---@field generation integer Поколение; `-1` — вне группы
---@field member string Номер участника; пусто — вне группы

---@class TntKafkaCommitted Зафиксированное смещение раздела
---@field offset number Смещение; `NONE` — не фиксировалось
---@field deliveries integer Сколько раз сообщение со смещением уже выдано

--- Узел, который ведёт группу.
---@param cluster TntKafkaCluster
---@param group string
---@param deadline number
---@return TntKafkaAddress|nil
---@return TntStorageFailure|nil err
function Module.coordinator(cluster, group, deadline)
    local last

    for _, address in ipairs(cluster.settings.brokers) do
        local answer, err =
            cluster:exchange(address, coordination.FIND_COORDINATOR, { key = group, key_type = GROUP }, deadline, true)

        if answer ~= nil then
            if answer.error_code ~= codes.NONE then
                return nil,
                    codes.failure(
                        answer.error_code,
                        ('kafka: у группы %s нет ведущего'):format(group)
                    )
            end

            return { host = answer.host, port = answer.port }
        end

        last = err
    end

    return nil, last
end

--- Дело с ведущим группы: найти его, если он не известен, и забыть,
--- если группу теперь ведёт другой узел. Повторы — по сроку `cluster:call`.
---@param cluster TntKafkaCluster
---@param holder { coordinator: TntKafkaAddress|nil } Кто помнит ведущего
---@param group string
---@param timeout number
---@param action fun(coordinator: TntKafkaAddress, deadline: number): any, TntStorageFailure|nil
---@return any value
---@return TntStorageFailure|nil err
function Module.call(cluster, holder, group, timeout, action)
    return cluster:call(function(deadline)
        if holder.coordinator == nil then
            local found, err = Module.coordinator(cluster, group, deadline)

            if found == nil then
                return nil, err
            end

            holder.coordinator = found
        end

        local value, err = action(holder.coordinator, deadline)

        -- Узел не отвечает либо сказал, что группу ведёт не он: следующая
        -- попытка спросит ведущего заново.
        if err ~= nil and (err.server_code == nil or codes.of(err.server_code).coordinator) then
            holder.coordinator = nil
        end

        return value, err
    end, timeout)
end

--- Зафиксированные смещения разделов темы.
---@param cluster TntKafkaCluster
---@param coordinator TntKafkaAddress
---@param group string
---@param topic string
---@param partitions integer[]
---@param deadline number
---@return table<integer, TntKafkaCommitted>|nil
---@return TntStorageFailure|nil err
function Module.committed(cluster, coordinator, group, topic, partitions, deadline)
    local values = { group_id = group, topics = { { name = topic, partition_indexes = partitions } } }
    local answer, err = cluster:exchange(coordinator, coordination.OFFSET_FETCH, values, deadline, true)

    if answer == nil then
        return nil, err
    end

    if answer.error_code ~= codes.NONE then
        return nil,
            codes.failure(
                answer.error_code,
                ('kafka: смещения группы %s не прочитаны'):format(group)
            )
    end

    local found = {}

    for _, entry in ipairs(answer.topics[1] and answer.topics[1].partitions or {}) do
        if entry.error_code ~= codes.NONE then
            return nil,
                codes.failure(
                    entry.error_code,
                    ('kafka: смещение %s/%d не прочитано'):format(topic, entry.partition_index)
                )
        end

        found[entry.partition_index] = {
            offset = entry.committed_offset,
            deliveries = math.floor(tonumber(entry.metadata) or 0),
        }
    end

    return found
end

--- Фиксирует смещение раздела.
---@param cluster TntKafkaCluster
---@param coordinator TntKafkaAddress
---@param member TntKafkaMember
---@param topic string
---@param partition integer
---@param offset number
---@param deliveries integer|nil Сколько раз сообщение со смещением выдано
---@param deadline number
---@return boolean|nil ok
---@return TntStorageFailure|nil err
function Module.commit(cluster, coordinator, member, topic, partition, offset, deliveries, deadline)
    local values = {
        group_id = member.group,
        generation_id = member.generation,
        member_id = member.member,
        topics = {
            {
                name = topic,
                partitions = {
                    {
                        partition_index = partition,
                        committed_offset = offset,
                        committed_metadata = deliveries and tostring(deliveries) or '',
                    },
                },
            },
        },
    }
    local answer, err = cluster:exchange(coordinator, coordination.OFFSET_COMMIT, values, deadline, true)

    if answer == nil then
        return nil, err
    end

    local code = answer.topics[1].partitions[1].error_code

    if code ~= codes.NONE then
        return nil,
            codes.failure(
                code,
                ('kafka: смещение %s/%d не зафиксировано'):format(topic, partition)
            )
    end

    return true
end

--- Самое раннее смещение раздела, которое ещё хранится.
---@param cluster TntKafkaCluster
---@param meta TntKafkaTopicMeta
---@param partition integer
---@param deadline number
---@return number|nil offset
---@return TntStorageFailure|nil err
function Module.earliest(cluster, meta, partition, deadline)
    local leader = cluster:leader(meta, partition)

    if leader == nil then
        cluster:forget(meta.name)

        return nil,
            codes.failure(5, ('kafka: у раздела %s/%d нет ведущего'):format(meta.name, partition))
    end

    local values = {
        replica_id = -1,
        topics = { { name = meta.name, partitions = { { partition_index = partition, timestamp = EARLIEST } } } },
    }
    local answer, err = cluster:exchange(leader, api.LIST_OFFSETS, values, deadline, true)

    if answer == nil then
        return nil, err
    end

    local reply = answer.topics[1].partitions[1]

    if reply.error_code ~= codes.NONE then
        if codes.of(reply.error_code).stale then
            cluster:forget(meta.name)
        end

        return nil,
            codes.failure(
                reply.error_code,
                ('kafka: начало раздела %s/%d не прочитано'):format(meta.name, partition)
            )
    end

    return reply.offset
end

return Module
