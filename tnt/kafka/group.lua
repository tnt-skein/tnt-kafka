--- Группа получателей: вход, раздача разделов, стук и уход.
---
--- Раздел темы в группе ведёт один участник, и подтверждение — смещение
--- раздела — идёт строго по порядку. Кто что ведёт, решает раздача:
--- участники входят (`JoinGroup`), брокер выбирает ведущего группы, тот
--- раздаёт разделы и присылает раздачу (`SyncGroup`), остальные её
--- получают. Участник жив, пока стучит (`Heartbeat`); стук, услышавший
--- «идёт раздача», велит войти заново.
---
--- **Раздача по кругу** (`roundrobin`): участники по порядку номеров,
--- разделы по порядку тем и номеров, каждый следующий раздел — следующему
--- участнику, подписанному на его тему. Имя и запись раздачи — те же,
--- что у прочих клиентов Kafka, и группа, где рядом стоят чужие
--- получатели с той же раздачей, делится между всеми.
---
--- **Вход с номером.** Брокер отвечает новичку без номера
--- `MEMBER_ID_REQUIRED` и номером, с которым войти, — вход повторяется
--- с ним сразу, одним делом.

local codes = require('tnt.kafka.codes')
local coordination = require('tnt.kafka.coordination')
local schema = require('tnt.kafka.schema')
local wire = require('tnt.kafka.wire')

local Module = {}

--- Род группы: получатели.
Module.PROTOCOL_TYPE = 'consumer'

--- Имя раздачи.
Module.ASSIGNOR = 'roundrobin'

--- Подписка участника: версия, темы, свои данные.
local SUBSCRIPTION = { { 'version', 'int16' }, { 'topics', '[string]' }, { 'user_data', 'bytes' } }

--- Раздача участнику: версия, разделы по темам, свои данные.
local ASSIGNMENT = {
    { 'version', 'int16' },
    { 'partitions', { { 'topic', 'string' }, { 'partitions', '[int32]' } } },
    { 'user_data', 'bytes' },
}

---@class TntKafkaSeat Место участника в группе
---@field group string
---@field member string Номер участника; пусто до первого входа
---@field generation integer Поколение группы; `-1` вне группы
---@field topics string[] Темы, на которые подписан участник
---@field session_ms integer
---@field rebalance_ms integer

---@class TntKafkaJoined Итог входа
---@field leader boolean Ведёт ли участник раздачу
---@field members { member_id: string, metadata: string }[] Участники; только у ведущего

--- Подписка байтами.
---@param topics string[]
---@return string
function Module.subscription(topics)
    return schema.encode(SUBSCRIPTION, { version = 0, topics = topics })
end

--- Раздача байтами: темы по порядку имён.
---@param owned table<string, integer[]> Разделы по темам
---@return string
function Module.assignment(owned)
    local names = {}

    for topic in pairs(owned) do
        names[#names + 1] = topic
    end

    table.sort(names)

    local partitions = {}

    for index, topic in ipairs(names) do
        partitions[index] = { topic = topic, partitions = owned[topic] }
    end

    return schema.encode(ASSIGNMENT, { version = 0, partitions = partitions })
end

--- Разбирает раздачу: разделы по темам. Пустая раздача — ничего.
---@param data string|nil
---@return table<string, integer[]>
function Module.owned(data)
    local owned = {}

    if data == nil or data == '' then
        return owned
    end

    for _, entry in ipairs(schema.decode(ASSIGNMENT, wire.reader(data)).partitions or {}) do
        owned[entry.topic] = entry.partitions
    end

    return owned
end

--- Раздаёт разделы по кругу.
---@param members { member_id: string, metadata: string }[] Участники с подписками
---@param counts table<string, integer> Число разделов по темам
---@return table<string, table<string, integer[]>> Разделы по участникам и темам
function Module.assign(members, counts)
    local order, wants, result = {}, {}, {}

    for _, member in ipairs(members) do
        local topics = schema.decode(SUBSCRIPTION, wire.reader(member.metadata)).topics or {}

        order[#order + 1] = member.member_id
        wants[member.member_id] = {}
        result[member.member_id] = {}

        for _, topic in ipairs(topics) do
            wants[member.member_id][topic] = true
        end
    end

    table.sort(order)

    local names = {}

    for topic in pairs(counts) do
        names[#names + 1] = topic
    end

    table.sort(names)

    local turn = 0

    for _, topic in ipairs(names) do
        for partition = 0, counts[topic] - 1 do
            for step = 1, #order do
                local member = order[(turn + step - 1) % #order + 1]

                if wants[member][topic] then
                    local owned = result[member]

                    owned[topic] = owned[topic] or {}
                    table.insert(owned[topic], partition)
                    turn = turn + step

                    break
                end
            end
        end
    end

    return result
end

--- Входит в группу; новичку без номера — сразу второй раз, с выданным.
---@param cluster TntKafkaCluster
---@param coordinator TntKafkaAddress
---@param seat TntKafkaSeat
---@param deadline number
---@return TntKafkaJoined|nil
---@return TntStorageFailure|nil err
function Module.join(cluster, coordinator, seat, deadline)
    for _ = 1, 2 do
        local values = {
            group_id = seat.group,
            session_timeout_ms = seat.session_ms,
            rebalance_timeout_ms = seat.rebalance_ms,
            member_id = seat.member,
            protocol_type = Module.PROTOCOL_TYPE,
            protocols = { { name = Module.ASSIGNOR, metadata = Module.subscription(seat.topics) } },
        }
        local answer, err = cluster:exchange(coordinator, coordination.JOIN_GROUP, values, deadline, true)

        if answer == nil then
            return nil, err
        end

        seat.member = answer.member_id

        if answer.error_code == codes.NONE then
            seat.generation = answer.generation_id

            return { leader = answer.leader == answer.member_id, members = answer.members }
        end

        if answer.error_code ~= codes.MEMBER_ID_REQUIRED then
            return nil,
                codes.failure(
                    answer.error_code,
                    ('kafka: вход в группу %s не удался'):format(seat.group)
                )
        end
    end

    return nil,
        codes.failure(
            codes.MEMBER_ID_REQUIRED,
            ('kafka: брокер дважды не принял номер в группе %s'):format(seat.group)
        )
end

--- Раздача: ведущий присылает её всем, прочие получают свою.
---@param cluster TntKafkaCluster
---@param coordinator TntKafkaAddress
---@param seat TntKafkaSeat
---@param assignments table<string, table<string, integer[]>>|nil Раздача ведущего
---@param deadline number
---@return table<string, integer[]>|nil Разделы участника по темам
---@return TntStorageFailure|nil err
function Module.sync(cluster, coordinator, seat, assignments, deadline)
    local list = {}

    for member, owned in pairs(assignments or {}) do
        list[#list + 1] = { member_id = member, assignment = Module.assignment(owned) }
    end

    local values = {
        group_id = seat.group,
        generation_id = seat.generation,
        member_id = seat.member,
        assignments = list,
    }
    local answer, err = cluster:exchange(coordinator, coordination.SYNC_GROUP, values, deadline, true)

    if answer == nil then
        return nil, err
    end

    if answer.error_code ~= codes.NONE then
        return nil,
            codes.failure(
                answer.error_code,
                ('kafka: раздача группы %s не получена'):format(seat.group)
            )
    end

    return Module.owned(answer.assignment)
end

--- Стук: жив и держит свои разделы.
---@param cluster TntKafkaCluster
---@param coordinator TntKafkaAddress
---@param seat TntKafkaSeat
---@param deadline number
---@return boolean|nil ok
---@return TntStorageFailure|nil err
function Module.heartbeat(cluster, coordinator, seat, deadline)
    local values = { group_id = seat.group, generation_id = seat.generation, member_id = seat.member }
    local answer, err = cluster:exchange(coordinator, coordination.HEARTBEAT, values, deadline, true)

    if answer == nil then
        return nil, err
    end

    if answer.error_code ~= codes.NONE then
        return nil,
            codes.failure(
                answer.error_code,
                ('kafka: стук в группу %s не принят'):format(seat.group)
            )
    end

    return true
end

--- Уходит из группы: разделы участника раздаются сразу, а не по сроку
--- его стука. Отказ ухода не важен — срок стука выйдет и так.
---@param cluster TntKafkaCluster
---@param coordinator TntKafkaAddress
---@param seat TntKafkaSeat
---@param deadline number
function Module.leave(cluster, coordinator, seat, deadline)
    cluster:exchange(
        coordinator,
        coordination.LEAVE_GROUP,
        { group_id = seat.group, member_id = seat.member },
        deadline,
        true
    )
    seat.generation = -1
end

return Module
