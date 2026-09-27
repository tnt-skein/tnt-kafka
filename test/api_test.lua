--- Проверки каталога запросов: номер и версия каждого запроса.
---
--- Двойник брокера читает запросы тем же каталогом, что и клиент, и
--- чужой номер либо версия прошли бы через него незамеченными: сами
--- с собой мы договоримся о чём угодно. Поэтому номер и версия каждого
--- запроса сверяются здесь с протоколом Kafka, а то, что брокер эти
--- версии принимает, показывает живая проверка.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local api = helper.api

local g = t.group('tnt.kafka.api')

g.test_keys_and_versions_follow_the_kafka_protocol = function()
    local seen = {}

    for _, request in ipairs({ api.API_VERSIONS, api.SASL_HANDSHAKE, api.SASL_AUTHENTICATE, unpack(api.USED) }) do
        seen[request.name] = { request.key, request.version }
    end

    t.assert_equals(seen, {
        ApiVersions = { 18, 0 },
        SaslHandshake = { 17, 1 },
        SaslAuthenticate = { 36, 1 },
        Metadata = { 3, 5 },
        CreateTopics = { 19, 4 },
        InitProducerId = { 22, 1 },
        Produce = { 0, 8 },
        Fetch = { 1, 4 },
        ListOffsets = { 2, 1 },
        FindCoordinator = { 10, 2 },
        JoinGroup = { 11, 4 },
        SyncGroup = { 14, 2 },
        Heartbeat = { 12, 2 },
        LeaveGroup = { 13, 2 },
        OffsetCommit = { 8, 5 },
        OffsetFetch = { 9, 3 },
    })
end

g.test_request_frame_carries_key_version_correlation_and_client = function()
    local frame = api.request(helper.coordination.HEARTBEAT, 7, 'app', {
        group_id = 'g',
        generation_id = 3,
        member_id = 'm',
    })
    local reader = helper.wire.reader(frame)

    t.assert_equals(reader:int32(), #frame - api.PREFIX)
    t.assert_equals({ reader:int16(), reader:int16(), reader:int32(), reader:string() }, { 12, 2, 7, 'app' })
end
