--- Проверки кодов ошибок брокера: имя, род отказа и пометки.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local codes = helper.codes
local failure = helper.failure

local g = t.group('tnt.kafka.codes')

g.test_known_codes_have_kind_and_marks = function()
    t.assert_equals(codes.of(6), {
        code = 6,
        name = 'NOT_LEADER_OR_FOLLOWER',
        kind = failure.UNREACHABLE,
        stale = true,
        coordinator = false,
        rejoin = false,
    })
    t.assert_equals(codes.of(16).coordinator, true)
    t.assert_equals(codes.of(16).stale, false)
    t.assert_equals(codes.of(27).rejoin, true)
    t.assert_equals(codes.of(27).kind, failure.CONFLICT)
    t.assert_equals(codes.of(58).kind, failure.DENIED)
    t.assert_equals(codes.of(10).kind, failure.REJECTED)
    t.assert_equals(codes.of(7).kind, failure.TIMEOUT)
    t.assert_equals(codes.of(-1).kind, failure.BROKEN)
end

g.test_unknown_code_is_broken = function()
    t.assert_equals(codes.of(9999), {
        code = 9999,
        name = 'код 9999',
        kind = failure.BROKEN,
        stale = false,
        coordinator = false,
        rejoin = false,
    })
    t.assert_equals(codes.of('42P01').kind, failure.BROKEN)
end

g.test_name_shows_code = function()
    t.assert_equals(codes.name(3), 'UNKNOWN_TOPIC_OR_PARTITION (3)')
    t.assert_equals(codes.name(9999), 'код 9999 (9999)')
end

g.test_failure_carries_kind_code_and_verdict = function()
    local err = codes.failure(6, 'kafka: запись не принята', { idempotent = true })

    t.assert_equals(err.kind, failure.UNREACHABLE)
    t.assert_equals(err.message, 'kafka: запись не принята: NOT_LEADER_OR_FOLLOWER (6)')
    t.assert_equals(err.server_code, 6)
    t.assert_equals(err.retriable, true)

    local late = codes.failure(7, 'срок', { idempotent = true, sent = true })

    t.assert_equals(late.kind, failure.TIMEOUT)
    t.assert_equals(late.sent, true)
    t.assert_equals(late.retriable, true)
    t.assert_equals(codes.failure(7, 'срок').retriable, false)
    t.assert_equals(codes.failure(6, 'нет', { retriable = false }).retriable, false)
    t.assert_equals(codes.failure(6, 'нет', { sent = true }).sent, true)
end

g.test_producer_reset_codes = function()
    t.assert_equals(codes.PRODUCER_RESET, { [45] = true, [47] = true, [59] = true })
    t.assert_equals({
        codes.NONE,
        codes.OFFSET_OUT_OF_RANGE,
        codes.TOPIC_ALREADY_EXISTS,
        codes.DUPLICATE_SEQUENCE_NUMBER,
        codes.MEMBER_ID_REQUIRED,
    }, { 0, 1, 36, 46, 79 })
end

g.test_every_known_code_is_named_with_its_kind_and_marks = function()
    local expected = {
        [-1] = 'UNKNOWN_SERVER_ERROR broken',
        [1] = 'OFFSET_OUT_OF_RANGE rejected',
        [2] = 'CORRUPT_MESSAGE broken',
        [3] = 'UNKNOWN_TOPIC_OR_PARTITION unreachable stale',
        [5] = 'LEADER_NOT_AVAILABLE unreachable stale',
        [6] = 'NOT_LEADER_OR_FOLLOWER unreachable stale',
        [7] = 'REQUEST_TIMED_OUT timeout',
        [10] = 'MESSAGE_TOO_LARGE rejected',
        [13] = 'NETWORK_EXCEPTION broken',
        [14] = 'COORDINATOR_LOAD_IN_PROGRESS unreachable',
        [15] = 'COORDINATOR_NOT_AVAILABLE unreachable coordinator',
        [16] = 'NOT_COORDINATOR unreachable coordinator',
        [17] = 'INVALID_TOPIC_EXCEPTION rejected',
        [18] = 'RECORD_LIST_TOO_LARGE rejected',
        [19] = 'NOT_ENOUGH_REPLICAS unreachable',
        [20] = 'NOT_ENOUGH_REPLICAS_AFTER_APPEND broken',
        [22] = 'ILLEGAL_GENERATION conflict rejoin',
        [25] = 'UNKNOWN_MEMBER_ID conflict rejoin',
        [26] = 'INVALID_SESSION_TIMEOUT rejected',
        [27] = 'REBALANCE_IN_PROGRESS conflict rejoin',
        [29] = 'TOPIC_AUTHORIZATION_FAILED denied',
        [30] = 'GROUP_AUTHORIZATION_FAILED denied',
        [31] = 'CLUSTER_AUTHORIZATION_FAILED denied',
        [32] = 'INVALID_TIMESTAMP rejected',
        [33] = 'UNSUPPORTED_SASL_MECHANISM denied',
        [34] = 'ILLEGAL_SASL_STATE denied',
        [35] = 'UNSUPPORTED_VERSION rejected',
        [37] = 'INVALID_PARTITIONS rejected',
        [38] = 'INVALID_REPLICATION_FACTOR rejected',
        [40] = 'INVALID_CONFIG rejected',
        [42] = 'INVALID_REQUEST rejected',
        [44] = 'POLICY_VIOLATION rejected',
        [45] = 'OUT_OF_ORDER_SEQUENCE_NUMBER unreachable',
        [47] = 'INVALID_PRODUCER_EPOCH unreachable',
        [56] = 'KAFKA_STORAGE_ERROR unreachable stale',
        [58] = 'SASL_AUTHENTICATION_FAILED denied',
        [59] = 'UNKNOWN_PRODUCER_ID unreachable',
        [74] = 'FENCED_LEADER_EPOCH unreachable stale',
        [79] = 'MEMBER_ID_REQUIRED conflict rejoin',
        [87] = 'INVALID_RECORD rejected',
        [89] = 'THROTTLING_QUOTA_EXCEEDED unreachable',
    }
    local seen = {}

    -- Каждый код от -1 до 100: известные — как в таблице, прочие — обрыв.
    for code = -1, 100 do
        local meaning = codes.of(code)
        local marks = {}

        for _, mark in ipairs({ 'stale', 'coordinator', 'rejoin' }) do
            if meaning[mark] then
                marks[#marks + 1] = mark
            end
        end

        local line = table.concat({ meaning.name, meaning.kind, unpack(marks) }, ' ')

        if expected[code] ~= nil then
            seen[code] = line
        else
            t.assert_equals(line, ('код %d broken'):format(code))
        end
    end

    t.assert_equals(seen, expected)
end
