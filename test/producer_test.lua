--- Проверки производителя: раздел ключа, номер производителя, счёт
--- записей, замок раздела и сомнение после неизвестного итога.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local cluster_of = helper.cluster
local failure = helper.failure
local producer_of = helper.producer
local within = helper.within

local g = t.group('tnt.kafka.producer')

g.after_each(function()
    if g.cluster ~= nil then
        g.cluster:close()
    end

    g.cluster = nil
    helper.release(g)
end)

--- Производитель к двойнику.
---@param opts table|nil Настройки двойника
---@return table producer
---@return table fake
local function started(opts)
    local given = opts or {}

    given.topics = given.topics or { orders = 3 }
    g.fake = helper.serve(given)
    g.cluster = cluster_of.new(helper.checked(helper.settings_of(g.fake)))

    return producer_of.new(g.cluster), g.fake
end

--- Двойник, отвечающий на запись в раздел 0 названным кодом.
---@param code integer
---@param text string|nil Текст отказа брокера
---@return function
local function refusing(code, text)
    return function(name, values)
        if name ~= 'Produce' then
            return nil
        end

        local reply = { index = 0, error_code = code, base_offset = -1, error_message = text }

        return { responses = { { name = values.topic_data[1].name, partition_responses = { reply } } } }
    end
end

--- Запись для отправки.
---@param key string|nil
---@return table
local function record(key)
    return { key = key, value = '{}', headers = { { 'x-a', '1' } }, timestamp = 1790000000000 }
end

g.test_murmur2_matches_kafka_clients = function()
    t.assert_equals(producer_of.murmur2('21'), -973932308)
    t.assert_equals(producer_of.murmur2('foobar'), -790332482)
    t.assert_equals(producer_of.murmur2('a-little-bit-long-string'), -985981536)
    t.assert_equals(producer_of.murmur2('a-little-bit-longer-string'), -1486304829)
    t.assert_equals(producer_of.murmur2('lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8'), -58897971)
    t.assert_equals(producer_of.murmur2('abc'), 479470107)
    t.assert_equals(producer_of.murmur2(''), 275646681)
    t.assert_equals(producer_of.murmur2('a'), -1563381124)
    t.assert_equals(producer_of.murmur2('ab'), 316155434)
end

g.test_partition_of_key_is_positive_modulo = function()
    t.assert_equals(producer_of.partition_of('21', 3), 0)
    t.assert_equals(producer_of.partition_of('a', 3), 1)
    t.assert_equals(producer_of.partition_of('ab', 3), 2)
    t.assert_equals(producer_of.partition_of('a-little-bit-long-string', 7), bit.band(-985981536, 0x7fffffff) % 7)
end

g.test_choose_by_key_or_round = function()
    local producer = producer_of.new({})
    local meta = { name = 'orders', count = 3 }

    t.assert_equals(producer:choose(meta, 'a'), 1)
    t.assert_equals(producer:choose(meta, 'a'), 1)
    t.assert_equals(
        { producer:choose(meta), producer:choose(meta), producer:choose(meta), producer:choose(meta) },
        { 0, 1, 2, 0 }
    )
    t.assert_equals(producer:choose({ name = 'other', count = 2 }), 0)
end

g.test_attempt_writes_with_identity_and_sequence = function()
    local producer = started()
    local written = producer:attempt('orders', record('a'), within.deadline(1), {})

    t.assert_equals(written, { partition = 1, offset = 0 })
    t.assert_equals(g.fake.calls, { 'ApiVersions', 'Metadata', 'InitProducerId', 'Produce' })
    t.assert_equals(g.fake.requests.InitProducerId[1], { transaction_timeout_ms = 0 })

    local request = g.fake.requests.Produce[1]

    t.assert_equals(request.acks, -1)
    t.assert_equals(request.transactional_id, nil)
    t.assert_equals(request.timeout_ms > 0 and request.timeout_ms <= 1000, true)
    t.assert_equals(g.fake.produced[1].record.key, 'a')
    t.assert_equals(g.fake.produced[1].record.headers, { { 'x-a', '1' } })

    t.assert_equals(producer:attempt('orders', record('a'), within.deadline(1), {}), { partition = 1, offset = 1 })
    t.assert_equals(producer.identity, { id = 1001, epoch = 0 })
    t.assert_equals(producer:lane('orders', 1).sequence, 2)
    t.assert_is(producer.lanes['orders/1'], producer:lane('orders', 1))
    t.assert_equals(#g.fake.requests.InitProducerId, 1)
end

g.test_produce_timeout_is_the_rest_of_the_deadline = function()
    local producer = started()

    -- Часы стоят: остаток срока — ровно разность мига и «сейчас».
    within._set_source({
        monotonic = function()
            return 100
        end,
        scheduler_now = function()
            return 100
        end,
    })

    producer:attempt('orders', record(), 101, { partition = 0 })
    within._set_source(nil)

    t.assert_equals(g.fake.requests.Produce[1].timeout_ms, 1000)
end

g.test_state_keeps_the_partition_between_attempts = function()
    local producer = started()
    local state = {}

    producer:attempt('orders', record(), within.deadline(1), state)
    producer:attempt('orders', record(), within.deadline(1), state)

    t.assert_equals(state.partition, 0)
    t.assert_equals(g.fake.produced[2].partition, 0)
end

g.test_missing_topic_is_the_failure = function()
    local producer = started()
    local written, err = producer:attempt('missing', record(), within.deadline(1), {})

    t.assert_equals(written, nil)
    t.assert_equals(err.server_code, 3)
end

g.test_busy_partition_waits_its_turn = function()
    local producer = started()
    local lane = producer:lane('orders', 1)

    lane.lock:put(true)

    local started_at = require('fiber').clock()
    local written, err = producer:attempt('orders', record('a'), within.deadline(0.05), {})

    t.assert_equals(written, nil)
    t.assert(require('fiber').clock() - started_at < 0.5, 'замок ждали дольше срока')
    t.assert_equals(err.kind, failure.BUSY)
    t.assert_equals(
        err.message,
        'kafka: раздел orders/1 занят другой записью дольше срока'
    )
    lane.lock:get()
end

g.test_busy_partition_after_the_deadline_does_not_wait = function()
    local producer = started()
    local lane = producer:lane('orders', 1)

    g.cluster:topic('orders', within.deadline(1))
    lane.lock:put(true)

    local _, err = producer:attempt('orders', record('a'), within.deadline(-1), {})

    t.assert_equals(err.kind, failure.BUSY)
    lane.lock:get()
end

g.test_partition_without_leader_forgets_the_topic = function()
    local producer = started({ topics = { orders = 1 } })
    local meta = g.cluster:topic('orders', within.deadline(1))

    meta.partitions[0].leader = 42

    local _, err = producer:attempt('orders', record(), within.deadline(1), {})

    t.assert_equals(err.kind, failure.UNREACHABLE)
    t.assert_equals(err.message, 'kafka: у раздела orders/0 нет ведущего')
    t.assert_equals(g.cluster.topics.orders, nil)
end

g.test_identity_refusal_is_the_failure = function()
    local producer = started({
        respond = function(name)
            if name == 'InitProducerId' then
                return { throttle_time_ms = 0, error_code = 31, producer_id = -1, producer_epoch = -1 }
            end
        end,
    })
    local _, err = producer:attempt('orders', record(), within.deadline(1), {})

    t.assert_equals(err.kind, failure.DENIED)
    t.assert_equals(
        err.message,
        'kafka: брокер не выдал номер производителя: CLUSTER_AUTHORIZATION_FAILED (31)'
    )
end

g.test_identity_without_answer_is_the_failure = function()
    local producer = started({
        respond = function(name)
            if name == 'InitProducerId' then
                return 'close'
            end
        end,
    })
    local _, err = producer:attempt('orders', record(), within.deadline(1), {})

    t.assert_equals(err.kind, failure.BROKEN)
    t.assert_equals(err.retriable, true)
end

g.test_duplicate_sequence_is_success = function()
    local producer = started()

    producer:attempt('orders', record(), within.deadline(1), { partition = 0 })
    producer:lane('orders', 0).sequence = 0

    local written = producer:attempt('orders', record(), within.deadline(1), { partition = 0 })

    t.assert_equals(written, { partition = 0, offset = -1 })
    t.assert_equals(producer:lane('orders', 0).sequence, 1)
end

g.test_out_of_order_resets_the_identity = function()
    local producer = started()

    producer:attempt('orders', record(), within.deadline(1), { partition = 0 })
    producer:lane('orders', 0).sequence = 5

    local _, err = producer:attempt('orders', record(), within.deadline(1), { partition = 0 })

    t.assert_equals(err.kind, failure.UNREACHABLE)
    t.assert_equals(err.retriable, true)
    t.assert_equals(
        err.message,
        'kafka: запись в orders/0 не принята: OUT_OF_ORDER_SEQUENCE_NUMBER (45)'
    )
    t.assert_equals(producer.identity, nil)

    local written = producer:attempt('orders', record(), within.deadline(1), { partition = 0 })

    t.assert_equals(written, { partition = 0, offset = 1 })
    t.assert_equals(producer.identity.id, 1002)
    t.assert_equals(producer:lane('orders', 0).sequence, 1)
end

g.test_stale_refusal_forgets_the_topic_and_keeps_text = function()
    local producer = started({ respond = refusing(6, 'ведущий сменился') })
    local _, err = producer:attempt('orders', record(), within.deadline(1), { partition = 0 })

    t.assert_equals(
        err.message,
        'kafka: запись в orders/0 не принята (ведущий сменился): NOT_LEADER_OR_FOLLOWER (6)'
    )
    t.assert_equals(g.cluster.topics.orders, nil)
    t.assert_not_equals(producer.identity, nil)
end

g.test_timed_out_write_is_retried_by_its_sequence = function()
    local producer = started({ respond = refusing(7) })
    local _, err = producer:attempt('orders', record(), within.deadline(1), { partition = 0 })

    t.assert_equals(err.kind, failure.TIMEOUT)
    t.assert_equals(err.retriable, true)
end

g.test_final_refusal_keeps_identity_and_topic = function()
    local producer = started({ respond = refusing(10) })
    local _, err = producer:attempt('orders', record(), within.deadline(1), { partition = 0 })

    t.assert_equals(err.kind, failure.REJECTED)
    t.assert_equals(err.message, 'kafka: запись в orders/0 не принята: MESSAGE_TOO_LARGE (10)')
    t.assert_not_equals(g.cluster.topics.orders, nil)
    t.assert_not_equals(producer.identity, nil)
end

g.test_unknown_outcome_leaves_doubt_for_others = function()
    local cut = true
    local producer = started({
        respond = function(name)
            if name == 'Produce' and cut then
                return 'close'
            end
        end,
    })
    local mine = { partition = 0 }
    local _, err = producer:attempt('orders', record(), within.deadline(1), mine)

    t.assert_equals(err.kind, failure.BROKEN)
    t.assert_equals(err.sent, true)
    t.assert_equals(err.retriable, true)
    t.assert_is(producer:lane('orders', 0).doubt, mine)

    -- Та же отправка повторяет с тем же номером в счёте.
    cut = false
    t.assert_equals(producer:attempt('orders', record(), within.deadline(1), mine), { partition = 0, offset = 0 })
    t.assert_equals(producer.identity.id, 1001)
    t.assert_equals(producer:lane('orders', 0).doubt, nil)

    -- Чужая отправка после сомнения берёт новый номер производителя.
    cut = true
    producer:attempt('orders', record(), within.deadline(1), mine)
    cut = false
    producer:attempt('orders', record(), within.deadline(1), { partition = 0 })
    t.assert_equals(producer.identity.id, 1002)
    t.assert_equals(producer:lane('orders', 0).sequence, 1)
end

g.test_failure_before_sending_leaves_no_doubt = function()
    local producer = started()

    producer:attempt('orders', record(), within.deadline(1), { partition = 0 })
    g.fake.stop()
    g.cluster:pool({ host = '127.0.0.1', port = g.fake.port }):close()

    local _, err = producer:attempt('orders', record(), within.deadline(0.2), { partition = 0 })

    t.assert_not_equals(err, nil)
    t.assert_equals(producer:lane('orders', 0).doubt, nil)
end

g.test_throw_inside_the_lock_releases_it = function()
    local producer = started()

    t.assert_error(
        producer.attempt,
        producer,
        'orders',
        { value = '{}', headers = {} },
        within.deadline(1),
        { partition = 0 }
    )

    local lane = producer:lane('orders', 0)

    t.assert_equals(lane.lock:count(), 0)
end

g.test_sequence_wraps_after_the_largest = function()
    local producer = started()
    local lane = producer:lane('orders', 0)

    producer:attempt('orders', record(), within.deadline(1), { partition = 0 })
    lane.sequence = 0x7fffffff
    g.fake.sequences[('%d/orders/0'):format(producer.identity.id)] = 0x7fffffff
    producer:attempt('orders', record(), within.deadline(1), { partition = 0 })

    t.assert_equals(lane.sequence, 0)
end
