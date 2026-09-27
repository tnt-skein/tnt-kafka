--- Проверки кластера: пулы на узел, обращение, описание тем и кэш,
--- вызов с повторами и сроком, заведение тем.

local fiber = require('fiber')
local t = require('luatest')

local helper = dofile('test/helper.lua')

local admin = helper.admin
local api = helper.api
local cluster_of = helper.cluster
local failure = helper.failure
local within = helper.within

local g = t.group('tnt.kafka.cluster')

--- Срок описания темы, каким его завёл модуль: проверки меняют его и
--- возвращают назад.
local AGE = cluster_of.METADATA_AGE

g.after_each(function()
    if g.cluster ~= nil then
        g.cluster:close()
    end

    g.cluster = nil
    cluster_of.METADATA_AGE = AGE
    helper.release(g)
end)

--- Кластер к двойнику.
---@param opts table|nil Настройки двойника
---@param settings table|nil Настройки клиента поверх
---@return table cluster
---@return table fake
local function started(opts, settings)
    g.fake = helper.serve(opts)
    g.cluster = cluster_of.new(helper.checked(helper.settings_of(g.fake, settings)))

    return g.cluster, g.fake
end

--- Адрес двойника.
---@return table
local function home()
    return { host = '127.0.0.1', port = g.fake.port }
end

g.test_new_refuses_bad_retry = function()
    local cluster, err = cluster_of.new(helper.checked({ retry = { factor = 0 } }))

    t.assert_equals(cluster, nil)
    t.assert_str_contains(err, 'factor')
end

g.test_exchange_takes_and_returns_the_connection = function()
    local cluster = started()
    local answer =
        cluster:exchange(home(), api.METADATA, { topics = {}, allow_auto_topic_creation = false }, within.deadline(1))

    t.assert_equals(answer.controller_id, 1)

    local stats = cluster:stats()[cluster_of.where(home())]

    t.assert_equals(stats.total, 1)
    t.assert_equals(stats.idle, 1)
    t.assert_is(cluster:pool(home()), cluster:pool(home()))
    t.assert_equals(cluster:pool(home()).settings.name, ('kafka/127.0.0.1:%d'):format(g.fake.port))
end

g.test_exchange_after_deadline_is_timeout_unsent = function()
    local cluster = started()
    local answer, err = cluster:exchange(home(), api.METADATA, {}, within.deadline(-1))

    t.assert_equals(answer, nil)
    t.assert_equals(err.kind, failure.TIMEOUT)
    t.assert_equals(err.sent, false)
    t.assert_equals(
        err.message,
        ('срок вызова вышел до обращения к 127.0.0.1:%d'):format(g.fake.port)
    )
    t.assert_equals(g.fake.connections, 0)
end

g.test_node_that_does_not_open_is_unreachable = function()
    local cluster = started(nil, { pool = { wait_timeout = 0.2 } })
    local port = helper.closed_port()
    local _, err = cluster:exchange({ host = '127.0.0.1', port = port }, api.METADATA, {}, within.deadline(1))

    t.assert_equals(err.kind, failure.UNREACHABLE)
    t.assert_str_contains(err.message, ('kafka 127.0.0.1:%d: '):format(port))
end

g.test_full_pool_is_busy = function()
    local cluster = started(nil, { pool = { size = 1, wait_timeout = 0.05 } })
    local held = cluster:pool(home()):take(1)
    local _, err = cluster:exchange(home(), api.METADATA, {}, within.deadline(1))

    t.assert_equals(err.kind, failure.BUSY)
    cluster:pool(home()):give(held)
end

g.test_final_open_refusal_passes_through = function()
    local cluster = started({ sasl = { username = 'app', password = 'secret' } }, {
        sasl = { username = 'app', password = 'wrong' },
    })
    local _, err = cluster:exchange(home(), api.METADATA, {}, within.deadline(1))

    t.assert_equals(err.kind, failure.DENIED)
    t.assert_str_contains(err.message, 'Authentication failed')
end

g.test_trouble_drops_the_connection = function()
    local cluster = started({
        respond = function(name)
            if name == 'Metadata' then
                return 'close'
            end
        end,
    })
    local _, err = cluster:exchange(
        home(),
        api.METADATA,
        { topics = {}, allow_auto_topic_creation = false },
        within.deadline(1),
        true
    )

    t.assert_equals(err.kind, failure.BROKEN)
    t.assert_equals(err.sent, true)
    t.assert_equals(err.retriable, true)
    t.assert_str_contains(
        err.message,
        ('kafka 127.0.0.1:%d: Metadata: связь оборвалась'):format(g.fake.port)
    )
    t.assert_equals(cluster:stats()[cluster_of.where(home())].total, 0)

    _, err =
        cluster:exchange(home(), api.METADATA, { topics = {}, allow_auto_topic_creation = false }, within.deadline(1))
    t.assert_equals(err.retriable, false)
end

g.test_throw_inside_exchange_goes_up = function()
    local cluster = started()

    t.assert_error_msg_contains('bad argument', cluster.exchange, cluster, home(), api.METADATA, {
        topics = 'строка',
        allow_auto_topic_creation = false,
    }, within.deadline(1))
    t.assert_error(cluster.exchange, cluster, home(), helper.coordination.HEARTBEAT, {}, within.deadline(1))
    t.assert_equals(cluster:stats()[cluster_of.where(home())].total, 0)
end

g.test_metadata_remembers_nodes_and_topics = function()
    local cluster = started({ topics = { orders = 2, empty = 0 } })
    local answer = cluster:metadata(nil, within.deadline(1))

    t.assert_equals(#answer.topics, 2)
    t.assert_equals(cluster.nodes[1], { host = '127.0.0.1', port = g.fake.port })
    t.assert_equals(cluster.topics.orders.count, 2)
    t.assert_equals(cluster.topics.orders.partitions[1], { leader = 1 })
    t.assert_equals(cluster.topics.empty, nil)
    t.assert_equals(g.fake.requests.Metadata[1].topics, nil)
    t.assert_equals(g.fake.requests.Metadata[1].allow_auto_topic_creation, false)

    cluster:metadata({ 'orders', 'missing' }, within.deadline(1))
    t.assert_equals(g.fake.requests.Metadata[2].topics, { { name = 'orders' }, { name = 'missing' } })
    t.assert_equals(cluster.topics.missing, nil)
end

g.test_metadata_asks_known_nodes_then_bootstrap = function()
    local cluster = started({ topics = { orders = 1 } })

    cluster.nodes[7] = { host = '127.0.0.1', port = helper.closed_port() }

    local answer = cluster:metadata({ 'orders' }, within.deadline(2))

    t.assert_not_equals(answer, nil)
end

g.test_metadata_from_nobody_is_the_last_failure = function()
    g.cluster = cluster_of.new(helper.checked({
        brokers = { ('127.0.0.1:%d'):format(helper.closed_port()) },
        pool = { wait_timeout = 0.1, sweep_interval = 0 },
    }))

    local answer, err = g.cluster:metadata(nil, within.deadline(1))

    t.assert_equals(answer, nil)
    t.assert_equals(err.kind, failure.UNREACHABLE)
end

g.test_topic_is_cached_until_it_ages = function()
    local cluster = started({ topics = { orders = 3 } })
    local meta = cluster:topic('orders', within.deadline(1))

    t.assert_equals(meta.count, 3)
    t.assert_equals(meta.name, 'orders')
    t.assert_is(cluster:topic('orders', within.deadline(1)), meta)
    t.assert_equals(#g.fake.requests.Metadata, 1)

    cluster_of.METADATA_AGE = 0
    fiber.sleep(0.01)
    cluster:topic('orders', within.deadline(1))
    t.assert_equals(#g.fake.requests.Metadata, 2)

    cluster_of.METADATA_AGE = AGE
    cluster:forget('orders')
    cluster:topic('orders', within.deadline(1))
    t.assert_equals(#g.fake.requests.Metadata, 3)
end

g.test_missing_topic_is_unreachable_with_code = function()
    local cluster = started({ topics = { empty = 0 } })
    local meta, err = cluster:topic('missing', within.deadline(1))

    t.assert_equals(meta, nil)
    t.assert_equals(err.kind, failure.UNREACHABLE)
    t.assert_equals(err.server_code, 3)
    t.assert_equals(err.message, 'kafka: тема missing не описана: UNKNOWN_TOPIC_OR_PARTITION (3)')

    local _, empty = cluster:topic('empty', within.deadline(1))

    t.assert_equals(empty.message, 'kafka: тема empty не описана: у темы нет разделов')
    t.assert_equals(empty.server_code, 0)
end

g.test_topic_without_answer_is_the_failure = function()
    local cluster = started({
        respond = function(name)
            if name == 'Metadata' then
                return 'close'
            end
        end,
    })
    local _, err = cluster:topic('orders', within.deadline(1))

    t.assert_equals(err.kind, failure.BROKEN)
end

g.test_topic_answer_without_topics_is_empty = function()
    local cluster = started({
        respond = function(name, values, talk)
            if name == 'Metadata' then
                local answer = helper.default(talk.fake, name, values)

                answer.topics = {}

                return answer
            end
        end,
    })
    local _, err = cluster:topic('orders', within.deadline(1))

    t.assert_equals(err.message, 'kafka: тема orders не описана: у темы нет разделов')
end

g.test_leader_of_partition = function()
    local cluster = started({ topics = { orders = 1 } })
    local meta = cluster:topic('orders', within.deadline(1))

    t.assert_equals(cluster:leader(meta, 0), { host = '127.0.0.1', port = g.fake.port })
    t.assert_equals(cluster:leader(meta, 5), nil)

    meta.partitions[0].leader = 99
    t.assert_equals(cluster:leader(meta, 0), nil)
end

g.test_call_retries_retriable_until_success = function()
    local cluster = started()
    local runs = 0
    local value = cluster:call(function()
        runs = runs + 1

        if runs < 6 then
            return nil, failure.new(failure.UNREACHABLE, 'ещё нет')
        end

        return 'готово'
    end, 1)

    -- Больше трёх попыток по умолчанию: повторы идут до срока.
    t.assert_equals(value, 'готово')
    t.assert_equals(runs, 6)
end

g.test_call_stops_at_final_failure = function()
    local cluster = started()
    local runs = 0
    local value, err = cluster:call(function()
        runs = runs + 1

        return nil, failure.new(failure.REJECTED, 'нет')
    end, 1)

    t.assert_equals(value, nil)
    t.assert_equals(err.message, 'нет')
    t.assert_equals(runs, 1)
end

g.test_call_after_deadline_returns_last_failure_final = function()
    local cluster = started()
    local now = 0

    -- Срок кончается между попытками, а повторы ещё думают, что время
    -- есть: у них свои часы.
    within._set_source({
        monotonic = function()
            return now
        end,
        scheduler_now = function()
            return now
        end,
    })

    local runs = 0
    local _, err = cluster:call(function()
        runs = runs + 1
        now = now + 10

        return nil, failure.new(failure.UNREACHABLE, 'узел молчит', { server_code = 5 })
    end, 1)

    within._set_source(nil)

    t.assert_equals(runs, 1)
    t.assert_equals(err.kind, failure.UNREACHABLE)
    t.assert_equals(err.message, 'узел молчит')
    t.assert_equals(err.retriable, false)
    t.assert_equals(err.server_code, 5)
    t.assert_equals(err.sent, false)
end

g.test_call_that_outlives_the_deadline_keeps_the_verdict = function()
    local cluster = started()
    local _, err = cluster:call(function()
        fiber.sleep(0.06)

        return nil, failure.new(failure.UNREACHABLE, 'узел молчит')
    end, 0.05)

    t.assert_equals(err.message, 'узел молчит')
    t.assert_equals(err.retriable, true)
end

g.test_call_without_time_is_timeout_unsent = function()
    local cluster = started()

    within._set_source({
        monotonic = function()
            return 100
        end,
        scheduler_now = function()
            return 200
        end,
    })

    local runs = 0
    local _, err = cluster:call(function()
        runs = runs + 1
    end, 1)

    within._set_source(nil)

    t.assert_equals(runs, 0)
    t.assert_equals(err.kind, failure.TIMEOUT)
    t.assert_equals(err.sent, false)
    t.assert_equals(err.message, 'срок вызова вышел до отправки дела')
end

g.test_call_on_closed_cluster_is_closed = function()
    local cluster = started()

    cluster:close()

    local _, err = cluster:call(function()
        return 'не дойдёт'
    end, 1)

    t.assert_equals(err.kind, failure.CLOSED)
    t.assert_equals(err.message, 'kafka: клиент закрыт')
    g.cluster = nil
end

g.test_ensure_creates_only_missing = function()
    local cluster = started({ topics = { orders = 1 }, partitions = 4 })
    local ok = admin.ensure(
        cluster,
        { 'orders', 'orders.retry', 'orders.dead' },
        { partitions = 2 },
        within.deadline(1)
    )

    t.assert_equals(ok, true)
    t.assert_equals(#g.fake.created, 2)
    t.assert_equals(g.fake.created[1].name, 'orders.retry')
    t.assert_equals(g.fake.created[1].num_partitions, 2)
    t.assert_equals(g.fake.created[1].replication_factor, -1)
    t.assert_equals(g.fake.requests.CreateTopics[1].validate_only, false)
    t.assert_equals(g.fake.requests.CreateTopics[1].timeout_ms > 0, true)
    t.assert_equals(#g.fake.topics['orders.dead'], 2)

    t.assert_equals(admin.ensure(cluster, { 'orders' }, {}, within.deadline(1)), true)
    t.assert_equals(#g.fake.requests.CreateTopics, 1)
end

g.test_ensure_uses_broker_defaults = function()
    local cluster = started({ partitions = 3 })

    admin.ensure(cluster, { 'orders' }, { replication = 1 }, within.deadline(1))

    t.assert_equals(g.fake.created[1].num_partitions, -1)
    t.assert_equals(g.fake.created[1].replication_factor, 1)
    t.assert_equals(#g.fake.topics.orders, 3)
end

g.test_ensure_takes_existing_as_success = function()
    local cluster = started({
        respond = function(name, values)
            if name == 'CreateTopics' then
                return { throttle_time_ms = 0, topics = { { name = values.topics[1].name, error_code = 36 } } }
            end
        end,
    })

    t.assert_equals(admin.ensure(cluster, { 'orders' }, {}, within.deadline(1)), true)
end

g.test_ensure_failures = function()
    local cluster = started({
        respond = function(name, values, talk)
            if name == 'CreateTopics' then
                return {
                    throttle_time_ms = 0,
                    topics = {
                        {
                            name = values.topics[1].name,
                            error_code = 38,
                            error_message = 'копий больше, чем узлов',
                        },
                    },
                }
            end

            if name == 'Metadata' and values.topics[1].name == 'secret' then
                local answer = helper.default(talk.fake, name, values)

                answer.topics[1].error_code = 29

                return answer
            end
        end,
    })
    local ok, err = admin.ensure(cluster, { 'orders' }, { replication = 5 }, within.deadline(1))

    t.assert_equals(ok, nil)
    t.assert_equals(err.kind, failure.REJECTED)
    t.assert_equals(
        err.message,
        'kafka: тема orders не заведена (копий больше, чем узлов): INVALID_REPLICATION_FACTOR (38)'
    )

    ok, err = admin.ensure(cluster, { 'secret' }, {}, within.deadline(1))
    t.assert_equals(ok, nil)
    t.assert_equals(err.kind, failure.DENIED)
    t.assert_equals(err.message, 'kafka: тема secret не описана: TOPIC_AUTHORIZATION_FAILED (29)')
end

g.test_ensure_failure_without_text = function()
    local cluster = started({
        respond = function(name, values)
            if name == 'CreateTopics' then
                return { throttle_time_ms = 0, topics = { { name = values.topics[1].name, error_code = 44 } } }
            end
        end,
    })
    local _, err = admin.ensure(cluster, { 'orders' }, {}, within.deadline(1))

    t.assert_equals(err.message, 'kafka: тема orders не заведена: POLICY_VIOLATION (44)')
end

g.test_ensure_without_answers = function()
    local cluster = started({
        respond = function(name)
            if name == 'CreateTopics' then
                return 'close'
            end
        end,
    })
    local _, err = admin.ensure(cluster, { 'orders' }, {}, within.deadline(1))

    t.assert_equals(err.kind, failure.BROKEN)

    g.fake.stop()

    local ok
    ok, err = admin.ensure(cluster, { 'orders' }, {}, within.deadline(0.3))
    t.assert_equals(ok, nil)
    t.assert_not_equals(err, nil)
end

g.test_ensure_goes_to_bootstrap_without_known_controller = function()
    local cluster = started({
        respond = function(name, values, talk)
            if name == 'Metadata' then
                local answer = helper.default(talk.fake, name, values)

                answer.controller_id = 42

                return answer
            end
        end,
    })

    t.assert_equals(admin.ensure(cluster, { 'orders' }, {}, within.deadline(1)), true)
end

--- Часы, которые стоят на мигах `monotonic` и `scheduler_now`.
---@param monotonic number
---@param now number
local function frozen(monotonic, now)
    within._set_source({
        monotonic = function()
            return monotonic
        end,
        scheduler_now = function()
            return now
        end,
    })
end

g.test_milliseconds_are_the_rest_but_at_least_one = function()
    frozen(100, 100)

    local values = {
        cluster_of.milliseconds(101),
        cluster_of.milliseconds(100.0025),
        cluster_of.milliseconds(100.0004),
        cluster_of.milliseconds(99),
    }

    within._set_source(nil)
    t.assert_equals(values, { 1000, 2, 1, 1 })
end

g.test_topic_cache_expires_by_the_age = function()
    local cluster = started({ topics = { orders = 1 } })

    t.assert_equals(AGE, 300)
    frozen(100, 100)
    cluster_of.METADATA_AGE = 0.5
    cluster:topic('orders', 101)
    cluster:topic('orders', 101)
    cluster_of.METADATA_AGE = 0
    cluster:topic('orders', 101)
    within._set_source(nil)

    t.assert_equals(#g.fake.requests.Metadata, 2)
end

g.test_exchange_at_the_exact_deadline_is_timeout = function()
    local cluster = started()

    frozen(100, 100)

    local answer, err = cluster:exchange(home(), api.METADATA, {}, 100)

    within._set_source(nil)
    t.assert_equals(answer, nil)
    t.assert_equals(err.kind, failure.TIMEOUT)
    t.assert_equals(g.fake.connections, 0)
end

g.test_call_at_the_exact_deadline_is_late = function()
    local cluster = started()
    local runs = 0

    frozen(100, 101)

    local _, err = cluster:call(function()
        runs = runs + 1
    end, 1)

    within._set_source(nil)
    t.assert_equals(runs, 0)
    t.assert_equals(err.message, 'срок вызова вышел до отправки дела')
end

g.test_metadata_asks_each_address_once = function()
    local cut = false
    local cluster = started({
        respond = function(name)
            if name == 'Metadata' and cut then
                cut = false

                return 'close'
            end
        end,
    })

    t.assert_not_equals(cluster:metadata(nil, within.deadline(1)), nil)

    cut = true

    local answer, err = cluster:metadata(nil, within.deadline(1))

    t.assert_equals(answer, nil)
    t.assert_equals(err.kind, failure.BROKEN)
    t.assert_equals(err.retriable, true)
end

g.test_metadata_splits_the_deadline_between_nodes = function()
    g.fake = helper.serve()

    local dead = ('127.0.0.1:%d'):format(helper.closed_port())
    local live = ('127.0.0.1:%d'):format(g.fake.port)

    g.cluster = cluster_of.new(helper.checked(helper.settings_of(g.fake, { brokers = { dead, live } })))

    local started_at = fiber.clock()
    local answer = g.cluster:metadata(nil, within.deadline(1))

    t.assert_not_equals(answer, nil)
    t.assert_almost_equals(fiber.clock() - started_at, 0.5, 0.1)
end

g.test_full_pool_after_a_failed_open_is_busy = function()
    ---@type TntPool|nil
    local connections

    -- Двойник рвёт вход, пока пул этой проверки сам не запишет отказ
    -- открытия, а не один первый вход. Порты петли общие на машину: клиент
    -- соседнего прогона, стучащийся на освободившийся порт, мог войти
    -- к двойнику раньше пула, забрать единственный отказ, и открытие пула
    -- удавалось с первого раза.
    local cluster = started({
        respond = function(name)
            if name == 'ApiVersions' and (connections == nil or connections:stats().last_open_error == nil) then
                return 'close'
            end
        end,
    }, { pool = { size = 1, wait_timeout = 0.2 } })

    connections = cluster:pool(home())

    -- Второе открытие удаётся за миллисекунды, и срок взятия ничего
    -- не проверяет — он только не даёт поломке повесить прогон. Поэтому
    -- он с запасом: в полном прогоне рядом с чужими секунды не хватило.
    local held = connections:take(10)

    t.assert_not_equals(held, nil)
    t.assert_not_equals(connections:stats().last_open_error, nil)

    local _, err = cluster:exchange(home(), api.METADATA, {}, within.deadline(1))

    t.assert_equals(err.kind, failure.BUSY)
    connections:give(held)
end

g.test_create_topics_cut_is_retriable = function()
    local cluster = started({
        respond = function(name)
            if name == 'CreateTopics' then
                return 'close'
            end
        end,
    })
    local _, err = admin.ensure(cluster, { 'orders' }, {}, within.deadline(1))

    t.assert_equals(err.kind, failure.BROKEN)
    t.assert_equals(err.retriable, true)
end
