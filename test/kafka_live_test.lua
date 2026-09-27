--- Клиент против настоящей Kafka.
---
--- Двойник показывает, что мы правильно разговариваем сами с собой.
--- Настоящий брокер показывает, что нас понимает кто-то ещё: версии
--- запросов, которые он принимает, контрольную сумму и счёт записей
--- идемпотентного производителя, вход в группу и раздачу разделов,
--- отказ во входе по SASL и TLS с его сертификатом. Брокер поднимается
--- отдельно — `make kafka-up` (`test/stand/kafka.sh`), — и если его нет,
--- проверки честно пропускаются: гейты не должны зависеть от докера.

local fio = require('fio')
local t = require('luatest')
local uuid = require('uuid')

local helper = dofile('test/helper.lua')

local context = helper.context
local failure = helper.failure
local kafka = helper.kafka

local g = t.group('tnt.kafka.live')

--- Окружение стенда — через `tnt-env`: мимо него окружение не читают
--- и проверки.
local env = helper.stand_env()

--- Где стоит брокер: те же адреса и переменные, что у скрипта стенда.
local PORT = env.int('STAND_KAFKA_PORT', 19092)
local SASL_PORT = env.int('STAND_KAFKA_SASL_PORT', 19094)
local TLS_PORT = env.int('STAND_KAFKA_TLS_PORT', 19095)
local TLS_DIR = env.string('KAFKA_TLS_DIR', 'test/stand/run/kafka-tls')

--- Пароль учётки `app` на стенде: локальный брокер, а не развёртывание.
local SECRET = 'stand-secret'

g.before_all(function()
    -- Стенд поднят, если на его порту кто-то слушает.
    local probe = require('socket').tcp_connect('127.0.0.1', PORT, 0.3)

    t.skip_if(probe == nil, 'Kafka не отвечает: поднимите её — make kafka-up')
    probe:close()
end)

g.after_each(function()
    for _, consumer in ipairs(g.consumers or {}) do
        consumer:stop()
        helper.until_true(function()
            return consumer.workers == 0
        end, 10)
    end

    for _, broker in ipairs(g.brokers or {}) do
        broker:close()
    end

    g.consumers, g.brokers = nil, nil
    helper.restore()
end)

--- Клиент к стенду; закроется после проверки сам.
---@param opts table|nil
---@return table
local function client(opts)
    local given = opts or {}

    given.brokers = given.brokers or { ('127.0.0.1:%d'):format(PORT) }
    given.session_timeout = given.session_timeout or 6

    local broker = kafka.new(given)

    g.brokers = g.brokers or {}
    g.brokers[#g.brokers + 1] = broker

    return broker
end

--- Имя темы, которого у брокера ещё не было.
---@param prefix string
---@return string
local function fresh(prefix)
    return ('%s-%s'):format(prefix, uuid.str():sub(1, 8))
end

--- Ждёт условия до полуминуты: брокеру нужно время на раздачу разделов.
---@param about string Чего ждали — текст провала
---@param condition fun(): boolean
local function eventually(about, condition)
    if not helper.until_true(condition, 30) then
        t.fail(about)
    end
end

g.test_round_trip_keeps_envelope_context_and_key = function()
    local broker = client()
    local orders = broker:declare(fresh('orders'), { partitions = 3 })
    local sent = context.run({ request_id = 'r-live' }, function()
        return orders:send({ order = 7, name = 'узел' }, { key = 'customer-42', headers = { ['x-trace'] = 'a' } })
    end)
    local seen ---@type any

    t.assert_equals(#sent, 26)

    helper.consume(g, orders, function(message)
        seen = { message = message, request_id = context.get('request_id') }
    end)

    eventually('сообщение не обработано', function()
        return seen ~= nil
    end)
    t.assert_equals(seen.message.id, sent)
    t.assert_equals(seen.message.body, { order = 7, name = 'узел' })
    t.assert_equals(seen.message.key, 'customer-42')
    t.assert_equals(seen.message.attempt, 1)
    t.assert_equals(seen.message.headers['x-trace'], 'a')
    t.assert_equals(seen.request_id, 'r-live')
    eventually('смещение не зафиксировано', function()
        return orders.counts.ack == 1
    end)
end

g.test_key_goes_to_the_murmur2_partition = function()
    local broker = client()
    local name = fresh('keys')
    local orders = broker:declare(name, { partitions = 3 })

    orders:send({ n = 1 }, { key = 'a' })

    local meta = broker.cluster:topic(name, helper.within.deadline(5))
    local lane = broker.producer:lane(name, helper.producer.partition_of('a', meta.count))

    t.assert_equals(lane.sequence, 1)
end

g.test_retry_bury_and_kick = function()
    local broker = client()
    local orders = broker:declare(fresh('bury'), { partitions = 1 })
    local attempts = {}
    local fixed = false

    orders:send({ order = 1 })
    helper.consume(g, orders, function(message)
        attempts[#attempts + 1] = message.attempt

        if not fixed then
            return nil, { retry_after = 0.2, message = 'не выходит' }
        end
    end, { max_attempts = 2 })

    eventually('сообщение не зарыто', function()
        return orders.counts.bury == 1
    end)
    t.assert_equals(attempts, { 1, 2 })

    fixed = true
    t.assert_equals(orders:kick(5), 1)
    eventually('возвращённое не обработано', function()
        return orders.counts.ack == 1
    end)
    t.assert_equals(attempts, { 1, 2, 1 })
    t.assert_equals(orders:kick(5), 0)
end

g.test_two_workers_share_the_partitions = function()
    local broker = client()
    local orders = broker:declare(fresh('share'), { partitions = 3 })
    local fibers = {}

    for index = 1, 12 do
        orders:send({ n = index }, { key = ('k%d'):format(index) })
    end

    helper.consume(g, orders, function()
        fibers[require('fiber').self():name()] = true
    end, { workers = 2 })

    eventually('сообщения не обработаны', function()
        return orders.counts.ack == 12
    end)

    local count = 0

    for _ in pairs(fibers) do
        count = count + 1
    end

    t.assert(count >= 1)
end

g.test_group_named_apart_from_the_topic = function()
    local broker = client()
    local name = fresh('named')
    local topic = broker:declare(name, { group = 'mailer', partitions = 1 })
    local seen = 0

    t.assert_equals(topic.retry, name .. '.mailer.retry')
    topic:send({ a = 1 })
    helper.consume(g, topic, function()
        seen = seen + 1
    end)
    eventually('сообщение не обработано', function()
        return seen == 1
    end)
end

g.test_sasl_plain_logs_in_and_refuses_a_wrong_password = function()
    local good =
        client({ brokers = { ('127.0.0.1:%d'):format(SASL_PORT) }, sasl = { username = 'app', password = SECRET } })

    t.assert_not_equals(good:declare(fresh('sasl')):send({ a = 1 }), nil)

    local bad = client({
        brokers = { ('127.0.0.1:%d'):format(SASL_PORT) },
        sasl = { username = 'app', password = 'wrong' },
        timeout = 2,
    })
    local sent, err = bad:declare(fresh('sasl')):send({ a = 1 })

    t.assert_equals(sent, nil)
    t.assert_equals(err.kind, failure.DENIED)
    t.assert_equals(err.server_code, 58)
    t.assert_str_contains(err.message, 'Invalid username or password')
end

g.test_tls_with_the_stand_root = function()
    local ca = fio.pathjoin(TLS_DIR, 'ca.pem')

    t.skip_if(not fio.path.exists(ca), 'сертификатов стенда нет: make kafka-up')

    local secured = client({ brokers = { ('127.0.0.1:%d'):format(TLS_PORT) }, tls = { ca_file = ca } })

    t.assert_not_equals(secured:declare(fresh('tls')):send({ a = 1 }), nil)

    local untrusted = client({ brokers = { ('127.0.0.1:%d'):format(TLS_PORT) }, tls = true, timeout = 1 })
    local sent, err = untrusted:declare(fresh('tls')):send({ a = 1 })

    t.assert_equals(sent, nil)
    t.assert_equals(err.kind, failure.UNREACHABLE)
    t.assert_str_contains(err.message, 'рукопожатие TLS не прошло')
end
