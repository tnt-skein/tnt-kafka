--- Проверки фасада: клиент, объявление темы, отправка, отправитель для
--- ящика исходящих, сводка и закрытие.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local context = helper.context
local failure = helper.failure
local kafka = helper.kafka

local g = t.group('tnt.kafka')

g.after_each(function()
    helper.release(g)
end)

--- Форма тела: поле `order` — целое.
local SHAPE = {
    from = function(body)
        if type(body) ~= 'table' or type(body.order) ~= 'number' then
            return nil, { order = 'целое' }
        end

        return body
    end,
}

g.test_new_opens_nothing = function()
    g.fake = helper.serve()

    local broker = helper.client(g.fake)

    t.assert_equals(broker.name, 'kafka')
    t.assert_equals(broker.closed, false)
    t.assert_equals(broker.limits, { timeout = 5, max_timeout = 60 })
    t.assert_equals(g.fake.connections, 0)
    t.assert_equals(kafka.features, helper.message.FEATURES)
    t.assert_is(kafka.hook, helper.hook.set)
    t.assert_is(kafka.hooks, helper.hook.names)
    broker:close()
end

g.test_declare_names_retry_and_dead_topics = function()
    g.fake = helper.serve()
    g.broker = helper.client(g.fake)

    local own = g.broker:declare('orders')
    local named = g.broker:declare('orders', { group = 'mailer', partitions = 3, replication = 1, shape = SHAPE })

    t.assert_equals({ own.group, own.retry, own.dead }, { 'orders', 'orders.retry', 'orders.dead' })
    t.assert_equals({ named.group, named.retry, named.dead }, { 'mailer', 'orders.mailer.retry', 'orders.mailer.dead' })
    t.assert_equals(named.spec, { partitions = 3, replication = 1 })
    t.assert_is(g.broker.topics.orders, own)
    t.assert_is(g.broker.topics['orders/mailer'], named)
    t.assert_equals(g.fake.connections, 0)
end

g.test_declare_blames_the_caller = function()
    g.fake = helper.serve()
    g.broker = helper.client(g.fake)

    local long = string.rep('a', 243)

    helper.assert_blamed({
        {
            function()
                g.broker:declare('заказы')
            end,
            'имя темы — строка по образцу ^[%w%._%-]+$, а не «заказы»',
        },
        {
            function()
                g.broker:declare('orders', { group = 'a b' })
            end,
            'настройки темы.group — строка по образцу ^[%w%._%-]+$, а не «a b»',
        },
        {
            function()
                g.broker:declare('orders', { partitions = 0 })
            end,
            'настройки темы.partitions — число больше 0, а не 0',
        },
        {
            function()
                g.broker:declare('orders', { replication = 0 })
            end,
            'настройки темы.replication — число больше 0, а не 0',
        },
        {
            function()
                g.broker:declare('orders', { shape = {} })
            end,
            'настройки темы.shape.from — функция или вызываемая таблица, а не nil',
        },
        {
            function()
                g.broker:declare('orders', { ttl = 1 })
            end,
            'настройки темы: ключа «ttl» нет, есть group, partitions, replication, shape',
        },
        {
            function()
                g.broker:declare(long .. 'b')
            end,
            'длина имени темы повтора — число от 1 до 249, а не 250',
        },
    })

    t.assert_equals(g.broker:declare(long).retry, long .. '.retry')
end

g.test_send_creates_topics_and_returns_id = function()
    g.fake = helper.serve({ partitions = 2 })
    g.broker = helper.client(g.fake)

    local orders = g.broker:declare('orders', { partitions = 2 })
    local sent = context.run({ request_id = 'r-7' }, function()
        return orders:send({ order = 1 }, { key = 'k', headers = { ['x-a'] = 'b' } })
    end)

    t.assert_equals(#sent, 26)

    local created = {}

    for _, entry in ipairs(g.fake.created) do
        created[#created + 1] = entry.name
    end

    t.assert_items_equals(created, { 'orders', 'orders.retry', 'orders.dead' })
    t.assert_equals(orders.ready, true)
    t.assert_equals(orders.counts.sent, 1)

    local record = g.fake.produced[1].record
    local envelope, own = helper.message.delivered('orders', record, 0)

    t.assert_equals(envelope.id, sent)
    t.assert_equals(envelope.body, { order = 1 })
    t.assert_equals(envelope.key, 'k')
    t.assert_equals(envelope.headers['x-request-id'], 'r-7')
    t.assert_equals(envelope.headers['x-a'], 'b')
    t.assert_equals(own['x-tnt-attempt'], '0')

    orders:send({ order = 2 }, { id = 'готовый', timeout = 1 })
    -- Срок — названный, а не пять секунд умолчания; время планировщика
    -- отстаёт от настоящих часов, и остаток бывает чуть больше секунды.
    t.assert(g.fake.requests.Produce[2].timeout_ms < 2000, 'срок отправки не тот, что назван')
    t.assert_equals(#g.fake.requests.CreateTopics, 1)
    t.assert_equals(helper.message.delivered('orders', g.fake.produced[2].record, 0).id, 'готовый')
end

g.test_send_failure_is_a_pair = function()
    g.fake = helper.serve({
        respond = function(name)
            if name == 'CreateTopics' then
                return { topics = { { name = 'orders', error_code = 29 } } }
            end
        end,
    })
    g.broker = helper.client(g.fake)

    local orders = g.broker:declare('orders')
    local sent, err = orders:send({ order = 1 })

    t.assert_equals(sent, nil)
    t.assert_equals(err.kind, failure.DENIED)
    t.assert_equals(orders.ready, false)
    t.assert_equals(orders.counts.sent, 0)
end

g.test_send_goes_through_hooks = function()
    g.fake = helper.serve()
    g.broker = helper.client(g.fake)

    local seen = {}

    kafka.hook('trace', function(call, proceed)
        local id, err = proceed()

        seen = { kind = call.kind, name = call.name, id = call.message.id, got = id, err = err }

        return id, err
    end)

    local sent = g.broker:declare('orders'):send({ order = 1 })

    t.assert_equals(seen, { kind = 'send', name = 'orders', id = sent, got = sent })
    t.assert_equals(kafka.hooks(), { 'trace' })
end

g.test_send_blames_the_caller = function()
    g.fake = helper.serve()
    g.broker = helper.client(g.fake)

    local orders = g.broker:declare('orders')
    local shaped = g.broker:declare('shaped', { shape = SHAPE })

    helper.assert_blamed({
        {
            function()
                orders:send({ order = 1 }, { delay = 5 })
            end,
            'настройки отправки: «delay» — отсрочки у Kafka нет: запись выдаётся, как только легла',
        },
        {
            function()
                orders:send({ f = print })
            end,
            'тело.f — простые данные, а не function',
        },
        {
            function()
                shaped:send({ order = 'один' })
            end,
            'тело для темы shaped не по форме: order: целое',
        },
        {
            function()
                orders:send({ order = 1 }, { timeout = 100 })
            end,
            'timeout 100 с длиннее потолка max_timeout 60 с',
        },
    })

    helper.topic._set_source({
        in_txn = function()
            return true
        end,
    })

    helper.assert_blamed({
        {
            function()
                orders:send({ order = 1 })
            end,
            'отправка в orders внутри транзакции box: она ждёт брокера, а уступка порвёт транзакцию — '
                .. 'отправляйте через tnt-outbox',
        },
    })
    t.assert_equals(g.fake.connections, 0)
end

g.test_shaped_body_passes = function()
    g.fake = helper.serve()
    g.broker = helper.client(g.fake)

    t.assert_not_equals(g.broker:declare('shaped', { shape = SHAPE }):send({ order = 1 }), nil)
end

g.test_sender_declares_what_it_does_not_know = function()
    g.fake = helper.serve()
    g.broker = helper.client(g.fake)

    local sender = g.broker:sender()

    t.assert_equals(sender.features, kafka.features)
    t.assert_not_equals(sender.send('events', { a = 1 }, { key = 'k' }), nil)
    t.assert_not_equals(g.broker.topics.events, nil)

    local known = g.broker.topics.events

    sender.send('events', { a = 2 })
    t.assert_is(g.broker.topics.events, known)
    t.assert_equals(known.counts.sent, 2)
end

g.test_status_shows_topics_and_pools = function()
    g.fake = helper.serve()
    g.broker = helper.client(g.fake)

    local orders = g.broker:declare('orders', { group = 'mailer' })

    orders:send({ order = 1 })

    local status = g.broker:status()
    local where = ('127.0.0.1:%d'):format(g.fake.port)

    t.assert_equals(status.name, 'kafka')
    t.assert_equals(status.closed, false)
    t.assert_equals(status.hooks, {})
    t.assert_equals(status.pools[where].total, 1)
    t.assert_equals(status.topics['orders/mailer'], {
        name = 'orders',
        group = 'mailer',
        retry = 'orders.mailer.retry',
        dead = 'orders.mailer.dead',
        counts = { sent = 1, ack = 0, retry = 0, bury = 0, late = 0, failures = 0, skipped = 0, kicked = 0 },
        workers = 0,
        hooks = {},
    })
end

g.test_close_twice_is_a_pair = function()
    g.fake = helper.serve()

    local broker = helper.client(g.fake)
    local orders = broker:declare('orders')
    local consumer = orders:consume(function() end)

    t.assert_equals(orders:status().workers, 1)
    t.assert_equals(broker:close(), true)
    t.assert_equals(consumer.stopping, true)

    local ok, err = broker:close()

    t.assert_equals(ok, false)
    t.assert_equals(err.kind, failure.CLOSED)
    t.assert_equals(err.message, 'kafka: клиент уже закрыт')

    local sent, refused = orders:send({ order = 1 })

    t.assert_equals(sent, nil)
    t.assert_equals(refused.kind, failure.CLOSED)
    helper.until_true(function()
        return consumer.workers == 0
    end)
end

g.test_consume_blames_the_caller = function()
    g.fake = helper.serve()
    g.broker = helper.client(g.fake)

    local orders = g.broker:declare('orders')

    helper.assert_blamed({
        {
            function()
                orders:consume('не функция')
            end,
            'обработчик — функция или вызываемая таблица, а не строка',
        },
        {
            function()
                orders:consume(function() end, { atomic = true })
            end,
            'настройки получателя: «atomic» брокер не умеет — смещение фиксируется по сети, '
                .. 'а транзакция box уступки не переживает',
        },
        {
            function()
                orders:kick(0)
            end,
            'сколько вернуть — число больше 0, а не 0',
        },
        {
            function()
                orders:kick(1.5)
            end,
            'сколько вернуть — целое число, а не 1.5',
        },
    })
end
