--- Проверки конверта: настройки отправки, тело, запись Kafka из конверта
--- и обратно, копии для темы повтора и мёртвых.

local json = require('json')
local t = require('luatest')

local helper = dofile('test/helper.lua')

local context = helper.context
local message = helper.message

local g = t.group('tnt.kafka.message')

--- Настройки отправки, проверенные с виной на строке вызова.
---@param opts any
---@return table
local function options(opts)
    local checked = message.options(opts, 2)

    return checked
end

--- Проверка тела с виной на строке вызова.
---@param value any
local function body(value)
    message.body(value, 2)
end

g.test_own_names_and_limits = function()
    t.assert_equals(message.MAX_SECONDS, 100 * 365 * 86400)
    t.assert_equals(
        { message.ID, message.ATTEMPT, message.CREATED, message.REASON, message.DUE },
        { 'x-tnt-id', 'x-tnt-attempt', 'x-tnt-created', 'x-tnt-reason', 'x-tnt-due' }
    )
    t.assert_equals(
        { message.CONTENT_TYPE, message.JSON, message.MAX_DEPTH },
        { 'content-type', 'application/json', 100 }
    )
end

g.test_features_say_only_key = function()
    t.assert_equals(message.FEATURES, {
        delay = false,
        ttl = false,
        ttr = false,
        touch = false,
        priority = false,
        key = true,
        transactional = false,
        atomic = false,
    })
end

g.test_known_options_pass = function()
    local given = { key = 'k', id = '01ARZ3NDEKTSV4RRFFQ69G5FAV', timeout = 1, headers = { ['x-a'] = 'b' } }

    t.assert_is(options(given), given)
    t.assert_equals(options(nil), {})
end

g.test_unsupported_options_explain_themselves = function()
    helper.assert_blamed({
        {
            function()
                options({ delay = 1 })
            end,
            'настройки отправки: «delay» — отсрочки у Kafka нет: запись выдаётся, как только легла',
        },
        {
            function()
                options({ ttl = 1 })
            end,
            'настройки отправки: «ttl» — срок жизни у Kafka один на тему — её хранение, а не у сообщения',
        },
        {
            function()
                options({ priority = 0 })
            end,
            'настройки отправки: «priority» — приоритетов у Kafka нет: раздел выдаётся по порядку смещений',
        },
        {
            function()
                options({ keys = 'k' })
            end,
            'настройки отправки: ключа «keys» нет, есть delay, headers, id, key, priority, timeout, ttl',
        },
        {
            function()
                options({ timeout = 0 })
            end,
            'настройки отправки.timeout — число больше 0, а не 0',
        },
        {
            function()
                options({ headers = { ['X-Big'] = 'b' } })
            end,
            'имя заголовка — строка по образцу ^[a-z0-9][a-z0-9%-]*$, а не «X-Big»',
        },
        {
            function()
                options({ headers = { ['x-n'] = 1 } })
            end,
            'заголовок x-n — строка, а не число',
        },
    })
end

g.test_simple_bodies_pass = function()
    body(nil)
    body(true)
    body('строка')
    body(1.5)
    body({ a = { b = { 1, 2, 'три' } }, [5] = false })

    local shared = { 1 }

    body({ left = shared, right = shared })
end

g.test_wrong_bodies_blame_the_caller = function()
    local ring = {}

    ring.self = ring

    local deep = {}
    local cursor = deep

    for _ = 1, message.MAX_DEPTH do
        cursor.next = {}
        cursor = cursor.next
    end

    helper.assert_blamed({
        {
            function()
                body({ n = 0 / 0 })
            end,
            'тело.n — конечное число, а не nan',
        },
        {
            function()
                body(math.huge)
            end,
            'тело — конечное число, а не inf',
        },
        {
            function()
                body({ -math.huge })
            end,
            'тело.1 — конечное число, а не -inf',
        },
        {
            function()
                body({ f = print })
            end,
            'тело.f — простые данные, а не function',
        },
        {
            function()
                body(require('uuid').new())
            end,
            'тело — простые данные, а не cdata',
        },
        {
            function()
                body(ring)
            end,
            'тело.self — кольцо: таблица лежит сама в себе',
        },
        {
            function()
                body({ [true] = 1 })
            end,
            'тело: ключ — строка или число, а не boolean',
        },
        {
            function()
                body(deep)
            end,
            'тело' .. string.rep('.next', message.MAX_DEPTH) .. ' — вложенность глубже 100',
        },
    })
end

g.test_body_at_the_depth_limit_passes = function()
    local deep = {}
    local cursor = deep

    for _ = 1, message.MAX_DEPTH - 1 do
        cursor.next = {}
        cursor = cursor.next
    end

    body(deep)
end

g.test_envelope_takes_context_and_own_headers = function()
    local envelope = context.run({ request_id = 'r-1' }, function()
        return message.envelope(
            'orders',
            { a = 1 },
            { key = 'k', headers = { ['x-request-id'] = 'mine', ['x-b'] = 'c' } }
        )
    end)

    t.assert_equals(envelope.name, 'orders')
    t.assert_equals(envelope.body, { a = 1 })
    t.assert_equals(envelope.key, 'k')
    t.assert_equals(envelope.attempt, 0)
    t.assert_equals(envelope.headers['x-request-id'], 'mine')
    t.assert_equals(envelope.headers['x-b'], 'c')
    t.assert_equals(#envelope.id, 26)
    t.assert_almost_equals(envelope.created, require('clock').realtime(), 1)
    t.assert_equals(message.envelope('orders', 1, { id = 'given' }).id, 'given')
end

g.test_record_carries_json_and_sorted_headers = function()
    local record = message.record({
        id = 'I',
        name = 'orders',
        body = { a = 1 },
        headers = { ['x-b'] = '2', ['a-first'] = '1' },
        key = 'k',
        attempt = 0,
        created = 1790000000.25,
    })

    t.assert_equals(record, {
        key = 'k',
        value = '{"a":1}',
        headers = {
            { 'a-first', '1' },
            { 'content-type', 'application/json' },
            { 'x-b', '2' },
            { 'x-tnt-attempt', '0' },
            { 'x-tnt-created', '1790000000.250000' },
            { 'x-tnt-id', 'I' },
        },
        timestamp = 1790000000250,
    })
end

g.test_forward_keeps_value_and_adds_marks = function()
    local envelope = {
        id = 'I',
        name = 'orders',
        headers = { ['x-a'] = '1' },
        key = 'k',
        attempt = 3,
        created = 10,
    }
    local copy = message.forward('raw', envelope, { [message.REASON] = 'плохо' })

    t.assert_equals(copy.key, 'k')
    t.assert_equals(copy.value, 'raw')
    t.assert_equals(copy.headers, {
        { 'content-type', 'application/json' },
        { 'x-a', '1' },
        { 'x-tnt-attempt', '3' },
        { 'x-tnt-created', '10.000000' },
        { 'x-tnt-id', 'I' },
        { 'x-tnt-reason', 'плохо' },
    })
    t.assert_almost_equals(copy.timestamp / 1000, require('clock').realtime(), 1)
end

g.test_delivered_reads_back_the_envelope = function()
    local record = message.record({
        id = 'I',
        name = 'orders',
        body = { a = 1 },
        headers = { ['x-a'] = '1' },
        key = 'k',
        attempt = 2,
        created = 1790000000.5,
    })

    record.offset, record.timestamp = 7, 1
    record.headers[#record.headers + 1] = { 'x-tnt-due', '5.0' }
    record.headers[#record.headers + 1] = { 'x-empty', nil }

    local envelope, own = message.delivered('orders', record, 1)

    t.assert_equals(envelope, {
        id = 'I',
        name = 'orders',
        headers = { ['x-a'] = '1' },
        key = 'k',
        attempt = 4,
        created = 1790000000.5,
        body = { a = 1 },
    })
    t.assert_equals(own['x-tnt-due'], '5.0')
    t.assert_equals(own['content-type'], 'application/json')
end

g.test_delivered_without_redeliveries_is_the_first = function()
    local envelope = message.delivered('orders', { value = '1', headers = {}, timestamp = 0 })

    t.assert_equals(envelope.attempt, 1)
end

g.test_foreign_record_gets_own_id_and_first_attempt = function()
    local envelope = message.delivered('orders', { value = 'не json', headers = {}, timestamp = 1500 }, 0)

    t.assert_equals(envelope.foreign, true)
    t.assert_equals(envelope.attempt, 1)
    t.assert_equals(envelope.created, 1.5)
    t.assert_equals(#envelope.id, 26)
    t.assert_equals(message.delivered('orders', { headers = {}, timestamp = 0 }, 0).foreign, true)

    local plain = message.delivered('orders', { value = '{"x":1}', headers = {}, timestamp = 0 }, 0)

    t.assert_equals(plain.foreign, nil)
    t.assert_equals(plain.body, { x = 1 })
    t.assert_equals(message.delivered('orders', { value = 'null', headers = {}, timestamp = 0 }, 0).body, nil)
    t.assert_equals(json.decode('null'), box.NULL)
end

g.test_shaped_lists_errors_by_place = function()
    t.assert_equals(message.shaped({ b = 'нет', a = 'мало' }), 'a: мало; b: нет')
end
