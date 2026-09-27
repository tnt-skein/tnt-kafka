--- Проверки связи с узлом: сокет, TLS, кадр запроса и ответ, беды обмена
--- и живость свободного соединения.

local fiber = require('fiber')
local t = require('luatest')

local helper = dofile('test/helper.lua')

local api = helper.api
local failure = helper.failure
local link = helper.link
local within = helper.within
local wire = helper.wire

local g = t.group('tnt.kafka.link')

g.after_each(function()
    helper.release(g)
end)

--- Проверенные настройки клиента.
---@param opts table|nil
---@return table
local function settings_of(opts)
    return helper.checked(opts)
end

--- Подставной сокет: читает из заготовки, пишет в память.
---@param opts table|nil `answer` — что читать; `read`, `write` — свои
---@return table
local function socket_of(opts)
    local given = opts or {}
    local inbox = given.answer or ''

    return {
        written = {},
        read = given.read or function(_, request)
            local piece = inbox:sub(1, request.chunk)

            inbox = inbox:sub(request.chunk + 1)

            return piece
        end,
        write = given.write or function(self, data)
            self.written[#self.written + 1] = data

            return #data
        end,
        error = function()
            return 'ошибка сокета'
        end,
        close = function(self)
            self.closed = true
        end,
    }
end

--- Соединение поверх подставного сокета.
---@param socket table
---@param opts table|nil Поля соединения поверх
---@return table
local function linked(socket, opts)
    link._set_source({
        dial = function()
            return socket
        end,
        cipher = function(over)
            return over
        end,
    })

    local opened = link.dial(settings_of(opts), 'h', 1, within.deadline(1))

    return opened
end

--- Ответ брокера байтами: рамка, номер обращения и поля.
---@param request table
---@param correlation integer
---@param values table
---@return string
local function reply(request, correlation, values)
    local body = wire.int32(correlation) .. helper.schema.encode(request.response, values)

    return wire.int32(#body) .. body
end

g.test_exchange_with_a_real_node = function()
    g.fake = helper.serve()

    local settings = settings_of({ brokers = { ('127.0.0.1:%d'):format(g.fake.port) } })
    local opened = link.dial(settings, '127.0.0.1', g.fake.port, within.deadline(1))
    local answer = link.exchange(opened, api.API_VERSIONS, {}, within.deadline(1))

    t.assert_equals(answer.error_code, 0)
    t.assert_equals(opened.correlation, 1)
    t.assert_equals(opened.where, ('127.0.0.1:%d'):format(g.fake.port))
    t.assert_equals(opened.client, 'tnt-kafka')
    t.assert_equals(opened.secured, false)
    t.assert_equals(opened.frame_max, 1024 * 1024 + link.SLACK)
    t.assert_equals(link.alive(opened), true)

    link.close(opened)
    link.close(opened)
end

g.test_dead_idle_connection_is_not_alive = function()
    g.fake = helper.serve({
        respond = function()
            return 'close'
        end,
    })

    local opened = link.dial(settings_of(), '127.0.0.1', g.fake.port, within.deadline(1))

    link.exchange(opened, api.API_VERSIONS, {}, within.deadline(1))
    helper.until_true(function()
        return not link.alive(opened)
    end)
    t.assert_equals(link.alive(opened), false)
    link.close(opened)
end

g.test_dial_to_nowhere_is_unreachable = function()
    local opened, err = link.dial(settings_of(), '127.0.0.1', helper.closed_port(), within.deadline(1))

    t.assert_equals(opened, nil)
    t.assert_equals(err.kind, failure.UNREACHABLE)
    t.assert_str_contains(err.message, 'kafka 127.0.0.1:')
    t.assert_str_contains(err.message, 'соединение не открылось: Connection refused')
    t.assert_equals(link.SLACK, 1048576)
end

g.test_dial_refusal_keeps_its_reason = function()
    link._set_source({
        dial = function()
            return nil, 'нет маршрута'
        end,
    })

    local _, err = link.dial(settings_of(), 'h', 1, within.deadline(1))

    t.assert_equals(err.message, 'kafka h:1: соединение не открылось: нет маршрута')
end

--- Часы, которые стоят на одном миге.
---@param now number
local function frozen(now)
    within._set_source({
        monotonic = function()
            return now
        end,
        scheduler_now = function()
            return now
        end,
    })
end

g.test_handshake_at_the_exact_deadline_does_not_start = function()
    local socket = socket_of()

    link._set_source({
        dial = function()
            return socket
        end,
        cipher = function(over)
            return over
        end,
    })
    frozen(100)

    local opened, err = link.dial(settings_of({ tls = true }), 'h', 1, 100)

    within._set_source(nil)
    t.assert_equals(opened, nil)
    t.assert_equals(err.message, 'kafka h:1: срок вышел до рукопожатия TLS')
end

g.test_cut_at_the_exact_deadline_is_timeout = function()
    local opened = linked(socket_of({
        write = function()
            return nil
        end,
    }))

    frozen(100)

    local _, trouble = link.exchange(opened, api.API_VERSIONS, {}, 100)

    within._set_source(nil)
    t.assert_equals(trouble.kind, failure.TIMEOUT)
end

g.test_bytes_in_an_idle_socket_mean_death = function()
    local raw = {
        sysread = function()
            return 'x'
        end,
        errno = function()
            return require('errno').EAGAIN
        end,
    }

    t.assert_equals(link.alive({ secured = false, socket = raw }), false)
end

g.test_answer_of_exactly_the_limit_is_read = function()
    local opened = linked(socket_of({ answer = wire.int32(1 + link.SLACK) }), { max_bytes = 1 })
    local _, trouble = link.exchange(opened, api.API_VERSIONS, {}, within.deadline(1))

    t.assert_equals(
        trouble.message,
        'связь оборвалась: поток кончился посреди ответа'
    )
end

g.test_throwing_dial_is_unreachable = function()
    link._set_source({
        dial = function()
            error('сети нет', 0)
        end,
    })

    local _, err = link.dial(settings_of(), 'h', 1, within.deadline(1))

    t.assert_equals(err.kind, failure.UNREACHABLE)
    t.assert_equals(err.message, 'kafka h:1: соединение не открылось: сети нет')
end

g.test_tls_wraps_the_socket = function()
    local socket = socket_of({ answer = reply(api.API_VERSIONS, 1, { error_code = 0, api_keys = {} }) })
    local seen ---@type any

    link._set_source({
        dial = function()
            return socket
        end,
        cipher = function(over, opts)
            seen = opts

            return {
                read = function(_, request, timeout)
                    return over:read(request, timeout)
                end,
                write = function(_, data)
                    over:write(data)

                    return true
                end,
                close = function()
                    over:close()
                end,
            }
        end,
    })

    local opened = link.dial(
        settings_of({ tls = { ca_file = '/ca.pem', verify = false, sni = 's', ca_path = '/p' } }),
        'h',
        1,
        within.deadline(1)
    )

    t.assert_equals(seen.host, 'h')
    t.assert_equals(seen.ca_file, '/ca.pem')
    t.assert_equals(seen.ca_path, '/p')
    t.assert_equals(seen.verify, false)
    t.assert_equals(seen.sni, 's')
    t.assert_almost_equals(seen.timeout, 1, 0.1)
    t.assert_equals(opened.secured, true)
    t.assert_equals(link.alive(opened), true)
    t.assert_equals(link.exchange(opened, api.API_VERSIONS, {}, within.deadline(1)).error_code, 0)

    link.close(opened)
    t.assert_equals(socket.closed, true)
end

g.test_real_tls_to_a_plain_node_fails_the_handshake = function()
    local server = require('socket').tcp_server('127.0.0.1', 0, function(peer)
        peer:close()
    end)
    local port = server:name().port
    local opened, err = link.dial(settings_of({ tls = true }), '127.0.0.1', port, within.deadline(1))

    server:close()

    t.assert_equals(opened, nil)
    t.assert_equals(err.kind, failure.UNREACHABLE)
    t.assert_str_contains(err.message, 'рукопожатие TLS не прошло')
    t.assert_not_str_contains(err.message, ': nil')
end

g.test_failed_handshake_is_unreachable_and_closes = function()
    local socket = socket_of()

    link._set_source({
        dial = function()
            return socket
        end,
        cipher = function()
            return nil, 'сертификат не принят'
        end,
    })

    local opened, err = link.dial(settings_of({ tls = true }), 'h', 1, within.deadline(1))

    t.assert_equals(opened, nil)
    t.assert_equals(err.kind, failure.UNREACHABLE)
    t.assert_equals(
        err.message,
        'kafka h:1: рукопожатие TLS не прошло: сертификат не принят'
    )
    t.assert_equals(socket.closed, true)
end

g.test_no_time_left_for_handshake = function()
    local socket = socket_of()

    link._set_source({
        dial = function()
            return socket
        end,
    })

    local _, err = link.dial(settings_of({ tls = true }), 'h', 1, within.deadline(-1))

    t.assert_equals(err.message, 'kafka h:1: срок вышел до рукопожатия TLS')
end

g.test_write_failure_is_broken_and_not_sent = function()
    local opened = linked(socket_of({
        write = function()
            return nil
        end,
    }))
    local answer, trouble = link.exchange(opened, api.API_VERSIONS, {}, within.deadline(1))

    t.assert_equals(answer, nil)
    t.assert_equals(trouble.kind, failure.BROKEN)
    t.assert_equals(trouble.sent, false)
    t.assert_equals(trouble.message, 'связь оборвалась: ошибка сокета')
end

g.test_throwing_write_is_broken = function()
    local opened = linked(socket_of({
        write = function()
            error('сокет закрыт', 0)
        end,
    }))
    local _, trouble = link.exchange(opened, api.API_VERSIONS, {}, within.deadline(1))

    t.assert_equals(trouble.message, 'связь оборвалась: сокет закрыт')
end

g.test_write_after_deadline_is_timeout = function()
    local opened = linked(socket_of({
        write = function()
            return nil
        end,
    }))
    local _, trouble = link.exchange(opened, api.API_VERSIONS, {}, within.deadline(-1))

    t.assert_equals(trouble.kind, failure.TIMEOUT)
    t.assert_equals(
        trouble.message,
        'брокер не ответил за срок вызова: ошибка сокета'
    )
end

g.test_short_answers_are_broken_and_sent = function()
    local cases = {
        {
            answer = '\0\0',
            text = 'связь оборвалась: поток кончился посреди ответа',
        },
        {
            answer = wire.int32(10) .. '\0\0\0\1',
            text = 'связь оборвалась: поток кончился посреди ответа',
        },
    }

    for _, case in ipairs(cases) do
        local opened = linked(socket_of({ answer = case.answer }))
        local _, trouble = link.exchange(opened, api.API_VERSIONS, {}, within.deadline(1))

        t.assert_equals(trouble.kind, failure.BROKEN)
        t.assert_equals(trouble.sent, true)
        t.assert_equals(trouble.message, case.text)
    end
end

g.test_read_failures_are_named = function()
    local opened = linked(socket_of({
        read = function()
            return nil, 'сброс соединения'
        end,
    }))
    local _, trouble = link.exchange(opened, api.API_VERSIONS, {}, within.deadline(1))

    t.assert_equals(trouble.message, 'связь оборвалась: сброс соединения')

    opened = linked(socket_of({
        read = function()
            return nil
        end,
    }))
    _, trouble = link.exchange(opened, api.API_VERSIONS, {}, within.deadline(1))
    t.assert_equals(trouble.message, 'связь оборвалась: ошибка сокета')

    opened = linked(socket_of({
        read = function()
            error('закрыт соседом', 0)
        end,
    }))
    _, trouble = link.exchange(opened, api.API_VERSIONS, {}, within.deadline(1))
    t.assert_equals(trouble.message, 'связь оборвалась: закрыт соседом')
end

g.test_silence_until_deadline_is_timeout = function()
    g.fake = helper.serve({
        respond = function()
            return 'skip'
        end,
    })

    local opened = link.dial(settings_of(), '127.0.0.1', g.fake.port, within.deadline(1))
    local started = fiber.clock()
    local _, trouble = link.exchange(opened, api.API_VERSIONS, {}, within.deadline(0.1))

    t.assert_equals(trouble.kind, failure.TIMEOUT)
    t.assert_equals(trouble.sent, true)
    t.assert_str_contains(trouble.message, 'брокер не ответил за срок вызова')
    t.assert_almost_equals(fiber.clock() - started, 0.1, 0.08)
    link.close(opened)
end

g.test_answer_size_outside_limits_is_broken = function()
    for _, size in ipairs({ 3, 1024 * 1024 + link.SLACK + 1 }) do
        local opened = linked(socket_of({ answer = wire.int32(size) }))
        local _, trouble = link.exchange(opened, api.API_VERSIONS, {}, within.deadline(1))

        t.assert_equals(trouble.kind, failure.BROKEN)
        t.assert_equals(
            trouble.message,
            ('ответ ApiVersions длиной %d байт вне предела %d'):format(
                size,
                1024 * 1024 + link.SLACK
            )
        )
    end

    local edge = linked(socket_of({ answer = wire.int32(4) .. wire.int32(1) }), { max_bytes = 1 })
    local _, trouble = link.exchange(
        edge,
        helper.coordination.HEARTBEAT,
        { group_id = 'g', generation_id = 1, member_id = 'm' },
        within.deadline(1)
    )

    t.assert_str_contains(trouble.message, 'ответ Heartbeat не разобран')
end

g.test_unparsed_answer_is_broken = function()
    local opened = linked(socket_of({ answer = wire.int32(6) .. wire.int32(1) .. '\0\0' }))
    local _, trouble = link.exchange(opened, api.API_VERSIONS, {}, within.deadline(1))

    t.assert_equals(trouble.kind, failure.BROKEN)
    t.assert_equals(
        trouble.message,
        'ответ ApiVersions не разобран: ответ короче, чем обещают поля: нужно 4 байт с 7, а их 6'
    )
end

g.test_foreign_correlation_is_broken = function()
    local opened = linked(socket_of({ answer = reply(api.API_VERSIONS, 9, { error_code = 0, api_keys = {} }) }))
    local _, trouble = link.exchange(opened, api.API_VERSIONS, {}, within.deadline(1))

    t.assert_equals(trouble.kind, failure.BROKEN)
    t.assert_equals(trouble.message, 'ответ на обращение 9 пришёл вместо 1')
end

g.test_trouble_keeps_given_fields = function()
    t.assert_equals(
        link.trouble('broken', 'текст', { sent = true, retriable = false, code = 7 }),
        { kind = 'broken', message = 'текст', sent = true, retriable = false, code = 7 }
    )
    t.assert_equals(link.trouble('timeout', 'т'), { kind = 'timeout', message = 'т' })
end
