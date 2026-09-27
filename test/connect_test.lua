--- Проверки входа: сверка версий и учётная запись SASL PLAIN.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local connect = helper.connect
local failure = helper.failure
local link = helper.link

local g = t.group('tnt.kafka.connect')

g.after_each(function()
    helper.release(g)
end)

--- Открывает соединение к двойнику.
---@param fake table
---@param opts table|nil Настройки клиента поверх
---@return any link
---@return any err
local function open(fake, opts)
    local settings = helper.checked(helper.settings_of(fake, opts))

    return connect.open(settings, { host = '127.0.0.1', port = fake.port }, 1)
end

--- Перечень версий, где у названного запроса свои границы.
---@param name string
---@param low integer
---@param high integer
---@return table[]
local function versions_with(name, low, high)
    local keys = {}

    for key, request in pairs(helper.CATALOG) do
        local range = request.name == name and { low, high } or { 0, 20 }

        keys[#keys + 1] = { api_key = key, min_version = range[1], max_version = range[2] }
    end

    return keys
end

g.test_open_without_login_asks_versions_only = function()
    g.fake = helper.serve()

    local opened = open(g.fake)

    t.assert_not_equals(opened, nil)
    t.assert_equals(g.fake.calls, { 'ApiVersions' })
    link.close(opened)
end

g.test_open_with_login = function()
    g.fake = helper.serve({ sasl = { username = 'app', password = 'secret' } })

    local opened = open(g.fake, { sasl = { username = 'app', password = 'secret' } })

    t.assert_not_equals(opened, nil)
    t.assert_equals(g.fake.calls, { 'ApiVersions', 'SaslHandshake', 'SaslAuthenticate' })
    t.assert_equals(g.fake.requests.SaslHandshake[1].mechanism, 'PLAIN')
    t.assert_equals(g.fake.requests.SaslAuthenticate[1].auth_bytes, '\0app\0secret')
    link.close(opened)
end

g.test_wrong_password_is_denied = function()
    g.fake = helper.serve({ sasl = { username = 'app', password = 'secret' } })

    local opened, err = open(g.fake, { sasl = { username = 'app', password = 'wrong' } })

    t.assert_equals(opened, nil)
    t.assert_equals(err.kind, failure.DENIED)
    t.assert_equals(err.retriable, false)
    t.assert_equals(err.server_code, 58)
    t.assert_equals(
        err.message,
        ('kafka 127.0.0.1:%d: вход не удался: Authentication failed'):format(g.fake.port)
    )
end

g.test_refusal_without_text_names_the_code = function()
    g.fake = helper.serve({
        sasl = { username = 'app', password = 'secret' },
        respond = function(name)
            if name == 'SaslAuthenticate' then
                return { error_code = 34, session_lifetime_ms = 0 }
            end
        end,
    })

    local _, err = open(g.fake, { sasl = { username = 'app', password = 'secret' } })

    t.assert_equals(err.kind, failure.DENIED)
    t.assert_str_contains(err.message, 'вход не удался: ILLEGAL_SASL_STATE (34)')
end

g.test_broker_without_plain_is_denied = function()
    g.fake = helper.serve({
        respond = function(name)
            if name == 'SaslHandshake' then
                return { error_code = 33, mechanisms = { 'SCRAM-SHA-512', 'GSSAPI' } }
            end
        end,
    })

    local _, err = open(g.fake, { sasl = { username = 'app', password = 'secret' } })

    t.assert_equals(err.kind, failure.DENIED)
    t.assert_equals(err.server_code, 33)
    t.assert_str_contains(err.message, 'брокер не умеет PLAIN, а умеет SCRAM-SHA-512, GSSAPI')

    g.fake.stop()
    g.fake = helper.serve({
        respond = function(name)
            if name == 'SaslHandshake' then
                return { error_code = 33 }
            end
        end,
    })

    _, err = open(g.fake, { sasl = { username = 'app', password = 'secret' } })
    t.assert_str_contains(err.message, 'брокер не умеет PLAIN, а умеет ')
end

g.test_cut_during_login_is_unreachable = function()
    for _, step in ipairs({ 'ApiVersions', 'SaslHandshake', 'SaslAuthenticate' }) do
        local fake = helper.serve({
            respond = function(name)
                if name == step then
                    return 'close'
                end
            end,
        })
        local _, err = open(fake, { sasl = { username = 'app', password = 'secret' } })

        t.assert_equals(err.kind, failure.UNREACHABLE)
        t.assert_str_contains(
            err.message,
            ('kafka 127.0.0.1:%d: вход не завершился: связь оборвалась'):format(
                fake.port
            )
        )
        fake.stop()
    end
end

g.test_versions_refusal_is_unreachable = function()
    g.fake = helper.serve({
        respond = function(name)
            if name == 'ApiVersions' then
                return { error_code = 35 }
            end
        end,
    })

    local _, err = open(g.fake)

    t.assert_equals(err.kind, failure.UNREACHABLE)
    t.assert_str_contains(
        err.message,
        'вход не завершился: брокер отказал в перечне версий: UNSUPPORTED_VERSION (35)'
    )
end

g.test_unknown_request_is_rejected = function()
    g.fake = helper.serve({ versions = {} })

    local _, err = open(g.fake)

    t.assert_equals(err.kind, failure.REJECTED)
    t.assert_equals(err.sent, false)
    t.assert_equals(err.retriable, false)
    t.assert_equals(
        err.message,
        ('kafka 127.0.0.1:%d: брокер не знает запроса Metadata'):format(g.fake.port)
    )
end

g.test_versions_outside_range_are_rejected = function()
    g.fake = helper.serve({ versions = versions_with('Produce', 0, 7) })

    local _, err = open(g.fake)

    t.assert_str_contains(
        err.message,
        'брокер принимает Produce версий 0–7, а пакет говорит версией 8'
    )

    g.fake.stop()
    g.fake = helper.serve({ versions = versions_with('Fetch', 5, 17) })
    _, err = open(g.fake)
    t.assert_str_contains(
        err.message,
        'брокер принимает Fetch версий 5–17, а пакет говорит версией 4'
    )

    g.fake.stop()
    g.fake = helper.serve({ versions = versions_with('Fetch', 4, 4) })
    t.assert_not_equals(open(g.fake), nil)
end

g.test_login_versions_are_checked_only_with_login = function()
    g.fake = helper.serve({ versions = versions_with('SaslAuthenticate', 2, 2) })

    local opened = open(g.fake)

    t.assert_not_equals(opened, nil)
    link.close(opened)

    local _, err = open(g.fake, { sasl = { username = 'app', password = 'secret' } })

    t.assert_str_contains(
        err.message,
        'брокер принимает SaslAuthenticate версий 2–2, а пакет говорит версией 1'
    )
end

g.test_cut_versions_without_login_is_unreachable = function()
    g.fake = helper.serve({
        respond = function(name)
            if name == 'ApiVersions' then
                return 'close'
            end
        end,
    })

    local opened, err = open(g.fake)

    t.assert_equals(opened, nil)
    t.assert_equals(err.kind, failure.UNREACHABLE)
end

g.test_login_adds_its_versions_to_the_check = function()
    g.fake = helper.serve({ versions = versions_with('OffsetFetch', 0, 0) })

    local _, err = open(g.fake, { sasl = { username = 'app', password = 'secret' } })

    t.assert_str_contains(
        err.message,
        'брокер принимает OffsetFetch версий 0–0, а пакет говорит версией 3'
    )

    g.fake.stop()
    g.fake = helper.serve({ versions = versions_with('OffsetCommit', 0, 0) })
    _, err = open(g.fake, { sasl = { username = 'app', password = 'secret' } })
    t.assert_str_contains(
        err.message,
        'брокер принимает OffsetCommit версий 0–0, а пакет говорит версией 5'
    )
end

g.test_dial_failure_passes_through = function()
    local settings = helper.checked({ brokers = { '127.0.0.1:1' } })
    local port = helper.closed_port()
    local opened, err = connect.open(settings, { host = '127.0.0.1', port = port }, 0.5)

    t.assert_equals(opened, nil)
    t.assert_equals(err.kind, failure.UNREACHABLE)
    t.assert_str_contains(err.message, 'соединение не открылось')
end
