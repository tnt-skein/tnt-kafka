--- Проверки настроек клиента: умолчания, адреса узлов, вход, шифрование
--- и отказы с виной на строке вызывающего.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local settings = helper.settings

local g = t.group('tnt.kafka.settings')

g.test_defaults = function()
    local checked = helper.checked(nil)

    t.assert_equals(checked.name, 'kafka')
    t.assert_equals(checked.brokers, { { host = '127.0.0.1', port = 9092 } })
    t.assert_equals(checked.tls, nil)
    t.assert_equals(checked.sasl, nil)
    t.assert_equals(checked.max_bytes, 1024 * 1024)
    t.assert_equals(checked.session_timeout, 10)
    t.assert_equals(checked.rebalance_timeout, 60)
    t.assert_equals(checked.client_id, 'tnt-kafka')
    t.assert_equals(checked.limits, { timeout = 5, max_timeout = 60 })
    t.assert_equals(checked.pool, {})
    t.assert_equals(checked.retry, {})
    t.assert_equals(settings.DEFAULT_BROKER, '127.0.0.1:9092')
end

g.test_given_values_win = function()
    local checked = helper.checked({
        brokers = { 'kafka-1:9092', '[::1]:9093', '10.0.0.7:1' },
        tls = { verify = false, ca_file = '/ca.pem', sni = 'kafka' },
        sasl = { username = 'app', password = 'secret' },
        timeout = 2,
        max_timeout = 30,
        max_bytes = 2048,
        session_timeout = 6,
        rebalance_timeout = 20,
        client_id = 'panel',
        pool = { size = 3 },
        retry = { attempts = 2 },
        name = 'orders',
    })

    t.assert_equals(checked.brokers, {
        { host = 'kafka-1', port = 9092 },
        { host = '::1', port = 9093 },
        { host = '10.0.0.7', port = 1 },
    })
    t.assert_equals(checked.tls, { verify = false, ca_file = '/ca.pem', sni = 'kafka' })
    t.assert_equals(checked.sasl, { mechanism = 'PLAIN', username = 'app', password = 'secret' })
    t.assert_equals(checked.limits, { timeout = 2, max_timeout = 30 })
    t.assert_equals(checked.max_bytes, 2048)
    t.assert_equals(checked.session_timeout, 6)
    t.assert_equals(checked.rebalance_timeout, 20)
    t.assert_equals(checked.client_id, 'panel')
    t.assert_equals(checked.pool, { size = 3 })
    t.assert_equals(checked.retry, { attempts = 2 })
    t.assert_equals(checked.name, 'orders')
end

g.test_tls_true_is_defaults_and_false_is_off = function()
    t.assert_equals(helper.checked({ tls = true }).tls, {})
    t.assert_equals(helper.checked({ tls = false }).tls, nil)
    t.assert_equals(
        helper.checked({ sasl = { mechanism = 'PLAIN', username = 'u', password = '' } }).sasl.mechanism,
        'PLAIN'
    )
end

g.test_port_borders = function()
    t.assert_equals(helper.checked({ brokers = { 'h:65535' } }).brokers[1].port, 65535)
end

g.test_wrong_settings_blame_the_caller = function()
    helper.assert_blamed({
        {
            function()
                helper.kafka.new({ pasword = 'x' })
            end,
            'настройки kafka: ключа «pasword» нет, есть brokers, client_id, max_bytes, max_timeout, name, '
                .. 'pool, rebalance_timeout, retry, sasl, session_timeout, timeout, tls',
        },
        {
            function()
                helper.kafka.new({ brokers = {} })
            end,
            'настройки kafka.brokers: число узлов — число больше 0, а не 0',
        },
        {
            function()
                helper.kafka.new({ brokers = { 'kafka' } })
            end,
            'настройки kafka.brokers[1] — строка по образцу ^%[?([^%[%]]-)%]?:(%d+)$, а не «kafka»',
        },
        {
            function()
                helper.kafka.new({ brokers = { ':9092' } })
            end,
            'настройки kafka.brokers[1]: узел — непустая строка, а не пустая',
        },
        {
            function()
                helper.kafka.new({ brokers = { 'h:0' } })
            end,
            'настройки kafka.brokers[1]: порт — число от 1 до 65535, а не 0',
        },
        {
            function()
                helper.kafka.new({ brokers = { 'h:65536' } })
            end,
            'настройки kafka.brokers[1]: порт — число от 1 до 65535, а не 65536',
        },
        {
            function()
                helper.kafka.new({ brokers = 'h:1' })
            end,
            'настройки kafka.brokers — таблица, а не строка',
        },
        {
            function()
                helper.kafka.new({ sasl = { mechanism = 'SCRAM-SHA-256', username = 'u', password = 'p' } })
            end,
            'настройки kafka.sasl.mechanism — одно из «PLAIN», а не «SCRAM-SHA-256»',
        },
        {
            function()
                helper.kafka.new({ tls = { verfy = false } })
            end,
            'настройки kafka.tls: ключа «verfy» нет, есть ca_file, ca_path, sni, verify',
        },
        {
            function()
                helper.kafka.new({ max_bytes = 0 })
            end,
            'настройки kafka.max_bytes — число больше 0, а не 0',
        },
        {
            function()
                helper.kafka.new({ session_timeout = 0 })
            end,
            'настройки kafka.session_timeout — число больше 0, а не 0',
        },
        {
            function()
                helper.kafka.new({ rebalance_timeout = -1 })
            end,
            'настройки kafka.rebalance_timeout — число больше 0, а не -1',
        },
    })
end

g.test_bad_retry_settings_blame_the_caller = function()
    helper.assert_blamed({
        {
            function()
                helper.kafka.new({ retry = { attempts = 0 } })
            end,
            'настройки kafka.retry: настройка attempts — целое число от 1, а пришло: 0',
        },
    })
end
