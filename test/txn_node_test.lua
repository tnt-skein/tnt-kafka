--- Отправка в транзакции настоящего `box` — ошибка программиста.
---
--- В процессе проверок `box` не настроен, и ветка транзакции идёт там
--- двойником внешней зависимости. Здесь — настоящий `box.is_in_txn`:
--- отправка ждёт брокера, уступка порвала бы транзакцию, и пакет обязан
--- отказать исключением ещё до сети.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local g = t.group('tnt.kafka.txn')

g.before_all(function()
    g.server = helper.start_node()
end)

g.after_all(function()
    helper.stop_node(g.server)
end)

g.test_send_inside_a_transaction_is_a_programmer_error = function()
    local seen = g.server:exec(function()
        local kafka = require('tnt.kafka')
        local broker = kafka.new({ brokers = { '127.0.0.1:1' }, timeout = 0.1, retry = { attempts = 1 } })
        local orders = broker:declare('orders')

        box.begin()

        local inside, err = pcall(orders.send, orders, { order = 1 })

        box.rollback()

        local outside, _, refusal = pcall(orders.send, orders, { order = 1 })

        broker:close()

        return { inside = inside, err = tostring(err), outside = outside, kind = refusal and refusal.kind }
    end)

    t.assert_equals(seen.inside, false)
    t.assert_str_contains(seen.err, 'отправка в orders внутри транзакции box')
    t.assert_equals(seen.outside, true)
    t.assert_equals(seen.kind, 'unreachable')
end
