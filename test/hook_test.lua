--- Проверки крюков: постановка, замена, снятие, порядок и сорвавшийся крюк.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local hook = helper.hook

local g = t.group('tnt.kafka.hook')

g.before_each(function()
    helper.journal.forget()
end)

g.after_each(function()
    helper.restore()
end)

--- Обращение для крюков.
local CALL = { kind = 'send', name = 'orders' }

g.test_without_hooks_proceed_runs_once = function()
    local runs = 0

    t.assert_equals({
        hook.around(CALL, function()
            runs = runs + 1

            return 'id', nil, 3
        end),
    }, { 'id', nil, 3 })
    t.assert_equals(runs, 1)
end

g.test_hooks_wrap_outside_in_by_order = function()
    local trail = {}

    hook.set('outer', function(call, proceed)
        trail[#trail + 1] = 'outer:' .. call.kind
        local first, second = proceed()

        trail[#trail + 1] = 'outer.'

        return first, second
    end)
    hook.set('inner', function(_, proceed)
        trail[#trail + 1] = 'inner'

        return proceed()
    end)

    local first, second = hook.around(CALL, function()
        trail[#trail + 1] = 'body'

        return 'id', 'err'
    end)

    t.assert_equals({ first, second }, { 'id', 'err' })
    t.assert_equals(trail, { 'outer:send', 'inner', 'body', 'outer.' })
    t.assert_equals(hook.names(), { 'outer', 'inner' })
end

g.test_replacing_keeps_place_and_nil_removes = function()
    hook.set('a', function(_, proceed)
        return proceed()
    end)
    hook.set('b', function(_, proceed)
        return proceed()
    end)
    hook.set('a', function(_, proceed)
        return proceed()
    end)

    t.assert_equals(hook.names(), { 'a', 'b' })

    hook.set('a', nil)
    t.assert_equals(hook.names(), { 'b' })

    hook.set('missing', nil)
    t.assert_equals(hook.names(), { 'b' })
end

g.test_what_a_hook_returns_is_ignored = function()
    hook.set('liar', function(_, proceed)
        proceed()

        return 'подмена'
    end)

    t.assert_equals(
        hook.around(CALL, function()
            return 'настоящее'
        end),
        'настоящее'
    )
end

g.test_throwing_hook_is_skipped_with_warning = function()
    hook.set('broken', function()
        error('крюк упал', 0)
    end)

    local runs = 0
    local result = hook.around(CALL, function()
        runs = runs + 1

        return 'id'
    end)

    t.assert_equals(result, 'id')
    t.assert_equals(runs, 1)

    local record = helper.journal.find('крюк очереди сорвался и пропущен')

    t.assert_str_contains(record.line, 'hook=broken')
    t.assert_str_contains(record.line, 'err="крюк упал"')
    t.assert_str_contains(record.line, 'destination=orders')
end

g.test_hook_without_proceed_is_skipped_and_proceed_runs = function()
    hook.set('silent', function() end)

    t.assert_equals(
        hook.around(CALL, function()
            return 'id'
        end),
        'id'
    )
    t.assert_str_contains(
        helper.journal.find('крюк очереди сорвался').line,
        'крюк не позвал proceed'
    )
end

g.test_hook_calling_proceed_twice_runs_it_once = function()
    local runs = 0

    hook.set('twice', function(_, proceed)
        proceed()

        return proceed()
    end)
    hook.around(CALL, function()
        runs = runs + 1
    end)

    t.assert_equals(runs, 1)
    t.assert_equals(helper.journal.logged('сорвался'), false)
end

g.test_throw_after_proceed_keeps_the_result = function()
    hook.set('late', function(_, proceed)
        proceed()
        error('после')
    end)

    t.assert_equals(
        hook.around(CALL, function()
            return 'id'
        end),
        'id'
    )
    t.assert_equals(helper.journal.logged('сорвался'), true)
end

g.test_wrong_hook_blames_the_caller = function()
    helper.assert_blamed({
        {
            function()
                hook.set('', function() end)
            end,
            'имя крюка — непустая строка, а не пустая',
        },
        {
            function()
                hook.set('x', 5)
            end,
            'крюк — функция или вызываемая таблица, а не число',
        },
    })
end
