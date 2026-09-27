--- Проверки одной выданной записи на стоящих часах: срок копии повтора,
--- пауза раздела после отказа и вход заново только по отказу поколения.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local failure = helper.failure
local handle = helper.handle

--- Часы пакета: подменяются полями модуля на время проверки.
local clock = helper.clock

local g = t.group('tnt.kafka.handle')

local saved = {}

g.before_each(function()
    saved.realtime, saved.monotonic, saved.commit = clock.realtime, clock.monotonic, handle.commit

    clock.realtime = function()
        return 1000
    end
    clock.monotonic = function()
        return 50
    end

    helper.journal.forget()
end)

g.after_each(function()
    clock.realtime, clock.monotonic, handle.commit = saved.realtime, saved.monotonic, saved.commit
end)

--- Получатель без брокера: счётчики и настройки.
---@return table
local function consumer_of()
    return {
        topic = { name = 'orders', counts = { late = 0, failures = 0, ack = 0, retry = 0, bury = 0, skipped = 0 } },
        policy = { max_attempts = 5, backoff = { base = 1, max = 60 } },
        handler = function() end,
    }
end

--- Раздел темы повтора с одной копией в запасе.
---@param due string|nil Срок копии
---@return table
local function lane_of(due)
    ---@type table[]
    local headers = { { 'x-tnt-id', 'I' } }

    if due ~= nil then
        table.insert(headers, { 'x-tnt-due', due })
    end

    return {
        topic = 'orders.retry',
        partition = 0,
        position = 1,
        marked = { offset = -1, deliveries = 0 },
        buffer = { { offset = 0, timestamp = 0, value = '{}', headers = headers } },
    }
end

g.test_copy_before_its_due_pauses_the_lane_until_then = function()
    local lane = lane_of('1002.000000')
    local commits = 0

    handle.commit = function()
        commits = commits + 1
    end

    handle.process(consumer_of(), {}, lane)

    t.assert_equals(lane.resume, 52)
    t.assert_equals(commits, 0)
    t.assert_equals(#lane.buffer, 1)
end

g.test_copy_at_or_past_its_due_goes_to_the_handler = function()
    for _, due in ipairs({ '1000.000000', '999.500000' }) do
        local lane = lane_of(due)
        local consumer = consumer_of()
        local commits = 0

        handle.commit = function()
            commits = commits + 1

            return nil, failure.new(failure.UNREACHABLE, 'нет')
        end

        handle.process(consumer, {}, lane)

        t.assert_equals(commits, 1, due)
        t.assert_equals(consumer.topic.counts.failures, 1, due)
        t.assert_equals(lane.resume, 50 + handle.PAUSE, due)
        t.assert_equals(lane.position, 0, due)
    end
end

g.test_lost_rejoins_only_when_out_of_the_generation = function()
    local cases = {
        { failure.new(failure.UNREACHABLE, 'связь'), nil },
        { failure.new(failure.REJECTED, 'длинно', { server_code = 10 }), nil },
        { failure.new(failure.CONFLICT, 'поколение', { server_code = 22 }), true },
    }

    for _, case in ipairs(cases) do
        local station = {}
        local lane = lane_of(nil)

        handle.lost(consumer_of(), station, lane, case[1], 'late', 'итог не дошёл', {})

        t.assert_equals(station.rejoin, case[2], case[1].message)
        t.assert_equals(lane.buffer, {})
    end

    t.assert_equals(handle.PAUSE, 0.1)
end
