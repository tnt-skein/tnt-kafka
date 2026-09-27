--- Проверки возврата зарытых: из темы мёртвых в тему повтора, по одному,
--- со смещением своей группы возврата.

local clock = require('clock')
local t = require('luatest')

local helper = dofile('test/helper.lua')

local failure = helper.failure
local message = helper.message

local g = t.group('tnt.kafka.kick')

g.after_each(function()
    helper.release(g)
end)

--- Тема со своими темами повтора и мёртвых в двойнике.
local TOPICS = { orders = 1, ['orders.retry'] = 1, ['orders.dead'] = 2 }

--- Кладёт зарытое в раздел темы мёртвых.
---@param fake table
---@param partition integer
---@param id string
---@param reason string|nil
local function bury(fake, partition, id, reason)
    local record = message.forward('{"n":1}', {
        id = id,
        name = 'orders',
        headers = { ['x-request-id'] = 'r-' .. id },
        key = 'k',
        attempt = 5,
        created = clock.realtime(),
    }, { [message.REASON] = reason or 'плохо' })

    helper.append(fake, 'orders.dead', partition, helper.batch(record))
end

--- Копии, ушедшие в тему повтора.
---@param fake table
---@return table[]
local function revived(fake)
    local found = {}

    for _, entry in ipairs(fake.produced) do
        if entry.topic == 'orders.retry' then
            local envelope, own = message.delivered('orders', entry.record, 0)

            found[#found + 1] = { id = envelope.id, attempt = own['x-tnt-attempt'], reason = own['x-tnt-reason'] }
        end
    end

    return found
end

g.test_kick_moves_buried_back_with_zero_attempts = function()
    local broker, fake = helper.talking(g, { topics = TOPICS })

    bury(fake, 0, 'A')
    bury(fake, 0, 'B')
    bury(fake, 1, 'C')

    local orders = broker:declare('orders')

    t.assert_equals(orders:kick(2), 2)
    t.assert_equals(revived(fake), {
        { id = 'A', attempt = '0' },
        { id = 'B', attempt = '0' },
    })
    t.assert_equals(orders.counts.kicked, 2)

    t.assert_equals(orders:kick(5), 1)
    t.assert_equals((revived(fake)[3] --[[@as table]]).id, 'C')
    t.assert_equals(orders:kick(), 0)
    t.assert_equals(orders.counts.kicked, 3)

    local commits = {}

    for _, entry in ipairs(fake.commits) do
        commits[#commits + 1] = ('%s %s/%d %d'):format(entry.group, entry.topic, entry.partition, entry.offset)
    end

    t.assert_equals(commits, {
        'orders.dead orders.dead/0 1',
        'orders.dead orders.dead/0 2',
        'orders.dead orders.dead/1 1',
    })
end

g.test_kick_by_default_returns_one = function()
    local broker, fake = helper.talking(g, { topics = TOPICS })

    bury(fake, 0, 'A')
    bury(fake, 0, 'B')

    t.assert_equals(broker:declare('orders'):kick(), 1)
    t.assert_equals(#revived(fake), 1)
end

g.test_kick_continues_from_committed = function()
    local broker, fake = helper.talking(g, { topics = TOPICS })

    bury(fake, 0, 'A')
    bury(fake, 0, 'B')
    fake.groups['orders.dead'] = {
        members = {},
        generation = 0,
        state = 'empty',
        offsets = { ['orders.dead/0'] = { offset = 1, metadata = '' } },
        cond = require('fiber').cond(),
        joined = {},
    }

    t.assert_equals(broker:declare('orders'):kick(5), 1)
    t.assert_equals((revived(fake)[1] --[[@as table]]).id, 'B')
end

g.test_kick_failures_are_pairs = function()
    local refuse = 'Produce'
    local broker, fake = helper.talking(g, {
        topics = TOPICS,
        respond = function(name, values)
            if refuse == 'FetchClose' and name == 'Fetch' then
                return 'close'
            end

            if name ~= refuse then
                return nil
            end

            if name == 'Produce' then
                return {
                    responses = {
                        {
                            name = values.topic_data[1].name,
                            partition_responses = { { index = 0, error_code = 10, base_offset = -1 } },
                        },
                    },
                }
            end

            if name == 'OffsetCommit' then
                return {
                    topics = {
                        { name = 'orders.dead', partitions = { { partition_index = 0, error_code = 30 } } },
                    },
                }
            end

            if name == 'Fetch' and refuse == 'Fetch' then
                return {
                    responses = {
                        {
                            topic = 'orders.dead',
                            partitions = {
                                { partition_index = values.topics[1].partitions[1].partition, error_code = 29 },
                            },
                        },
                    },
                }
            end

            if name == 'FindCoordinator' then
                return { error_code = 30 }
            end

            if name == 'OffsetFetch' then
                return { error_code = 30 }
            end

            if name == 'ListOffsets' then
                return 'close'
            end
        end,
    }, { retry = { attempts = 1 } })

    bury(fake, 0, 'A')

    local orders = broker:declare('orders')
    local moved, err = orders:kick(1)

    t.assert_equals(moved, nil)
    t.assert_equals(err.kind, failure.REJECTED)

    for _, step in ipairs({ 'OffsetCommit', 'Fetch', 'FetchClose', 'FindCoordinator', 'OffsetFetch', 'ListOffsets' }) do
        refuse = step
        moved, err = orders:kick(1)

        t.assert_equals(moved, nil, step)
        -- Отказ — с родом: бросок посреди возврата пришёл бы строкой.
        t.assert_not_equals(err.kind, nil, step)
    end

    t.assert_equals(orders.counts.kicked, 0)
end

g.test_kick_without_topics_or_leader = function()
    local broker = helper.talking(g, {
        respond = function(name)
            if name == 'CreateTopics' then
                return { topics = { { name = 'orders', error_code = 31 } } }
            end
        end,
    }, { retry = { attempts = 1 } })
    local moved, err = broker:declare('orders'):kick(1)

    t.assert_equals(moved, nil)
    t.assert_equals(err.kind, failure.DENIED)

    g.fake.stop()
    g.broker:close()

    local second, fake = helper.talking(g, { topics = TOPICS }, { retry = { attempts = 1 } })
    local orders = second:declare('orders')

    fake.leader = 42
    moved, err = orders:kick(1)
    t.assert_equals(moved, nil)
    t.assert_equals(err.kind, failure.UNREACHABLE)
    t.assert_str_contains(err.message, 'нет ведущего')

    -- Смещение группы возврата есть: без ведущего не читается уже выборка.
    fake.groups['orders.dead'] = {
        members = {},
        generation = 0,
        state = 'empty',
        offsets = {
            ['orders.dead/0'] = { offset = 0, metadata = '' },
            ['orders.dead/1'] = { offset = 0, metadata = '' },
        },
        cond = require('fiber').cond(),
        joined = {},
    }
    moved, err = orders:kick(1)
    t.assert_equals(moved, nil)
    t.assert_equals(
        err.message,
        'kafka: у раздела orders.dead/0 нет ведущего: LEADER_NOT_AVAILABLE (5)'
    )
end

g.test_kick_missing_dead_topic_description = function()
    local broker = helper.talking(g, {
        topics = TOPICS,
        respond = function(name, values, talk)
            if name == 'Metadata' then
                local answer = helper.default(talk.fake, name, values)

                -- Тема мёртвых есть, но разделов у неё брокер не назвал.
                for _, topic in ipairs(answer.topics) do
                    if topic.name == 'orders.dead' then
                        topic.partitions = {}
                    end
                end

                return answer
            end
        end,
    }, { retry = { attempts = 1 } })
    local moved, err = broker:declare('orders'):kick(1)

    t.assert_equals(moved, nil)
    t.assert_str_contains(err.message, 'тема orders.dead не описана')
end

g.test_kick_reads_dead_letters_without_waiting = function()
    local broker, fake = helper.talking(g, { topics = TOPICS })

    bury(fake, 0, 'A')
    bury(fake, 0, 'B')

    t.assert_equals(broker:declare('orders'):kick(2), 2)

    local request = fake.requests.Fetch[1]

    t.assert_equals(
        { request.replica_id, request.max_wait_ms, request.min_bytes, request.isolation_level },
        { -1, 0, 0, 1 }
    )
    -- Одна выборка на обе записи и одна, чтобы увидеть, что больше нет.
    t.assert_equals(#fake.requests.Fetch, 2)
    -- Второй раздел не трогали: набрано в первом.
    t.assert_equals(#fake.requests.FindCoordinator, 1)
end

g.test_kick_shares_the_count_between_partitions = function()
    local broker, fake = helper.talking(g, { topics = TOPICS })

    bury(fake, 0, 'A')
    bury(fake, 1, 'C')
    bury(fake, 1, 'D')

    t.assert_equals(broker:declare('orders'):kick(2), 2)
    t.assert_equals(#revived(fake), 2)
end

g.test_kick_counts_what_moved_before_a_retried_failure = function()
    ---@type table<string, integer>
    local refusals = { FindCoordinator = 1, OffsetFetch = 1, ListOffsets = 1 }
    local copies = 0
    local broker, fake = helper.talking(g, {
        topics = TOPICS,
        respond = function(name, values)
            if name == 'Produce' and values.topic_data[1].name == 'orders.retry' then
                copies = copies + 1

                -- Вторая копия не уходит с первого раза.
                if copies == 2 then
                    return {
                        responses = {
                            {
                                name = 'orders.retry',
                                partition_responses = { { index = 0, error_code = 6, base_offset = -1 } },
                            },
                        },
                    }
                end
            end

            if (refusals[name] or 0) == 0 then
                return nil
            end

            refusals[name] = refusals[name] - 1

            if name == 'FindCoordinator' then
                return { error_code = 15 }
            end

            if name == 'OffsetFetch' then
                return { error_code = 14 }
            end

            return {
                topics = {
                    {
                        name = values.topics[1].name,
                        partitions = { { partition_index = 0, error_code = 6, timestamp = -1, offset = -1 } },
                    },
                },
            }
        end,
    })

    bury(fake, 0, 'A')
    bury(fake, 0, 'B')

    -- Просят больше, чем есть: отказы по дороге не прибавляют к счёту.
    local orders = broker:declare('orders')

    t.assert_equals(orders:kick(5), 2)
    t.assert_equals(orders.counts.kicked, 2)
    t.assert_equals(copies, 3)
end

g.test_kick_reads_again_when_a_fetch_brings_less = function()
    local broker, fake = helper.talking(g, { topics = TOPICS })

    fake.fetch_limit = 2

    for _, name in ipairs({ 'A', 'B', 'C', 'D', 'E' }) do
        bury(fake, 0, name)
    end

    t.assert_equals(broker:declare('orders'):kick(3), 3)
    t.assert_equals(#revived(fake), 3)
end

g.test_kick_repeats_a_cut_fetch = function()
    local cut = true
    local broker, fake = helper.talking(g, {
        topics = TOPICS,
        respond = function(name)
            if name == 'Fetch' and cut then
                cut = false

                return 'close'
            end
        end,
    })

    bury(fake, 0, 'A')
    t.assert_equals(broker:declare('orders'):kick(1), 1)
end

g.test_kick_fetch_refusal_names_the_partition = function()
    local broker, fake = helper.talking(g, {
        topics = TOPICS,
        respond = function(name, values)
            if name == 'Fetch' then
                return {
                    responses = {
                        {
                            topic = values.topics[1].topic,
                            partitions = {
                                { partition_index = values.topics[1].partitions[1].partition, error_code = 29 },
                            },
                        },
                    },
                }
            end
        end,
    }, { retry = { attempts = 1 } })

    bury(fake, 0, 'A')

    local moved, err = broker:declare('orders'):kick(1)

    t.assert_equals(moved, nil)
    t.assert_equals(
        err.message,
        'kafka: зарытые orders.dead/0 не прочитаны: TOPIC_AUTHORIZATION_FAILED (29)'
    )
end
