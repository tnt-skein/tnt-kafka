--- Проверки группы и смещений: ведущий группы, вход с номером, раздача
--- по кругу, стук, уход, чтение и фиксация смещений, начало раздела.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local cluster_of = helper.cluster
local failure = helper.failure
local group = helper.group
local offsets = helper.offsets
local within = helper.within

local g = t.group('tnt.kafka.group')

g.after_each(function()
    local cluster = g.cluster

    g.cluster = nil

    if cluster then
        cluster:close()
    end

    helper.release(g)
end)

--- Кластер к двойнику и адрес двойника.
---@param opts table|nil
---@param settings table|nil Настройки клиента поверх
---@return table cluster
---@return table address
local function started(opts, settings)
    local fake = helper.serve(opts)
    local checked = helper.checked(helper.settings_of(fake, settings))

    g.fake, g.cluster = fake, cluster_of.new(checked)

    return g.cluster, { host = '127.0.0.1', port = fake.port }
end

--- Место участника.
---@param topics string[]|nil
---@return table
local function seat_of(topics)
    return {
        group = 'workers',
        member = '',
        generation = -1,
        topics = topics or { 'orders' },
        session_ms = 6000,
        rebalance_ms = 1000,
    }
end

--- Подписка участника для раздачи.
---@param id string
---@param topics string[]
---@return table
local function member(id, topics)
    return { member_id = id, metadata = group.subscription(topics) }
end

g.test_subscription_and_assignment_bytes = function()
    local reader = helper.wire.reader(group.subscription({ 'a', 'b' }))

    t.assert_equals(reader:int16(), 0)
    t.assert_equals(reader:int32(), 2)

    local bytes = group.assignment({ zeta = { 1 }, alpha = { 0, 2 } })

    t.assert_equals(group.owned(bytes), { zeta = { 1 }, alpha = { 0, 2 } })

    local order = helper.wire.reader(bytes)

    t.assert_equals(order:int16(), 0)
    t.assert_equals(order:int32(), 2)
    t.assert_equals(order:string(), 'alpha')

    local many = helper.wire.reader(group.assignment({ e = {}, c = {}, a = {}, d = {}, b = {} }))
    local names = {}

    many:int16()

    for index = 1, many:int32() do
        names[index] = many:string()
        many:int32()
    end

    t.assert_equals(names, { 'a', 'b', 'c', 'd', 'e' })
    t.assert_equals(group.owned(nil), {})
    t.assert_equals(group.owned(''), {})
    t.assert_equals(group.owned(group.assignment({})), {})
end

g.test_round_robin_by_member_and_partition_order = function()
    local result = group.assign(
        { member('m-2', { 'orders' }), member('m-1', { 'orders', 'orders.retry' }), member('m-3', { 'orders.retry' }) },
        { orders = 3, ['orders.retry'] = 2, stray = 4 }
    )

    t.assert_equals(result, {
        ['m-1'] = { orders = { 0, 2 }, ['orders.retry'] = { 1 } },
        ['m-2'] = { orders = { 1 } },
        ['m-3'] = { ['orders.retry'] = { 0 } },
    })
end

g.test_round_robin_starts_from_the_first_member = function()
    local all = { member('m-3', { 'a' }), member('m-1', { 'a' }), member('m-2', { 'a' }) }

    t.assert_equals(group.assign(all, { a = 4 }), {
        ['m-1'] = { a = { 0, 3 } },
        ['m-2'] = { a = { 1 } },
        ['m-3'] = { a = { 2 } },
    })
end

g.test_round_robin_with_one_member_takes_all = function()
    t.assert_equals(group.assign({ member('m', { 'a' }) }, { a = 2 }), { m = { a = { 0, 1 } } })
    t.assert_equals(group.assign({ member('m', {}) }, { a = 2 }), { m = {} })
end

g.test_subscription_without_topics_reads_as_empty = function()
    local empty = helper.schema.encode(
        { { 'version', 'int16' }, { 'topics', '[string]' }, { 'user_data', 'bytes' } },
        { version = 0 }
    )

    t.assert_equals(group.assign({ { member_id = 'm', metadata = empty } }, { a = 1 }), { m = {} })
end

g.test_join_with_issued_member_id = function()
    local cluster, home = started()
    local seat = seat_of()
    local joined = group.join(cluster, home, seat, within.deadline(2))

    t.assert_equals(joined.leader, true)
    t.assert_equals(#joined.members, 1)
    t.assert_equals(seat.member, 'member-1')
    t.assert_equals(seat.generation, 1)
    t.assert_equals(#g.fake.requests.JoinGroup, 2)

    local first = g.fake.requests.JoinGroup[1]

    t.assert_equals(first.member_id, '')
    t.assert_equals(first.protocol_type, 'consumer')
    t.assert_equals(first.protocols[1].name, 'roundrobin')
    t.assert_equals(first.session_timeout_ms, 6000)
    t.assert_equals(first.rebalance_timeout_ms, 1000)
    t.assert_equals(g.fake.requests.JoinGroup[2].member_id, 'member-1')

    local owned = group.sync(cluster, home, seat, { ['member-1'] = { orders = { 0 } } }, within.deadline(2))

    t.assert_equals(owned, { orders = { 0 } })
    t.assert_equals(group.heartbeat(cluster, home, seat, within.deadline(1)), true)

    group.leave(cluster, home, seat, within.deadline(1))
    t.assert_equals(seat.generation, -1)
    t.assert_equals(g.fake.left, { 'member-1' })
end

g.test_join_refusals = function()
    local cluster, home = started({
        respond = function(name)
            if name == 'JoinGroup' then
                return {
                    throttle_time_ms = 0,
                    error_code = 30,
                    generation_id = -1,
                    protocol_name = '',
                    leader = '',
                    member_id = '',
                    members = {},
                }
            end
        end,
    })
    local joined, err = group.join(cluster, home, seat_of(), within.deadline(1))

    t.assert_equals(joined, nil)
    t.assert_equals(err.kind, failure.DENIED)
    t.assert_equals(
        err.message,
        'kafka: вход в группу workers не удался: GROUP_AUTHORIZATION_FAILED (30)'
    )
end

g.test_join_asked_for_id_twice_gives_up = function()
    local cluster, home = started({
        respond = function(name)
            if name == 'JoinGroup' then
                return {
                    throttle_time_ms = 0,
                    error_code = 79,
                    generation_id = -1,
                    protocol_name = '',
                    leader = '',
                    member_id = 'again',
                    members = {},
                }
            end
        end,
    })
    local _, err = group.join(cluster, home, seat_of(), within.deadline(1))

    t.assert_equals(
        err.message,
        'kafka: брокер дважды не принял номер в группе workers: MEMBER_ID_REQUIRED (79)'
    )
    t.assert_equals(#g.fake.requests.JoinGroup, 2)
end

g.test_join_without_answer = function()
    local cluster, home = started({
        respond = function(name)
            if name == 'JoinGroup' then
                return 'close'
            end
        end,
    })
    local _, err = group.join(cluster, home, seat_of(), within.deadline(1))

    t.assert_equals(err.kind, failure.BROKEN)
    t.assert_equals(err.retriable, true)
end

g.test_sync_heartbeat_refusals = function()
    local cluster, home = started()
    local seat = seat_of()

    seat.member, seat.generation = 'ghost', 3

    local owned, err = group.sync(cluster, home, seat, nil, within.deadline(1))

    t.assert_equals(owned, nil)
    t.assert_equals(err.kind, failure.CONFLICT)
    t.assert_equals(
        err.message,
        'kafka: раздача группы workers не получена: UNKNOWN_MEMBER_ID (25)'
    )

    local ok
    ok, err = group.heartbeat(cluster, home, seat, within.deadline(1))
    t.assert_equals(ok, nil)
    t.assert_equals(err.message, 'kafka: стук в группу workers не принят: UNKNOWN_MEMBER_ID (25)')
end

g.test_sync_and_heartbeat_without_answer = function()
    local cluster, home = started({
        respond = function(name)
            if name == 'SyncGroup' or name == 'Heartbeat' then
                return 'close'
            end
        end,
    })
    local seat = seat_of()
    local _, err = group.sync(cluster, home, seat, nil, within.deadline(1))

    t.assert_equals(err.kind, failure.BROKEN)
    t.assert_equals(err.retriable, true)

    _, err = group.heartbeat(cluster, home, seat, within.deadline(1))
    t.assert_equals(err.kind, failure.BROKEN)
    t.assert_equals(err.retriable, true)
end

g.test_coordinator_lookup = function()
    local cluster, home = started()

    t.assert_equals(offsets.coordinator(cluster, 'workers', within.deadline(1)), home)
    t.assert_equals(g.fake.requests.FindCoordinator[1], { key = 'workers', key_type = 0 })
    t.assert_equals(offsets.NONE, -1)
end

g.test_coordinator_cut_is_retriable = function()
    local cluster = started({
        respond = function(name)
            if name == 'FindCoordinator' then
                return 'close'
            end
        end,
    })
    local _, err = offsets.coordinator(cluster, 'workers', within.deadline(1))

    t.assert_equals(err.kind, failure.BROKEN)
    t.assert_equals(err.retriable, true)
end

g.test_call_forgets_the_coordinator_only_when_it_moved_or_went_silent = function()
    local cluster = started(nil, { retry = { attempts = 10 } })
    local runs = 0

    offsets.call(cluster, {}, 'workers', 1, function()
        runs = runs + 1

        if runs == 1 then
            return nil, failure.new(failure.UNREACHABLE, 'связь')
        end

        return true
    end)
    t.assert_equals(#g.fake.requests.FindCoordinator, 2)

    runs = 0
    offsets.call(cluster, {}, 'workers', 1, function()
        runs = runs + 1

        if runs == 1 then
            return nil, failure.new(failure.UNREACHABLE, 'загрузка', { server_code = 14 })
        end

        return true
    end)
    t.assert_equals(#g.fake.requests.FindCoordinator, 3)
end

g.test_coordinator_refusal_and_silence = function()
    local cluster = started({
        respond = function(name)
            if name == 'FindCoordinator' then
                return { throttle_time_ms = 0, error_code = 15, node_id = -1, host = '', port = -1 }
            end
        end,
    })
    local found, err = offsets.coordinator(cluster, 'workers', within.deadline(1))

    t.assert_equals(found, nil)
    t.assert_equals(err.kind, failure.UNREACHABLE)
    t.assert_equals(
        err.message,
        'kafka: у группы workers нет ведущего: COORDINATOR_NOT_AVAILABLE (15)'
    )

    g.cluster:close()
    g.cluster = cluster_of.new(helper.checked({
        brokers = { ('127.0.0.1:%d'):format(helper.closed_port()) },
        pool = { wait_timeout = 0.05, sweep_interval = 0 },
    }))
    found, err = offsets.coordinator(g.cluster, 'workers', within.deadline(0.3))
    t.assert_equals(found, nil)
    t.assert_equals(err.kind, failure.UNREACHABLE)
end

g.test_committed_and_commit = function()
    local cluster, home = started({ topics = { orders = 2 } })
    local outsider = { group = 'workers', generation = -1, member = '' }

    t.assert_equals(offsets.committed(cluster, home, 'workers', 'orders', { 0, 1 }, within.deadline(1)), {
        [0] = { offset = -1, deliveries = 0 },
        [1] = { offset = -1, deliveries = 0 },
    })
    t.assert_equals(offsets.commit(cluster, home, outsider, 'orders', 1, 7, 2, within.deadline(1)), true)
    t.assert_equals(offsets.commit(cluster, home, outsider, 'orders', 0, 3, nil, within.deadline(1)), true)
    t.assert_equals(g.fake.commits[1].metadata, '2')
    t.assert_equals(g.fake.commits[2].metadata, '')
    t.assert_equals(offsets.committed(cluster, home, 'workers', 'orders', { 0, 1 }, within.deadline(1)), {
        [0] = { offset = 3, deliveries = 0 },
        [1] = { offset = 7, deliveries = 2 },
    })
end

g.test_commit_refusal = function()
    local cluster, home = started({ topics = { orders = 1 } })
    local stale = { group = 'workers', generation = 5, member = 'ghost' }
    local ok, err = offsets.commit(cluster, home, stale, 'orders', 0, 1, nil, within.deadline(1))

    t.assert_equals(ok, nil)
    t.assert_equals(err.kind, failure.CONFLICT)
    t.assert_equals(
        err.message,
        'kafka: смещение orders/0 не зафиксировано: UNKNOWN_MEMBER_ID (25)'
    )
end

g.test_committed_refusals = function()
    local cluster, home = started({
        respond = function(name, values)
            if name == 'OffsetFetch' and values.group_id == 'top' then
                return { throttle_time_ms = 0, topics = {}, error_code = 30 }
            end

            if name == 'OffsetFetch' and values.group_id == 'part' then
                return {
                    throttle_time_ms = 0,
                    topics = {
                        {
                            name = 'orders',
                            partitions = {
                                { partition_index = 0, committed_offset = -1, metadata = '', error_code = 3 },
                            },
                        },
                    },
                    error_code = 0,
                }
            end

            if name == 'OffsetFetch' and values.group_id == 'none' then
                return { throttle_time_ms = 0, topics = {}, error_code = 0 }
            end
        end,
    })
    local found, err = offsets.committed(cluster, home, 'top', 'orders', { 0 }, within.deadline(1))

    t.assert_equals(found, nil)
    t.assert_equals(
        err.message,
        'kafka: смещения группы top не прочитаны: GROUP_AUTHORIZATION_FAILED (30)'
    )

    found, err = offsets.committed(cluster, home, 'part', 'orders', { 0 }, within.deadline(1))
    t.assert_equals(found, nil)
    t.assert_equals(
        err.message,
        'kafka: смещение orders/0 не прочитано: UNKNOWN_TOPIC_OR_PARTITION (3)'
    )
    t.assert_equals(offsets.committed(cluster, home, 'none', 'orders', { 0 }, within.deadline(1)), {})
end

g.test_offsets_without_answer = function()
    local cluster, home = started({
        respond = function(name)
            if name == 'OffsetFetch' or name == 'OffsetCommit' or name == 'ListOffsets' then
                return 'close'
            end
        end,
        topics = { orders = 1 },
    })
    local _, err = offsets.committed(cluster, home, 'workers', 'orders', { 0 }, within.deadline(1))

    t.assert_equals(err.kind, failure.BROKEN)
    t.assert_equals(err.retriable, true)

    _, err = offsets.commit(
        cluster,
        home,
        { group = 'g', generation = -1, member = '' },
        'orders',
        0,
        1,
        nil,
        within.deadline(1)
    )
    t.assert_equals(err.kind, failure.BROKEN)
    t.assert_equals(err.retriable, true)

    local meta = cluster:topic('orders', within.deadline(1))

    _, err = offsets.earliest(cluster, meta, 0, within.deadline(1))
    t.assert_equals(err.kind, failure.BROKEN)
    t.assert_equals(err.retriable, true)
end

g.test_earliest_offset = function()
    local cluster = started({ topics = { orders = 1 } })

    g.fake.topics.orders[1].start = 4
    g.fake.topics.orders[1].next = 9

    local meta = cluster:topic('orders', within.deadline(1))

    t.assert_equals(offsets.earliest(cluster, meta, 0, within.deadline(1)), 4)
    t.assert_equals(g.fake.requests.ListOffsets[1].replica_id, -1)
    t.assert_equals(g.fake.requests.ListOffsets[1].topics[1].partitions[1].timestamp, -2)
end

g.test_earliest_refusals = function()
    local cluster = started({
        topics = { orders = 1 },
        respond = function(name, values)
            if name == 'ListOffsets' then
                local code = values.topics[1].name == 'orders' and 6 or 29

                return {
                    topics = {
                        {
                            name = values.topics[1].name,
                            partitions = { { partition_index = 0, error_code = code, timestamp = -1, offset = -1 } },
                        },
                    },
                }
            end
        end,
    })
    local meta = cluster:topic('orders', within.deadline(1))
    local offset, err = offsets.earliest(cluster, meta, 0, within.deadline(1))

    t.assert_equals(offset, nil)
    t.assert_equals(
        err.message,
        'kafka: начало раздела orders/0 не прочитано: NOT_LEADER_OR_FOLLOWER (6)'
    )
    t.assert_equals(cluster.topics.orders, nil)

    local other = { name = 'secret', count = 1, partitions = { [0] = { leader = 1 } } }

    offset, err = offsets.earliest(cluster, other, 0, within.deadline(1))
    t.assert_equals(offset, nil)
    t.assert_equals(err.kind, failure.DENIED)

    other.partitions[0].leader = 42
    offset, err = offsets.earliest(cluster, other, 0, within.deadline(1))
    t.assert_equals(offset, nil)
    t.assert_equals(err.message, 'kafka: у раздела secret/0 нет ведущего: LEADER_NOT_AVAILABLE (5)')
end

g.test_call_finds_and_forgets_the_coordinator = function()
    local cluster, home = started(nil, { retry = { attempts = 10 } })
    local holder = {}
    local seen = {}
    local runs = 0
    local value = offsets.call(cluster, holder, 'workers', 1, function(coordinator)
        runs = runs + 1
        seen[#seen + 1] = coordinator

        if runs == 1 then
            return nil, failure.new(failure.UNREACHABLE, 'ведёт другой', { server_code = 16 })
        end

        if runs == 2 then
            return nil, failure.new(failure.UNREACHABLE, 'связь')
        end

        if runs == 3 then
            return nil, failure.new(failure.UNREACHABLE, 'загрузка', { server_code = 14 })
        end

        return 'готово'
    end)

    t.assert_equals(value, 'готово')
    t.assert_equals(seen, { home, home, home, home })
    t.assert_equals(#g.fake.requests.FindCoordinator, 3)
    t.assert_equals(holder.coordinator, home)
end

g.test_call_without_coordinator_is_the_failure = function()
    local cluster = started({
        respond = function(name)
            if name == 'FindCoordinator' then
                return { throttle_time_ms = 0, error_code = 30, node_id = -1, host = '', port = -1 }
            end
        end,
    })
    local _, err = offsets.call(cluster, {}, 'workers', 1, function()
        return 'не дойдёт'
    end)

    t.assert_equals(err.kind, failure.DENIED)
end
