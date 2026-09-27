--- Проверки получателя: итог обработчика, тема повтора со сроком,
--- зарытое, чужое тело, форма, счёт выдач после падения, сжатая пачка,
--- стёртое смещение, отказы выборки и фиксации, вход в группу и стук.

local clock = require('clock')
local fiber = require('fiber')
local json = require('json')
local t = require('luatest')

local helper = dofile('test/helper.lua')

local context = helper.context
local message = helper.message

local g = t.group('tnt.kafka.consumer')

--- Паузы, которые проверки укорачивают, и их настоящие значения.
local PAUSES = {
    { helper.fetch, 'POLL' },
    { helper.fetch, 'PAUSE' },
    { helper.enter, 'PAUSE' },
    { helper.worker, 'FAILURE_PAUSE' },
    { helper.handle, 'PAUSE' },
}

local saved = {}

--- Настоящие монотонные часы пакета. Проверка на стоящих часах
--- возвращает их сама, а здесь они возвращаются и тогда, когда она упала
--- раньше: иначе стоящие часы достались бы всем следующим проверкам.
local real_monotonic = helper.clock.monotonic

g.before_each(function()
    for index, entry in ipairs(PAUSES) do
        saved[index] = entry[1][entry[2]]
        entry[1][entry[2]] = 0.02
    end

    helper.journal.forget()
end)

g.after_each(function()
    helper.clock.monotonic = real_monotonic
    helper.release(g)

    for index, entry in ipairs(PAUSES) do
        entry[1][entry[2]] = saved[index]
    end
end)

--- Клиент и двойник; тема заводится при первом деле.
---@param opts table|nil Настройки двойника
---@param settings table|nil Настройки клиента
---@return table broker
---@return table fake
local function started(opts, settings)
    local broker, fake = helper.talking(g, opts, settings)

    return broker, fake
end

--- Получатель, которого остановит `release` после проверки.
---@param topic table
---@param handler function
---@param opts table|nil
---@return table
local function consume(topic, handler, opts)
    return helper.consume(g, topic, handler, opts)
end

--- Ждёт условия; иначе валит проверку с текстом.
---@param about string
---@param condition fun(): boolean
---@param timeout number|nil
local function await(about, condition, timeout)
    t.assert(helper.until_true(condition, timeout), about)
end

--- Записи, легшие в тему, по порядку.
---@param fake table
---@param topic string
---@return table[]
local function produced_to(fake, topic)
    local found = {}

    for _, entry in ipairs(fake.produced) do
        if entry.topic == topic then
            found[#found + 1] = entry
        end
    end

    return found
end

--- Заголовки записи таблицей.
---@param record table
---@return table<string, string>
local function headers_of(record)
    local found = {}

    for _, header in ipairs(record.headers) do
        found[header[1]] = header[2]
    end

    return found
end

--- Получатель раздела `orders/0` с двумя записями, первую из которых
--- хранение темы уже стёрло, а группа `orders` помнит смещение на
--- стёртой; ждёт, пока обработается одно сообщение.
---@param broker table
---@param fake table
---@return integer[] handled Номера обработанных записей
local function consume_erased(broker, fake)
    local orders = broker:declare('orders')
    local handled = {}

    helper.append(fake, 'orders', 0, helper.batch({ value = '{"n":0}' }))
    helper.append(fake, 'orders', 0, helper.batch({ value = '{"n":1}' }))
    fake.topics.orders[1].start = 1
    fake.groups.orders = {
        members = {},
        generation = 0,
        state = 'empty',
        offsets = { ['orders/0'] = { offset = 0, metadata = '' } },
        cond = fiber.cond(),
        joined = {},
    }
    consume(orders, function(delivered)
        handled[#handled + 1] = delivered.body.n
    end)

    await('сообщение не обработано', function()
        return #handled == 1
    end)

    return handled
end

--- Отправляет одно сообщение в `orders` и ждёт, пока получатель его
--- подтвердит.
---@param broker table
---@param about string Текст провала
---@return table orders
local function acked_once(broker, about)
    local orders = broker:declare('orders')

    orders:send({ order = 1 })
    consume(orders, function() end)
    await(about, function()
        return orders.counts.ack == 1
    end)

    return orders
end

--- Сверяет, что раздел выбран снова не раньше паузы выборки.
---@param times number[] Мгновения выборок раздела
local function assert_rested(times)
    local gap = (times[2] or 0) - (times[1] or math.huge)

    t.assert(gap >= 0.2, ('раздел выбран снова через %.3f с'):format(gap))
end

g.test_ack_commits_mark_then_next_offset = function()
    local broker, fake = started()
    local orders = broker:declare('orders')
    local seen ---@type any

    context.run({ request_id = 'r-1' }, function()
        orders:send({ order = 1 }, { key = 'k' })
    end)

    consume(orders, function(delivered)
        seen = {
            message = delivered,
            request_id = context.get('request_id'),
        }
    end)

    await('обработчик не позван', function()
        return orders.counts.ack == 1
    end)

    t.assert_equals(seen.message.body, { order = 1 })
    t.assert_equals(seen.message.attempt, 1)
    t.assert_equals(seen.message.key, 'k')
    t.assert_equals(seen.message.name, 'orders')
    t.assert_equals(seen.request_id, 'r-1')

    local commits = {}

    for _, entry in ipairs(fake.commits) do
        if entry.topic == 'orders' then
            commits[#commits + 1] = { entry.offset, entry.metadata }
        end
    end

    t.assert_equals(commits, { { 0, '1' }, { 1, '' } })
    t.assert(helper.journal.logged('участник вошёл в группу'))
    t.assert_equals(orders.counts.failures, 0)
end

g.test_handler_without_request_id_gets_its_own = function()
    local broker, fake = started({ topics = { orders = 1, ['orders.retry'] = 1, ['orders.dead'] = 1 } })
    local orders = broker:declare('orders')
    local request_id

    helper.append(fake, 'orders', 0, helper.batch({ value = '{"a":1}' }))
    consume(orders, function()
        request_id = context.get('request_id')
    end)

    await('обработчик не позван', function()
        return request_id ~= nil
    end)
    t.assert_equals(#request_id, 26)
end

g.test_failure_goes_to_retry_topic_with_due = function()
    local broker, fake = started()
    local orders = broker:declare('orders')
    local attempts = {}
    local started_at = clock.realtime()

    orders:send({ order = 1 })
    consume(orders, function(delivered)
        attempts[#attempts + 1] = { attempt = delivered.attempt, at = clock.realtime() }

        if delivered.attempt == 1 then
            return nil, { retry_after = 0.3, message = 'подождите' }
        end
    end)

    await('повтор не обработан', function()
        return orders.counts.ack == 1
    end)

    t.assert_equals(#attempts, 2)
    t.assert_equals(attempts[2].attempt, 2)
    t.assert(attempts[2].at - started_at >= 0.3, 'повтор пришёл раньше срока')
    t.assert_equals(orders.counts.retry, 1)

    local copy = produced_to(fake, 'orders.retry')[1] --[[@as table]]
    local headers = headers_of(copy.record)

    t.assert_equals(headers['x-tnt-attempt'], '1')
    t.assert_equals(copy.record.value, '{"order":1}')
    t.assert(tonumber(headers['x-tnt-due']) > started_at, 'срок повтора в прошлом')
    t.assert(helper.journal.logged('сообщение выдадут снова'))
end

g.test_last_attempt_buries_with_reason = function()
    local broker, fake = started()
    local orders = broker:declare('orders')

    orders:send({ order = 1 })
    consume(orders, function()
        return nil, 'не выходит'
    end, { max_attempts = 2, backoff = { base = 0, max = 0 } })

    await('сообщение не зарыто', function()
        return orders.counts.bury == 1
    end)

    local dead = produced_to(fake, 'orders.dead')[1] --[[@as table]]

    t.assert_equals(headers_of(dead.record)['x-tnt-reason'], 'попыток 2 из 2: не выходит')
    t.assert_equals(orders.counts.retry, 1)

    local record = helper.journal.find('сообщение зарыто')

    t.assert_str_contains(record.line, 'попыток 2 из 2')
end

g.test_hopeless_failure_and_raise = function()
    local broker = started()
    local orders = broker:declare('orders')

    orders:send({ kind = 'hopeless' })
    orders:send({ kind = 'raise' })
    consume(orders, function(delivered)
        if delivered.body.kind == 'hopeless' then
            return nil, { retriable = false, message = 'тело негодно' }
        end

        error('обработчик упал')
    end, { max_attempts = 1 })

    await('сообщения не зарыты', function()
        return orders.counts.bury == 2
    end)

    local raised = helper.journal.find('обработчик сообщения бросил')

    t.assert_str_contains(raised.line, 'обработчик упал')
    t.assert_str_contains(raised.line, 'traceback')
end

g.test_foreign_and_unshaped_bodies_are_buried_without_handler = function()
    local shape = {
        from = function(body)
            if body.order == nil then
                return nil, { order = 'нужно' }
            end

            return body
        end,
    }
    local broker, fake = started({ topics = { orders = 1, ['orders.retry'] = 1, ['orders.dead'] = 1 } })
    local orders = broker:declare('orders', { shape = shape })
    local called = 0

    helper.append(fake, 'orders', 0, helper.batch({ value = 'не json' }))
    helper.append(fake, 'orders', 0, helper.batch({ value = '{"other":1}' }))
    consume(orders, function()
        called = called + 1
    end)

    await('сообщения не зарыты', function()
        return orders.counts.bury == 2
    end)

    local reasons = {}

    for _, entry in ipairs(produced_to(fake, 'orders.dead')) do
        reasons[#reasons + 1] = headers_of(entry.record)['x-tnt-reason']
    end

    t.assert_equals(reasons, {
        'в теме не конверт tnt-kafka: тело не JSON',
        'тело не по форме: order: нужно',
    })
    t.assert_equals(called, 0)
end

g.test_redelivery_counts_from_the_mark = function()
    local broker, fake = started({ topics = { orders = 1, ['orders.retry'] = 1, ['orders.dead'] = 1 } })
    local orders = broker:declare('orders')
    local attempts = {}
    local groups = fake.groups

    helper.append(fake, 'orders', 0, helper.batch({ value = '{"a":1}' }))
    helper.append(fake, 'orders', 0, helper.batch({ value = '{"a":2}' }))
    groups.orders = {
        members = {},
        generation = 0,
        state = 'empty',
        offsets = { ['orders/0'] = { offset = 0, metadata = '2' } },
        cond = require('fiber').cond(),
        joined = {},
    }
    consume(orders, function(delivered)
        attempts[#attempts + 1] = delivered.attempt
    end, { max_attempts = 5 })

    await('сообщения не обработаны', function()
        return orders.counts.ack == 2
    end)
    t.assert_equals(attempts, { 3, 1 })
end

g.test_too_many_deliveries_are_buried_without_handler = function()
    local broker, fake = started({ topics = { orders = 1, ['orders.retry'] = 1, ['orders.dead'] = 1 } })
    local orders = broker:declare('orders')
    local called = 0

    helper.append(fake, 'orders', 0, helper.batch({ value = '{"a":1}' }))
    fake.groups.orders = {
        members = {},
        generation = 0,
        state = 'empty',
        offsets = { ['orders/0'] = { offset = 0, metadata = '3' } },
        cond = require('fiber').cond(),
        joined = {},
    }
    consume(orders, function()
        called = called + 1
    end, { max_attempts = 3 })

    await('сообщение не зарыто', function()
        return orders.counts.bury == 1
    end)
    t.assert_equals(called, 0)
    t.assert_equals(
        headers_of((produced_to(fake, 'orders.dead')[1] --[[@as table]]).record)['x-tnt-reason'],
        'выдач 4 при потолке 3: прежние выдачи итога не вернули'
    )
end

g.test_due_copy_waits_while_others_go = function()
    local broker, fake = started({ topics = { orders = 1, ['orders.retry'] = 1, ['orders.dead'] = 1 } })
    local orders = broker:declare('orders')
    local order = {}
    local due = ('%.6f'):format(clock.realtime() + 0.3)
    local later = message.forward('{"n":"later"}', {
        id = 'L',
        name = 'orders',
        headers = {},
        attempt = 1,
        created = clock.realtime(),
    }, { [message.DUE] = due })

    helper.append(fake, 'orders.retry', 0, helper.batch(later))
    helper.append(fake, 'orders', 0, helper.batch({ value = '{"n":"now"}' }))
    consume(orders, function(delivered)
        order[#order + 1] = { delivered.body.n, delivered.attempt }
    end)

    await('сообщения не обработаны', function()
        return orders.counts.ack == 2
    end)
    t.assert_equals(order, { { 'now', 1 }, { 'later', 2 } })
end

g.test_compressed_batch_is_skipped_with_error = function()
    local broker, fake = started({ topics = { orders = 1, ['orders.retry'] = 1, ['orders.dead'] = 1 } })
    local orders = broker:declare('orders')
    local handled = {}
    local batch = helper.batch({ value = '{}' })
    local head = batch:sub(1, 17)
    local summed = helper.wire.int16(2) .. batch:sub(24)

    batch = head .. helper.wire.int32(helper.records.crc32c(summed)) .. summed
    helper.append(fake, 'orders', 0, batch)
    helper.append(fake, 'orders', 0, helper.batch({ value = '{"after":1}' }))
    consume(orders, function(delivered)
        handled[#handled + 1] = delivered.body
    end)

    await('сообщение за пачкой не обработано', function()
        return #handled == 1
    end)
    t.assert_equals(handled, { { after = 1 } })
    t.assert_equals(orders.counts.skipped, 1)
    t.assert_str_contains(helper.journal.find('пачка сжата').line, 'codec=snappy')
end

g.test_compressed_batch_of_many_records_is_skipped_whole = function()
    local broker, fake = started({ topics = { orders = 1, ['orders.retry'] = 1, ['orders.dead'] = 1 } })
    local orders = broker:declare('orders')
    local handled = {}
    -- Пачка из двух записей: её последнее смещение за началом раздела.
    local batch = helper.records.encode({
        producer_id = -1,
        producer_epoch = 0,
        base_sequence = -1,
        timestamp = 1790000000000,
        records = { { value = '{}' }, { value = '{}' } },
    })
    local summed = helper.wire.int16(1) .. batch:sub(24)

    helper.append(fake, 'orders', 0, batch:sub(1, 17) .. helper.wire.int32(helper.records.crc32c(summed)) .. summed)
    helper.append(fake, 'orders', 0, helper.batch({ value = '{"after":2}' }))
    consume(orders, function(delivered)
        handled[#handled + 1] = delivered.body
    end)

    await('сообщение за пачкой не обработано', function()
        return #handled == 1
    end)
    t.assert_equals(handled, { { after = 2 } })
    t.assert_equals(orders.counts.skipped, 1)
    t.assert_str_contains(helper.journal.find('пачка сжата').line, 'last=1')
end

g.test_erased_offset_starts_from_the_beginning = function()
    local broker, fake = started({ topics = { orders = 1, ['orders.retry'] = 1, ['orders.dead'] = 1 } })
    local handled = consume_erased(broker, fake)

    t.assert_equals(handled, { 1 })
    t.assert(helper.journal.logged('смещение раздела вне журнала'))
end

g.test_fetch_troubles_are_survived = function()
    local failing = { Fetch = 3 }
    local broker = started({
        respond = function(name, values)
            if name ~= 'Fetch' or failing.Fetch == 0 then
                return nil
            end

            failing.Fetch = failing.Fetch - 1

            if failing.Fetch == 2 then
                return 'close'
            end

            local partitions = {}

            for _, entry in ipairs(values.topics[1].partitions) do
                partitions[#partitions + 1] = {
                    partition_index = entry.partition,
                    error_code = failing.Fetch == 1 and 6 or 35,
                }
            end

            return { responses = { { topic = values.topics[1].topic, partitions = partitions } } }
        end,
    })
    local orders = broker:declare('orders')

    orders:send({ order = 1 })
    consume(orders, function() end)

    await('сообщение не обработано после бед выборки', function()
        return orders.counts.ack == 1
    end)
    t.assert(helper.journal.logged('выборка не удалась'))
    t.assert(helper.journal.logged('выборка раздела не удалась'))
end

g.test_fetch_asks_committed_records_and_waits_for_them = function()
    local broker, fake = started(nil, { max_bytes = 4096 })
    local orders = broker:declare('orders')

    -- Ожидание в целую секунду: у доли секунды округление вниз прятало бы
    -- лишнюю тысячную.
    helper.fetch.POLL = 1
    orders:send({ order = 1 })
    consume(orders, function() end)

    await('сообщение не обработано', function()
        return orders.counts.ack == 1
    end)

    local asked = fake.requests.Fetch[1]

    t.assert_equals({
        replica_id = asked.replica_id,
        max_wait_ms = asked.max_wait_ms,
        min_bytes = asked.min_bytes,
        max_bytes = asked.max_bytes,
        isolation_level = asked.isolation_level,
        partition_max_bytes = asked.topics[1].partitions[1].partition_max_bytes,
    }, {
        replica_id = -1,
        max_wait_ms = 1000,
        min_bytes = 1,
        max_bytes = 4096,
        isolation_level = 1,
        partition_max_bytes = 4096,
    })
end

g.test_fetch_waits_for_the_poll_and_the_call_together = function()
    -- Брокер держит выборку, пока ждёт данных: срок выборки — ожидание
    -- и срок вызова вместе, а ответ здесь идёт 0,3 с.
    local broker = started({
        respond = function(name)
            if name == 'Fetch' then
                fiber.sleep(0.3)
            end
        end,
    }, { timeout = 1 })
    local orders = broker:declare('orders')

    orders:send({ order = 1 })
    consume(orders, function() end)

    await('выборка не дождалась брокера', function()
        return orders.counts.ack == 1
    end)
    t.assert_not(helper.journal.logged('выборка не удалась'))
end

g.test_busy_worker_does_not_wait_between_fetches = function()
    local slept = {}

    -- Ожидание выборки — своим значением: пауза сбоя шага в проверках
    -- та же, что ожидание, и их сна было бы не различить.
    helper.fetch.POLL = 0.03
    helper.worker._set_source({
        sleep = function(seconds)
            slept[seconds] = true
            fiber.sleep(math.min(seconds, 0.01))
        end,
    })

    local broker = started()
    local orders = broker:declare('orders')

    orders:send({ order = 1 })
    orders:send({ order = 2 })
    consume(orders, function() end)

    await('сообщения не обработаны', function()
        return orders.counts.ack == 2
    end)
    -- Выбирать работнику с разделами есть что всегда: ждёт данных брокер,
    -- а не сон работника.
    t.assert_not(slept[0.03], 'работник с разделами спал ожиданием выборки')
end

--- Мгновения выборок раздела `orders/0` у двойника, пока раздел
--- не отдал запись; первую выборку с записью `spoil` вправе испортить.
---@param spoil fun(answer: table, fake: table, values: table): table|nil
---@return table broker
---@return number[] times
local function paused_fetches(spoil)
    local times = {}
    local spoiled = false
    local broker = started({
        respond = function(name, values, talk)
            if name ~= 'Fetch' then
                return nil
            end

            for _, topic in ipairs(values.topics) do
                for _, entry in ipairs(topic.partitions) do
                    if topic.topic == 'orders' and entry.partition == 0 then
                        times[#times + 1] = fiber.clock()
                    end
                end
            end

            if not spoiled and #times > 0 then
                spoiled = true

                return spoil(helper.default(talk.fake, name, values), talk.fake, values)
            end
        end,
    })

    helper.fetch.PAUSE = 0.2

    return broker, times
end

g.test_partition_refusal_rests_the_lane = function()
    local broker, times = paused_fetches(function(answer)
        for _, topic in ipairs(answer.responses) do
            for _, reply in ipairs(topic.partitions) do
                reply.error_code, reply.records = 35, nil
            end
        end

        return answer
    end)
    local orders = broker:declare('orders')

    orders:send({ order = 1 })
    consume(orders, function() end)

    await('сообщение не обработано после отказа раздела', function()
        return orders.counts.ack == 1
    end)
    assert_rested(times)
end

g.test_fetch_waits_for_the_start_of_an_erased_lane = function()
    local offsets_asked = {}
    local cut = false
    local broker, fake = started({
        topics = { orders = 1, ['orders.retry'] = 1, ['orders.dead'] = 1 },
        respond = function(name, values)
            if name == 'Fetch' then
                for _, topic in ipairs(values.topics) do
                    for _, entry in ipairs(topic.partitions) do
                        offsets_asked[#offsets_asked + 1] = entry.fetch_offset
                    end
                end
            elseif name == 'ListOffsets' and values.topics[1].name == 'orders' and not cut then
                -- Начало раздела не узнать с первого раза: раздел ждёт
                -- следующего шага, а не выбирается со смещением «нет».
                cut = true

                return 'close'
            end
        end,
    })
    t.assert_equals(consume_erased(broker, fake), { 1 })
    t.assert(cut, 'начало раздела не спрошено')
    t.assert_equals(#helper.journal.find_all('смещение раздела вне журнала'), 1)

    for _, offset in ipairs(offsets_asked) do
        t.assert(offset >= 0, 'выборка со смещением «нет»')
    end
end

g.test_corrupt_fetch_pauses_the_lane = function()
    local broker, times = paused_fetches(function(answer)
        for _, topic in ipairs(answer.responses) do
            for _, reply in ipairs(topic.partitions) do
                if reply.records ~= nil and #reply.records > 0 then
                    reply.records = reply.records:sub(1, 20) .. string.rep('\0', #reply.records - 20)
                end
            end
        end

        return answer
    end)
    local orders = broker:declare('orders')

    orders:send({ order = 1 })
    consume(orders, function() end)

    await('сообщение не обработано после порчи', function()
        return orders.counts.ack == 1
    end)
    t.assert(helper.journal.logged('выборка раздела не разобрана'))
    assert_rested(times)
end

g.test_mark_failure_counts_and_rejoins = function()
    local refused = true
    local broker, fake = started({
        respond = function(name, values)
            if name == 'OffsetCommit' and refused and values.topics[1].name == 'orders' then
                refused = false

                return {
                    topics = {
                        { name = 'orders', partitions = { { partition_index = 0, error_code = 22 } } },
                    },
                }
            end
        end,
    })
    local orders = broker:declare('orders')
    local handled = 0

    orders:send({ order = 1 })
    consume(orders, function()
        handled = handled + 1
    end)

    await('сообщение не обработано', function()
        return orders.counts.ack == 1
    end)
    t.assert_equals(handled, 1)
    t.assert_equals(orders.counts.failures >= 1, true)
    t.assert(helper.journal.logged('выдача сообщения не отмечена'))
    t.assert(fake.rounds >= 2, 'участник не вошёл заново')
end

g.test_late_outcome_is_counted_and_redelivered = function()
    local refused = 0
    local broker = started({
        respond = function(name, values)
            local entry = values.topics and values.topics[1]

            if name == 'OffsetCommit' and entry.partitions[1].committed_offset == 1 and refused == 0 then
                refused = 1

                return {
                    topics = { { name = 'orders', partitions = { { partition_index = 0, error_code = 10 } } } },
                }
            end
        end,
    })
    local orders = broker:declare('orders')
    local attempts = {}

    orders:send({ order = 1 })
    consume(orders, function(delivered)
        attempts[#attempts + 1] = delivered.attempt
    end)

    await('сообщение не обработано второй раз', function()
        return orders.counts.ack == 1
    end)
    t.assert_equals(attempts, { 1, 2 })
    t.assert_equals(orders.counts.late, 1)
    t.assert(helper.journal.logged('итог обработчика не дошёл до брокера'))
end

g.test_failed_copy_is_late = function()
    local refused = true
    local broker = started({
        respond = function(name, values)
            if name == 'Produce' and values.topic_data[1].name == 'orders.retry' and refused then
                refused = false

                return {
                    responses = {
                        {
                            name = 'orders.retry',
                            partition_responses = { { index = 0, error_code = 10, base_offset = -1 } },
                        },
                    },
                }
            end
        end,
    })
    local orders = broker:declare('orders')

    orders:send({ order = 1 })
    consume(orders, function(delivered)
        if delivered.attempt < 3 then
            return nil, { retry_after = 0 }
        end
    end)

    await('сообщение не обработано', function()
        return orders.counts.ack == 1
    end)
    t.assert_equals(orders.counts.late, 1)
end

g.test_step_failure_is_survived = function()
    local broker = started()
    local orders = broker:declare('orders')
    local process = helper.handle.process
    local broke = false

    helper.handle.process = function(...)
        if not broke then
            broke = true
            error('шаг сломался')
        end

        return process(...)
    end

    orders:send({ order = 1 })
    consume(orders, function() end)

    local ok = helper.until_true(function()
        return orders.counts.ack == 1
    end)

    helper.handle.process = process

    t.assert(ok, 'работник не пережил сбой шага')
    t.assert_equals(orders.counts.failures, 1)
    t.assert_str_contains(
        helper.journal.find('шаг работника получателя сорвался').line,
        'шаг сломался'
    )
end

g.test_enter_failures_are_retried = function()
    local refusals = { FindCoordinator = 1, JoinGroup = 1 }
    local broker = started({
        respond = function(name, values)
            if refusals[name] == nil or refusals[name] == 0 then
                return nil
            end

            if name == 'FindCoordinator' then
                refusals[name] = 0

                return { error_code = 15 }
            end

            -- Новичка брокер впускает; участника с номером — забывает.
            if values.member_id ~= '' then
                refusals[name] = 0

                return { error_code = 25, member_id = values.member_id }
            end
        end,
    })
    local orders = broker:declare('orders')

    orders:send({ order = 1 })
    consume(orders, function() end)

    await('сообщение не обработано', function()
        return orders.counts.ack == 1
    end)
    t.assert(helper.journal.logged('вход в группу не удался'))
    t.assert(helper.journal.logged('UNKNOWN_MEMBER_ID'))
end

g.test_lane_description_failure_is_retried = function()
    local armed = false
    local broker, fake = started({
        respond = function(name, values)
            if armed and name == 'Metadata' and values.topics ~= nil and #values.topics == 1 then
                armed = false

                return 'close'
            end
        end,
    })
    local orders = broker:declare('orders')

    orders:send({ order = 1 })
    helper.cluster.METADATA_AGE = 0
    armed = true
    consume(orders, function() end)

    local ok = helper.until_true(function()
        return orders.counts.ack == 1
    end)

    helper.cluster.METADATA_AGE = 300
    t.assert(ok, 'сообщение не обработано')
    t.assert_equals(armed, false)
    t.assert(fake.rounds >= 2, 'участник не вошёл заново')
    t.assert(
        helper.journal.logged('вход в группу не удался'),
        'отказ описания не записан отказом входа'
    )
    t.assert_not(
        helper.journal.logged('шаг работника получателя сорвался'),
        'отказ описания сорвал шаг'
    )
end

g.test_enter_refusals_on_each_step = function()
    local steps = { 'CreateTopics', 'FindCoordinator', 'SyncGroup', 'OffsetFetch', 'ListOffsets', 'Metadata' }

    for _, step in ipairs(steps) do
        local remaining = 1

        helper.journal.forget()
        local fake = helper.serve({
            respond = function(name, values)
                if name ~= step or remaining == 0 then
                    return nil
                end

                if step == 'Metadata' and values.topics ~= nil then
                    return nil
                end

                remaining = remaining - 1

                return 'close'
            end,
        })
        local broker = helper.client(fake)
        local orders = broker:declare('orders')
        local done = false

        -- Получатель раньше отправки: тогда темы заводит вход, и отказ
        -- заведения приходится на него.
        local consumer = orders:consume(function()
            done = true
        end)

        helper.until_true(function()
            return remaining == 0
        end)
        orders:send({ order = 1 })

        t.assert(
            helper.until_true(function()
                return done
            end),
            ('вход не пережил отказ %s'):format(step)
        )
        -- Отказ доходит до входа целым: пишется отказом входа, а не сбоем
        -- шага работника.
        t.assert(
            helper.journal.logged('вход в группу не удался'),
            ('отказ %s не записан отказом входа'):format(step)
        )
        t.assert_not(
            helper.journal.logged('шаг работника получателя сорвался'),
            ('отказ %s сорвал шаг'):format(step)
        )
        consumer:stop()
        helper.until_true(function()
            return consumer.workers == 0
        end)
        broker:close()
        fake.stop()
    end
end

g.test_forgotten_member_enters_again_as_a_newcomer = function()
    local refused = false
    local broker, fake = started({
        respond = function(name, values)
            if name == 'JoinGroup' and values.member_id ~= '' and not refused then
                refused = true

                return { error_code = 25, member_id = values.member_id }
            end
        end,
    })
    acked_once(broker, 'сообщение не обработано')

    local joins = fake.requests.JoinGroup

    t.assert(refused)
    t.assert_not_equals(joins[2].member_id, '')
    t.assert_equals(joins[3].member_id, '')
end

g.test_sync_refusal_keeps_the_member_and_the_coordinator = function()
    local refused = false
    local broker, fake = started({
        respond = function(name)
            if name == 'SyncGroup' and not refused then
                refused = true

                return { error_code = 30 }
            end
        end,
    })
    local orders = acked_once(broker, 'сообщение не обработано')

    local joins = fake.requests.JoinGroup

    t.assert(refused)
    t.assert_equals(orders.counts.failures, 1)
    t.assert_str_contains(
        helper.journal.find('вход в группу не удался').line,
        'GROUP_AUTHORIZATION_FAILED'
    )
    t.assert_equals(#fake.requests.FindCoordinator, 1)
    t.assert_equals(joins[3].member_id, joins[2].member_id)
end

g.test_enter_waits_for_the_rebalance_and_the_session_together = function()
    -- Брокер держит вход, пока собирает участников: срок входа — срок
    -- раздачи и стука вместе, 0,5 + 0,3 с, а вход здесь идёт 0,4 с.
    local broker = started({
        respond = function(name, values)
            if name == 'JoinGroup' and values.member_id ~= '' then
                fiber.sleep(0.4)
            end
        end,
    }, { rebalance_timeout = 0.5 })
    local orders = acked_once(broker, 'вход не дождался брокера')
    t.assert_equals(orders.counts.failures, 0)
end

g.test_skip_that_did_not_commit_is_retried = function()
    local refused = false
    local broker, fake = started({
        topics = { orders = 1, ['orders.retry'] = 1, ['orders.dead'] = 1 },
        respond = function(name, values)
            if name == 'OffsetCommit' and not refused and values.topics[1].partitions[1].committed_offset == 1 then
                refused = true

                return {
                    topics = { { name = 'orders', partitions = { { partition_index = 0, error_code = 25 } } } },
                }
            end
        end,
    })
    local orders = broker:declare('orders')
    local batch = helper.batch({ value = '{}' })
    local summed = helper.wire.int16(1) .. batch:sub(24)

    helper.append(fake, 'orders', 0, batch:sub(1, 17) .. helper.wire.int32(helper.records.crc32c(summed)) .. summed)
    consume(orders, function() end)

    await('пачка не пропущена', function()
        return orders.counts.skipped == 1
    end)
    t.assert(refused)
    t.assert(helper.journal.logged('пропуск сжатой пачки не зафиксирован'))
end

g.test_two_workers_share_partitions = function()
    local broker, fake = started({ partitions = 2 })
    local orders = broker:declare('orders', { partitions = 2 })
    local by_fiber = {}

    for index = 1, 6 do
        orders:send({ n = index }, { key = ('k%d'):format(index) })
    end

    local consumer = consume(orders, function()
        local name = require('fiber').self():name()

        by_fiber[name] = (by_fiber[name] or 0) + 1
    end, { workers = 2 })

    await('сообщения не обработаны', function()
        return orders.counts.ack == 6
    end, 10)

    local workers = 0

    for _ in pairs(by_fiber) do
        workers = workers + 1
    end

    for name in pairs(by_fiber) do
        t.assert_items_include({ 'kafka/orders/1', 'kafka/orders/2' }, { name })
    end

    t.assert_equals(consumer.workers, 2)
    t.assert_equals(orders:status().workers, 2)
    t.assert(fake.rounds >= 1)
    t.assert(workers >= 1)

    consumer:stop()
    await('работники не остановились', function()
        return consumer.workers == 0
    end)
    t.assert_equals(#fake.left, 2)
end

g.test_rebalance_by_heartbeat_rejoins = function()
    local broker, fake = started()
    local orders = broker:declare('orders')
    local handled = 0

    orders:send({ order = 1 })
    consume(orders, function()
        handled = handled + 1
    end)

    await('первое сообщение не обработано', function()
        return handled == 1
    end)

    local rounds = fake.rounds

    helper.rebalance(fake, 'orders')

    await('участник не вошёл заново по стуку', function()
        return fake.rounds > rounds
    end)

    orders:send({ order = 2 })
    await('второе сообщение не обработано', function()
        return handled == 2
    end)
end

g.test_lost_coordinator_by_heartbeat = function()
    local lost = false
    local broker, fake = started({
        respond = function(name)
            if name == 'Heartbeat' and lost then
                lost = false

                return { error_code = 16 }
            end
        end,
    })
    local orders = broker:declare('orders')

    consume(orders, function() end)
    await('участник не вошёл', function()
        return fake.rounds >= 1
    end)

    local finds = #fake.requests.FindCoordinator

    lost = true
    await('ведущий группы не спрошен заново', function()
        return #fake.requests.FindCoordinator > finds
    end)

    orders:send({ order = 1 })
    await('сообщение не обработано', function()
        return orders.counts.ack == 1
    end)
end

g.test_heartbeat_troubles_do_not_rejoin_when_passing = function()
    local broker, fake = started({
        respond = function(name)
            if name == 'Heartbeat' then
                return { error_code = 7 }
            end
        end,
    })
    local orders = broker:declare('orders')

    consume(orders, function() end)
    await('участник не вошёл', function()
        return fake.rounds >= 1
    end)

    local rounds = fake.rounds

    await('стук не шёл', function()
        return #(fake.requests.Heartbeat or {}) >= 2
    end)
    t.assert_equals(fake.rounds, rounds)
end

g.test_join_carries_the_session_and_rebalance_timeouts = function()
    local broker, fake = started(nil, { session_timeout = 3, rebalance_timeout = 7 })
    local orders = broker:declare('orders')

    consume(orders, function() end)
    await('участник не вошёл', function()
        return fake.rounds >= 1
    end)

    t.assert(#fake.requests.JoinGroup >= 2, 'вход идёт новичком, затем с номером')

    for _, values in ipairs(fake.requests.JoinGroup) do
        t.assert_equals({ values.session_timeout_ms, values.rebalance_timeout_ms }, { 3000, 7000 })
    end
end

g.test_heartbeat_sleeps_a_third_of_the_session_and_keeps_beating = function()
    local slept = {}

    -- Сон работника записывается и укорачивается: стук раз в секунду
    -- растянул бы проверку, а шаг стука виден только по просьбе поспать.
    helper.worker._set_source({
        sleep = function(seconds)
            slept[seconds] = true
            fiber.sleep(math.min(seconds, 0.01))
        end,
    })

    local broker, fake = started(nil, { session_timeout = 3 })
    local orders = broker:declare('orders')

    consume(orders, function() end)
    await('стук не шёл после удачного', function()
        return #(fake.requests.Heartbeat or {}) >= 3
    end)

    t.assert(slept[1], 'стук спит треть срока сессии')
    t.assert_not(slept[1.5], 'стук спит не половину срока сессии')

    for _, values in ipairs(fake.requests.Heartbeat) do
        t.assert_not_equals(values.generation_id, -1)
        t.assert_not_equals(values.member_id, '')
    end
end

g.test_member_that_never_joined_neither_beats_nor_leaves = function()
    local beats = 0

    helper.worker._set_source({
        sleep = function()
            beats = beats + 1
            fiber.sleep(0.01)
        end,
    })

    local broker, fake = started({
        respond = function(name, values)
            -- Новичку брокер выдаёт номер, а участника с номером не пускает.
            if name == 'JoinGroup' and values.member_id ~= '' then
                return { error_code = 30, member_id = values.member_id }
            end
        end,
    })
    local orders = broker:declare('orders')
    local consumer = consume(orders, function() end)

    await('стук не спал', function()
        return beats >= 5
    end)
    t.assert(orders.counts.failures >= 1, 'вход не отказал')

    consumer:stop()
    await('работник не остановился', function()
        return consumer.workers == 0
    end)

    t.assert_equals(fake.requests.Heartbeat, nil)
    t.assert_equals(fake.requests.LeaveGroup, nil)
    t.assert_equals(fake.left, {})
end

--- Стук, отвергнутый брокером, обязан позвать вход заново сам: сообщений
--- нет, и позвать вход больше некому.
---@param answer table|string Ответ двойника на стук: код либо `close`
local function rejoins_after_heartbeat(answer)
    local armed = false
    local broker, fake = started({
        respond = function(name)
            if name == 'JoinGroup' then
                armed = false
            elseif name == 'Heartbeat' and armed then
                return answer
            end
        end,
    })

    consume(broker:declare('orders'), function() end)
    await('участник не вошёл', function()
        return fake.rounds >= 1 and #(fake.requests.Heartbeat or {}) >= 1
    end)

    local joins = #fake.requests.JoinGroup

    armed = true
    await(
        ('стук с ответом %s не позвал вход заново'):format(json.encode(answer)),
        function()
            return #fake.requests.JoinGroup > joins
        end
    )
end

g.test_heartbeat_to_a_moved_coordinator_rejoins = function()
    -- «Ведущий сменился» входа сам не требует, но стук, так и не дошедший
    -- до ведущего за свой срок, — участник, которого группа вот-вот
    -- сочтёт умершим.
    rejoins_after_heartbeat({ error_code = 16 })
end

g.test_heartbeat_during_a_rebalance_rejoins = function()
    rejoins_after_heartbeat({ error_code = 27 })
end

g.test_heartbeat_that_never_reached_the_coordinator_rejoins = function()
    -- Кода брокера нет вовсе: связь рвётся на каждом стуке до срока.
    rejoins_after_heartbeat('close')
end

g.test_worker_without_partitions_waits_for_them = function()
    local slept = {}

    helper.worker._set_source({
        sleep = function(seconds)
            local name = fiber.self():name()

            slept[name] = slept[name] or {}
            slept[name][seconds] = true
            fiber.sleep(math.min(seconds, 0.01))
        end,
    })

    -- Два раздела на трёх работников: один остаётся без разделов, и ему
    -- выбирать нечего — он обязан спать, а не крутить шаги без уступки.
    local broker = started({ topics = { orders = 1, ['orders.retry'] = 1, ['orders.dead'] = 1 } })
    local orders = broker:declare('orders')

    orders:send({ order = 1 })
    consume(orders, function() end, { workers = 3 })

    await('сообщение не обработано', function()
        return orders.counts.ack == 1
    end)
    await('работник без разделов не ждал выборки', function()
        for _, seconds in pairs(slept) do
            if seconds[helper.fetch.POLL] then
                return true
            end
        end

        return false
    end)
end

g.test_paused_lane_resumes_on_the_very_moment = function()
    local refused = false
    local broker, fake = started({
        respond = function(name, values)
            -- Первая отметка выдачи не ложится: раздел отдыхает `PAUSE`.
            if name == 'OffsetCommit' and not refused and values.topics[1].partitions[1].committed_metadata ~= '' then
                refused = true

                return {
                    topics = { { name = 'orders', partitions = { { partition_index = 0, error_code = 30 } } } },
                }
            end
        end,
    })
    local orders = broker:declare('orders')

    orders:send({ order = 1 })

    -- Часы получателя стоят, а пауза раздела нулевая: раздел отдыхает
    -- ровно до «сейчас» и обязан быть готов в тот же миг.
    helper.handle.PAUSE = 0
    helper.clock.monotonic = function()
        return 100
    end

    consume(orders, function() end)

    local ok = helper.until_true(function()
        return orders.counts.ack == 1
    end)

    helper.clock.monotonic = real_monotonic
    t.assert(refused, 'отметка выдачи не отказала')
    t.assert(ok, 'раздел не вернулся к работе на миге конца паузы')
    t.assert_equals(orders.counts.failures, 1)
    t.assert(fake.rounds >= 1, 'участник не вошёл в группу')
end

g.test_json_of_sent_body_is_what_handler_sees = function()
    local broker = started()
    local orders = broker:declare('orders')
    local seen ---@type any

    orders:send({ list = { 1, 2 }, map = { a = 'б' }, flag = false })
    consume(orders, function(delivered)
        seen = delivered.body
    end)

    await('обработчик не позван', function()
        return seen ~= nil
    end)
    t.assert_equals(seen, { list = { 1, 2 }, map = { a = 'б' }, flag = false })
    t.assert_equals(json.decode(json.encode(seen)), seen)
end
