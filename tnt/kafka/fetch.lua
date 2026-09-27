--- Выборка: записи для разделов участника у их ведущих.
---
--- Выбираются разделы с пустым запасом и без отсрочки, узел за узлом:
--- у каждого узла одна выборка на все его разделы, с ожиданием данных
--- до `POLL`, — пустая тема не крутит цикл работника.
---
--- Уровень чтения — «только зафиксированное»: записи транзакций, которые
--- ещё не закончены, брокер не отдаёт, а отменённые отсеивает разбор
--- (`tnt.kafka.records`). Смещение, стёртое хранением темы
--- (`OFFSET_OUT_OF_RANGE`), — начало раздела заново с записью в журнал:
--- стёртого не вернуть, а стоять на нём незачем.

local fiber = require('fiber')

local api = require('tnt.kafka.api')
local clock = require('tnt.clock')
local codes = require('tnt.kafka.codes')
local external = require('tnt.external')
local offsets = require('tnt.kafka.offsets')
local records = require('tnt.kafka.records')
local within = require('tnt.storage.within')

local journal = require('tnt.log')

--- Журнал пакета; отказы выборки пишутся с подавлением повторов.
local log, steps = journal.new('tnt.kafka'), journal.changes('tnt.kafka')

local Module = {}

--- Сколько брокер держит выборку в ожидании данных, секунд.
Module.POLL = 0.5

--- Сколько раздел отдыхает после отказа выборки, секунд.
Module.PAUSE = 0.5

--- Читать только зафиксированное в транзакциях.
local READ_COMMITTED = 1

--- Внешние средства: сон.
local source = external.install(Module, { sleep = fiber.sleep })

--- Кладёт выборку раздела в его запас.
---
--- Следующая выборка начнётся за последней увиденной пачкой — и видимой,
--- и отсеянной: служебная пачка в конце раздела иначе выбиралась бы
--- снова и снова. Запись раньше `position` пропускается: брокер отдаёт
--- пачку целиком, а смещение бывает посреди неё.
---@param lane TntKafkaLane
---@param reply table Ответ выборки по разделу
local function stock(lane, reply)
    local batches = records.decode(reply.records)
    local after = lane.position

    for _, batch in ipairs(records.visible(batches, reply.aborted_transactions)) do
        if batch.codec ~= nil then
            if batch.last_offset >= lane.position then
                table.insert(lane.buffer, { offset = batch.base_offset, last = batch.last_offset, codec = batch.codec })
            end
        else
            for _, record in ipairs(batch.records) do
                if record.offset >= lane.position then
                    table.insert(lane.buffer, record)
                end
            end
        end
    end

    for _, batch in ipairs(batches) do
        after = math.max(after, batch.last_offset + 1)
    end

    lane.position = after
end

--- Разбирает ответ выборки по разделам одного узла.
---@param consumer TntKafkaConsumer
---@param lanes table<string, TntKafkaLane> Разделы по «тема/номер»
---@param answer table
local function sorted(consumer, lanes, answer)
    local cluster = consumer.topic.broker.cluster

    for _, topic in ipairs(answer.responses) do
        for _, reply in ipairs(topic.partitions) do
            local lane = lanes[('%s/%d'):format(topic.topic, reply.partition_index)]
            local code = reply.error_code

            if code == codes.NONE then
                local ok, err = pcall(stock, lane, reply)

                if not ok then
                    steps.warn(
                        'выборка раздела не разобрана',
                        { destination = topic.topic, err = tostring(err) }
                    )
                    lane.resume = clock.monotonic() + Module.PAUSE
                end
            elseif code == codes.OFFSET_OUT_OF_RANGE then
                -- Хранение темы стёрло то, что не прочитано: начало заново.
                log.warn(
                    'смещение раздела вне журнала: непрочитанное стёрто хранением темы',
                    {
                        destination = topic.topic,
                        partition = reply.partition_index,
                        offset = lane.position,
                    }
                )
                lane.position = offsets.NONE
            else
                if codes.of(code).stale then
                    cluster:forget(topic.topic)
                end

                steps.warn(
                    'выборка раздела не удалась',
                    { destination = topic.topic, err = codes.name(code) }
                )
                lane.resume = clock.monotonic() + Module.PAUSE
            end
        end
    end
end

--- Выбирает записи для разделов с пустым запасом, узел за узлом.
---@param consumer TntKafkaConsumer
---@param station TntKafkaStation
---@return boolean fetched Была ли выборка
function Module.fetch(consumer, station)
    local cluster = consumer.topic.broker.cluster
    local settings = cluster.settings
    local deadline = within.deadline(Module.POLL + consumer.topic.broker.limits.timeout)
    local now = clock.monotonic()
    local by_node, lanes = {}, {}

    for _, lane in ipairs(station.lanes) do
        if #lane.buffer == 0 and (lane.resume == nil or lane.resume <= now) then
            local meta = cluster:topic(lane.topic, deadline)
            local leader = meta and cluster:leader(meta, lane.partition)

            if lane.position == offsets.NONE and meta ~= nil then
                lane.position = offsets.earliest(cluster, meta, lane.partition, deadline) or offsets.NONE
            end

            if leader ~= nil and lane.position ~= offsets.NONE then
                local where = ('%s:%d'):format(leader.host, leader.port)

                by_node[where] = by_node[where] or { address = leader, topics = {} }
                by_node[where].topics[lane.topic] = by_node[where].topics[lane.topic] or {}
                table.insert(by_node[where].topics[lane.topic], {
                    partition = lane.partition,
                    fetch_offset = lane.position,
                    partition_max_bytes = settings.max_bytes,
                })
                lanes[('%s/%d'):format(lane.topic, lane.partition)] = lane
            end
        end
    end

    local fetched = false

    for _, node in pairs(by_node) do
        local topics = {}

        for name, partitions in pairs(node.topics) do
            topics[#topics + 1] = { topic = name, partitions = partitions }
        end

        -- Приговор повтору выборке не нужен: её отказ только пишется,
        -- а следующая выборка идёт своим шагом работника.
        local answer, err = cluster:exchange(node.address, api.FETCH, {
            replica_id = -1,
            max_wait_ms = math.floor(Module.POLL * 1000),
            min_bytes = 1,
            max_bytes = settings.max_bytes,
            isolation_level = READ_COMMITTED,
            topics = topics,
        }, deadline)

        fetched = true

        if answer == nil then
            steps.warn('выборка не удалась', { destination = consumer.topic.name, err = tostring(err) })
            source().sleep(Module.PAUSE)
        else
            sorted(consumer, lanes, answer)
        end
    end

    return fetched
end

return Module
