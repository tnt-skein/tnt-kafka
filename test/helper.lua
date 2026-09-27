--- Общие средства проверок клиента Kafka.
---
--- Двойник брокера — настоящий сокет `tcp_server` на петле, говорящий
--- протоколом Kafka нашим же кодеком: сроки, обрыв посреди ответа,
--- закрытое в простое соединение и отмена файбера держатся на ядре, и
--- подделка сокета доказала бы только, что мы правильно разговариваем
--- сами с собой. Поведение настоящей Kafka проверяет `kafka_live_test.lua`.
---
--- Двойник ведёт себя как маленький кластер из одного узла: заводит темы,
--- выдаёт номер производителя и сверяет счёт его записей, кладёт пачки
--- в журнал раздела и отдаёт их выборкой, ведёт группы — вход кругами,
--- раздачу, стук и смещения. Чего он не умеет — задаёт проверка:
--- `respond` получает каждый пришедший запрос и вправе ответить чем
--- угодно, промолчать либо закрыть соединение.
---
--- Исходники читаются с диска, а не через `require`: у Tarantool свой
--- загрузчик `.rocks`, он идёт раньше `package.path` и подсунул бы
--- установленную копию пакета, если она есть. Проверки тогда шли бы
--- против вчерашнего кода, а покрытие считалось бы по нему. Зависимости
--- пакета — `tnt.must`, `tnt.clock`, `tnt.context`, `tnt.id`, `tnt.log`,
--- `tnt.pool`, `tnt.retry`, `tnt.external`, `tnt.storage`, `tnt.tls` —
--- берутся из `.rocks` обычным `require`: проверяется этот пакет, а не они.
---
--- Оснастка в `test/testing/` — загрузчик исходников, ловушка журнала,
--- запись файлов и временный узел — грузится так же, файлами, и один раз
--- на процесс: второй экземпляр загрузчика не знал бы, что вытеснил
--- первый, и не вернул бы вытесненное на место.
---
--- Проверки берут всё через этот помощник, а не из оснастки напрямую:
--- помощник — единственное, чем файл проверок отличается от того же файла
--- в наборе, где пакет живёт рядом со своими зависимостями.

local fiber = require('fiber')
local fio = require('fio')
local socket = require('socket')
local t = require('luatest')

--- Модули оснастки в порядке зависимостей: узел берёт файлы и загрузчик,
--- ловушка журнала — загрузчик.
local TESTING = {
    { name = 'tnt.testing.sources', path = 'test/testing/sources.lua' },
    { name = 'tnt.testing.files', path = 'test/testing/files.lua' },
    { name = 'tnt.testing.journal', path = 'test/testing/journal.lua' },
    { name = 'tnt.testing.node', path = 'test/testing/node.lua' },
}

for _, module in ipairs(TESTING) do
    if package.loaded[module.name] == nil then
        local chunk, failure = loadfile(fio.abspath(module.path))

        if chunk == nil then
            error(('оснастка %s не читается: %s'):format(module.name, tostring(failure)))
        end

        package.loaded[module.name] = chunk()
    end
end

--- Оснастка проверок под теми именами, что зовёт помощник.
local testing = {
    load_sources = package.loaded['tnt.testing.sources'].load,
    module = package.loaded['tnt.testing.sources'].module,
    capture_log = package.loaded['tnt.testing.journal'].capture,
    start_node = package.loaded['tnt.testing.node'].start,
    stop_node = package.loaded['tnt.testing.node'].stop,
}

--- Средства проверок клиента Kafka.
---
--- Части пакета и двойник отдаются без точных типов: проверки читают
--- поля отказа и ответа, не сверяя их с пустотой, — пустое поле само
--- уронит проверку.
---@class TntKafkaTestHelper
---@field kafka table Фасад пакета
---@field wire table
---@field schema table
---@field coordination table
---@field api table
---@field records table
---@field codes table
---@field link table
---@field connect table
---@field settings table
---@field cluster table
---@field admin table
---@field producer table
---@field message table
---@field outcome table
---@field hook table
---@field offsets table
---@field group table
---@field handle table
---@field enter table
---@field fetch table
---@field worker table
---@field kick table
---@field topic table
---@field context table Контекст файбера
---@field failure table Отказ хранилища
---@field within table Срок вызова
---@field clock table Часы пакета
---@field journal table Ловушка журнала
local helper = {
    --- Модули пакета в порядке зависимостей.
    MODULES = {
        { name = 'tnt.kafka.wire', path = 'tnt/kafka/wire.lua' },
        { name = 'tnt.kafka.schema', path = 'tnt/kafka/schema.lua' },
        { name = 'tnt.kafka.coordination', path = 'tnt/kafka/coordination.lua' },
        { name = 'tnt.kafka.api', path = 'tnt/kafka/api.lua' },
        { name = 'tnt.kafka.records', path = 'tnt/kafka/records.lua' },
        { name = 'tnt.kafka.codes', path = 'tnt/kafka/codes.lua' },
        { name = 'tnt.kafka.link', path = 'tnt/kafka/link.lua' },
        { name = 'tnt.kafka.connect', path = 'tnt/kafka/connect.lua' },
        { name = 'tnt.kafka.settings', path = 'tnt/kafka/settings.lua' },
        { name = 'tnt.kafka.cluster', path = 'tnt/kafka/cluster.lua' },
        { name = 'tnt.kafka.admin', path = 'tnt/kafka/admin.lua' },
        { name = 'tnt.kafka.producer', path = 'tnt/kafka/producer.lua' },
        { name = 'tnt.kafka.message', path = 'tnt/kafka/message.lua' },
        { name = 'tnt.kafka.outcome', path = 'tnt/kafka/outcome.lua' },
        { name = 'tnt.kafka.hook', path = 'tnt/kafka/hook.lua' },
        { name = 'tnt.kafka.offsets', path = 'tnt/kafka/offsets.lua' },
        { name = 'tnt.kafka.group', path = 'tnt/kafka/group.lua' },
        { name = 'tnt.kafka.handle', path = 'tnt/kafka/handle.lua' },
        { name = 'tnt.kafka.enter', path = 'tnt/kafka/enter.lua' },
        { name = 'tnt.kafka.fetch', path = 'tnt/kafka/fetch.lua' },
        { name = 'tnt.kafka.worker', path = 'tnt/kafka/worker.lua' },
        { name = 'tnt.kafka.kick', path = 'tnt/kafka/kick.lua' },
        { name = 'tnt.kafka.topic', path = 'tnt/kafka/topic.lua' },
        { name = 'tnt.kafka', path = 'tnt/kafka.lua' },
    },
}

--- Значение мимо проверки типов: негодный аргумент нарочно.
---@param value any
---@return any
function helper.wrong(value)
    return value
end

--- Сверяет, что каждый вызов бросает названный отказ и винит строку
--- вызова в файле проверок, а не внутри пакета.
---
--- Вызов стоит в замыкании первой строкой тела, то есть строкой ниже
--- слова `function`: место броска сверяется с ней целиком — файлом,
--- строкой и текстом.
---@param cases table[] Пары: замыкание с вызовом и текст броска
function helper.assert_blamed(cases)
    for _, case in ipairs(cases) do
        local _, err = pcall(case[1])
        local info = debug.getinfo(case[1], 'S') --[[@as { short_src: string, linedefined: integer }]]

        t.assert_equals(err, ('%s:%d: %s'):format(info.short_src, info.linedefined + 1, case[2]))
    end
end

--- Фасад пакета из исходников.
helper.kafka = testing.load_sources(helper.MODULES, 'tnt.kafka')

-- Части пакета берутся из той же загрузки, что и фасад: взятые `require`,
-- они пришли бы установленной копией из `.rocks`. Контекст, отказ, срок
-- и часы — зависимости, и они те же, что зовёт пакет.
for _, module in ipairs(helper.MODULES) do
    local part = module.name:match('^tnt%.kafka%.(.+)$')

    if part ~= nil then
        helper[part] = testing.module(module.name)
    end
end

helper.context = require('tnt.context')
helper.failure = require('tnt.storage.failure')
helper.within = require('tnt.storage.within')

--- Часы пакета: проверки на стоящих часах подменяют их поля и обязаны
--- вернуть назад.
helper.clock = require('tnt.clock')

--- Ловушка журнала: записи о зарытом, входе в группу и сорвавшемся
--- крюке видны только ей.
helper.journal = testing.capture_log()

--- Возвращает пакету настоящие сеть, сон, часы и транзакцию и снимает
--- крюки: проверки подменяют их и обязаны вернуть назад.
function helper.restore()
    local seamed = { helper.link, helper.worker, helper.enter, helper.fetch, helper.topic, helper.within }

    for index = 1, #seamed do
        seamed[index]._set_source(nil)
    end

    local names = helper.hook.names()

    while #names > 0 do
        helper.hook.set(table.remove(names), nil)
    end
end

--- Запросы каталога по номеру: так двойник узнаёт, что пришло.
---@type table<integer, table>
local CATALOG = {}

for _, source in ipairs({ helper.api, helper.coordination }) do
    for _, entry in pairs(source) do
        if type(entry) == 'table' and entry.key ~= nil then
            CATALOG[entry.key] = entry
        end
    end
end

helper.CATALOG = CATALOG

--- Роды полей, которым двойник сам ставит ноль, если ответ их не назвал.
local ZERO = { int8 = 0, int16 = 0, int32 = 0, int64 = 0, boolean = false }

--- Дополняет ответ нулями по описанию: ответу двойника и проверки
--- незачем называть паузы и сдвиги, которых клиент не читает.
---@param fields table[]
---@param values table
---@return table
local function completed(fields, values)
    for _, field in ipairs(fields) do
        local name, kind = field[1], field[2]

        if type(kind) == 'table' then
            for _, entry in ipairs(values[name] or {}) do
                completed(kind, entry)
            end
        elseif values[name] == nil and ZERO[kind] ~= nil then
            values[name] = ZERO[kind]
        end
    end

    return values
end

--- Пишет ответ двойника: рамка, номер обращения и поля.
---@param peer any
---@param request table Запрос из каталога
---@param correlation integer
---@param values table
function helper.answer(peer, request, correlation, values)
    local full = completed(request.response, values)
    local body = helper.wire.int32(correlation) .. helper.schema.encode(request.response, full)

    peer:write(helper.wire.int32(#body) .. body)
end

--- Читает запрос клиента; пустота — молчание либо конец потока.
---@param peer any
---@param timeout number
---@return table|nil request `{ api, correlation, client, values }`
---@return boolean finished Поток кончился
local function incoming(peer, timeout)
    local head = peer:read({ chunk = 4 }, timeout)

    if head == nil or #head < 4 then
        return nil, head ~= nil
    end

    local size = helper.wire.reader(head):int32()
    local body = peer:read({ chunk = size }, 1)

    if body == nil or #body < size then
        return nil, true
    end

    local reader = helper.wire.reader(body)
    local key, _, correlation, client = reader:int16(), reader:int16(), reader:int32(), reader:string()
    local request = CATALOG[key]

    return {
        api = request,
        correlation = correlation,
        client = client,
        values = helper.schema.decode(request.request, reader),
    },
        false
end

--- Раздел двойника по теме и номеру.
---@param fake table
---@param topic string
---@param partition integer
---@return table|nil
local function partition_of(fake, topic, partition)
    local found = fake.topics[topic]

    return found and found[partition + 1]
end

--- Заводит тему в двойнике.
---@param fake table
---@param name string
---@param count integer
function helper.create(fake, name, count)
    local partitions = {}

    for index = 1, count do
        partitions[index] = { batches = {}, next = 0, start = 0 }
    end

    fake.topics[name] = partitions
end

--- Кладёт пачку в раздел: смещение первой записи ставит двойник.
---@param fake table
---@param topic string
---@param partition integer
---@param batch string Пачка, собранная `records.encode`
---@return integer base Смещение первой записи
function helper.append(fake, topic, partition, batch)
    local slot = partition_of(fake, topic, partition) --[[@as table]]
    local count = #helper.records.decode(batch)[1].records
    local base = slot.next

    table.insert(slot.batches, {
        base = base,
        last = base + math.max(count, 1) - 1,
        bytes = helper.wire.int64(base) .. batch:sub(9),
    })
    slot.next = base + math.max(count, 1)

    return base
end

--- Описание темы в ответе `Metadata`.
---@param fake table
---@param name string
---@return table
local function described(fake, name)
    local partitions = fake.topics[name]

    if partitions == nil then
        return { error_code = 3, name = name, is_internal = false, partitions = {} }
    end

    local listed = {}

    for index = 1, #partitions do
        listed[index] = {
            error_code = 0,
            partition_index = index - 1,
            leader_id = fake.leader or 1,
            replica_nodes = { 1 },
            isr_nodes = { 1 },
            offline_replicas = {},
        }
    end

    return { error_code = 0, name = name, is_internal = false, partitions = listed }
end

--- Группа двойника; заводится при первом обращении.
---@param fake table
---@param id string
---@return table
local function group_of(fake, id)
    fake.groups[id] = fake.groups[id]
        or {
            members = {},
            generation = 0,
            state = 'empty',
            offsets = {},
            cond = fiber.cond(),
            joined = {},
        }

    return fake.groups[id]
end

--- Завершает круг входа: кто не вошёл — выбывает, поколение растёт.
---@param fake table
---@param group table
local function complete(fake, group)
    for member in pairs(group.members) do
        if not group.joined[member] then
            group.members[member] = nil
        end
    end

    local order = {}

    for member in pairs(group.members) do
        order[#order + 1] = member
    end

    table.sort(order)

    group.generation = group.generation + 1
    group.leader = order[1]
    group.state = 'syncing'
    group.assignments = nil
    group.joined = {}
    fake.rounds = fake.rounds + 1
    group.cond:broadcast()
end

--- Открывает круг входа: известные участники узнают о нём стуком
--- и входят заново; круг ждёт всех, но не дольше `round` секунд, затем
--- раздаёт поколение.
---@param fake table
---@param id string Группа
function helper.rebalance(fake, id)
    local group = group_of(fake, id)

    group.state = 'joining'
    group.joined = {}

    fiber.create(function()
        local deadline = fiber.clock() + fake.round
        local earliest = fiber.clock() + 0.05

        while group.state == 'joining' and fiber.clock() < deadline do
            local all = next(group.joined) ~= nil

            for known in pairs(group.members) do
                all = all and group.joined[known] == true
            end

            if all and fiber.clock() > earliest then
                break
            end

            fiber.sleep(0.01)
        end

        -- Круг мог закрыть уход участника раньше срока: состояние меняют
        -- соседние файберы, и проверка их не видит.
        ---@diagnostic disable-next-line: unnecessary-if
        if group.state ~= 'syncing' then
            complete(fake, group)
        end
    end)
end

--- Вход в группу кругом.
---@param fake table
---@param values table
---@return table
local function join(fake, values)
    local group = group_of(fake, values.group_id)
    local member = values.member_id

    if member == '' then
        fake.members = fake.members + 1

        return {
            error_code = 79,
            generation_id = -1,
            protocol_name = '',
            leader = '',
            member_id = ('member-%d'):format(fake.members),
            members = {},
        }
    end

    local generation = group.generation

    if group.state ~= 'joining' then
        helper.rebalance(fake, values.group_id)
    end

    group.members[member] = { metadata = values.protocols[1].metadata }
    group.joined[member] = true

    while group.generation == generation do
        group.cond:wait(0.1)
    end

    if group.members[member] == nil then
        return {
            error_code = 25,
            generation_id = -1,
            protocol_name = '',
            leader = '',
            member_id = member,
            members = {},
        }
    end

    local members = {}

    if member == group.leader then
        for id, entry in pairs(group.members) do
            members[#members + 1] = { member_id = id, metadata = entry.metadata }
        end
    end

    return {
        error_code = 0,
        generation_id = group.generation,
        protocol_name = 'roundrobin',
        leader = group.leader,
        member_id = member,
        members = members,
    }
end

--- Раздача: ведущий кладёт её, остальные ждут своей доли.
---@param fake table
---@param values table
---@return table
local function sync(fake, values)
    local group = group_of(fake, values.group_id)

    if group.members[values.member_id] == nil then
        return { error_code = 25 }
    end

    if values.generation_id ~= group.generation then
        return { error_code = 22 }
    end

    if values.member_id == group.leader then
        group.assignments = {}

        for _, entry in ipairs(values.assignments) do
            group.assignments[entry.member_id] = entry.assignment
        end

        group.state = 'stable'
        group.cond:broadcast()
    end

    local deadline = fiber.clock() + 1

    while group.assignments == nil and fiber.clock() < deadline do
        group.cond:wait(0.05)
    end

    if group.assignments == nil then
        return { error_code = 27 }
    end

    return { error_code = 0, assignment = group.assignments[values.member_id] or '' }
end

--- Код стука участника.
---@param group table
---@param values table
---@return integer
local function beat(group, values)
    if group.members[values.member_id] == nil then
        return 25
    end

    if group.state == 'joining' then
        return 27
    end

    if values.generation_id ~= group.generation then
        return 22
    end

    return 0
end

--- Фиксация смещений: вне поколения — только у пустой группы.
---@param fake table
---@param values table
---@return table
local function commit(fake, values)
    local group = group_of(fake, values.group_id)
    local code = 0

    if values.generation_id ~= -1 then
        code = beat(group, values)

        -- Пока идёт вход, фиксация от прежнего поколения ещё принимается.
        if code == 27 then
            code = 0
        end
    end

    local topics = {}

    for _, topic in ipairs(values.topics) do
        local partitions = {}

        for _, entry in ipairs(topic.partitions) do
            if code == 0 then
                group.offsets[('%s/%d'):format(topic.name, entry.partition_index)] = {
                    offset = entry.committed_offset,
                    metadata = entry.committed_metadata,
                }
                fake.commits[#fake.commits + 1] = {
                    group = values.group_id,
                    topic = topic.name,
                    partition = entry.partition_index,
                    offset = entry.committed_offset,
                    metadata = entry.committed_metadata,
                }
            end

            partitions[#partitions + 1] = { partition_index = entry.partition_index, error_code = code }
        end

        topics[#topics + 1] = { name = topic.name, partitions = partitions }
    end

    return { topics = topics }
end

--- Запись пачки со сверкой счёта производителя.
---@param fake table
---@param values table
---@return table
local function produce(fake, values)
    local responses = {}

    for _, topic in ipairs(values.topic_data) do
        local replies = {}

        for _, entry in ipairs(topic.partition_data) do
            local reply =
                { index = entry.index, error_code = 0, base_offset = -1, log_append_time_ms = -1, log_start_offset = 0 }
            local slot = partition_of(fake, topic.name, entry.index)
            local reader = helper.wire.reader(entry.records)

            reader:raw(8 + 4 + 4 + 1 + 4 + 2 + 4 + 8 + 8)

            local producer, _, sequence = reader:int64(), reader:int16(), reader:int32()
            local key = ('%d/%s/%d'):format(producer, topic.name, entry.index)
            local expected = fake.sequences[key] or 0

            if slot == nil then
                reply.error_code = 3
            elseif sequence == expected - 1 then
                reply.error_code = 46
            elseif sequence ~= expected then
                reply.error_code = 45
            else
                reply.base_offset = helper.append(fake, topic.name, entry.index, entry.records)
                fake.sequences[key] = expected + 1
                fake.produced[#fake.produced + 1] = {
                    topic = topic.name,
                    partition = entry.index,
                    offset = reply.base_offset,
                    record = helper.records.decode(entry.records)[1].records[1],
                }
            end

            replies[#replies + 1] = reply
        end

        responses[#responses + 1] = { name = topic.name, partition_responses = replies }
    end

    return { responses = responses, throttle_time_ms = 0 }
end

--- Выборка: пачки с места; пусто — ждать данных до `max_wait_ms`.
---@param fake table
---@param values table
---@return table
local function fetch(fake, values)
    local deadline = fiber.clock() + values.max_wait_ms / 1000
    local responses

    repeat
        local any = false

        responses = {}

        for _, topic in ipairs(values.topics) do
            local partitions = {}

            for _, entry in ipairs(topic.partitions) do
                local slot = partition_of(fake, topic.topic, entry.partition)
                local reply =
                    { partition_index = entry.partition, error_code = 0, high_watermark = 0, last_stable_offset = 0 }

                if slot == nil then
                    reply.error_code = 3
                elseif entry.fetch_offset < slot.start or entry.fetch_offset > slot.next then
                    reply.error_code = 1
                else
                    local parts = {}

                    -- Предел пачек на выборку — как у брокера, который
                    -- отдаёт не больше `partition_max_bytes`.
                    for _, batch in ipairs(slot.batches) do
                        if batch.last >= entry.fetch_offset and #parts < (fake.fetch_limit or math.huge) then
                            parts[#parts + 1] = batch.bytes
                        end
                    end

                    reply.high_watermark, reply.last_stable_offset = slot.next, slot.next
                    reply.records = table.concat(parts)
                    any = any or #parts > 0
                end

                partitions[#partitions + 1] = reply
            end

            responses[#responses + 1] = { topic = topic.topic, partitions = partitions }
        end

        if not any then
            fiber.sleep(0.01)
        end
    until any or fiber.clock() >= deadline

    return { throttle_time_ms = 0, responses = responses }
end

--- Что двойник отвечает сам на каждый запрос.
---@type table<string, fun(fake: table, values: table): table>
local ANSWERS = {
    ApiVersions = function(fake)
        local keys = {}

        for key in pairs(CATALOG) do
            keys[#keys + 1] = { api_key = key, min_version = 0, max_version = 20 }
        end

        return { error_code = 0, api_keys = fake.versions or keys }
    end,

    SaslHandshake = function(_, values)
        return { error_code = values.mechanism == 'PLAIN' and 0 or 33, mechanisms = { 'PLAIN' } }
    end,

    SaslAuthenticate = function(fake, values)
        local good = ('\0%s\0%s'):format(fake.sasl.username, fake.sasl.password)

        if values.auth_bytes ~= good then
            return { error_code = 58, error_message = 'Authentication failed', session_lifetime_ms = 0 }
        end

        return { error_code = 0, auth_bytes = '', session_lifetime_ms = 0 }
    end,

    Metadata = function(fake, values)
        local topics = {}

        if values.topics == nil then
            for name in pairs(fake.topics) do
                topics[#topics + 1] = described(fake, name)
            end
        else
            for _, entry in ipairs(values.topics) do
                topics[#topics + 1] = described(fake, entry.name)
            end
        end

        return {
            throttle_time_ms = 0,
            brokers = { { node_id = 1, host = '127.0.0.1', port = fake.port } },
            cluster_id = 'двойник',
            controller_id = 1,
            topics = topics,
        }
    end,

    CreateTopics = function(fake, values)
        local topics = {}

        for _, entry in ipairs(values.topics) do
            local code = 0

            if fake.topics[entry.name] ~= nil then
                code = 36
            else
                local count = entry.num_partitions

                helper.create(fake, entry.name, count == -1 and fake.partitions or count)
                fake.created[#fake.created + 1] = entry
            end

            topics[#topics + 1] = { name = entry.name, error_code = code }
        end

        return { throttle_time_ms = 0, topics = topics }
    end,

    InitProducerId = function(fake)
        fake.producer_ids = fake.producer_ids + 1

        return { throttle_time_ms = 0, error_code = 0, producer_id = 1000 + fake.producer_ids, producer_epoch = 0 }
    end,

    Produce = produce,

    Fetch = fetch,

    ListOffsets = function(fake, values)
        local topics = {}

        for _, topic in ipairs(values.topics) do
            local partitions = {}

            for _, entry in ipairs(topic.partitions) do
                local slot = partition_of(fake, topic.name, entry.partition_index)

                partitions[#partitions + 1] = {
                    partition_index = entry.partition_index,
                    error_code = slot and 0 or 3,
                    timestamp = -1,
                    offset = slot and (entry.timestamp == -2 and slot.start or slot.next) or -1,
                }
            end

            topics[#topics + 1] = { name = topic.name, partitions = partitions }
        end

        return { topics = topics }
    end,

    FindCoordinator = function(fake)
        return { throttle_time_ms = 0, error_code = 0, node_id = 1, host = '127.0.0.1', port = fake.port }
    end,

    JoinGroup = join,

    SyncGroup = sync,

    Heartbeat = function(fake, values)
        return { throttle_time_ms = 0, error_code = beat(group_of(fake, values.group_id), values) }
    end,

    LeaveGroup = function(fake, values)
        local group = group_of(fake, values.group_id)

        group.members[values.member_id] = nil
        fake.left[#fake.left + 1] = values.member_id

        if next(group.members) ~= nil and group.state ~= 'joining' then
            helper.rebalance(fake, values.group_id)
        end

        return { throttle_time_ms = 0, error_code = 0 }
    end,

    OffsetCommit = commit,

    OffsetFetch = function(fake, values)
        local group = group_of(fake, values.group_id)
        local topics = {}

        for _, topic in ipairs(values.topics) do
            local partitions = {}

            for _, index in ipairs(topic.partition_indexes) do
                local found = group.offsets[('%s/%d'):format(topic.name, index)] or { offset = -1, metadata = '' }

                partitions[#partitions + 1] = {
                    partition_index = index,
                    committed_offset = found.offset,
                    metadata = found.metadata,
                    error_code = 0,
                }
            end

            topics[#topics + 1] = { name = topic.name, partitions = partitions }
        end

        return { throttle_time_ms = 0, topics = topics, error_code = 0 }
    end,
}

--- Поднимает двойника брокера.
---
--- `opts.respond(name, values, talk)` зовётся на каждый пришедший запрос
--- до ответа двойника: вернул таблицу — это и есть ответ; `'skip'` —
--- двойник молчит; `'close'` — соединение закрывается.
---@param opts table|nil `respond`, `topics` (имя → число разделов), `partitions`, `sasl`, `versions`, `round`
---@return table fake
function helper.serve(opts)
    local given = opts or {}
    local fake = {
        calls = {},
        requests = {},
        topics = {},
        groups = {},
        created = {},
        produced = {},
        commits = {},
        left = {},
        sequences = {},
        producer_ids = 0,
        members = 0,
        rounds = 0,
        connections = 0,
        peers = {},
        partitions = given.partitions or 1,
        round = given.round or 0.3,
        sasl = given.sasl,
        versions = given.versions,
    }

    for name, count in pairs(given.topics or {}) do
        helper.create(fake, name, count)
    end

    local server = socket.tcp_server('127.0.0.1', 0, function(peer)
        fake.connections = fake.connections + 1
        fake.peers[#fake.peers + 1] = peer

        local talking = true

        while talking do
            -- Под pcall: сокет, закрытый гасящей проверкой, бросает.
            local alive, got, finished = pcall(incoming, peer, 0.05)

            if not alive or finished then
                talking = false
            elseif got ~= nil then
                local name = got.api.name

                fake.calls[#fake.calls + 1] = name
                fake.requests[name] = fake.requests[name] or {}
                table.insert(fake.requests[name], got.values)

                local told = given.respond
                    and given.respond(name, got.values, { fake = fake, peer = peer, request = got })

                if told == 'close' then
                    talking = false
                elseif type(told) == 'table' then
                    helper.answer(peer, got.api, got.correlation, told)
                elseif told ~= 'skip' then
                    helper.answer(peer, got.api, got.correlation, ANSWERS[name](fake, got.values))
                end
            end
        end

        pcall(peer.close, peer)
    end)

    fake.port = server:name().port --[[@as integer]]

    --- Гасит двойника: сначала слушателя, чтобы новых не было, потом
    --- пришедших, с последнего.
    fake.stop = function()
        local peers = fake.peers

        pcall(server.close, server)

        for index = #peers, 1, -1 do
            pcall(peers[index].close, peers[index])
        end
    end

    return fake
end

--- Ответ двойника по умолчанию на запрос — для `respond`, которому нужно
--- поправить ответ, а не сочинить его заново.
---@param fake table
---@param name string
---@param values table
---@return table
function helper.default(fake, name, values)
    return ANSWERS[name](fake, values)
end

--- Настройки клиента к двойнику: без уборки пула, без пауз повторов
--- и с коротким стуком. Уборка заводила бы файбер на каждый пул, пауза
--- повтора — десятые доли секунды на каждую проверку, а стук раз
--- в три секунды растянул бы каждую проверку группы на секунды.
---@param fake table
---@param opts table|nil Настройки поверх
---@return table
function helper.settings_of(fake, opts)
    local given = table.deepcopy(opts or {})
    local pool, retry = given.pool or {}, given.retry or {}

    pool.sweep_interval = pool.sweep_interval or 0
    pool.open_cooldown = pool.open_cooldown or 0.02
    retry.base = retry.base or 0

    given.pool, given.retry = pool, retry
    given.brokers = given.brokers or { '127.0.0.1:' .. fake.port }
    given.session_timeout = given.session_timeout or 0.3

    return given
end

--- Настройки клиента, проверенные так же, как их проверяет `kafka.new`.
---
--- Вызов не хвостовой: `check` винит строку на два кадра выше себя,
--- и хвостовой вызов увёл бы вину в luatest.
---@param opts table|nil
---@return table
function helper.checked(opts)
    local settings = helper.settings.check(opts)

    return settings
end

--- Клиент к двойнику.
---@param fake table
---@param opts table|nil Настройки поверх
---@return any
function helper.client(fake, opts)
    local settings = helper.settings_of(fake, opts)

    return helper.kafka.new(settings)
end

--- Поднимает двойника и клиента к нему, кладёт их в группу проверок;
--- гасит их `release`.
---@param g table Группа проверок
---@param opts table|nil Настройки двойника
---@param settings table|nil Настройки клиента
---@return any broker
---@return table fake
function helper.talking(g, opts, settings)
    local fake = helper.serve(opts)

    g.fake, g.broker = fake, helper.client(fake, settings)

    return g.broker, fake
end

--- Гасит всё, что подняла проверка, и возвращает пакету настоящие средства.
---@param g table Группа проверок
function helper.release(g)
    for _, consumer in ipairs(g.consumers or {}) do
        consumer:stop()
        helper.until_true(function()
            return consumer.workers == 0
        end)
    end

    if g.broker ~= nil then
        g.broker:close()
    end

    if g.fake ~= nil then
        g.fake.stop()
    end

    g.consumers, g.broker, g.fake = nil, nil, nil

    helper.restore()
end

--- Свободный порт на петле: слушатель заводится и сразу закрывается,
--- и соединение туда отвергается ядром.
---@return integer
function helper.closed_port()
    local probe = socket.tcp_server('127.0.0.1', 0, function() end)
    local free = probe:name().port --[[@as integer]]

    probe:close()

    return free
end

--- Ждёт, пока условие станет истинным, не дольше срока; отдаёт, дождался ли.
---@param condition fun(): boolean
---@param timeout number|nil Секунд; по умолчанию 5
---@return boolean
function helper.until_true(condition, timeout)
    local stop_at = fiber.clock() + (timeout or 5)

    repeat
        if condition() then
            return true
        end

        fiber.sleep(0.01)
    until fiber.clock() >= stop_at

    return condition() == true
end

--- Объявляет получателя и запоминает его в группе проверок, чтобы
--- `release` остановил его и дождался работников.
---@param g table Группа проверок
---@param topic table
---@param handler function
---@param opts table|nil
---@return table
function helper.consume(g, topic, handler, opts)
    g.consumers = g.consumers or {}

    local consumer = topic:consume(handler, opts)

    table.insert(g.consumers, consumer)

    return consumer
end

--- Пачка из одной записи — для двойника и для разбора.
---@param record table `{ key, value, headers }`
---@param opts table|nil `producer_id`, `sequence`, `timestamp`
---@return string
function helper.batch(record, opts)
    local given = opts or {}

    return helper.records.encode({
        producer_id = given.producer_id or -1,
        producer_epoch = 0,
        base_sequence = given.sequence or -1,
        timestamp = given.timestamp or 1790000000000,
        records = { record },
    })
end

--- Останавливает временный узел и убирает его каталог.
helper.stop_node = testing.stop_node

--- Временный узел с исходниками пакета: транзакция box бывает только
--- на нём, в процессе проверок `box` не настроен. Остановить узел
--- проверка обязана сама — `stop_node`.
---@return table server
function helper.start_node()
    return testing.start_node({ modules = helper.MODULES })
end

--- Чтение окружения для живых проверок — порты и каталог стенда.
---
--- `tnt-env` приходит из `.rocks`: сам пакет окружения не читает,
--- и его зависимостью он не объявлен — его ставит `make deps`.
---@return table
function helper.stand_env()
    return require('tnt.env')
end

return helper
