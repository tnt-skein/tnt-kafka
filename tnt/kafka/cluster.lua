--- Кластер: узлы, пулы соединений к ним и описание тем.
---
--- Kafka — не один сервер, а узлы: у каждого раздела свой ведущий, у
--- каждой группы — свой ведущий узел, и запись идёт туда, где ведущий.
--- Поэтому пул здесь не один на клиента, а свой на каждый узел (`tnt-pool`):
--- заводится при первом обращении к узлу. Узлы знакомства из настроек —
--- только чтобы спросить описание кластера: дальше адреса называет он.
---
--- **Описание темы держится до беды.** Разделы и их ведущие меняются
--- редко, и спрашивать о них на каждое дело — лишний запрос. Кэш
--- выбрасывается кодом с пометкой `stale` (ведущий сменился, тема только
--- заведена) и через `METADATA_AGE`: новый раздел темы иначе не увидел
--- бы никто до перезапуска.
---
--- **Вызов — одним сроком, с повторами по `retriable`** (`tnt-retry`).
--- Дело само решает, что считать отказом, и отдаёт `TntStorageFailure`;
--- повтор идёт, пока срок не вышел, и последний отказ отдаётся без
--- повтора, если время кончилось. Число попыток по умолчанию не
--- ограничено — только сроком.

local fiber = require('fiber')

local api = require('tnt.kafka.api')
local codes = require('tnt.kafka.codes')
local connect = require('tnt.kafka.connect')
local fail = require('tnt.must.fail')
local failure = require('tnt.storage.failure')
local link_of = require('tnt.kafka.link')
local pool = require('tnt.pool')
local retry = require('tnt.retry')
local within = require('tnt.storage.within')

local Module = {}

--- Сколько живёт описание темы, секунд: пять минут, как у самого брокера
--- по умолчанию (`metadata.max.age.ms`).
Module.METADATA_AGE = 300

---@class TntKafkaPartition
---@field leader integer Номер узла-ведущего; `-1` — ведущего нет

---@class TntKafkaTopicMeta Описание темы
---@field name string
---@field count integer Число разделов
---@field partitions table<integer, TntKafkaPartition> Разделы по номеру
---@field taken number Когда спрошено, по монотонным часам

---@class TntKafkaCluster Узлы, пулы и описание тем
---@field settings TntKafkaSettings
---@field retry TntRetry Повторы
---@field pools table<string, TntPool> Пулы по адресу узла
---@field nodes table<integer, TntKafkaAddress> Узлы по номеру
---@field topics table<string, TntKafkaTopicMeta> Описания тем
---@field closed boolean
local Cluster = {}
Cluster.__index = Cluster

--- Заводит кластер. Соединений не открывает.
---@param settings TntKafkaSettings
---@return TntKafkaCluster|nil
---@return string|nil err Отказ настроек повторов
function Module.new(settings)
    local retries = table.copy(settings.retry)

    -- Повторы по умолчанию — до срока вызова, а не три попытки: ведущий
    -- раздела сразу после заведения темы и после смены узла готов через
    -- сотни миллисекунд, и три быстрые попытки кончались бы раньше него.
    -- Срок здесь — только потолок на случай вызова без своего срока:
    -- каждый вызов передаёт свой.
    retries.attempts = retries.attempts or math.huge
    retries.deadline = settings.limits.max_timeout
    retries.scope = settings.name

    local retrier, wrong = retry.new(retries)

    if retrier == nil then
        return nil, wrong
    end

    return setmetatable({
        settings = settings,
        retry = retrier,
        pools = {},
        nodes = {},
        topics = {},
        closed = false,
    }, Cluster)
end

--- Остаток срока в миллисекундах для полей `timeout_ms` запросов, хотя бы
--- одна: ноль брокер читает как «не ждать вовсе».
---@param deadline number
---@return integer
function Module.milliseconds(deadline)
    return math.max(1, math.floor(within.left(deadline) * 1000))
end

--- Адрес узла текстом: ключ пула и место в тексте отказа.
---@param address TntKafkaAddress
---@return string
function Module.where(address)
    return ('%s:%d'):format(address.host, address.port)
end

--- Пул к узлу; заводится при первом обращении.
---@param address TntKafkaAddress
---@return TntPool
function Cluster:pool(address)
    local where = Module.where(address)
    local found = self.pools[where]

    if found == nil then
        local settings = self.settings
        local options = table.copy(settings.pool)

        options.name = ('%s/%s'):format(settings.name, where)
        options.close = link_of.close
        options.alive = link_of.alive
        options.open = function(left)
            return connect.open(settings, address, left)
        end

        found = pool.new(options)
        self.pools[where] = found
    end

    return found
end

--- Отказ, когда срок вышел до обращения.
---@param what string
---@return TntStorageFailure
local function late(what)
    return failure.new(failure.TIMEOUT, ('срок вызова вышел до %s'):format(what), { sent = false })
end

--- Одно обращение к узлу: взять соединение, спросить, вернуть либо
--- выбросить.
---
--- Бросок внутри обмена — ошибка программиста (поле запроса не того рода)
--- либо отмена файбера: соединение выбрасывается, а брошенное идёт
--- дальше как есть.
---@param address TntKafkaAddress
---@param request TntKafkaApi
---@param values table
---@param deadline number
---@param idempotent boolean|nil Повтор после отправки безвреден
---@return table|nil answer
---@return TntStorageFailure|nil err
function Cluster:exchange(address, request, values, deadline, idempotent)
    local where = Module.where(address)
    local left = within.left(deadline)

    if left <= 0 then
        return nil, late(('обращения к %s'):format(where))
    end

    local connections = self:pool(address)
    local conn, why = connections:take(math.min(left, connections.settings.wait_timeout))

    if conn == nil then
        if failure.is(why) then
            return nil, why
        end

        local stats = connections:stats()
        local opening = stats.total < stats.size and stats.last_open_error ~= nil

        return nil,
            failure.new(opening and failure.UNREACHABLE or failure.BUSY, ('kafka %s: %s'):format(where, tostring(why)))
    end

    local done, answer, trouble = pcall(link_of.exchange, conn, request, values, deadline)

    if done and answer ~= nil then
        connections:give(conn)

        return answer
    end

    connections:drop(conn)

    if not done then
        fail.raise(answer)
    end

    ---@cast trouble TntKafkaTrouble
    return nil,
        failure.new(trouble.kind, ('kafka %s: %s: %s'):format(where, request.name, trouble.message), {
            sent = trouble.sent,
            idempotent = idempotent,
        })
end

--- Запоминает узлы и темы из описания кластера.
---@param cluster TntKafkaCluster
---@param answer table
local function remember(cluster, answer)
    for _, node in ipairs(answer.brokers) do
        cluster.nodes[node.node_id] = { host = node.host, port = node.port }
    end

    for _, topic in ipairs(answer.topics) do
        if topic.error_code == codes.NONE and #topic.partitions > 0 then
            local partitions = {}

            for _, partition in ipairs(topic.partitions) do
                partitions[partition.partition_index] = { leader = partition.leader_id }
            end

            cluster.topics[topic.name] = {
                name = topic.name,
                count = #topic.partitions,
                partitions = partitions,
                taken = within.deadline(0),
            }
        end
    end
end

--- Узлы, у которых можно спросить описание: знакомые, затем узлы
--- знакомства, каждый адрес — один раз. Узел знакомства обычно уже среди
--- знакомых, и спрашивать его дважды значило бы ждать молчащий узел
--- дважды.
---@param cluster TntKafkaCluster
---@return TntKafkaAddress[]
local function askable(cluster)
    local found, seen = {}, {}

    local function add(address)
        local where = Module.where(address)

        if not seen[where] then
            seen[where] = true
            found[#found + 1] = address
        end
    end

    for _, address in pairs(cluster.nodes) do
        add(address)
    end

    for _, address in ipairs(cluster.settings.brokers) do
        add(address)
    end

    return found
end

--- Спрашивает описание тем у первого узла, который ответит.
---
--- Тема не заводится сама (`allow_auto_topic_creation = false`): тему
--- заводит объявление, и опечатка в имени не должна плодить темы.
--- Узлы спрашиваются по очереди, и каждому достаётся равная доля
--- оставшегося срока.
---@param names string[]|nil Темы; `nil` — все
---@param deadline number
---@return table|nil answer
---@return TntStorageFailure|nil err
function Cluster:metadata(names, deadline)
    local topics = nil

    if names ~= nil then
        topics = {}

        for index, name in ipairs(names) do
            topics[index] = { name = name }
        end
    end

    local values = { topics = topics, allow_auto_topic_creation = false }
    local nodes = askable(self)
    local last

    for index, address in ipairs(nodes) do
        -- Остаток срока делится поровну между оставшимися узлами: мёртвый
        -- узел иначе съел бы весь срок ожиданием пула, и до живых дело
        -- не дошло бы.
        local share = within.left(deadline) / (#nodes - index + 1)
        local answer, err =
            self:exchange(address, api.METADATA, values, math.min(deadline, within.deadline(share)), true)

        if answer ~= nil then
            remember(self, answer)

            return answer
        end

        last = err
    end

    return nil, last
end

--- Описание темы: из кэша, а устаревшее или не бывшее — у брокера.
---
--- Темы нет — отказ `unreachable` с кодом брокера: её могли только что
--- завести, и повтор увидит её.
---@param name string
---@param deadline number
---@return TntKafkaTopicMeta|nil
---@return TntStorageFailure|nil err
function Cluster:topic(name, deadline)
    local known = self.topics[name]

    if known ~= nil and within.left(known.taken + Module.METADATA_AGE) > 0 then
        return known
    end

    local answer, err = self:metadata({ name }, deadline)

    if answer == nil then
        return nil, err
    end

    known = self.topics[name]

    if known ~= nil then
        return known
    end

    local code = answer.topics[1] and answer.topics[1].error_code or codes.NONE
    local shown = code == codes.NONE and 'у темы нет разделов' or codes.name(code)

    return nil,
        failure.new(failure.UNREACHABLE, ('kafka: тема %s не описана: %s'):format(name, shown), {
            server_code = code,
        })
end

--- Забывает описание темы: следующее дело спросит его заново.
---@param name string
function Cluster:forget(name)
    self.topics[name] = nil
end

--- Адрес ведущего раздела; `nil` — ведущего нет либо узел не назван.
---@param meta TntKafkaTopicMeta
---@param partition integer
---@return TntKafkaAddress|nil
function Cluster:leader(meta, partition)
    local known = meta.partitions[partition]

    return known and self.nodes[known.leader]
end

--- Дело с кластером целиком: один миг срока, попытки по приговору отказа.
---
--- Последний отказ после вышедшего срока отдаётся без повтора: пауза
--- `tnt-retry` меряет свой срок от своего начала и могла в него уложиться,
--- не уложившись в миг вызова.
---@param action fun(deadline: number): any, TntStorageFailure|nil
---@param timeout number Срок вызова
---@return any value
---@return TntStorageFailure|nil err
function Cluster:call(action, timeout)
    local deadline = within.deadline(timeout)
    ---@type TntStorageFailure|nil
    local last

    local value, err = self.retry:run(function()
        if self.closed then
            return nil, failure.new(failure.CLOSED, ('%s: клиент закрыт'):format(self.settings.name))
        end

        if within.left(deadline) <= 0 then
            if last == nil then
                return nil, late('отправки дела')
            end

            return nil,
                failure.new(last.kind, last.message, {
                    retriable = false,
                    sent = last.sent,
                    server_code = last.server_code,
                })
        end

        local got, refusal = action(deadline)

        last = refusal

        return got, refusal
    end, { deadline = timeout })

    -- Отменённого вызывающего пара не останавливает: отмена идёт дальше.
    fiber.testcancel()

    return value, err
end

--- Закрывает все пулы: свободные соединения сразу, занятые — когда вернут.
function Cluster:close()
    self.closed = true

    for _, connections in pairs(self.pools) do
        connections:close()
    end
end

--- Пулы по адресам: сводка для `status`.
---@return table<string, table>
function Cluster:stats()
    local stats = {}

    for where, connections in pairs(self.pools) do
        stats[where] = connections:stats()
    end

    return stats
end

return Module
