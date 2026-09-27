--- Очередь Kafka: свой протокол поверх сокета, пул на узел, срок, повторы
--- и отказ парой.
---
---     local kafka = require('tnt.kafka')
---
---     local broker = kafka.new({ brokers = { 'kafka-1:9092', 'kafka-2:9092' } })
---     local orders = broker:declare('orders', { partitions = 6 })
---
---     local sent, err = orders:send({ order = 7 }, { key = 'customer-42' })
---
---     local consumer = orders:consume(function(message)
---         local ok, reason = ship(message.body)
---
---         if not ok then
---             return nil, reason              -- выдать снова, потом зарыть
---         end
---     end, { workers = 3 })
---
--- Готового неблокирующего клиента Kafka у Tarantool нет — ни в ядре,
--- ни среди официальных роков, а обёртки над librdkafka ждут сети в своих
--- потоках и отдают итог обратными вызовами мимо файберов. Поэтому
--- протокол свой (`tnt.kafka.wire`, `tnt.kafka.schema`, `tnt.kafka.api`,
--- `tnt.kafka.records`), поверх встроенного сокета: он неблокирующий
--- и живёт с файберами. Всё прочее взято готовым: пул — `tnt-pool`, отказ
--- и срок — `tnt-storage`, повторы — `tnt-retry` по полю `retriable`,
--- шифрование — `tnt-tls`.
---
--- Решения, которые стоит знать заранее:
---
--- * **Отправка ждёт всех синхронных копий** (`acks = -1`), производитель
---   идемпотентный: повтор после обрыва брокер узнаёт и не пишет дважды.
--- * **Подтверждение — итог обработчика**: значение или ничего —
---   смещение раздела фиксируется, `nil, err` — копия в тему повтора,
---   после `max_attempts` — в тему мёртвых с причиной.
--- * **Порядок — внутри ключа**: ключ задаёт раздел, раздел ведёт один
---   участник группы. Повтор порядок ломает: копия идёт в тему повтора.
--- * **Гарантия — хотя бы раз**: обрыв между обработкой и фиксацией
---   отдаёт сообщение снова. Повтор отсекает получатель по `message.id`.
--- * **Отсрочки, срока жизни и приоритета у Kafka нет**: `delay`, `ttl`
---   и `priority` — исключение, а не молчаливый пропуск.
--- * **Отправка внутри транзакции box — ошибка программиста**; «сохранили
---   и отправили» — через `tnt-outbox` (`broker:sender()`).
---
--- Подробно — `docs/kafka.md`.

local cluster_of = require('tnt.kafka.cluster')
local hook = require('tnt.kafka.hook')
local message = require('tnt.kafka.message')
local producer_of = require('tnt.kafka.producer')
local settings_of = require('tnt.kafka.settings')
local storage = require('tnt.storage')
local topic_of = require('tnt.kafka.topic')

local failure = storage.failure

--- Фасад: признаки очереди и крюки — те же таблицы и функции, что у частей.
---
--- `features` — что умеет эта очередь: настройка, которой она не умеет,
--- исключение. `hook` ставит, заменяет или снимает крюк отправки
--- и обработки, `hooks` называет поставленные по порядку.
local Module = { features = message.FEATURES, hook = hook.set, hooks = hook.names }

---@class TntKafkaBroker Клиент кластера Kafka
---@field name string Имя клиента
---@field settings TntKafkaSettings Настройки как проверены
---@field limits TntStorageLimits Сроки вызова
---@field cluster TntKafkaCluster Узлы, пулы и описание тем
---@field producer TntKafkaProducer Производитель
---@field topics table<string, TntKafkaTopic> Объявленные темы по «тема» либо «тема/группа»
---@field closed boolean Закрыт ли клиент
local Broker = {}
Broker.__index = Broker

--- Заводит клиента. Соединений не открывает: первое откроет первое дело.
---@param opts TntKafkaOptions|nil
---@return TntKafkaBroker
function Module.new(opts)
    local settings = settings_of.check(opts)
    local cluster, wrong = cluster_of.new(settings)

    if cluster == nil then
        error(('настройки kafka.retry: %s'):format(wrong), 2)
    end

    return setmetatable({
        name = settings.name,
        settings = settings,
        limits = settings.limits,
        cluster = cluster,
        producer = producer_of.new(cluster),
        topics = {},
        closed = false,
    }, Broker)
end

--- Объявляет тему для группы. На брокере темы заводятся при первом деле.
---@param name string Имя темы
---@param opts TntKafkaDeclareOptions|nil
---@return TntKafkaTopic
function Broker:declare(name, opts)
    local topic = topic_of.new(self, name, opts, 2)
    local label = topic.group == name and name or ('%s/%s'):format(name, topic.group)

    self.topics[label] = topic

    return topic
end

--- Отправитель по договору для `tnt-outbox`: `send(name, body, opts)`
--- и признаки. Не объявленная раньше тема объявляется умолчаниями при
--- первой отправке.
---@return { send: fun(name: string, body: any, opts: table|nil): string|nil, TntStorageFailure|nil, features: table }
function Broker:sender()
    return {
        features = Module.features,

        send = function(name, body, opts)
            local topic = self.topics[name] or self:declare(name)

            return topic:send(body, opts)
        end,
    }
end

--- Закрывает клиента: получатели останавливаются, свободные соединения
--- закрываются сразу, занятые — когда их вернут.
---
--- Повторное закрытие — пара `closed`, а не исключение: закрытие при
--- остановке узла гонится с отправками, и «так бывает».
---@return boolean ok
---@return TntStorageFailure|nil err
function Broker:close()
    if self.closed then
        return false, failure.new(failure.CLOSED, ('%s: клиент уже закрыт'):format(self.name))
    end

    self.closed = true

    for _, topic in pairs(self.topics) do
        for _, consumer in ipairs(topic.consumers) do
            consumer:stop()
        end
    end

    self.cluster:close()

    return true
end

--- Что с клиентом сейчас: узлы, пулы, темы и их счётчики.
---@return table
function Broker:status()
    local topics = {}

    for label, topic in pairs(self.topics) do
        topics[label] = topic:status()
    end

    return {
        name = self.name,
        closed = self.closed,
        pools = self.cluster:stats(),
        topics = topics,
        hooks = hook.names(),
    }
end

return Module
