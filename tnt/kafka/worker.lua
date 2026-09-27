--- Получатель: участники группы, выборка и стук.
---
--- **Работник — участник группы.** Каждый из `workers` входит в группу
--- сам, получает свою долю разделов и ведёт их один: подтверждение Kafka —
--- смещение раздела, накопительное, и два работника на одном разделе
--- подтверждали бы друг за друга. Поэтому работников больше, чем разделов
--- в теме и теме повтора вместе, заводить незачем: лишние стоят без дела.
---
--- **Стук — своим файбером.** Пока обработчик работает, участник обязан
--- стучать, иначе группа сочтёт его умершим и раздаст его разделы.
--- Стук, услышавший «идёт раздача» либо «ты вне поколения», велит войти
--- заново; вход ждёт, пока взятое сообщение дорабатывается.
---
--- **Выборка — у ведущих разделов**, по узлу за раз, с ожиданием данных
--- до `POLL`: пустая тема не крутит цикл. Раздел, чья голова ждёт срока
--- повтора, откладывается, а участник идёт к другим разделам.
---
--- Шаг работника идёт под `pcall`: сбой считается и пишется, работник
--- живёт дальше, а после сбоя спит, чтобы беда, повторяющаяся на каждом
--- шаге, не крутила цикл без уступки.

local fiber = require('fiber')

local clock = require('tnt.clock')
local codes = require('tnt.kafka.codes')
local enter = require('tnt.kafka.enter')
local external = require('tnt.external')
local fetch = require('tnt.kafka.fetch')
local group = require('tnt.kafka.group')
local handle = require('tnt.kafka.handle')
local offsets = require('tnt.kafka.offsets')
local within = require('tnt.storage.within')

--- Сбой шага и вход пишутся с подавлением повторов: беда, которая
--- держится, иначе писала бы строку на каждом шаге.
local steps = require('tnt.log').changes('tnt.kafka')

local Module = {}

--- Сколько спать после сбоя шага, секунд.
Module.FAILURE_PAUSE = 0.5

--- Внешние средства: сон.
local source = external.install(Module, { sleep = fiber.sleep })

---@class TntKafkaConsumer Получатель: `stop()` перестаёт брать новые сообщения
---@field topic TntKafkaTopic
---@field handler fun(message: TntKafkaMessage): any, any
---@field policy TntKafkaPolicy
---@field stopping boolean Попросили остановиться
---@field workers integer Сколько работников ещё живо
local Consumer = {}
Consumer.__index = Consumer

--- Раздел, голову которого можно обрабатывать сейчас.
---@param station TntKafkaStation
---@return TntKafkaLane|nil
local function ready_lane(station)
    local now = clock.monotonic()

    for _, lane in ipairs(station.lanes) do
        if #lane.buffer > 0 and (lane.resume == nil or lane.resume <= now) then
            return lane
        end
    end

    return nil
end

--- Стучит, пока участник в группе и получатель не остановлен.
---
--- Стук идёт через ведущего группы с повторами (`offsets.call`): узел,
--- который перестал вести группу, забывается, и следующая попытка ищет
--- ведущего заново. Отказ «идёт раздача», «ты вне поколения» либо стук,
--- так и не дошедший до ведущего, велят войти заново.
---@param consumer TntKafkaConsumer
---@param station TntKafkaStation
local function beat(consumer, station)
    local cluster = consumer.topic.broker.cluster
    local interval = cluster.settings.session_timeout / 3

    while not consumer.stopping do
        source().sleep(interval)

        if not (station.rejoin or consumer.stopping) then
            local ok, err = offsets.call(cluster, station, station.seat.group, interval, function(coordinator, deadline)
                return group.heartbeat(cluster, coordinator, station.seat, deadline)
            end)

            if not ok then
                ---@cast err TntStorageFailure
                local meaning = err.server_code and codes.of(err.server_code)

                if meaning == nil or meaning.coordinator or meaning.rejoin then
                    station.rejoin = true
                end
            end
        end
    end
end

--- Один шаг работника: войти, обработать готовое либо выбрать новое.
---@param consumer TntKafkaConsumer
---@param station TntKafkaStation
local function step(consumer, station)
    if station.rejoin then
        return enter.rejoin(consumer, station)
    end

    local lane = ready_lane(station)

    if lane ~= nil then
        return handle.process(consumer, station, lane)
    end

    if not fetch.fetch(consumer, station) then
        -- Все разделы ждут срока либо разделов нет: выбирать нечего.
        source().sleep(fetch.POLL)
    end
end

--- Тело работника: шаги до остановки, затем уход из группы.
---@param consumer TntKafkaConsumer
---@param number integer
local function work(consumer, number)
    local settings = consumer.topic.broker.cluster.settings
    local topic = consumer.topic

    ---@type TntKafkaStation
    local station = {
        seat = {
            group = topic.group,
            member = '',
            generation = -1,
            topics = { topic.name, topic.retry },
            session_ms = math.floor(settings.session_timeout * 1000),
            rebalance_ms = math.floor(settings.rebalance_timeout * 1000),
        },
        lanes = {},
        rejoin = true,
    }

    -- Обрезать имя незачем: имя темы не длиннее 243 знаков (имя темы
    -- повтора — не длиннее 249), и имя файбера укладывается в предел ядра
    -- в 255 знаков при числе работников меньше ста тысяч.
    fiber.self():name(('kafka/%s/%d'):format(topic.name, number))
    fiber.new(beat, consumer, station)

    while not consumer.stopping do
        local ok, err = pcall(step, consumer, station)

        if not ok then
            topic.counts.failures = topic.counts.failures + 1
            steps.warn(
                'шаг работника получателя сорвался',
                { destination = topic.name, err = tostring(err) }
            )
            source().sleep(Module.FAILURE_PAUSE)
        end
    end

    if station.coordinator ~= nil and station.seat.generation ~= -1 then
        pcall(
            group.leave,
            topic.broker.cluster,
            station.coordinator,
            station.seat,
            within.deadline(topic.broker.limits.timeout)
        )
    end

    consumer.workers = consumer.workers - 1
end

--- Запускает работников получателя: по участнику группы на каждого.
---
--- Счёт живых ставится сразу: новый файбер не идёт, пока запускающий
--- не уступил, а каждый работник снимает себя со счёта сам, когда кончится.
---@param topic TntKafkaTopic
---@param handler fun(message: TntKafkaMessage): any, any
---@param policy TntKafkaPolicy
---@return TntKafkaConsumer
function Module.start(topic, handler, policy)
    ---@type TntKafkaConsumer
    local consumer = setmetatable({}, Consumer)

    consumer.topic, consumer.handler, consumer.policy = topic, handler, policy
    consumer.stopping, consumer.workers = false, policy.workers

    local number = 0

    while number < policy.workers do
        number = number + 1
        fiber.new(work, consumer, number)
    end

    return consumer
end

--- Перестаёт брать новые сообщения; взятое дорабатывается, участник
--- уходит из группы. Работник замечает остановку не позже чем через срок
--- ожидания выборки.
function Consumer:stop()
    self.stopping = true
end

return Module
