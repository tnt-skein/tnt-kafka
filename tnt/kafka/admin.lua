--- Заведение тем: основной, повтора и мёртвых.
---
--- Тема объявляется пакетом, а не берётся готовой: с ней нужны тема
--- повтора и тема мёртвых, и без них возврату и зарытому некуда лечь.
--- Сам брокер темы на запись не заводит (`allow_auto_topic_creation`
--- выключен в запросе описания): опечатка в имени иначе плодила бы темы
--- с умолчаниями, и заметили бы это по пропавшим сообщениям.
---
--- **Заводится только недостающее.** Сначала описание кластера: тема,
--- которая есть, не трогается вовсе, и учётке без права на заведение тем
--- хватает права на запись в готовые. Разделы и копии — из объявления,
--- а без них — умолчания брокера (`-1`): их выбирает тот, кто держит
--- кластер.
---
--- Заведённая тема получает ведущих не сразу: первое дело может
--- услышать `LEADER_NOT_AVAILABLE`, и это обычный повтор по сроку.

local api = require('tnt.kafka.api')
local cluster_of = require('tnt.kafka.cluster')
local codes = require('tnt.kafka.codes')

local Module = {}

--- Код брокера «такой темы нет».
local UNKNOWN_TOPIC = 3

--- Умолчание брокера для числа разделов и копий.
local BROKER_DEFAULT = -1

---@class TntKafkaTopicSpec Чем заводится тема
---@field partitions integer|nil Число разделов; `nil` — умолчание брокера
---@field replication integer|nil Число копий; `nil` — умолчание брокера

--- Заводит темы, которых нет.
---@param cluster TntKafkaCluster
---@param names string[]
---@param spec TntKafkaTopicSpec
---@param deadline number
---@return boolean|nil ok
---@return TntStorageFailure|nil err
function Module.ensure(cluster, names, spec, deadline)
    local answer, err = cluster:metadata(names, deadline)

    if answer == nil then
        return nil, err
    end

    local missing = {}

    for _, topic in ipairs(answer.topics) do
        if topic.error_code == UNKNOWN_TOPIC then
            missing[#missing + 1] = {
                name = topic.name,
                num_partitions = spec.partitions or BROKER_DEFAULT,
                replication_factor = spec.replication or BROKER_DEFAULT,
                assignments = {},
                configs = {},
            }
        elseif topic.error_code ~= codes.NONE then
            return nil, codes.failure(topic.error_code, ('kafka: тема %s не описана'):format(topic.name))
        end
    end

    if #missing == 0 then
        return true
    end

    -- Заводит темы ведущий кластера; узлы знакомства — если он не назван.
    local controller = cluster.nodes[answer.controller_id] or cluster.settings.brokers[1]
    local values = {
        topics = missing,
        timeout_ms = cluster_of.milliseconds(deadline),
        validate_only = false,
    }
    local created, cut = cluster:exchange(controller, api.CREATE_TOPICS, values, deadline, true)

    if created == nil then
        return nil, cut
    end

    for _, topic in ipairs(created.topics) do
        local code = topic.error_code

        if code ~= codes.NONE and code ~= codes.TOPIC_ALREADY_EXISTS then
            local told = topic.error_message and (' (%s)'):format(topic.error_message) or ''

            return nil, codes.failure(code, ('kafka: тема %s не заведена%s'):format(topic.name, told))
        end

        cluster:forget(topic.name)
    end

    return true
end

return Module
