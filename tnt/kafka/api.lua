--- Каталог запросов Kafka: номер, версия, поля запроса и ответа.
---
--- **Версии выбраны без гибкой записи.** С некоторой версии каждый
--- запрос Kafka переходит на «гибкую» запись — длины числами переменной
--- длины и поля с метками в конце каждой записи. Пакету она ничего
--- не даёт, а разбор удвоила бы. Поэтому версия каждого запроса —
--- последняя до гибкой из тех, что нужны, и не ниже наименьшей, которую
--- принимают выпуски брокера 4.x: те перестали принимать старые версии
--- производства, выборки, смещений и входа в группу. Что брокер
--- принимает, пакет спрашивает при входе (`ApiVersions`), и брокер,
--- не знающий нашей версии, — отказ входа с именем запроса, а не
--- обрыв на первом деле.
---
--- Шапка запроса — версия 1: номер, версия, номер обращения и имя
--- клиента; шапка ответа — только номер обращения. Номер обращения
--- сверяется: чужой ответ в соединении — это обрыв разговора, а не ответ.

local coordination = require('tnt.kafka.coordination')
local schema = require('tnt.kafka.schema')
local wire = require('tnt.kafka.wire')

local Module = {}

--- Сколько байт у рамки: длина кадра целым в четыре байта.
Module.PREFIX = 4

---@class TntKafkaApi Запрос из каталога
---@field name string Имя запроса в тексте отказа
---@field key integer Номер запроса
---@field version integer Версия, которой говорит пакет
---@field request table[] Поля запроса
---@field response table[] Поля ответа

--- Пустота или список целых.
local INTS = '[int32]'

--- Что брокер умеет: номер запроса и границы версий.
Module.API_VERSIONS = {
    name = 'ApiVersions',
    key = 18,
    version = 0,
    request = {},
    response = {
        { 'error_code', 'int16' },
        { 'api_keys', { { 'api_key', 'int16' }, { 'min_version', 'int16' }, { 'max_version', 'int16' } } },
    },
}

--- Вход по SASL: брокер называет способы, клиент выбирает.
Module.SASL_HANDSHAKE = {
    name = 'SaslHandshake',
    key = 17,
    version = 1,
    request = { { 'mechanism', 'string' } },
    response = { { 'error_code', 'int16' }, { 'mechanisms', '[string]' } },
}

--- Вход по SASL: учётные данные.
Module.SASL_AUTHENTICATE = {
    name = 'SaslAuthenticate',
    key = 36,
    version = 1,
    request = { { 'auth_bytes', 'bytes' } },
    response = {
        { 'error_code', 'int16' },
        { 'error_message', 'string' },
        { 'auth_bytes', 'bytes' },
        { 'session_lifetime_ms', 'int64' },
    },
}

--- Описание кластера: узлы, темы, разделы и их ведущие.
Module.METADATA = {
    name = 'Metadata',
    key = 3,
    version = 5,
    request = { { 'topics', { { 'name', 'string' } } }, { 'allow_auto_topic_creation', 'boolean' } },
    response = {
        { 'throttle_time_ms', 'int32' },
        { 'brokers', { { 'node_id', 'int32' }, { 'host', 'string' }, { 'port', 'int32' }, { 'rack', 'string' } } },
        { 'cluster_id', 'string' },
        { 'controller_id', 'int32' },
        {
            'topics',
            {
                { 'error_code', 'int16' },
                { 'name', 'string' },
                { 'is_internal', 'boolean' },
                {
                    'partitions',
                    {
                        { 'error_code', 'int16' },
                        { 'partition_index', 'int32' },
                        { 'leader_id', 'int32' },
                        { 'replica_nodes', INTS },
                        { 'isr_nodes', INTS },
                        { 'offline_replicas', INTS },
                    },
                },
            },
        },
    },
}

--- Заведение тем.
Module.CREATE_TOPICS = {
    name = 'CreateTopics',
    key = 19,
    version = 4,
    request = {
        {
            'topics',
            {
                { 'name', 'string' },
                { 'num_partitions', 'int32' },
                { 'replication_factor', 'int16' },
                { 'assignments', { { 'partition_index', 'int32' }, { 'broker_ids', INTS } } },
                { 'configs', { { 'name', 'string' }, { 'value', 'string' } } },
            },
        },
        { 'timeout_ms', 'int32' },
        { 'validate_only', 'boolean' },
    },
    response = {
        { 'throttle_time_ms', 'int32' },
        { 'topics', { { 'name', 'string' }, { 'error_code', 'int16' }, { 'error_message', 'string' } } },
    },
}

--- Номер идемпотентного производителя.
Module.INIT_PRODUCER_ID = {
    name = 'InitProducerId',
    key = 22,
    version = 1,
    request = { { 'transactional_id', 'string' }, { 'transaction_timeout_ms', 'int32' } },
    response = {
        { 'throttle_time_ms', 'int32' },
        { 'error_code', 'int16' },
        { 'producer_id', 'int64' },
        { 'producer_epoch', 'int16' },
    },
}

--- Запись пачки в раздел.
Module.PRODUCE = {
    name = 'Produce',
    key = 0,
    version = 8,
    request = {
        { 'transactional_id', 'string' },
        { 'acks', 'int16' },
        { 'timeout_ms', 'int32' },
        {
            'topic_data',
            { { 'name', 'string' }, { 'partition_data', { { 'index', 'int32' }, { 'records', 'bytes' } } } },
        },
    },
    response = {
        {
            'responses',
            {
                { 'name', 'string' },
                {
                    'partition_responses',
                    {
                        { 'index', 'int32' },
                        { 'error_code', 'int16' },
                        { 'base_offset', 'int64' },
                        { 'log_append_time_ms', 'int64' },
                        { 'log_start_offset', 'int64' },
                        { 'record_errors', { { 'batch_index', 'int32' }, { 'batch_index_error_message', 'string' } } },
                        { 'error_message', 'string' },
                    },
                },
            },
        },
        { 'throttle_time_ms', 'int32' },
    },
}

--- Выборка записей из разделов.
Module.FETCH = {
    name = 'Fetch',
    key = 1,
    version = 4,
    request = {
        { 'replica_id', 'int32' },
        { 'max_wait_ms', 'int32' },
        { 'min_bytes', 'int32' },
        { 'max_bytes', 'int32' },
        { 'isolation_level', 'int8' },
        {
            'topics',
            {
                { 'topic', 'string' },
                {
                    'partitions',
                    { { 'partition', 'int32' }, { 'fetch_offset', 'int64' }, { 'partition_max_bytes', 'int32' } },
                },
            },
        },
    },
    response = {
        { 'throttle_time_ms', 'int32' },
        {
            'responses',
            {
                { 'topic', 'string' },
                {
                    'partitions',
                    {
                        { 'partition_index', 'int32' },
                        { 'error_code', 'int16' },
                        { 'high_watermark', 'int64' },
                        { 'last_stable_offset', 'int64' },
                        { 'aborted_transactions', { { 'producer_id', 'int64' }, { 'first_offset', 'int64' } } },
                        { 'records', 'bytes' },
                    },
                },
            },
        },
    },
}

--- Смещение раздела по времени: самое раннее — `-2`, следующее — `-1`.
Module.LIST_OFFSETS = {
    name = 'ListOffsets',
    key = 2,
    version = 1,
    request = {
        { 'replica_id', 'int32' },
        {
            'topics',
            { { 'name', 'string' }, { 'partitions', { { 'partition_index', 'int32' }, { 'timestamp', 'int64' } } } },
        },
    },
    response = {
        {
            'topics',
            {
                { 'name', 'string' },
                {
                    'partitions',
                    {
                        { 'partition_index', 'int32' },
                        { 'error_code', 'int16' },
                        { 'timestamp', 'int64' },
                        { 'offset', 'int64' },
                    },
                },
            },
        },
    },
}

--- Все запросы, которыми говорит пакет: их версии сверяются при входе.
---@type TntKafkaApi[]
Module.USED = {
    Module.METADATA,
    Module.CREATE_TOPICS,
    Module.INIT_PRODUCER_ID,
    Module.PRODUCE,
    Module.FETCH,
    Module.LIST_OFFSETS,
    coordination.FIND_COORDINATOR,
    coordination.JOIN_GROUP,
    coordination.SYNC_GROUP,
    coordination.HEARTBEAT,
    coordination.LEAVE_GROUP,
    coordination.OFFSET_COMMIT,
    coordination.OFFSET_FETCH,
}

--- Кадр запроса: рамка, шапка версии 1 и поля.
---@param api TntKafkaApi
---@param correlation integer Номер обращения
---@param client string Имя клиента
---@param values table Поля запроса
---@return string
function Module.request(api, correlation, client, values)
    local body = wire.int16(api.key)
        .. wire.int16(api.version)
        .. wire.int32(correlation)
        .. wire.string(client)
        .. schema.encode(api.request, values)

    return wire.int32(#body) .. body
end

--- Разбирает тело ответа без рамки: номер обращения и поля.
---
--- Бросает на порче: разбор целиком идёт под `pcall` у того, кто читал.
---@param api TntKafkaApi
---@param payload string
---@return integer correlation
---@return table values
function Module.response(api, payload)
    local reader = wire.reader(payload)
    local correlation = reader:int32()

    return correlation, schema.decode(api.response, reader)
end

return Module
