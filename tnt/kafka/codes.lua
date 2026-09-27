--- Коды ошибок Kafka: имя, род отказа и что с ними делать.
---
--- Брокер отвечает на каждый раздел и на каждое дело своим кодом, и
--- решает по нему не текст, а род отказа `tnt-storage`: повторить ли,
--- дошла ли запись. Здесь названы коды, которые встречаются в запросах
--- пакета; незнакомый — обрыв посреди дела (`broken`): что случилось
--- на той стороне, пакет не знает, и записанное могло лечь.
---
--- Сверх рода у кода бывают три пометки:
---
--- * `stale` — описание кластера устарело: ведущий раздела сменился или
---   тема только заведена. Повтор сначала спрашивает описание заново;
--- * `coordinator` — группу ведёт другой узел, и его ищут заново;
--- * `rejoin` — участник выпал из поколения группы и входит в неё снова.

local failure = require('tnt.storage.failure')

local Module = {}

--- Нет ошибки.
Module.NONE = 0

--- Смещение вне журнала раздела: хранение стёрло то, что не прочитано.
Module.OFFSET_OUT_OF_RANGE = 1

--- Тема уже есть: заведение темы тогда удалось.
Module.TOPIC_ALREADY_EXISTS = 36

--- Запись с этим номером уже легла: повтор отправки узнан.
Module.DUPLICATE_SEQUENCE_NUMBER = 46

--- Брокер выдал участнику номер и просит войти с ним заново.
Module.MEMBER_ID_REQUIRED = 79

--- Коды, после которых номер производителя берут заново: брокер его
--- забыл, либо счёт записей разошёлся с брокерским.
---@type table<integer, boolean>
Module.PRODUCER_RESET = { [45] = true, [47] = true, [59] = true }

local UNREACHABLE, DENIED, TIMEOUT = failure.UNREACHABLE, failure.DENIED, failure.TIMEOUT
local BROKEN, REJECTED, CONFLICT = failure.BROKEN, failure.REJECTED, failure.CONFLICT

--- Известные коды: имя, род и пометки.
---@type table<integer, { [1]: string, [2]: string, stale: boolean|nil, coordinator: boolean|nil, rejoin: boolean|nil }>
local KNOWN = {
    [-1] = { 'UNKNOWN_SERVER_ERROR', BROKEN },
    [1] = { 'OFFSET_OUT_OF_RANGE', REJECTED },
    [2] = { 'CORRUPT_MESSAGE', BROKEN },
    [3] = { 'UNKNOWN_TOPIC_OR_PARTITION', UNREACHABLE, stale = true },
    [5] = { 'LEADER_NOT_AVAILABLE', UNREACHABLE, stale = true },
    [6] = { 'NOT_LEADER_OR_FOLLOWER', UNREACHABLE, stale = true },
    [7] = { 'REQUEST_TIMED_OUT', TIMEOUT },
    [10] = { 'MESSAGE_TOO_LARGE', REJECTED },
    [13] = { 'NETWORK_EXCEPTION', BROKEN },
    [14] = { 'COORDINATOR_LOAD_IN_PROGRESS', UNREACHABLE },
    [15] = { 'COORDINATOR_NOT_AVAILABLE', UNREACHABLE, coordinator = true },
    [16] = { 'NOT_COORDINATOR', UNREACHABLE, coordinator = true },
    [17] = { 'INVALID_TOPIC_EXCEPTION', REJECTED },
    [18] = { 'RECORD_LIST_TOO_LARGE', REJECTED },
    [19] = { 'NOT_ENOUGH_REPLICAS', UNREACHABLE },
    [20] = { 'NOT_ENOUGH_REPLICAS_AFTER_APPEND', BROKEN },
    [22] = { 'ILLEGAL_GENERATION', CONFLICT, rejoin = true },
    [25] = { 'UNKNOWN_MEMBER_ID', CONFLICT, rejoin = true },
    [26] = { 'INVALID_SESSION_TIMEOUT', REJECTED },
    [27] = { 'REBALANCE_IN_PROGRESS', CONFLICT, rejoin = true },
    [29] = { 'TOPIC_AUTHORIZATION_FAILED', DENIED },
    [30] = { 'GROUP_AUTHORIZATION_FAILED', DENIED },
    [31] = { 'CLUSTER_AUTHORIZATION_FAILED', DENIED },
    [32] = { 'INVALID_TIMESTAMP', REJECTED },
    [33] = { 'UNSUPPORTED_SASL_MECHANISM', DENIED },
    [34] = { 'ILLEGAL_SASL_STATE', DENIED },
    [35] = { 'UNSUPPORTED_VERSION', REJECTED },
    [37] = { 'INVALID_PARTITIONS', REJECTED },
    [38] = { 'INVALID_REPLICATION_FACTOR', REJECTED },
    [40] = { 'INVALID_CONFIG', REJECTED },
    [42] = { 'INVALID_REQUEST', REJECTED },
    [44] = { 'POLICY_VIOLATION', REJECTED },
    [45] = { 'OUT_OF_ORDER_SEQUENCE_NUMBER', UNREACHABLE },
    [47] = { 'INVALID_PRODUCER_EPOCH', UNREACHABLE },
    [56] = { 'KAFKA_STORAGE_ERROR', UNREACHABLE, stale = true },
    [58] = { 'SASL_AUTHENTICATION_FAILED', DENIED },
    [59] = { 'UNKNOWN_PRODUCER_ID', UNREACHABLE },
    [74] = { 'FENCED_LEADER_EPOCH', UNREACHABLE, stale = true },
    [79] = { 'MEMBER_ID_REQUIRED', CONFLICT, rejoin = true },
    [87] = { 'INVALID_RECORD', REJECTED },
    [89] = { 'THROTTLING_QUOTA_EXCEEDED', UNREACHABLE },
}

---@class TntKafkaCode Что значит код брокера
---@field code integer|string
---@field name string Имя кода по протоколу
---@field kind string Род отказа
---@field stale boolean Описание кластера устарело
---@field coordinator boolean Ведущий группы сменился
---@field rejoin boolean Участник выпал из поколения

--- Что значит код. Код отказа приходит и полем `server_code`, где
--- бывает и строкой, — незнакомое значение значит обрыв.
---@param code integer|string
---@return TntKafkaCode
function Module.of(code)
    local known = KNOWN[code] or { ('код %s'):format(code), BROKEN }

    return {
        code = code,
        name = known[1],
        kind = known[2],
        stale = known.stale == true,
        coordinator = known.coordinator == true,
        rejoin = known.rejoin == true,
    }
end

--- Имя кода для текста отказа: `NOT_LEADER_OR_FOLLOWER (6)`.
---@param code integer|string
---@return string
function Module.name(code)
    return ('%s (%s)'):format(Module.of(code).name, code)
end

--- Отказ по коду брокера: род от кода, код — в `server_code`.
---@param code integer
---@param text string Что не вышло и где
---@param opts { sent: boolean|nil, retriable: boolean|nil, idempotent: boolean|nil }|nil
---@return TntStorageFailure
function Module.failure(code, text, opts)
    local given = opts or {}

    return failure.new(Module.of(code).kind, ('%s: %s'):format(text, Module.name(code)), {
        server_code = code,
        sent = given.sent,
        retriable = given.retriable,
        idempotent = given.idempotent,
    })
end

return Module
