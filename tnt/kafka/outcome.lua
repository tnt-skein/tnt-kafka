--- Итог обработчика: настройки получателя и приговор — подтвердить,
--- вернуть с отсрочкой или зарыть.
---
--- **Подтверждение — то, что вернул обработчик**, а не вызов, который
--- можно забыть. Значение или ничего — подтвердить: смещение раздела
--- фиксируется за сообщением. `nil, err` либо `false, err` — вернуть:
--- копия с номером выдачи уходит в тему повтора, смещение фиксируется.
--- Выдача номер `max_attempts` зарывает с причиной «попыток N из M»;
--- `err.retriable == false` зарывает сразу — повтор негодного тела ничего
--- не даст; исключение — как отказ, причина — его текст.
---
--- `err` читается по полям, а не по типу: `retriable` и `retry_after` есть
--- и у `TntStorageFailure`, и у `TntHttpFailure`, а таблица без
--- `retriable` и строка — повторяемый отказ.
---
--- **`atomic` брокер не умеет**: обработчик ходит в сеть, а транзакция box
--- уступки не переживает. Названный `atomic` — исключение, а не
--- молчаливый пропуск.

local must = require('tnt.must')

local message = require('tnt.kafka.message')

local Module = {}

--- Приговоры: подтвердить, выдать снова после отсрочки, зарыть.
Module.ACK, Module.RETRY, Module.BURY = 'ack', 'retry', 'bury'

--- Сколько обработчиков по умолчанию: один участник группы.
Module.WORKERS = 1

--- После какой выдачи неудача зарывает по умолчанию.
Module.MAX_ATTEMPTS = 5

--- Отступ по умолчанию: от секунды до минуты.
Module.BACKOFF_BASE, Module.BACKOFF_MAX = 1, 60

--- Настройки получателя. `atomic` знаком, но невыполним.
local CONSUME = { atomic = '?', backoff = '?table', max_attempts = '?integer', workers = '?integer' }

--- Настройки отступа.
local BACKOFF = { max = '?number', base = '?number' }

---@class TntKafkaPolicy Настройки получателя с умолчаниями
---@field workers integer Сколько участников группы заводит получатель
---@field max_attempts integer После этой выдачи неудача зарывает
---@field backoff { base: number, max: number } Отступ перед повтором, секунды

--- Проверяет настройки получателя и ставит умолчания; негодные —
--- исключение на строке того, кто объявил получателя.
---@param opts any
---@param level integer Уровень вины в кадрах вызывающего эту функцию
---@return TntKafkaPolicy
function Module.policy(opts, level)
    local check = must.at(level + 1)
    local given = check.optional.options(opts, 'настройки получателя', CONSUME) or {}

    if given.atomic ~= nil then
        error(
            'настройки получателя: «atomic» брокер не умеет — смещение фиксируется по сети, '
                .. 'а транзакция box уступки не переживает',
            level + 1
        )
    end

    local backoff = check.optional.options(given.backoff, 'настройки получателя.backoff', BACKOFF)
        or {}

    for _, name in ipairs({ 'workers', 'max_attempts' }) do
        check.optional.positive(given[name], 'настройки получателя.' .. name)
    end

    for _, name in ipairs({ 'base', 'max' }) do
        check.optional.between(
            backoff[name],
            'настройки получателя.backoff.' .. name,
            0,
            message.MAX_SECONDS
        )
    end

    ---@type TntKafkaPolicy
    local policy = {
        workers = Module.WORKERS,
        max_attempts = Module.MAX_ATTEMPTS,
        backoff = { base = Module.BACKOFF_BASE, max = Module.BACKOFF_MAX },
    }

    policy.workers = given.workers or policy.workers
    policy.max_attempts = given.max_attempts or policy.max_attempts
    policy.backoff.base = backoff.base or policy.backoff.base
    policy.backoff.max = backoff.max or policy.backoff.max

    return policy
end

--- Текст причины: поле `message` отказа, а у прочего — `tostring`.
---
--- Без этого отказ `{ retry_after = 0.2, message = 'подождите' }` без
--- метатаблицы ушёл бы в журнал и в заголовок зарытого адресом
--- «table: 0x…».
---@param reason any
---@return string
function Module.text(reason)
    if type(reason) ~= 'table' or type(reason.message) ~= 'string' then
        return tostring(reason)
    end

    return reason.message
end

--- Отсрочка повтора: годное число `retry_after`, иначе отступ
--- `base · 2^(attempt−1)`, но не больше `max`.
---@param reason any
---@param attempt integer
---@param backoff { base: number, max: number }
---@return number
local function pause(reason, attempt, backoff)
    local asked = type(reason) == 'table' and reason.retry_after or nil

    -- NaN не равен сам себе; отрицательное и не число — мимо.
    local usable = type(asked) == 'number' and asked == asked and asked >= 0

    if usable then
        return math.min(asked, message.MAX_SECONDS)
    end

    return math.min(backoff.base * 2 ^ (attempt - 1), backoff.max)
end

--- Приговор обработчику.
---
--- Первые два аргумента — то, что отдал `xpcall`: дошёл ли обработчик
--- до возврата, а если нет — что он бросил. Второе значение приговора
--- у `retry` — отсрочка, у `bury` — причина, у `ack` его нет.
---@param ok boolean Дошёл ли обработчик до возврата
---@param value any Первое значение обработчика либо брошенное им
---@param err any Второе значение обработчика
---@param attempt integer Номер выдачи
---@param policy TntKafkaPolicy
---@return string verdict
---@return number|string|nil extra
function Module.decide(ok, value, err, attempt, policy)
    -- Значение — успех; пустота — успех, если отказа рядом нет.
    local failed = not ok or (not value and err ~= nil)

    if not failed then
        return Module.ACK
    end

    local reason = value

    if ok then
        reason = err
    end

    if type(reason) == 'table' and reason.retriable == false then
        return Module.BURY, Module.text(reason)
    end

    if attempt >= policy.max_attempts then
        return Module.BURY, ('попыток %d из %d: %s'):format(attempt, policy.max_attempts, Module.text(reason))
    end

    return Module.RETRY, pause(reason, attempt, policy.backoff)
end

return Module
