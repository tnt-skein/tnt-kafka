--- Проверки приговора обработчику: настройки получателя с умолчаниями
--- и решение — подтвердить, выдать снова либо зарыть.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local outcome = helper.outcome

local g = t.group('tnt.kafka.outcome')

--- Настройки получателя с виной на строке вызова.
---@param opts any
---@return table
local function policy(opts)
    local checked = outcome.policy(opts, 2)

    return checked
end

g.test_policy_fills_defaults = function()
    t.assert_equals(policy(nil), { workers = 1, max_attempts = 5, backoff = { base = 1, max = 60 } })
end

g.test_policy_takes_what_is_given = function()
    t.assert_equals(
        policy({ workers = 4, max_attempts = 2, backoff = { base = 0.5, max = 5 } }),
        { workers = 4, max_attempts = 2, backoff = { base = 0.5, max = 5 } }
    )
    t.assert_equals(policy({ backoff = { base = 2 } }).backoff, { base = 2, max = 60 })
    t.assert_equals(policy({ backoff = { max = 0 } }).backoff, { base = 1, max = 0 })
    t.assert_equals(policy({ backoff = { base = outcome.BACKOFF_MAX * 0 } }).backoff.base, 0)
end

g.test_wrong_policy_blames_the_caller = function()
    helper.assert_blamed({
        {
            function()
                policy({ worker = 2 })
            end,
            'настройки получателя: ключа «worker» нет, есть atomic, backoff, max_attempts, workers',
        },
        {
            function()
                policy({ atomic = true })
            end,
            'настройки получателя: «atomic» брокер не умеет — смещение фиксируется по сети, '
                .. 'а транзакция box уступки не переживает',
        },
        {
            function()
                policy({ workers = 0 })
            end,
            'настройки получателя.workers — число больше 0, а не 0',
        },
        {
            function()
                policy({ max_attempts = 0 })
            end,
            'настройки получателя.max_attempts — число больше 0, а не 0',
        },
        {
            function()
                policy({ backoff = { base = -1 } })
            end,
            'настройки получателя.backoff.base — число от 0 до 3153600000, а не -1',
        },
        {
            function()
                policy({ backoff = { max = 3153600001 } })
            end,
            'настройки получателя.backoff.max — число от 0 до 3153600000, а не 3153600001',
        },
        {
            function()
                policy({ backoff = { step = 1 } })
            end,
            'настройки получателя.backoff: ключа «step» нет, есть base, max',
        },
    })
end

--- Настройки получателя для приговоров.
local POLICY = { workers = 1, max_attempts = 3, backoff = { base = 1, max = 60 } }

g.test_value_or_nothing_acks = function()
    t.assert_equals({ outcome.decide(true, nil, nil, 1, POLICY) }, { 'ack' })
    t.assert_equals({ outcome.decide(true, 'готово', nil, 1, POLICY) }, { 'ack' })
    t.assert_equals({ outcome.decide(true, 'готово', 'и ещё', 1, POLICY) }, { 'ack' })
    t.assert_equals({ outcome.decide(true, false, nil, 1, POLICY) }, { 'ack' })
end

g.test_failure_retries_with_backoff = function()
    t.assert_equals({ outcome.decide(true, nil, 'нет', 1, POLICY) }, { 'retry', 1 })
    t.assert_equals({ outcome.decide(true, false, 'нет', 2, POLICY) }, { 'retry', 2 })
    t.assert_equals(
        { outcome.decide(true, nil, 'нет', 7, { max_attempts = 10, backoff = { base = 1, max = 60 } }) },
        { 'retry', 60 }
    )
    t.assert_equals(
        { outcome.decide(true, nil, 'нет', 3, { max_attempts = 10, backoff = { base = 0.5, max = 60 } }) },
        { 'retry', 2 }
    )
end

g.test_retry_after_wins_when_it_is_a_number = function()
    t.assert_equals({ outcome.decide(true, nil, { retry_after = 0.2 }, 1, POLICY) }, { 'retry', 0.2 })
    t.assert_equals({ outcome.decide(true, nil, { retry_after = 0 }, 1, POLICY) }, { 'retry', 0 })
    t.assert_equals({ outcome.decide(true, nil, { retry_after = 1e12 }, 1, POLICY) }, { 'retry', 3153600000 })
    t.assert_equals({ outcome.decide(true, nil, { retry_after = -1 }, 1, POLICY) }, { 'retry', 1 })
    t.assert_equals({ outcome.decide(true, nil, { retry_after = 0 / 0 }, 1, POLICY) }, { 'retry', 1 })
    t.assert_equals({ outcome.decide(true, nil, { retry_after = '5' }, 1, POLICY) }, { 'retry', 1 })
end

g.test_last_attempt_buries_with_count = function()
    t.assert_equals({ outcome.decide(true, nil, 'нет', 3, POLICY) }, { 'bury', 'попыток 3 из 3: нет' })
    t.assert_equals(
        { outcome.decide(true, nil, { message = 'подождите' }, 4, POLICY) },
        { 'bury', 'попыток 4 из 3: подождите' }
    )
end

g.test_hopeless_failure_buries_at_once = function()
    t.assert_equals(
        { outcome.decide(true, nil, { retriable = false, message = 'тело негодно' }, 1, POLICY) },
        { 'bury', 'тело негодно' }
    )
end

g.test_raise_is_a_failure_with_its_text = function()
    t.assert_equals({ outcome.decide(false, 'упал', nil, 1, POLICY) }, { 'retry', 1 })
    t.assert_equals(
        { outcome.decide(false, 'упал', nil, 3, POLICY) },
        { 'bury', 'попыток 3 из 3: упал' }
    )
end

g.test_text_prefers_message = function()
    t.assert_equals(outcome.text({ message = 'текст' }), 'текст')
    t.assert_equals(outcome.text('строка'), 'строка')
    t.assert_equals(outcome.text(42), '42')
    t.assert_equals(outcome.text({ message = 5 }):find('^table: ') ~= nil, true)
    t.assert_equals(outcome.text(nil), 'nil')
end

g.test_verdict_names = function()
    t.assert_equals({ outcome.ACK, outcome.RETRY, outcome.BURY }, { 'ack', 'retry', 'bury' })
end
