--- Крюки: то, что приложение добавляет к отправке и к обработке сверх
--- настроек вызывающего, — отрезки трассы `producer` и `consumer`, замер,
--- своя запись в журнал.
---
--- Пакет ни от трассировки, ни от метрик не зависит: знание о них
--- приходит крюком. Крюк — `hook(call, proceed)`, где `call = { kind =
--- 'send' | 'handle', name, message }`:
---
---     kafka.hook('timing', function(call, proceed)
---         local started = clock.monotonic()
---         local first, second, third = proceed()
---
---         observe(call.kind, call.name, clock.monotonic() - started)
---
---         return first, second, third
---     end)
---
--- На отправке конверт собирается **внутри** `proceed`: тогда заголовок
--- `traceparent` несёт отрезок, который завёл крюк. На обработке конверт
--- есть до крюка — из него крюк берёт родителя трассы.
---
--- Цепочка собирается изнутри наружу: `proceed` пакета — сердцевина,
--- каждый крюк — слой вокруг следующего. Каждый слой исполняется один
--- раз, сколько бы его ни звали: крюк, позвавший `proceed` дважды,
--- отправил бы сообщение дважды. Сорвавшийся крюк ничего не срывает:
--- бросивший и не позвавший `proceed` пропускается — `proceed` зовёт
--- пакет сам, — и пишется `warn` с именем крюка, с подавлением повторов.
--- Итог — всегда итог `proceed`; что вернул сам крюк, не читается.
---
--- Через крюк проходит не больше трёх значений — «опознаватель, отказ»
--- у отправки и «дошёл, значение, отказ» у обработки.
---
--- **Список — состояние процесса**: клиентов в узле бывает несколько,
--- и крюк, поставленный одному, до других не дошёл бы.

local must = require('tnt.must')

--- Журнал с подавлением повторов: крюк, бросающий на каждом сообщении,
--- иначе писал бы одну и ту же строку на каждое.
local log = require('tnt.log').changes('tnt.kafka')

local Module = {}

--- Род обращения: отправка.
Module.SEND = 'send'

--- Род обращения: обработка доставленного.
Module.HANDLE = 'handle'

---@class TntKafkaCall Что оборачивает крюк
---@field kind string `send` либо `handle`
---@field name string Имя темы
---@field message TntKafkaMessage|nil Конверт; у отправки — после `proceed`

---@alias TntKafkaHook fun(call: TntKafkaCall, proceed: fun(): ...): any

--- Поставленные крюки по порядку.
---@type { name: string, hook: TntKafkaHook }[]
local installed = {}

--- Ставит, заменяет или снимает крюк. Замена оставляет крюк на прежнем
--- месте: крюк ставят и при старте, и при перечитывании настроек.
---@param name string Имя крюка, например `timing`
---@param hook TntKafkaHook|nil Крюк; `nil` снимает
function Module.set(name, hook)
    local caller = must.at(2)

    caller.not_empty(name, 'имя крюка')
    caller.optional.callable(hook, 'крюк')

    ---@type integer|nil, { name: string, hook: TntKafkaHook }|nil
    local at, found

    for index, entry in ipairs(installed) do
        if entry.name == name then
            at, found = index, entry
        end
    end

    if found == nil then
        if hook ~= nil then
            installed[#installed + 1] = { name = name, hook = hook }
        end
    elseif hook == nil then
        table.remove(installed, at)
    else
        found.hook = hook
    end
end

--- Имена поставленных крюков в порядке постановки: их читает сводка,
--- и отдавать в неё чужие функции незачем.
---@return string[]
function Module.names()
    local listed = {}

    for _, entry in ipairs(installed) do
        table.insert(listed, entry.name)
    end

    return listed
end

--- Исполняет тело один раз и помнит его итог.
---@param body fun(): any, any, any
---@return fun(): any, any, any run Исполнить либо вернуть запомненное
---@return fun(): boolean started Звали ли уже
local function once(body)
    local started, first, second, third = false, nil, nil, nil

    local function run()
        if not started then
            started = true
            first, second, third = body()
        end

        return first, second, third
    end

    return run, function()
        return started
    end
end

--- Слой крюка вокруг следующего слоя.
---@param entry { name: string, hook: TntKafkaHook }
---@param call TntKafkaCall
---@param inner fun(): any, any, any
---@param started fun(): boolean
---@return fun(): any, any, any
---@return fun(): boolean
local function layer(entry, call, inner, started)
    return once(function()
        local ok, err = pcall(entry.hook, call, inner)

        if not (ok and started()) then
            log.warn('крюк очереди сорвался и пропущен', {
                hook = entry.name,
                kind = call.kind,
                destination = call.name,
                err = ok and 'крюк не позвал proceed' or tostring(err),
            })
        end

        return inner()
    end)
end

--- Исполняет `proceed` внутри всех крюков и отдаёт его итог.
---@param call TntKafkaCall
---@param proceed fun(): any, any, any
---@return any first
---@return any second
---@return any third
function Module.around(call, proceed)
    local run, started = once(proceed)

    for index = #installed, 1, -1 do
        run, started = layer(installed[index], call, run, started)
    end

    return run()
end

return Module
