--- Запрос и ответ по описанию полей.
---
--- Каждый запрос Kafka — это поля подряд без имён и разделителей: целые,
--- строки, списки таких же полей. Описание — список пар «имя, род» —
--- заменяет полтора десятка рукописных сборщиков и разборщиков: запрос
--- собирается и ответ разбирается одним обходом, а версия запроса — это
--- просто другой список. Ошибиться в порядке полей тогда можно только
--- в описании, и описание читается глазами рядом с протоколом.
---
--- Роды:
---
--- * `int8`, `int16`, `int32`, `int64`, `boolean`, `string`, `bytes` —
---   одно значение; пустота у строки и байтов — `nil`;
--- * `[int32]`, `[string]` — список таких значений;
--- * таблица — список записей с этими полями.
---
--- Список пустотой (`-1`) читается как `nil`: так Kafka говорит «все»,
--- например о темах в запросе описания кластера.

local fail = require('tnt.must.fail')
local wire = require('tnt.kafka.wire')

local Module = {}

--- Как пишется одно значение каждого рода.
---@type table<string, fun(value: any): string>
local WRITE = {
    int8 = wire.int8,
    int16 = wire.int16,
    int32 = wire.int32,
    int64 = wire.int64,
    string = wire.string,
    bytes = wire.bytes,
    boolean = function(value)
        return wire.int8(value and 1 or 0)
    end,
}

--- Как читается одно значение каждого рода.
---@type table<string, fun(reader: TntKafkaReader): any>
local READ = {
    int8 = function(reader)
        return reader:int8()
    end,
    int16 = function(reader)
        return reader:int16()
    end,
    int32 = function(reader)
        return reader:int32()
    end,
    int64 = function(reader)
        return reader:int64()
    end,
    string = function(reader)
        return reader:string()
    end,
    bytes = function(reader)
        return reader:bytes()
    end,
    boolean = function(reader)
        return reader:int8() ~= 0
    end,
}

--- Списки значений, которые знает описание, и род их элемента.
---@type table<string, string>
local ITEMS = { ['[int32]'] = 'int32', ['[string]'] = 'string' }

--- Род элемента у списка значений: `[int32]` → `int32`; иначе `nil`.
---@param kind string
---@return string|nil
local function item_of(kind)
    return ITEMS[kind]
end

--- Байты одного поля.
---@param kind string|table
---@param value any
---@return string
local function written(kind, value)
    if type(kind) == 'table' then
        return wire.array(value, function(entry)
            return Module.encode(kind, entry)
        end)
    end

    local item = item_of(kind)

    if item ~= nil then
        return wire.array(value, WRITE[item])
    end

    return WRITE[kind](value)
end

--- Собирает поля по описанию.
---@param fields table[] Пары «имя, род» по порядку
---@param values table Значения по именам
---@return string
function Module.encode(fields, values)
    local parts = {}

    for index, field in ipairs(fields) do
        parts[index] = written(field[2], values[field[1]])
    end

    return table.concat(parts)
end

--- Читает список: число элементов, затем элементы.
---
--- Число больше, чем осталось байт, — порча: каждый элемент занимает хотя
--- бы байт, и без этой сверки испорченная длина заставила бы разбор
--- крутиться миллиарды раз, прежде чем кончатся данные.
---@param reader TntKafkaReader
---@param each fun(reader: TntKafkaReader): any
---@return any[]|nil
local function listed(reader, each)
    local count = reader:int32()

    if count < -1 or count > reader:left() then
        fail.raise(
            ('в списке %d элементов, а байт осталось %d'):format(count, reader:left())
        )
    end

    if count == -1 then
        return nil
    end

    local items = {}

    for index = 1, count do
        items[index] = each(reader)
    end

    return items
end

--- Значение одного поля.
---@param kind string|table
---@param reader TntKafkaReader
---@return any
local function read(kind, reader)
    if type(kind) == 'table' then
        return listed(reader, function()
            return Module.decode(kind, reader)
        end)
    end

    local item = item_of(kind)

    if item ~= nil then
        return listed(reader, READ[item])
    end

    return READ[kind](reader)
end

--- Разбирает поля по описанию.
---@param fields table[] Пары «имя, род» по порядку
---@param reader TntKafkaReader
---@return table
function Module.decode(fields, reader)
    local values = {}

    for _, field in ipairs(fields) do
        values[field[1]] = read(field[2], reader)
    end

    return values
end

return Module
