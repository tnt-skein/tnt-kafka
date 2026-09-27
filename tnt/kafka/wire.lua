--- Значения байтами: целые старшим байтом вперёд, строки и байты
--- с длиной, числа переменной длины записей.
---
--- Протокол Kafka знает два вида длины. Поля запроса и ответа несут
--- длину целым: строка — двумя байтами, байты и список — четырьмя,
--- а `-1` значит пустоту (`null`). Поля записи внутри пачки несут её
--- числом переменной длины со знаком, свёрнутым зигзагом: `0, -1, 1, -2`
--- становятся `0, 1, 2, 3`, и малое по модулю число занимает один байт.
---
--- **Восьмибайтовые целые — числом Lua.** Смещение, отметка времени
--- и номер производителя в жизни не доходят до 2^53, а число Lua держит
--- целые точно до этой границы. cdata `int64` здесь дали бы свой род
--- значения всем, кто читает ответ, ради того, чего не бывает.
---
--- Чтение идёт чтецом, который помнит место: разбор ответа — это десяток
--- полей подряд, и позиция, передаваемая руками, ошибалась бы на каждом.
--- Ответ короче, чем обещают его поля, — исключение с местом: на него
--- отвечает разбор всего ответа, и выше он становится обрывом связи.

local bit = require('bit')

local fail = require('tnt.must.fail')

local Module = {}

--- 2^32: основание старшей половины восьмибайтового целого.
local WORD = 0x100000000

--- Сколько значений у целого по числу его байт: граница знака — половина.
---@type table<integer, integer>
local SPAN = { [1] = 0x100, [2] = 0x10000, [4] = 0x100000000 }

--- Сколько байт самое большее у числа переменной длины: десять
--- по семь бит покрывают любое восьмибайтовое.
local VARINT_BYTES = 10

--- Байты четырёхбайтового целого; отрицательное пишется дополнением.
---@param value integer
---@return string
local function quad(value)
    return string.char(
        bit.band(bit.rshift(value, 24), 0xff),
        bit.band(bit.rshift(value, 16), 0xff),
        bit.band(bit.rshift(value, 8), 0xff),
        bit.band(value, 0xff)
    )
end

--- Однобайтовое целое.
---@param value integer
---@return string
function Module.int8(value)
    return string.char(bit.band(value, 0xff))
end

--- Двухбайтовое целое.
---@param value integer
---@return string
function Module.int16(value)
    return string.char(bit.band(bit.rshift(value, 8), 0xff), bit.band(value, 0xff))
end

--- Четырёхбайтовое целое, со знаком или без: байты одни и те же.
Module.int32 = quad

--- Восьмибайтовое целое: старшая половина со знаком, младшая — без.
---@param value integer Целое, по модулю меньше 2^53
---@return string
function Module.int64(value)
    local high = math.floor(value / WORD)

    return quad(high) .. quad(value % WORD)
end

--- Строка с длиной в два байта; `nil` — пустота.
---@param value string|nil
---@return string
function Module.string(value)
    if value == nil then
        return Module.int16(-1)
    end

    return Module.int16(#value) .. value
end

--- Байты с длиной в четыре байта; `nil` — пустота.
---@param value string|nil
---@return string
function Module.bytes(value)
    if value == nil then
        return quad(-1)
    end

    return quad(#value) .. value
end

--- Число переменной длины со знаком, свёрнутое зигзагом.
---@param value integer Целое, по модулю меньше 2^52
---@return string
function Module.varint(value)
    local left = value >= 0 and value * 2 or -value * 2 - 1
    local parts = {}

    while left >= 0x80 do
        parts[#parts + 1] = string.char(left % 0x80 + 0x80)
        left = math.floor(left / 0x80)
    end

    parts[#parts + 1] = string.char(left)

    return table.concat(parts)
end

--- Список: число элементов и элементы; `nil` — пустота.
---@param items any[]|nil
---@param each fun(item: any): string Байты одного элемента
---@return string
function Module.array(items, each)
    if items == nil then
        return quad(-1)
    end

    local parts = { quad(#items) }

    for _, item in ipairs(items) do
        parts[#parts + 1] = each(item)
    end

    return table.concat(parts)
end

---@class TntKafkaReader Чтец ответа: байты и место, с которого читать
---@field data string Весь ответ
---@field at integer Место следующего байта
local Reader = {}
Reader.__index = Reader

--- Заводит чтеца с начала данных.
---@param data string
---@return TntKafkaReader
function Module.reader(data)
    return setmetatable({ data = data, at = 1 }, Reader)
end

--- Отрезает следующие `size` байт.
---
--- Короче просимого — исключение: ответ обещал больше, чем пришло,
--- и читать дальше значит читать чужие поля.
---@param size integer
---@return string
function Reader:raw(size)
    local till = self.at + size - 1

    if size < 0 or till > #self.data then
        local text = 'ответ короче, чем обещают поля: нужно %d байт с %d, а их %d'

        fail.raise(text:format(size, self.at, #self.data))
    end

    local piece = self.data:sub(self.at, till)

    self.at = till + 1

    return piece
end

--- Сколько байт ещё не прочитано.
---@return integer
function Reader:left()
    return #self.data - self.at + 1
end

--- Беззнаковое целое из `size` байт старшим вперёд.
---@param reader TntKafkaReader
---@param size integer
---@return integer
local function unsigned(reader, size)
    ---@type integer
    local value = 0

    -- Целое отрезается одним куском: отказ о коротком ответе называет
    -- всё целое, а не его последний байт.
    local piece = reader:raw(size)

    for index = 1, size do
        value = value * 0x100 + piece:byte(index)
    end

    return value
end

--- Целое со знаком из `size` байт.
---@param reader TntKafkaReader
---@param size integer
---@return integer
local function signed(reader, size)
    local value = unsigned(reader, size)
    local limit = SPAN[size]

    if value >= limit / 2 then
        return value - limit
    end

    return value
end

--- Однобайтовое целое со знаком.
---@return integer
function Reader:int8()
    return signed(self, 1)
end

--- Двухбайтовое целое со знаком.
---@return integer
function Reader:int16()
    return signed(self, 2)
end

--- Четырёхбайтовое целое со знаком.
---@return integer
function Reader:int32()
    return signed(self, 4)
end

--- Четырёхбайтовое целое без знака: контрольная сумма пачки.
---@return integer
function Reader:uint32()
    return unsigned(self, 4)
end

--- Восьмибайтовое целое со знаком.
---@return integer
function Reader:int64()
    local high = signed(self, 4)

    return high * WORD + unsigned(self, 4)
end

--- Строка с длиной в два байта; длина `-1` — пустота.
---@return string|nil
function Reader:string()
    local size = self:int16()

    if size == -1 then
        return nil
    end

    return self:raw(size)
end

--- Байты с длиной в четыре байта; длина `-1` — пустота.
---@return string|nil
function Reader:bytes()
    local size = self:int32()

    if size == -1 then
        return nil
    end

    return self:raw(size)
end

--- Число переменной длины со знаком.
---
--- Длиннее десяти байт числа не бывает: такое — порча, и читать его
--- до конца ответа значило бы съесть чужие поля.
---@return integer
function Reader:varint()
    local value, scale = 0, 1

    for _ = 1, VARINT_BYTES do
        local byte = self:raw(1):byte()

        value = value + (byte % 0x80) * scale

        if byte < 0x80 then
            if value % 2 == 0 then
                return math.floor(value / 2)
            end

            return -math.floor(value / 2) - 1
        end

        scale = scale * 0x80
    end

    -- Бросок не возвращается: целого здесь нет, и отдавать нечего.
    ---@diagnostic disable-next-line: return-type-mismatch
    return fail.raise(('число переменной длины длиннее %d байт'):format(VARINT_BYTES))
end

return Module
