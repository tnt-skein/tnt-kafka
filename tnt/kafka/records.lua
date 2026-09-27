--- Пачка записей второго вида: сборка для отправки и разбор выборки.
---
--- Kafka хранит и отдаёт записи пачками. Шапка пачки несёт смещение
--- первой записи, контрольную сумму, признаки, номер производителя
--- и номер первой записи в его счёте; дальше записи, у каждой — сдвиг
--- смещения и времени от шапки, ключ, значение и заголовки. Длины внутри
--- записи — числа переменной длины (`tnt.kafka.wire`).
---
--- **Контрольная сумма — CRC32C** от признаков до конца пачки. Встроенный
--- `digest.crc32` считает тот же многочлен Кастаньоли, но без последнего
--- обращения битов, поэтому сумма — его побитовое отрицание: своя
--- таблица на 256 слов не нужна.
---
--- **Сжатые пачки не читаются.** Производитель этого пакета не сжимает,
--- а разжать чужое — это gzip, snappy, lz4 и zstd, половины которых
--- у Tarantool нет. Такая пачка отдаётся разбором без записей, с родом
--- сжатия, и получатель зарыть её не может — только пропустить со
--- счётом и записью в журнал (`tnt.kafka.worker`).
---
--- **Отменённое в транзакции отсеивается здесь.** Выборка с уровнем
--- «только зафиксированное» не прячет пачки отменённых транзакций, а
--- присылает рядом их список: производитель и смещение, с которого его
--- транзакция отменена. Служебные пачки (метки конца транзакции)
--- получателю не нужны вовсе.

local bit = require('bit')
local digest = require('digest')

local fail = require('tnt.must.fail')
local wire = require('tnt.kafka.wire')

local Module = {}

--- Вид пачки, который пакет пишет и читает.
Module.MAGIC = 2

--- Сколько байт у начала пачки до её длины и сама длина: смещение (8)
--- и длина (4).
Module.HEAD = 12

--- Сколько байт у шапки пачки после её длины: эпоха ведущего (4), вид (1),
--- сумма (4), признаки (2), сдвиг последней (4), два времени (16), номер,
--- эпоха и счёт производителя (14) и число записей (4).
Module.BATCH_HEAD = 49

--- Признаки пачки: младшие три бита — сжатие.
local COMPRESSION = 0x07

--- Признак: пачка — часть транзакции.
local TRANSACTIONAL = 0x10

--- Признак: служебная пачка — метка конца транзакции.
local CONTROL = 0x20

--- Имена сжатия по номеру — для записи в журнал.
Module.CODECS = { 'gzip', 'snappy', 'lz4', 'zstd' }

---@class TntKafkaRecord Запись пачки
---@field offset number Смещение в разделе
---@field timestamp number Отметка времени, мс
---@field key string|nil Ключ
---@field value string|nil Значение
---@field headers { [1]: string, [2]: string|nil }[] Заголовки по порядку
---@field codec string|nil Сжатие пачки, которую прочесть нечем: запись тогда — её место
---@field last number|nil Смещение последней записи такой пачки

---@class TntKafkaBatch Разобранная пачка
---@field base_offset number Смещение первой записи
---@field last_offset number Смещение последней записи
---@field producer_id number
---@field transactional boolean
---@field control boolean
---@field codec string|nil Чем сжата; записей тогда нет
---@field records TntKafkaRecord[]

--- CRC32C строки как беззнаковое целое.
---@param data string
---@return integer
function Module.crc32c(data)
    return bit.bnot(digest.crc32(data)) % 2 ^ 32
end

--- Строка или байты с длиной числом переменной длины; `nil` — `-1`.
---@param value string|nil
---@return string
local function varbytes(value)
    if value == nil then
        return wire.varint(-1)
    end

    return wire.varint(#value) .. value
end

--- Байты одной записи.
---@param record { key: string|nil, value: string|nil, headers: table[]|nil, timestamp: integer|nil }
---@param delta integer Сдвиг смещения от шапки
---@param base integer Отметка времени шапки, мс
---@return string
local function record_bytes(record, delta, base)
    local headers = record.headers or {}
    local parts = {
        wire.int8(0),
        wire.varint((record.timestamp or base) - base),
        wire.varint(delta),
        varbytes(record.key),
        varbytes(record.value),
        wire.varint(#headers),
    }

    for _, header in ipairs(headers) do
        parts[#parts + 1] = varbytes(header[1]) .. varbytes(header[2])
    end

    local body = table.concat(parts)

    return wire.varint(#body) .. body
end

---@class TntKafkaBatchSpec Что уходит пачкой
---@field producer_id integer Номер производителя; `-1` — без номера
---@field producer_epoch integer Эпоха производителя
---@field base_sequence integer Номер первой записи в счёте производителя
---@field timestamp integer Отметка времени всех записей, мс
---@field records { key: string|nil, value: string|nil, headers: table[]|nil, timestamp: integer|nil }[]

--- Собирает пачку. Отметка времени шапки — `spec.timestamp`; у записи
--- своя, если названа, иначе та же.
---@param spec TntKafkaBatchSpec
---@return string
function Module.encode(spec)
    local records = {}

    for index, record in ipairs(spec.records) do
        records[index] = record_bytes(record, index - 1, spec.timestamp)
    end

    local summed = wire.int16(0)
        .. wire.int32(#spec.records - 1)
        .. wire.int64(spec.timestamp)
        .. wire.int64(spec.timestamp)
        .. wire.int64(spec.producer_id)
        .. wire.int16(spec.producer_epoch)
        .. wire.int32(spec.base_sequence)
        .. wire.int32(#spec.records)
        .. table.concat(records)
    local tail = wire.int32(-1) .. wire.int8(Module.MAGIC) .. wire.int32(Module.crc32c(summed)) .. summed

    return wire.int64(0) .. wire.int32(#tail) .. tail
end

--- Длина числом переменной длины и столько байт следом; `-1` — пустота.
---@param reader TntKafkaReader
---@return string|nil
local function read_varbytes(reader)
    local size = reader:varint()

    if size < 0 then
        return nil
    end

    return reader:raw(size)
end

--- Разбирает записи несжатой пачки.
---@param reader TntKafkaReader
---@param count integer
---@param base_offset number
---@param base_time number
---@return TntKafkaRecord[]
local function read_records(reader, count, base_offset, base_time)
    local records = {}

    for index = 1, count do
        local body = wire.reader(reader:raw(reader:varint()))

        body:int8()

        local timestamp = base_time + body:varint()
        local record = { offset = base_offset + body:varint(), timestamp = timestamp }

        record.key = read_varbytes(body)
        record.value = read_varbytes(body)
        record.headers = {}

        for place = 1, body:varint() do
            record.headers[place] = { read_varbytes(body) or '', read_varbytes(body) }
        end

        records[index] = record
    end

    return records
end

--- Разбирает одну пачку по её байтам после длины.
---@param base_offset number
---@param data string
---@return TntKafkaBatch
local function read_batch(base_offset, data)
    local reader = wire.reader(data)

    reader:int32()

    local magic = reader:int8()

    if magic ~= Module.MAGIC then
        fail.raise(
            ('пачка вида %d, а пакет читает только вид %d'):format(magic, Module.MAGIC)
        )
    end

    local crc = reader:uint32()

    if crc ~= Module.crc32c(data:sub(reader.at)) then
        fail.raise(
            ('контрольная сумма пачки со смещения %d не сошлась'):format(
                base_offset
            )
        )
    end

    local attributes = reader:int16()
    local last_delta = reader:int32()
    local base_time = reader:int64()

    reader:int64()

    local batch = {
        base_offset = base_offset,
        last_offset = base_offset + last_delta,
        producer_id = reader:int64(),
        transactional = bit.band(attributes, TRANSACTIONAL) ~= 0,
        control = bit.band(attributes, CONTROL) ~= 0,
        codec = Module.CODECS[bit.band(attributes, COMPRESSION)],
        records = {},
    }

    reader:int16()
    reader:int32()

    local count = reader:int32()

    if batch.codec == nil then
        batch.records = read_records(reader, count, base_offset, base_time)
    end

    return batch
end

--- Разбирает байты выборки в пачки.
---
--- Брокер вправе оборвать последнюю пачку на пределе выборки: неполная
--- пачка в конце не порча, а конец данных, и её дочитает следующая
--- выборка. Порча — неверная сумма и чужой вид — исключение: разбор
--- выборки идёт под `pcall`.
---@param data string|nil
---@return TntKafkaBatch[]
function Module.decode(data)
    local batches = {}
    local reader = wire.reader(data or '')

    while reader:left() >= Module.HEAD do
        local base_offset = reader:int64()
        local size = reader:int32()

        -- Длина короче шапки — порча: из неё не прочесть даже признаков.
        if size < Module.BATCH_HEAD then
            fail.raise(
                ('пачка со смещения %d длиной %d байт короче своей шапки'):format(
                    base_offset,
                    size
                )
            )
        end

        if size > reader:left() then
            break
        end

        batches[#batches + 1] = read_batch(base_offset, reader:raw(size))
    end

    return batches
end

--- Оставляет пачки, которые получателю положено видеть.
---
--- Служебные — прочь всегда. Пачка транзакции прочь, если её
--- производитель в списке отменённых и смещение отмены уже пройдено;
--- производитель выбывает из отменённых на своей метке конца транзакции.
--- Порядок списка не важен: запись списка срабатывает на первой пачке,
--- дошедшей до её смещения, и срабатывает один раз.
---@param batches TntKafkaBatch[]
---@param aborted { producer_id: number, first_offset: number }[]|nil
---@return TntKafkaBatch[]
function Module.visible(batches, aborted)
    local pending = table.copy(aborted or {})
    local cancelled = {}
    local kept = {}

    for _, batch in ipairs(batches) do
        for index = #pending, 1, -1 do
            if pending[index].first_offset <= batch.last_offset then
                cancelled[table.remove(pending, index).producer_id] = true
            end
        end

        if batch.control then
            cancelled[batch.producer_id] = nil
        elseif not (batch.transactional and cancelled[batch.producer_id]) then
            kept[#kept + 1] = batch
        end
    end

    return kept
end

return Module
