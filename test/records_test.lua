--- Проверки пачки записей: сборка, разбор, контрольная сумма, сжатые
--- и служебные пачки, отсев отменённого в транзакциях.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local records = helper.records
local wire = helper.wire

local g = t.group('tnt.kafka.records')

--- Пачка с другими признаками и номером производителя: сумма пересчитана.
---@param batch string
---@param attributes integer
---@param producer integer|nil
---@return string
local function patched(batch, attributes, producer)
    local head = batch:sub(1, 17)
    local summed = wire.int16(attributes) .. batch:sub(24, 43)

    summed = summed .. wire.int64(producer or -1) .. batch:sub(52)

    return head .. wire.int32(records.crc32c(summed)) .. summed
end

--- Пачка из одной записи со смещением первой записи.
---@param offset integer
---@param attributes integer|nil
---@param producer integer|nil
---@return string
local function at(offset, attributes, producer)
    local batch = helper.batch({ value = ('v%d'):format(offset) })

    batch = patched(batch, attributes or 0, producer)

    return wire.int64(offset) .. batch:sub(9)
end

g.test_crc32c_matches_the_reference = function()
    t.assert_equals(records.crc32c('123456789'), 0xe3069283)
    t.assert_equals(records.crc32c(''), 0)
end

g.test_batch_reads_back_with_offsets_keys_and_headers = function()
    local batch = records.encode({
        producer_id = 77,
        producer_epoch = 3,
        base_sequence = 5,
        timestamp = 1790000000123,
        records = {
            { key = 'k', value = '{"a":1}', headers = { { 'x-a', '1' }, { 'x-b', nil } } },
            { value = nil },
            { key = '', value = '' },
        },
    })
    local batches = records.decode(wire.int64(10) .. batch:sub(9))

    t.assert_equals(#batches, 1)

    local first = batches[1]

    t.assert_equals(first.base_offset, 10)
    t.assert_equals(first.last_offset, 12)
    t.assert_equals(first.producer_id, 77)
    t.assert_equals(first.transactional, false)
    t.assert_equals(first.control, false)
    t.assert_equals(first.codec, nil)
    t.assert_equals(first.records, {
        {
            offset = 10,
            timestamp = 1790000000123,
            key = 'k',
            value = '{"a":1}',
            headers = { { 'x-a', '1' }, { 'x-b', nil } },
        },
        { offset = 11, timestamp = 1790000000123, headers = {} },
        { offset = 12, timestamp = 1790000000123, key = '', value = '', headers = {} },
    })
end

g.test_batch_header_is_the_protocol_one = function()
    local batch = helper.batch({ value = 'x' }, { producer_id = 5, sequence = 9, timestamp = 1000 })
    local reader = wire.reader(batch)

    t.assert_equals(reader:int64(), 0)
    t.assert_equals(reader:int32(), #batch - 12)
    t.assert_equals(reader:int32(), -1)
    t.assert_equals(reader:int8(), 2)
    t.assert_equals(reader:uint32(), records.crc32c(batch:sub(22)))
    t.assert_equals(reader:int16(), 0)
    t.assert_equals(reader:int32(), 0)
    t.assert_equals(reader:int64(), 1000)
    t.assert_equals(reader:int64(), 1000)
    t.assert_equals(reader:int64(), 5)
    t.assert_equals(reader:int16(), 0)
    t.assert_equals(reader:int32(), 9)
    t.assert_equals(reader:int32(), 1)
end

g.test_record_bytes_are_the_protocol_ones = function()
    local batch = helper.batch({ value = 'x' }, { timestamp = 1000 })
    local reader = wire.reader(batch)

    reader:raw(61)

    local body = wire.reader(reader:raw(reader:varint()))

    t.assert_equals(body:int8(), 0)
    t.assert_equals(body:varint(), 0)
    t.assert_equals(body:varint(), 0)
    t.assert_equals(body:raw(1), wire.varint(-1))
    t.assert_equals(body:varint(), 1)
    t.assert_equals(body:raw(1), 'x')
    t.assert_equals(body:varint(), 0)
    t.assert_equals(reader:left(), 0)
end

g.test_records_keep_their_own_time = function()
    local batch = records.encode({
        producer_id = -1,
        producer_epoch = 0,
        base_sequence = -1,
        timestamp = 1000,
        records = { { value = 'a' }, { value = 'b', timestamp = 1005 } },
    })
    local decoded = records.decode(batch)[1].records

    t.assert_equals({ decoded[1].timestamp, decoded[2].timestamp }, { 1000, 1005 })
end

g.test_batch_without_records_is_read = function()
    local batch =
        records.encode({ producer_id = -1, producer_epoch = 0, base_sequence = -1, timestamp = 1, records = {} })
    local decoded = records.decode(batch)

    t.assert_equals(#decoded, 1)
    t.assert_equals(decoded[1].records, {})
    t.assert_equals(decoded[1].last_offset, -1)
end

g.test_batch_shorter_than_its_head_is_corrupt = function()
    t.assert_equals(records.BATCH_HEAD, 49)
    t.assert_error_msg_equals(
        'пачка со смещения 7 длиной 0 байт короче своей шапки',
        records.decode,
        wire.int64(7) .. wire.int32(0)
    )
    t.assert_error_msg_equals(
        'пачка со смещения 7 длиной 48 байт короче своей шапки',
        records.decode,
        wire.int64(7) .. wire.int32(48) .. string.rep('\0', 48)
    )
end

g.test_abort_before_the_batch_cancels_it = function()
    local batches = records.decode(at(3, 0x10, 7))

    t.assert_equals(records.visible(batches, { { producer_id = 7, first_offset = 0 } }), {})
end

g.test_many_batches_and_a_cut_tail = function()
    local data = at(0) .. at(1) .. at(2):sub(1, 30)
    local batches = records.decode(data)

    t.assert_equals(#batches, 2)
    t.assert_equals(batches[2].records[1].value, 'v1')
    t.assert_equals(records.decode(at(0) .. '\0\0\0'), records.decode(at(0)))
    t.assert_equals(records.decode(nil), {})
    t.assert_equals(records.decode(''), {})
end

g.test_batch_of_exactly_the_head_size_is_read = function()
    local data = at(0)

    -- Хвост на байт короче начала пачки — не пачка, а конец данных.
    t.assert_equals(records.HEAD, 12)
    t.assert_equals(#records.decode(data .. string.rep('\0', 11)), 1)
end

g.test_bad_sum_and_foreign_kind_are_corrupt = function()
    local batch = at(3)
    local broken = batch:sub(1, #batch - 1) .. string.char(math.floor((batch:byte(-1) + 1) % 256))

    t.assert_error_msg_equals(
        'контрольная сумма пачки со смещения 3 не сошлась',
        records.decode,
        broken
    )

    local old = batch:sub(1, 16) .. '\1' .. batch:sub(18)

    t.assert_error_msg_equals(
        'пачка вида 1, а пакет читает только вид 2',
        records.decode,
        old
    )
end

g.test_compressed_batch_has_no_records_but_its_place = function()
    for number, codec in ipairs(records.CODECS) do
        local batch = records.decode(at(4, number))[1]

        t.assert_equals(batch.codec, codec)
        t.assert_equals(batch.records, {})
        t.assert_equals(batch.base_offset, 4)
        t.assert_equals(batch.last_offset, 4)
    end
end

g.test_flags_are_read = function()
    local batch = records.decode(at(0, 0x30, 9))[1]

    t.assert_equals(batch.transactional, true)
    t.assert_equals(batch.control, true)
    t.assert_equals(batch.producer_id, 9)
end

g.test_visible_drops_control_and_aborted = function()
    local batches = records.decode(
        at(0, 0x10, 7) -- транзакция 7, отменена с 0
            .. at(1, 0)
            .. at(2, 0x30, 7) -- метка конца транзакции 7
            .. at(3, 0x10, 7) -- новая транзакция 7, не отменена
            .. at(4, 0x10, 8) -- транзакция 8, отменена с 4
            .. at(5, 0x10, 9)
    )
    local kept = records.visible(batches, {
        { producer_id = 8, first_offset = 4 },
        { producer_id = 7, first_offset = 0 },
    })
    local offsets = {}

    for _, batch in ipairs(kept) do
        offsets[#offsets + 1] = batch.base_offset
    end

    t.assert_equals(offsets, { 1, 3, 5 })
end

g.test_visible_without_aborted_keeps_all_but_control = function()
    local batches = records.decode(at(0, 0x10, 7) .. at(1, 0x30, 7) .. at(2))
    local kept = records.visible(batches, nil)

    t.assert_equals(#kept, 2)
    t.assert_equals(kept[2].base_offset, 2)
end

g.test_abort_starting_at_the_batch_end_cancels_it = function()
    local batches = records.decode(at(5, 0x10, 7))

    t.assert_equals(records.visible(batches, { { producer_id = 7, first_offset = 5 } }), {})
    t.assert_equals(#records.visible(batches, { { producer_id = 7, first_offset = 6 } }), 1)
end

g.test_every_abort_reaching_the_batch_cancels_its_producer = function()
    local batches = records.decode(at(0, 0x10, 9) .. at(1, 0x10, 7))
    local kept = records.visible(batches, {
        { producer_id = 9, first_offset = 0 },
        { producer_id = 7, first_offset = 0 },
    })

    t.assert_equals(kept, {})
end
