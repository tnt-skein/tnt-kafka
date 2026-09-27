--- Проверки значений байтами: целые, строки, байты, списки, числа
--- переменной длины и чтец ответа.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local wire = helper.wire

local g = t.group('tnt.kafka.wire')

g.test_integers_go_big_endian = function()
    t.assert_equals(wire.int8(-1), '\255')
    t.assert_equals(wire.int8(7), '\7')
    t.assert_equals(wire.int16(0x0102), '\1\2')
    t.assert_equals(wire.int16(-2), '\255\254')
    t.assert_equals(wire.int32(0x01020304), '\1\2\3\4')
    t.assert_equals(wire.int32(-1), '\255\255\255\255')
    t.assert_equals(wire.int64(0x0102030405), '\0\0\0\1\2\3\4\5')
    t.assert_equals(wire.int64(-1), string.rep('\255', 8))
    t.assert_equals(wire.int64(-2), string.rep('\255', 7) .. '\254')
end

g.test_integers_read_back_with_sign = function()
    local data = wire.int8(-5)
        .. wire.int16(-300)
        .. wire.int32(-70000)
        .. wire.int64(-3)
        .. wire.int64(1790000000123)
        .. wire.int32(-1)
        .. wire.int16(0x7fff)
        .. wire.int8(0x7f)
    local reader = wire.reader(data)

    t.assert_equals(reader:int8(), -5)
    t.assert_equals(reader:int16(), -300)
    t.assert_equals(reader:int32(), -70000)
    t.assert_equals(reader:int64(), -3)
    t.assert_equals(reader:int64(), 1790000000123)
    t.assert_equals(reader:uint32(), 0xffffffff)
    t.assert_equals(reader:int16(), 0x7fff)
    t.assert_equals(reader:int8(), 0x7f)
    t.assert_equals(reader:left(), 0)
end

g.test_signed_border_is_half_of_the_span = function()
    t.assert_equals(wire.reader('\128'):int8(), -128)
    t.assert_equals(wire.reader('\127'):int8(), 127)
    t.assert_equals(wire.reader('\128\0'):int16(), -32768)
    t.assert_equals(wire.reader('\128\0\0\0'):int32(), -2147483648)
    t.assert_equals(wire.reader('\127\255\255\255'):int32(), 2147483647)
end

g.test_strings_and_bytes_carry_length_and_null = function()
    t.assert_equals(wire.string('ab'), '\0\2ab')
    t.assert_equals(wire.string(nil), '\255\255')
    t.assert_equals(wire.bytes('ab'), '\0\0\0\2ab')
    t.assert_equals(wire.bytes(nil), '\255\255\255\255')

    local reader = wire.reader(
        wire.string('узел') .. wire.string(nil) .. wire.bytes('x') .. wire.bytes(nil) .. wire.string('')
    )

    t.assert_equals(reader:string(), 'узел')
    t.assert_equals(reader:string(), nil)
    t.assert_equals(reader:bytes(), 'x')
    t.assert_equals(reader:bytes(), nil)
    t.assert_equals(reader:string(), '')
end

g.test_array_counts_items_and_null = function()
    t.assert_equals(wire.array({ 1, 2 }, wire.int8), '\0\0\0\2\1\2')
    t.assert_equals(wire.array({}, wire.int8), '\0\0\0\0')
    t.assert_equals(wire.array(nil, wire.int8), '\255\255\255\255')
end

g.test_varint_is_zigzag = function()
    t.assert_equals(wire.varint(0), '\0')
    t.assert_equals(wire.varint(-1), '\1')
    t.assert_equals(wire.varint(1), '\2')
    t.assert_equals(wire.varint(-2), '\3')
    t.assert_equals(wire.varint(63), '\126')
    t.assert_equals(wire.varint(64), '\128\1')
    t.assert_equals(wire.varint(-65), '\129\1')
    t.assert_equals(wire.varint(300), '\216\4')
end

g.test_varint_reads_back = function()
    local values = { 0, 1, -1, 63, 64, -64, -65, 300, -300, 2 ^ 31, -(2 ^ 31), 1790000000000, -1790000000000 }

    for _, value in ipairs(values) do
        local reader = wire.reader(wire.varint(value))

        t.assert_equals(reader:varint(), value)
        t.assert_equals(reader:left(), 0)
    end
end

g.test_varint_takes_ten_bytes_but_not_eleven = function()
    t.assert_equals(wire.reader(string.rep('\128', 9) .. '\1'):varint(), 2 ^ 62)

    local reader = wire.reader(string.rep('\128', 10) .. '\1')

    t.assert_error_msg_equals(
        'число переменной длины длиннее 10 байт',
        reader.varint,
        reader
    )
end

g.test_negative_size_is_short = function()
    local reader = wire.reader('')

    t.assert_error_msg_equals(
        'ответ короче, чем обещают поля: нужно -1 байт с 1, а их 0',
        reader.raw,
        reader,
        -1
    )
end

g.test_varint_longer_than_ten_bytes_is_corrupt = function()
    local reader = wire.reader(string.rep('\128', 11))

    t.assert_error_msg_equals(
        'число переменной длины длиннее 10 байт',
        reader.varint,
        reader
    )
end

g.test_short_answer_is_an_error_with_place = function()
    local reader = wire.reader('\0\0\0')

    t.assert_error_msg_equals(
        'ответ короче, чем обещают поля: нужно 4 байт с 1, а их 3',
        reader.int32,
        reader
    )

    reader = wire.reader('\0\5ab')
    t.assert_error_msg_equals(
        'ответ короче, чем обещают поля: нужно 5 байт с 3, а их 4',
        reader.string,
        reader
    )

    reader = wire.reader('\255\254')
    t.assert_error_msg_equals(
        'ответ короче, чем обещают поля: нужно -2 байт с 3, а их 2',
        reader.string,
        reader
    )
end

g.test_raw_takes_exact_piece = function()
    local reader = wire.reader('abcdef')

    t.assert_equals(reader:raw(2), 'ab')
    t.assert_equals(reader:raw(0), '')
    t.assert_equals(reader:raw(4), 'cdef')
    t.assert_equals(reader:left(), 0)
end
