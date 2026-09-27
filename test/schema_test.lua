--- Проверки запроса и ответа по описанию полей, каталога запросов и кадра.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local api = helper.api
local schema = helper.schema
local wire = helper.wire

local g = t.group('tnt.kafka.schema')

--- Описание со всеми родами полей.
local FIELDS = {
    { 'small', 'int8' },
    { 'short', 'int16' },
    { 'number', 'int32' },
    { 'long', 'int64' },
    { 'flag', 'boolean' },
    { 'name', 'string' },
    { 'blob', 'bytes' },
    { 'ids', '[int32]' },
    { 'names', '[string]' },
    { 'entries', { { 'key', 'string' }, { 'sizes', '[int32]' } } },
}

g.test_encode_writes_fields_in_order = function()
    local bytes = schema.encode(FIELDS, {
        small = 1,
        short = 2,
        number = 3,
        long = 4,
        flag = true,
        name = 'n',
        blob = 'b',
        ids = { 5 },
        names = { 'x' },
        entries = { { key = 'k', sizes = { 6, 7 } } },
    })

    t.assert_equals(
        bytes,
        '\1'
            .. '\0\2'
            .. '\0\0\0\3'
            .. '\0\0\0\0\0\0\0\4'
            .. '\1'
            .. '\0\1n'
            .. '\0\0\0\1b'
            .. '\0\0\0\1\0\0\0\5'
            .. '\0\0\0\1\0\1x'
            .. '\0\0\0\1\0\1k\0\0\0\2\0\0\0\6\0\0\0\7'
    )
end

g.test_decode_reads_what_encode_wrote = function()
    local values = {
        small = -1,
        short = -2,
        number = -3,
        long = 1790000000000,
        flag = false,
        name = 'имя',
        blob = '\0\1',
        ids = { 1, -1 },
        names = { 'a', 'b' },
        entries = { { key = 'k', sizes = {} }, { key = 'm', sizes = { 9 } } },
    }
    local reader = wire.reader(schema.encode(FIELDS, values))

    t.assert_equals(schema.decode(FIELDS, reader), values)
    t.assert_equals(reader:left(), 0)
end

g.test_null_lists_and_strings_stay_empty = function()
    local reader = wire.reader(schema.encode(FIELDS, {
        small = 0,
        short = 0,
        number = 0,
        long = 0,
        flag = false,
    }))
    local values = schema.decode(FIELDS, reader)

    t.assert_equals(values.name, nil)
    t.assert_equals(values.blob, nil)
    t.assert_equals(values.ids, nil)
    t.assert_equals(values.names, nil)
    t.assert_equals(values.entries, nil)
end

g.test_list_longer_than_the_answer_is_corrupt = function()
    local reader = wire.reader('\0\0\0\9\0\0\0\1')

    t.assert_error_msg_equals(
        'в списке 9 элементов, а байт осталось 4',
        schema.decode,
        { { 'ids', '[int32]' } },
        reader
    )

    reader = wire.reader('\255\255\255\254')
    t.assert_error_msg_equals(
        'в списке -2 элементов, а байт осталось 0',
        schema.decode,
        { { 'ids', '[int32]' } },
        reader
    )
end

g.test_list_as_long_as_the_answer_is_read = function()
    local reader = wire.reader('\0\0\0\1\7')

    t.assert_equals(schema.decode({ { 'flags', { { 'flag', 'int8' } } } }, reader), { flags = { { flag = 7 } } })
end

g.test_request_frame_has_size_header_and_fields = function()
    local frame = api.request(api.SASL_HANDSHAKE, 7, 'tnt', { mechanism = 'PLAIN' })

    t.assert_equals(frame, wire.int32(2 + 2 + 4 + 5 + 7) .. '\0\17' .. '\0\1' .. '\0\0\0\7' .. '\0\3tnt' .. '\0\5PLAIN')
end

g.test_response_parses_correlation_and_fields = function()
    local payload = wire.int32(42)
        .. schema.encode(api.SASL_HANDSHAKE.response, { error_code = 33, mechanisms = { 'PLAIN' } })
    local correlation, values = api.response(api.SASL_HANDSHAKE, payload)

    t.assert_equals(correlation, 42)
    t.assert_equals(values, { error_code = 33, mechanisms = { 'PLAIN' } })
end

g.test_catalog_names_every_used_request = function()
    local names = {}

    for _, request in ipairs(api.USED) do
        names[#names + 1] = ('%s/%d/%d'):format(request.name, request.key, request.version)
    end

    t.assert_equals(names, {
        'Metadata/3/5',
        'CreateTopics/19/4',
        'InitProducerId/22/1',
        'Produce/0/8',
        'Fetch/1/4',
        'ListOffsets/2/1',
        'FindCoordinator/10/2',
        'JoinGroup/11/4',
        'SyncGroup/14/2',
        'Heartbeat/12/2',
        'LeaveGroup/13/2',
        'OffsetCommit/8/5',
        'OffsetFetch/9/3',
    })
    t.assert_equals(api.PREFIX, 4)
end
