--- Каталог запросов группы получателей: ведущий группы, вход, раздача,
--- стук, уход и смещения.
---
--- Версии выбраны по тому же правилу, что в `tnt.kafka.api`: последняя
--- до гибкой записи из тех, что нужны, и не ниже наименьшей, которую
--- принимают выпуски брокера 4.x. Здесь только описания: кадр собирает
--- и разбирает `tnt.kafka.api`.

local Module = {}

--- Пустота или список целых.
local INTS = '[int32]'

--- Узел, который ведёт группу.
Module.FIND_COORDINATOR = {
    name = 'FindCoordinator',
    key = 10,
    version = 2,
    request = { { 'key', 'string' }, { 'key_type', 'int8' } },
    response = {
        { 'throttle_time_ms', 'int32' },
        { 'error_code', 'int16' },
        { 'error_message', 'string' },
        { 'node_id', 'int32' },
        { 'host', 'string' },
        { 'port', 'int32' },
    },
}

--- Вход в группу.
Module.JOIN_GROUP = {
    name = 'JoinGroup',
    key = 11,
    version = 4,
    request = {
        { 'group_id', 'string' },
        { 'session_timeout_ms', 'int32' },
        { 'rebalance_timeout_ms', 'int32' },
        { 'member_id', 'string' },
        { 'protocol_type', 'string' },
        { 'protocols', { { 'name', 'string' }, { 'metadata', 'bytes' } } },
    },
    response = {
        { 'throttle_time_ms', 'int32' },
        { 'error_code', 'int16' },
        { 'generation_id', 'int32' },
        { 'protocol_name', 'string' },
        { 'leader', 'string' },
        { 'member_id', 'string' },
        { 'members', { { 'member_id', 'string' }, { 'metadata', 'bytes' } } },
    },
}

--- Раздача разделов: ведущий группы присылает её, прочие получают.
Module.SYNC_GROUP = {
    name = 'SyncGroup',
    key = 14,
    version = 2,
    request = {
        { 'group_id', 'string' },
        { 'generation_id', 'int32' },
        { 'member_id', 'string' },
        { 'assignments', { { 'member_id', 'string' }, { 'assignment', 'bytes' } } },
    },
    response = { { 'throttle_time_ms', 'int32' }, { 'error_code', 'int16' }, { 'assignment', 'bytes' } },
}

--- Поля, общие у стука и ухода: группа, поколение, участник.
local MEMBER = { { 'group_id', 'string' }, { 'generation_id', 'int32' }, { 'member_id', 'string' } }

--- Ответ, в котором только пауза и код.
local BARE = { { 'throttle_time_ms', 'int32' }, { 'error_code', 'int16' } }

--- Стук участника: жив и держит свои разделы.
Module.HEARTBEAT = { name = 'Heartbeat', key = 12, version = 2, request = MEMBER, response = BARE }

--- Уход из группы.
Module.LEAVE_GROUP = {
    name = 'LeaveGroup',
    key = 13,
    version = 2,
    request = { { 'group_id', 'string' }, { 'member_id', 'string' } },
    response = BARE,
}

--- Фиксация смещений группы.
Module.OFFSET_COMMIT = {
    name = 'OffsetCommit',
    key = 8,
    version = 5,
    request = {
        MEMBER[1],
        MEMBER[2],
        MEMBER[3],
        {
            'topics',
            {
                { 'name', 'string' },
                {
                    'partitions',
                    {
                        { 'partition_index', 'int32' },
                        { 'committed_offset', 'int64' },
                        { 'committed_metadata', 'string' },
                    },
                },
            },
        },
    },
    response = {
        { 'throttle_time_ms', 'int32' },
        {
            'topics',
            { { 'name', 'string' }, { 'partitions', { { 'partition_index', 'int32' }, { 'error_code', 'int16' } } } },
        },
    },
}

--- Зафиксированные смещения группы.
Module.OFFSET_FETCH = {
    name = 'OffsetFetch',
    key = 9,
    version = 3,
    request = { { 'group_id', 'string' }, { 'topics', { { 'name', 'string' }, { 'partition_indexes', INTS } } } },
    response = {
        { 'throttle_time_ms', 'int32' },
        {
            'topics',
            {
                { 'name', 'string' },
                {
                    'partitions',
                    {
                        { 'partition_index', 'int32' },
                        { 'committed_offset', 'int64' },
                        { 'metadata', 'string' },
                        { 'error_code', 'int16' },
                    },
                },
            },
        },
        { 'error_code', 'int16' },
    },
}

return Module
