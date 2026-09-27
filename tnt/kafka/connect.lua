--- Вход: сокет, сверка версий и учётная запись.
---
--- **Версии сверяются при входе.** Брокер, которому пришёл запрос
--- незнакомой версии, не отвечает ошибкой, а закрывает соединение, и
--- на первом деле это выглядело бы обрывом сети. Поэтому первым делом
--- спрашивается `ApiVersions`, и запрос, версии которого брокер
--- не принимает, — отказ входа с его именем и границами: повторять
--- бесполезно, чинится это выпуском брокера либо пакета.
---
--- **Вход только SASL PLAIN.** Брокер называет свои способы в ответе
--- на `SaslHandshake`; нет среди них PLAIN — отказ `denied`. Пароль
--- уходит строкой `\0имя\0пароль` в открытом виде, и поэтому TLS
--- здесь не украшение: без него учётная запись едет по сети как есть.
--- Неверная пара «имя и пароль» — `SASL_AUTHENTICATION_FAILED`, и это
--- `denied` без повтора: пул не будет стучаться с тем же паролем.

local api = require('tnt.kafka.api')
local codes = require('tnt.kafka.codes')
local failure = require('tnt.storage.failure')
local link_of = require('tnt.kafka.link')
local within = require('tnt.storage.within')

local Module = {}

--- Запросы входа по SASL: их версии сверяются, только если вход нужен.
local SASL = { api.SASL_HANDSHAKE, api.SASL_AUTHENTICATE }

--- Какой запрос брокер не примет и почему; `nil` — примет все.
---@param answer table Ответ `ApiVersions`
---@param needed TntKafkaApi[]
---@return string|nil
local function unsupported(answer, needed)
    ---@type table<integer, { min_version: integer, max_version: integer }>
    local ranges = {}

    for _, entry in ipairs(answer.api_keys or {}) do
        ranges[entry.api_key] = entry
    end

    for _, request in ipairs(needed) do
        local range = ranges[request.key]

        if range == nil then
            return ('брокер не знает запроса %s'):format(request.name)
        end

        if request.version < range.min_version or request.version > range.max_version then
            return ('брокер принимает %s версий %d–%d, а пакет говорит версией %d'):format(
                request.name,
                range.min_version,
                range.max_version,
                request.version
            )
        end
    end

    return nil
end

--- Спрашивает версии и сверяет их с каталогом.
---@param link TntKafkaLink
---@param settings TntKafkaSettings
---@param deadline number
---@return boolean ok
---@return TntStorageFailure|TntKafkaTrouble|nil why
local function negotiate(link, settings, deadline)
    local answer, trouble = link_of.exchange(link, api.API_VERSIONS, {}, deadline)

    if answer == nil then
        return false, trouble
    end

    if answer.error_code ~= codes.NONE then
        return false,
            link_of.trouble(
                failure.BROKEN,
                ('брокер отказал в перечне версий: %s'):format(codes.name(answer.error_code))
            )
    end

    local needed = table.copy(api.USED)

    if settings.sasl ~= nil then
        for _, request in ipairs(SASL) do
            needed[#needed + 1] = request
        end
    end

    local refusal = unsupported(answer, needed)

    if refusal ~= nil then
        return false, failure.new(failure.REJECTED, ('kafka %s: %s'):format(link.where, refusal), { sent = false })
    end

    return true
end

--- Входит по SASL PLAIN.
---@param link TntKafkaLink
---@param sasl { mechanism: string, username: string, password: string }
---@param deadline number
---@return boolean ok
---@return TntStorageFailure|TntKafkaTrouble|nil why
local function authenticate(link, sasl, deadline)
    local shake, trouble = link_of.exchange(link, api.SASL_HANDSHAKE, { mechanism = sasl.mechanism }, deadline)

    if shake == nil then
        return false, trouble
    end

    if shake.error_code ~= codes.NONE then
        return false,
            failure.new(
                failure.DENIED,
                ('kafka %s: брокер не умеет %s, а умеет %s'):format(
                    link.where,
                    sasl.mechanism,
                    table.concat(shake.mechanisms or {}, ', ')
                ),
                { server_code = shake.error_code }
            )
    end

    local token = ('\0%s\0%s'):format(sasl.username, sasl.password)
    local answer, cut = link_of.exchange(link, api.SASL_AUTHENTICATE, { auth_bytes = token }, deadline)

    if answer == nil then
        return false, cut
    end

    if answer.error_code ~= codes.NONE then
        return false,
            failure.new(
                failure.DENIED,
                ('kafka %s: вход не удался: %s'):format(
                    link.where,
                    answer.error_message or codes.name(answer.error_code)
                ),
                { server_code = answer.error_code }
            )
    end

    return true
end

--- Отказ входа: отказ как есть либо беда связи родом `unreachable`.
---
--- Беда связи при входе — это недоступный узел, а не оборванное дело:
--- ничего ещё не отправлено, и пул вправе открыть соединение заново.
---@param where string
---@param why TntStorageFailure|TntKafkaTrouble
---@return TntStorageFailure
local function refusal(where, why)
    if failure.is(why) then
        ---@cast why TntStorageFailure
        return why
    end

    return failure.new(
        failure.UNREACHABLE,
        ('kafka %s: вход не завершился: %s'):format(where, why.message)
    )
end

--- Открывает соединение с узлом и входит — в срок, который дал пул.
---@param settings TntKafkaSettings
---@param address TntKafkaAddress
---@param left number Остаток срока `take`, секунд; больше нуля
---@return TntKafkaLink|nil link
---@return TntStorageFailure|nil err
function Module.open(settings, address, left)
    local deadline = within.deadline(left)
    local link, err = link_of.dial(settings, address.host, address.port, deadline)

    if link == nil then
        return nil, err
    end

    local ready, why = negotiate(link, settings, deadline)

    if ready and settings.sasl ~= nil then
        ready, why = authenticate(link, settings.sasl, deadline)
    end

    if not ready then
        link_of.close(link)

        ---@cast why TntStorageFailure|TntKafkaTrouble
        return nil, refusal(link.where, why)
    end

    return link
end

return Module
