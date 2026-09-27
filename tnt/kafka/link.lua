--- Связь с узлом Kafka: сокет, кадр запроса и ответ на него.
---
--- Разговор Kafka — запрос и ответ: кадр с длиной, шапкой и полями уходит,
--- кадр с номером обращения приходит. Соединение берёт один файбер
--- на одно дело и отдаёт в пул (`tnt-pool`), поэтому ответы здесь
--- не перемешиваются: номер обращения сверяется, и чужой номер — это
--- обрыв разговора, а не ответ.
---
--- Срок у каждого ожидания — остаток одного мига вызова. Встроенный сокет
--- срок принимает сам, и ожидание, не дождавшееся срока, возвращает
--- управление; соединение тогда выбрасывается — в нём остался
--- недочитанный ответ, и следующий взявший прочёл бы его как свой.
---
--- **Предел длины ответа.** Длина кадра приходит от брокера, и испорченная
--- длина в два гигабайта заставила бы узел копить память, пока не придут
--- все байты. Предел — `max_bytes` клиента с запасом на шапки выборки;
--- длиннее — обрыв.
---
--- **Живость без сети**: у открытого сокета — `sysread` одного байта.
--- Свободному соединению брокер не шлёт ничего, и если читать есть что —
--- это конец потока после перезапуска брокера, и соединение негодно.
--- Под TLS в сокете бывают служебные записи, и умершее в простое
--- соединение там узнаётся первым же делом.

local errno = require('errno')

local api = require('tnt.kafka.api')
local external = require('tnt.external')
local failure = require('tnt.storage.failure')
local within = require('tnt.storage.within')
local wire = require('tnt.kafka.wire')

local Module = {}

--- Внешние средства: сокет и шифрование поверх него.
---
--- Две внешние зависимости, а не одна: проверке нужно уметь оборвать
--- сеть и рукопожатие по отдельности — у них разные отказы.
local source = external.install(Module, {
    dial = function(host, port, timeout)
        local connected = require('socket').tcp_connect(host, port, timeout)

        if connected == nil then
            return nil, errno.strerror()
        end

        return connected
    end,

    cipher = function(raw, opts)
        local tls = require('tnt.tls')

        return tls.wrap(raw, opts)
    end,
})

--- Ответ ядра «читать пока нечего»: в сокете пусто, и он цел.
local IDLE = errno.EAGAIN

--- Запас сверх `max_bytes` на шапки ответа выборки: темы, разделы, списки
--- отменённых транзакций.
Module.SLACK = 1024 * 1024

---@class TntKafkaTrouble Беда обмена: род, текст и то, что нужно повторам
---@field kind string Род отказа `tnt-storage`
---@field message string
---@field sent boolean|nil Могло ли дело дойти до брокера; пусто — по роду
---@field retriable boolean|nil Приговор повтору, если его выносит связь
---@field code integer|nil Код ошибки брокера

---@class TntKafkaStream Чтение и запись, одинаковые у сокета и у TLS
---@field read fun(size: integer, timeout: number): string|nil, any
---@field write fun(data: string, timeout: number): boolean, any
---@field shut fun()

---@class TntKafkaLink Открытое соединение с узлом
---@field stream TntKafkaStream Чтение и запись
---@field socket any Сам сокет: по нему судится живость
---@field secured boolean Идёт ли разговор под TLS
---@field where string Узел и порт для текста отказа
---@field client string Имя клиента в шапке запроса
---@field correlation integer Номер последнего обращения
---@field frame_max integer Предел длины ответа

--- Беда связи: род, текст и то, что о ней надо знать повторам.
---@param kind string
---@param message string
---@param opts { sent: boolean|nil, retriable: boolean|nil, code: integer|nil }|nil
---@return TntKafkaTrouble
function Module.trouble(kind, message, opts)
    local given = opts or {}

    return {
        kind = kind,
        message = message,
        sent = given.sent,
        retriable = given.retriable,
        code = given.code,
    }
end

--- Чтение и запись поверх того, что умеет `read` и `write`: сокета
--- либо соединения TLS. Отказ сокета читается у самого сокета, у TLS —
--- вторым значением.
---@param over any Сокет либо соединение TLS
---@param socket any Сокет под ним
---@return TntKafkaStream
local function stream_of(over, socket)
    return {
        read = function(size, timeout)
            local piece, why = over:read({ chunk = size }, timeout)

            return piece, why or socket:error()
        end,

        write = function(data, timeout)
            local done, why = over:write(data, timeout)

            return done ~= nil and done ~= false, why or socket:error()
        end,

        shut = function()
            over:close()
        end,
    }
end

--- Род беды посреди обмена: срок вышел либо связь оборвалась.
---
--- Сокет и TLS называют истёкший срок каждый по-своему, а отказ сети —
--- словами ядра. Надёжнее спросить часы.
---@param deadline number
---@param sent boolean
---@param text string
---@return TntKafkaTrouble
local function cut(deadline, sent, text)
    if within.left(deadline) > 0 then
        return Module.trouble(failure.BROKEN, ('связь оборвалась: %s'):format(text), { sent = sent })
    end

    return Module.trouble(
        failure.TIMEOUT,
        ('брокер не ответил за срок вызова: %s'):format(text),
        { sent = sent }
    )
end

--- Читает ровно `size` байт.
---@param link TntKafkaLink
---@param size integer
---@param deadline number
---@return string|nil
---@return TntKafkaTrouble|nil
local function take(link, size, deadline)
    -- Под pcall: сокет, закрытый соседом, бросает, а не отвечает.
    local ok, piece, why = pcall(link.stream.read, size, within.left(deadline))

    if ok and piece ~= nil and #piece == size then
        return piece
    end

    local said = not ok and piece or piece == nil and why or 'поток кончился посреди ответа'

    return nil, cut(deadline, true, tostring(said))
end

--- Отправляет запрос и ждёт ответа на него.
---@param link TntKafkaLink
---@param request TntKafkaApi Запрос из каталога
---@param values table Поля запроса
---@param deadline number
---@return table|nil answer Поля ответа
---@return TntKafkaTrouble|nil trouble
function Module.exchange(link, request, values, deadline)
    link.correlation = link.correlation + 1

    local number = link.correlation
    local frame = api.request(request, number, link.client, values)
    local ok, written, why = pcall(link.stream.write, frame, within.left(deadline))

    if not (ok and written) then
        return nil, cut(deadline, false, tostring(ok and why or written))
    end

    local head, lost = take(link, api.PREFIX, deadline)

    if head == nil then
        return nil, lost
    end

    local size = wire.reader(head):int32()

    if size < api.PREFIX or size > link.frame_max then
        return nil,
            Module.trouble(
                failure.BROKEN,
                ('ответ %s длиной %d байт вне предела %d'):format(
                    request.name,
                    size,
                    link.frame_max
                )
            )
    end

    local payload, gone = take(link, size, deadline)

    if payload == nil then
        return nil, gone
    end

    local parsed, correlation, answer = pcall(api.response, request, payload)

    if not parsed then
        return nil,
            Module.trouble(
                failure.BROKEN,
                ('ответ %s не разобран: %s'):format(request.name, tostring(correlation))
            )
    end

    if correlation ~= number then
        return nil,
            Module.trouble(
                failure.BROKEN,
                ('ответ на обращение %d пришёл вместо %d'):format(correlation, number)
            )
    end

    return answer
end

--- Закрывает соединение, чем бы оно ни кончилось.
---@param link TntKafkaLink
function Module.close(link)
    pcall(link.stream.shut)
end

--- Живо ли свободное соединение — без сети и без уступки.
---@param link TntKafkaLink
---@return boolean
function Module.alive(link)
    -- Под TLS в сокете бывают служебные записи: байты там не значат смерть.
    if not link.secured then
        local raw = link.socket
        local done, got = pcall(raw.sysread, raw)

        return done and got == nil and raw:errno() == IDLE
    end

    return true
end

--- Поднимает TLS поверх открытого сокета.
---@param link TntKafkaLink
---@param settings TntKafkaSettings
---@param host string
---@param deadline number
---@return boolean ok
---@return string|nil why
local function secure(link, settings, host, deadline)
    local span = within.left(deadline)
    local tls = settings.tls

    ---@cast tls table

    if span <= 0 then
        return false, 'срок вышел до рукопожатия TLS'
    end

    local wrapped, refusal = source().cipher(link.socket, {
        host = host,
        timeout = span,
        verify = tls.verify,
        ca_file = tls.ca_file,
        ca_path = tls.ca_path,
        sni = tls.sni,
    })

    if wrapped == nil then
        return false, ('рукопожатие TLS не прошло: %s'):format(tostring(refusal))
    end

    link.stream = stream_of(wrapped, link.socket)

    return true
end

--- Открывает сокет к узлу и, если просили, поднимает поверх него TLS.
---@param settings TntKafkaSettings
---@param host string
---@param port integer
---@param deadline number
---@return TntKafkaLink|nil link
---@return TntStorageFailure|nil err
function Module.dial(settings, host, port, deadline)
    local where = ('%s:%d'):format(host, port)
    local connected, socket, why = pcall(source().dial, host, port, within.left(deadline))

    if not connected or socket == nil then
        return nil,
            failure.new(
                failure.UNREACHABLE,
                ('kafka %s: соединение не открылось: %s'):format(
                    where,
                    tostring(connected and why or socket)
                )
            )
    end

    ---@type TntKafkaLink
    local link = {
        stream = stream_of(socket, socket),
        socket = socket,
        secured = settings.tls ~= nil,
        where = where,
        client = settings.client_id,
        correlation = 0,
        frame_max = settings.max_bytes + Module.SLACK,
    }

    if link.secured then
        local ok, refusal = secure(link, settings, host, deadline)

        if not ok then
            pcall(socket.close, socket)

            return nil, failure.new(failure.UNREACHABLE, ('kafka %s: %s'):format(where, refusal))
        end
    end

    return link
end

return Module
