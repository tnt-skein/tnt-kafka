# tnt-kafka

Клиент Kafka для Tarantool: свой протокол Kafka поверх встроенного
неблокирующего сокета. Тема объявляется вместе с темой повтора и темой
мёртвых, отправка возвращает опознаватель только после того, как запись
легла на все синхронные копии раздела, а получатель — участник группы —
подтверждает сообщение итогом обработчика.

```lua
local kafka = require('tnt.kafka')

local broker = kafka.new({ brokers = { 'kafka-1:9092', 'kafka-2:9092' } })
local orders = broker:declare('orders', { partitions = 6 })  -- и orders.retry, orders.dead

local id, err = orders:send({ order = 7 }, { key = 'customer-42' })  -- ULID после подтверждения

orders:consume(function(message)
    local ok, reason = ship(message.body)

    if not ok then
        return nil, reason                                  -- выдать снова, потом зарыть
    end
end, { workers = 3 })
```

Зависимости: `tnt-must` (проверки аргументов), `tnt-clock` (время
отправки и сроки повтора), `tnt-context` (контекст заголовками), `tnt-id`
(опознаватель ULID), `tnt-log` (журнал), `tnt-pool` (соединения к узлам),
`tnt-retry` (повторы), `tnt-external` (подмена сети, шифрования, сна
и транзакции в проверках), `tnt-storage` (срок вызова и отказ с родом)
и `tnt-tls` (шифрование).

## Зачем

Готового неблокирующего клиента Kafka у Tarantool нет ни в ядре, ни среди
официальных роков, а обёртки над `librdkafka` ждут сети в своих потоках
и отдают итог обратными вызовами мимо файберов. Пакет говорит протоколом
сам, на файберах, и закрывает места, где голый протокол Kafka теряет
и повторяет сообщения молча:

- **Запись ждёт всех синхронных копий** (`acks = -1`): запись,
  подтверждённая одним узлом, пропала бы с ним до копирования.
- **Производитель идемпотентный**: у записи номер в счёте производителя,
  и повтор после обрыва брокер узнаёт и не пишет второй раз.
- **Смещение фиксируется за каждым сообщением по итогу обработчика**:
  значение — подтвердить, `nil, err` — копия в тему повтора со сроком,
  после `max_attempts` — копия в тему мёртвых с причиной заголовком.
  Гарантия — хотя бы раз.
- **Выдача отмечается до обработчика**: сообщение, роняющее процесс,
  зарывается, а не ходит по кругу.
- **Отказ — пара `nil, err` с родом** (`unreachable`, `denied`, `busy`,
  `timeout`, `broken`, `rejected`, `conflict`, `closed`), признаком
  отправки и приговором повтору; повторяет `tnt-retry` по полю
  `retriable` — до срока вызова.

## Установка

```sh
tt rocks install tnt-kafka --server=https://tnt-skein.github.io/rocks
```

Или из исходников:

```sh
git clone https://github.com/tnt-skein/tnt-kafka.git
cd tnt-kafka && tt rocks make
```

## Как пользоваться

| Вызов | Что делает |
|---|---|
| `kafka.new(opts)` | заводит клиента, соединений не открывает; негодная или незнакомая настройка — исключение |
| `broker:declare(name, opts)` | объявляет тему с темами повтора и мёртвых; `group`, `partitions`, `replication`, `shape` |
| `topic:send(body, opts)` | ULID после подтверждения всех копий; `key`, `headers`, `id`, `timeout` |
| `topic:consume(handler, opts)` | работники — участники группы; `workers`, `max_attempts`, `backoff` |
| `consumer:stop()` | взятое дорабатывается, участники уходят из группы |
| `topic:kick(count)` | возвращает зарытые в тему повтора со счётом выдач с нуля |
| `broker:sender()` | отправитель `send(name, body, opts)` для ящика исходящих `tnt-outbox` |
| `kafka.hook(name, fn)` | крюк вокруг отправки и обработки: трасса, замер, журнал |
| `broker:status()`, `topic:status()` | пулы, темы, счётчики `sent`, `ack`, `retry`, `bury`, `late`, `failures`, `skipped`, `kicked` |
| `broker:close()` | останавливает получателей и закрывает соединения |

Настройки клиента: `brokers` (`host:port`, IPv6 — в скобках), `tls`
(`true` либо `{ verify, ca_file, ca_path, sni }`), `sasl` (`{ username,
password }`, способ — PLAIN), `timeout` и `max_timeout` (5 и 60 с),
`max_bytes` (1 МБ), `session_timeout` и `rebalance_timeout` (10 и 60 с),
`client_id`, `pool` и `retry` (как у `tnt-pool` и `tnt-retry`), `name`.

```lua
local consumer = orders:consume(function(message)
    -- message: id, name, body, headers, key, attempt, created
    if message.attempt > 1 then
        return nil, { retriable = false, message = 'второй раз не шлём' }   -- зарыть сразу
    end
end, { workers = 2, max_attempts = 3, backoff = { base = 1, max = 30 } })

local moved, err = orders:kick(10)   -- вернуть до десяти зарытых
```

Чего Kafka не умеет, то исключение, а не молчаливый пропуск: `delay`,
`ttl` и `priority` у отправки, `atomic` у получателя. Отправка внутри
транзакции box — тоже исключение: уступка порвала бы транзакцию.

## Проверки

```sh
make deps          # luatest, luacheck, luacov с cluacov и зависимости пакета в .rocks
make check         # форматирование, линт, проверки, покрытие с порогом 100 %
make mutants-all   # мутационное тестирование утилитой tnt-mutants из PATH, порог 100 % убитых
make kafka-up      # Kafka 3.9 в докере для живых проверок; make kafka-down — погасить
```

Покрытие строк — 100 %, убитых мутантов — 100 % (280 проверок,
2076 мутантов в двадцати четырёх модулях). Протокол проверяется
двойником брокера на настоящем сокете, а семь живых проверок идут против
Kafka стенда; без поднятого брокера они пропускаются.

## Документ

Полное описание с настройками, объявлением, отправкой, получателем,
итогом обработчика, возвратом зарытых, признаками, контекстом, крюками,
соединениями, входом и шифрованием, отказами, счётчиками, журналом,
подменой в проверках и обоснованием решений: [docs/kafka.md](docs/kafka.md).

## Лицензия

MIT.
