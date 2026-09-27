rockspec_format = '3.0'

package = 'tnt-kafka'
version = 'scm-1'

source = {
    url = 'git+https://github.com/tnt-skein/tnt-kafka.git',
    branch = 'main',
}

description = {
    summary = 'Очередь Kafka для Tarantool: свой протокол на неблокирующем сокете, группа, идемпотентная отправка, зарытые',
    detailed = [[
        Готового неблокирующего клиента Kafka у Tarantool нет ни в ядре,
        ни среди официальных роков, а обёртки над librdkafka ждут сети
        в своих потоках мимо файберов. Пакет говорит протоколом Kafka сам,
        поверх встроенного неблокирующего сокета, и ровно тем, чем
        пользуется: описание кластера, заведение тем, идемпотентная запись
        пачкой, выборка, смещения и группа получателей.

        broker:declare объявляет тему вместе с темой повтора и темой
        мёртвых и заводит недостающие при первом деле. send возвращает
        опознаватель после подтверждения всех синхронных копий раздела;
        производитель идемпотентный, и повтор после обрыва брокер
        не пишет дважды. Ключ задаёт раздел тем же murmur2, что у прочих
        клиентов Kafka. consume заводит работников — каждый входит
        в группу сам и ведёт свою долю разделов. Подтверждение — итог
        обработчика: значение — смещение фиксируется, nil, err — копия
        с номером выдачи и сроком в тему повтора, после max_attempts —
        копия в тему мёртвых с причиной заголовком. Выдача отмечается
        до обработчика, и сообщение, роняющее процесс, зарывается,
        а не ходит по кругу. Гарантия — хотя бы раз.

        Сообщение приходит конвертом: опознаватель ULID, имя темы, тело
        JSON, заголовки контекста, ключ, номер выдачи и время отправки.
        Отказ — пара nil, err с родом, признаком отправки и приговором
        повтору. broker:sender() отдаёт отправителя для ящика исходящих
        tnt-outbox. Отсрочки, срока жизни и приоритета у Kafka нет:
        delay, ttl и priority — исключение, а не молчаливый пропуск.

        Зависит от tnt-must (проверки аргументов), tnt-clock (время
        отправки и сроки повтора), tnt-context (контекст заголовками),
        tnt-id (опознаватель), tnt-log (журнал), tnt-pool (соединения
        к узлам), tnt-retry (повторы), tnt-external (подмена сети,
        шифрования и сна в проверках), tnt-storage (срок вызова и отказ
        с родом) и tnt-tls (шифрование).
    ]],
    homepage = 'https://github.com/tnt-skein/tnt-kafka',
    issues_url = 'https://github.com/tnt-skein/tnt-kafka/issues',
    maintainer = 'tnt-skein',
    license = 'MIT',
    labels = { 'tarantool', 'kafka', 'queue', 'messaging', 'at-least-once' },
}

dependencies = {
    'lua >= 5.1',
    -- Проверки настроек, имени темы, тела и крюков на строке вызывающего.
    'tnt-must',
    -- Стенное время отправки в конверте и сроки повтора.
    'tnt-clock',
    -- Контекст, который едет заголовками записи.
    'tnt-context',
    -- Опознаватель ULID сообщения и запроса обработчика.
    'tnt-id',
    -- Записи о зарытом, опоздавшем итоге, входе в группу и крюке.
    'tnt-log',
    -- Соединения к узлам кластера.
    'tnt-pool',
    -- Повторы вызова по полю retriable отказа.
    'tnt-retry',
    -- Подмена сети, шифрования и сна работника в проверках.
    'tnt-external',
    -- Срок вызова и отказ с родом, признаком отправки и приговором повтору.
    'tnt-storage',
    -- Шифрование соединения с брокером.
    'tnt-tls',
}

build = {
    type = 'builtin',
    modules = {
        ['tnt.kafka'] = 'tnt/kafka.lua',
        ['tnt.kafka.admin'] = 'tnt/kafka/admin.lua',
        ['tnt.kafka.api'] = 'tnt/kafka/api.lua',
        ['tnt.kafka.cluster'] = 'tnt/kafka/cluster.lua',
        ['tnt.kafka.codes'] = 'tnt/kafka/codes.lua',
        ['tnt.kafka.connect'] = 'tnt/kafka/connect.lua',
        ['tnt.kafka.coordination'] = 'tnt/kafka/coordination.lua',
        ['tnt.kafka.enter'] = 'tnt/kafka/enter.lua',
        ['tnt.kafka.fetch'] = 'tnt/kafka/fetch.lua',
        ['tnt.kafka.group'] = 'tnt/kafka/group.lua',
        ['tnt.kafka.handle'] = 'tnt/kafka/handle.lua',
        ['tnt.kafka.hook'] = 'tnt/kafka/hook.lua',
        ['tnt.kafka.kick'] = 'tnt/kafka/kick.lua',
        ['tnt.kafka.link'] = 'tnt/kafka/link.lua',
        ['tnt.kafka.message'] = 'tnt/kafka/message.lua',
        ['tnt.kafka.offsets'] = 'tnt/kafka/offsets.lua',
        ['tnt.kafka.outcome'] = 'tnt/kafka/outcome.lua',
        ['tnt.kafka.producer'] = 'tnt/kafka/producer.lua',
        ['tnt.kafka.records'] = 'tnt/kafka/records.lua',
        ['tnt.kafka.schema'] = 'tnt/kafka/schema.lua',
        ['tnt.kafka.settings'] = 'tnt/kafka/settings.lua',
        ['tnt.kafka.topic'] = 'tnt/kafka/topic.lua',
        ['tnt.kafka.wire'] = 'tnt/kafka/wire.lua',
        ['tnt.kafka.worker'] = 'tnt/kafka/worker.lua',
    },
}
