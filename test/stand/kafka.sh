#!/usr/bin/env bash
# Поднимает Kafka для живых проверок клиента tnt-kafka.
#
# Настоящий брокер, а не двойник: двойник показывает, что мы правильно
# разговариваем сами с собой, а чужой сервер — что нас понимает кто-то
# ещё. Разница вылезает на первом же отказе: версии протокола, которые
# брокер принимает, контрольная сумма пачки записей, порядок входа
# в группу, отказ во входе по SASL.
#
# Один узел KRaft — и брокер, и контроллер, — и три порта. Открытый —
# без входа. Второй — SASL PLAIN с учётной записью `app`: неверный
# пароль тогда отвергает настоящий брокер. Третий — TLS с сертификатом,
# выпущенным здесь же на сутки корнем из того же каталога; клиентский
# сертификат не требуется.
#
# Темы сами не заводятся (`auto.create.topics.enable=false`): их заводит
# пакет, и проверка обязана увидеть, что он это умеет. Разделов по
# умолчанию три — иначе ключ ни в какой раздел, кроме нулевого, не уйдёт.
#
# Каталог сертификатов — `KAFKA_TLS_DIR`, по умолчанию
# `test/stand/run/kafka-tls`; живая проверка читает ту же переменную.
#
#   test/stand/kafka.sh          # поднять
#   test/stand/kafka.sh stop     # погасить
set -euo pipefail

cd "$(dirname "$0")"

IMAGE='apache/kafka:3.9.1'
CONTAINER='tnt-stand-kafka'
PORT="${STAND_KAFKA_PORT:-19092}"
SASL_PORT="${STAND_KAFKA_SASL_PORT:-19094}"
TLS_PORT="${STAND_KAFKA_TLS_PORT:-19095}"
DIR="${KAFKA_TLS_DIR:-run/kafka-tls}"

# Пароль стенда: локальный брокер для проверок, а не развёртывание.
PASSWORD='stand-secret'

if ! command -v docker > /dev/null 2>&1; then
    echo 'docker не найден: Kafka поднять нечем' >&2
    exit 1
fi

if [ "${1:-up}" = 'stop' ]; then
    docker rm -f "${CONTAINER}" > /dev/null 2>&1 || true
    echo 'Kafka остановлена'
    exit 0
fi

if ! command -v openssl > /dev/null 2>&1; then
    echo 'openssl не найден: сертификаты выпустить нечем' >&2
    exit 1
fi

mkdir -p "${DIR}"
DIR="$(cd "${DIR}" && pwd)"

# Сертификаты выпускаются заново на каждый подъём: срок у них сутки,
# и вчерашний каталог дал бы отказ рукопожатия, неотличимый от того,
# ради которого проверка написана.
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj '/CN=tnt-kafka-live-ca' \
    -keyout "${DIR}/ca.key" -out "${DIR}/ca.pem" 2> /dev/null
printf 'subjectAltName=IP:127.0.0.1,DNS:localhost\nextendedKeyUsage=serverAuth\n' > "${DIR}/server.ext"
openssl req -newkey rsa:2048 -nodes -subj '/CN=localhost' \
    -keyout "${DIR}/server.key" -out "${DIR}/server.csr" 2> /dev/null
openssl x509 -req -in "${DIR}/server.csr" -days 1 \
    -CA "${DIR}/ca.pem" -CAkey "${DIR}/ca.key" -CAcreateserial \
    -extfile "${DIR}/server.ext" -out "${DIR}/server.pem" 2> /dev/null

# Хранилище ключей PEM брокер читает одним файлом: ключ в PKCS#8 без
# пароля и цепочка сертификатов следом.
openssl pkcs8 -topk8 -nocrypt -in "${DIR}/server.key" -out "${DIR}/server.pk8" 2> /dev/null
cat "${DIR}/server.pk8" "${DIR}/server.pem" > "${DIR}/keystore.pem"

# Брокер в образе запускается не от root: ключ должен читаться всеми.
chmod 644 "${DIR}"/*.key "${DIR}"/*.pk8 "${DIR}/keystore.pem"

# Повторный запуск безвреден: контейнер с тем же именем сносится
# и поднимается заново. Данные живут внутри контейнера и уходят с ним.
docker rm -f "${CONTAINER}" > /dev/null 2>&1 || true

docker run -d \
    --name "${CONTAINER}" \
    -p "127.0.0.1:${PORT}:9092" \
    -p "127.0.0.1:${SASL_PORT}:9094" \
    -p "127.0.0.1:${TLS_PORT}:9095" \
    -v "${DIR}:/certs:ro" \
    -e KAFKA_NODE_ID=1 \
    -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENERS=PLAIN://:9092,CONTROLLER://:9093,SASL://:9094,TLS://:9095 \
    -e KAFKA_ADVERTISED_LISTENERS="PLAIN://127.0.0.1:${PORT},SASL://127.0.0.1:${SASL_PORT},TLS://127.0.0.1:${TLS_PORT}" \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAIN:PLAINTEXT,SASL:SASL_PLAINTEXT,TLS:SSL \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAIN \
    -e KAFKA_SASL_ENABLED_MECHANISMS=PLAIN \
    -e KAFKA_LISTENER_NAME_SASL_PLAIN_SASL_JAAS_CONFIG="org.apache.kafka.common.security.plain.PlainLoginModule required user_app=\"${PASSWORD}\";" \
    -e KAFKA_SSL_KEYSTORE_TYPE=PEM \
    -e KAFKA_SSL_KEYSTORE_LOCATION=/certs/keystore.pem \
    -e KAFKA_SSL_CLIENT_AUTH=none \
    -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false \
    -e KAFKA_NUM_PARTITIONS=3 \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
    "${IMAGE}" > /dev/null

# Готовности ждём: проверки, запущенные сразу после подъёма, иначе
# пропустятся — и это выглядит как «всё хорошо», хотя ничего
# не проверено. Средства самого брокера внутри контейнера для этого
# не годятся: брокер называет клиентам адрес хоста, и изнутри
# контейнера по нему не достучаться. Готов брокер тогда, когда пишет
# в журнал о запуске.
for _ in $(seq 1 120); do
    if docker logs "${CONTAINER}" 2>&1 | grep -q 'Kafka Server started'; then
        echo "Kafka поднята: 127.0.0.1:${PORT}, SASL 127.0.0.1:${SASL_PORT}, TLS 127.0.0.1:${TLS_PORT}, сертификаты в ${DIR}"
        exit 0
    fi

    sleep 0.5
done

echo "Kafka не ответила за минуту: смотрите docker logs ${CONTAINER}" >&2
exit 1
