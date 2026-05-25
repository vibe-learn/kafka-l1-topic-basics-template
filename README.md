# Урок 1 — Что такое топик: домашка

> **Задача**: реализовать idempotent Kafka producer на Go, который шлёт N сообщений
> в топик `orders` и удовлетворяет всем требованиям ниже. Тесты автора в
> `producer_test.go` ДОЛЖНЫ позеленеть.

## Требования

Реализуй функции в `producer.go`:

### `func NewProducer(brokers []string, topic string) (*Producer, error)`
- Создаёт producer с конфигом:
  - `enable.idempotence=true` (gives exactly-once semantics в рамках одного producer).
  - `acks=all` (ждать подтверждения от всех ISR).
  - `retries=3` (auto-retry на network error).
  - `retry.backoff.ms` — экспоненциальный (50ms → 100ms → 200ms; реализация может быть в Send).
- Возвращает ошибку, если brokers пустой.

### `func (p *Producer) Send(ctx context.Context, key, value []byte) (partition int32, offset int64, err error)`
- Отправляет одно сообщение в топик.
- Ретраит до 3 раз на временных network errors (`net.Error.Temporary()` или конкретные kafka error codes — твой выбор).
- Логирует `[OK] topic=orders partition=N offset=M` на успехе.
- Возвращает (partition, offset, nil) на успехе или (0, 0, error) на финальном failure.

### `func (p *Producer) Close() error`
- Вызывает Flush с таймаутом 5 сек (ждёт что все pending writes ушли).
- Закрывает internal writer/connection.
- Идемпотентен (повторный вызов не падает).

### `func RunUntilSignal(ctx context.Context, p *Producer, topic string, n int) error`
- Шлёт N сообщений с ключами `order-1, order-2, ..., order-N` и telemetry-payload (любой JSON).
- Слушает SIGINT (Ctrl+C) — при получении:
  1. Останавливает отправку (не шлёт новые).
  2. Вызывает `p.Close()` чтобы flush и закрыть корректно.
  3. Возвращает `context.Canceled` (НЕ паника).

## Как проверять локально

```bash
go mod download
go test ./... -v
```

Все тесты в `producer_test.go` должны быть зелёными.

## Как сдать

Просто пуш в свой fork этого репо. GitHub Actions запустит тесты автоматически.

Если тесты позеленели — Vibe Learn получит webhook от GH Actions и засчитает урок.
Если красные — посмотри лог в Actions tab, исправь, запушь снова.

## Подсказки

- Можно использовать `github.com/segmentio/kafka-go` (рекомендую — pure Go, без Cgo).
- Можно использовать `github.com/confluentinc/confluent-kafka-go` (production-grade, но требует librdkafka).
- Для тестов мы НЕ запускаем настоящий Kafka — author tests используют интерфейс, который ты можешь mock'ать. Смотри `producer_test.go` чтобы понять контракт.
- `enable.idempotence=true` в segmentio kafka-go выставляется через `kafka.Writer{...}` field. В confluent-kafka-go — через `kafka.ConfigMap`.

Удачи. Это первая «настоящая» домашка курса — если задаст путаницу, открой Feynman-наставника в Vibe Learn и спрашивай.
