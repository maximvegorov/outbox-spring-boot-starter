# outbox-spring-boot-starter

Spring Boot стартер, реализующий
паттерн [Transactional Outbox](https://microservices.io/patterns/data/transactional-outbox.html) поверх PostgreSQL.

## Требования

- Java 21+
- Spring Boot 3.5+
- PostgreSQL

## Установка

Добавьте зависимость в `pom.xml`:

```xml

<dependency>
    <groupId>io.github.maximvegorov</groupId>
    <artifactId>outbox-spring-boot-starter</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

## Как это работает

1. `OutboxService.publish()` сохраняет сообщение в таблицу `transaction_outbox` **в рамках текущей транзакции** вместе с
   бизнес-логикой — атомарно
2. После коммита транзакции сообщение немедленно отправляется на обработку в пул worker-ов
3. Периодический poller подхватывает сообщения, которые не были обработаны сразу (сбой worker-а, рестарт приложения и
   т.д.)
4. При временных ошибках сообщение повторяется с экспоненциальной выдержкой до исчерпания лимита попыток, после чего
   переводится в статус `ERROR`
5. Устаревшие успешно обработанные сообщения удаляются cleaner-ом

Для конкурентной обработки в multi-instance окружении используется optimistic locking через поле `version`.

## Использование

### 1. Реализуйте обработчик

```java

@Component
public class OrderCreatedHandler implements OutboxHandler<OrderCreatedEvent> {

    @Override
    public String getType() {
        return "order.created";
    }

    @Override
    public Class<OrderCreatedEvent> getPayloadType() {
        return OrderCreatedEvent.class;
    }

    @Override
    public void handle(String key, OrderCreatedEvent payload) {
        // отправка в Kafka, HTTP-вызов, и т.д.
    }
}
```

### 2. Зарегистрируйте обработчик

```java

@Configuration
public class OutboxConfig {

    @Bean
    public OutboxCustomizer outboxCustomizer(OrderCreatedHandler handler) {
        return registry -> registry.register(handler);
    }
}
```

### 3. Публикуйте сообщения

```java

@Service
@RequiredArgsConstructor
public class OrderService {
    private final OutboxService outboxService;

    @Transactional
    public void createOrder(Order order) {
        // бизнес-логика...
        outboxService.publish("order.created", order.getId().toString(), new OrderCreatedEvent(order));
    }
}
```

### Временные ошибки

Если обработчик выбрасывает `TemporaryFailureException`, сообщение будет повторено позже. Задержка между попытками
растёт экспоненциально (`timeout * multiplier^n`) и ограничена сверху значением `max-timeout`. После исчерпания
`max-attempts` сообщение переводится в статус `ERROR`. Любое другое исключение переводит сообщение в `ERROR` немедленно.

```java

@Override
public void handle(String key, OrderCreatedEvent payload) throws Exception {
    try {
        kafkaTemplate.send(...).get();
    } catch (TimeoutException e) {
        throw new TemporaryFailureException("Kafka недоступна");
    }
}
```

### Уникальность сообщений

Комбинация `(handlerType, payloadKey)` должна быть уникальной среди необработанных сообщений. Попытка опубликовать
сообщение
с уже существующим ключом приведёт к исключению. Это намеренное ограничение, предотвращающее дублирующиеся события для
одного
и того же объекта.

## Конфигурация

```yaml
outbox:
  enabled: true                   # включить/выключить стартер (по умолчанию: true)

  poll-interval: 1m               # интервал опроса очереди (по умолчанию: 1m)

  worker:
    thread-type: PLATFORM         # PLATFORM или VIRTUAL (по умолчанию: PLATFORM)
    thread-name-prefix: outbox-worker-
    core-size: 2
    max-size: 2
    queue-capacity: 10
    await-termination: true
    termination-timeout: 30s

  scheduler:
    thread-name-prefix: outbox-scheduler-
    await-termination: true
    termination-timeout: 30s

  defaults: # настройки по умолчанию для всех обработчиков
    max-attempts: 5               # максимальное число попыток при TemporaryFailureException
    timeout: 30s                  # начальная задержка перед повторной попыткой
    multiplier: 1.5               # множитель экспоненциального backoff (>= 1.0)
    max-timeout: 5m               # максимальная задержка между попытками

  handlers: # переопределение настроек для конкретного обработчика
    order.created:
      max-attempts: 10
      timeout: 1m
      multiplier: 2.0
      max-timeout: 1h

  cleaner:
    enabled: false                # включить автоматическую очистку (по умолчанию: false)
    retention-period: 30d         # удалять сообщения в статусе DONE старше этого периода
    batch-size: 10000             # количество записей в одном DELETE
    run-interval: 1d              # интервал между запусками очистки
```

### Экспоненциальный backoff

Задержка перед N-й попыткой вычисляется по формуле:

```
delay(N) = min(timeout * multiplier^N, max-timeout)
```

Пример с дефолтными настройками (`timeout=30s`, `multiplier=1.5`, `max-timeout=5m`):

| Попытка | Задержка |
|---------|----------|
| 1       | 30s      |
| 2       | 45s      |
| 3       | 67s      |
| 4       | 101s     |
| 5       | 152s     |

## Очистка обработанных сообщений

По умолчанию обработанные сообщения (`DONE`) остаются в таблице. Для автоматической очистки включите cleaner:

```yaml
outbox:
  cleaner:
    enabled: true
    retention-period: 30d   # хранить DONE-сообщения 7 дней
    batch-size: 10000       # удалять по 10000 записей за раз
    run-interval: 1d        # запускать раз в сутки
```

Очистка запускается батчами: каждый батч — отдельный `DELETE`. Процесс продолжается до тех пор, пока есть записи старше
`retention-period`.

## Tracing

При наличии Micrometer Tracing в classpath стартер автоматически поддерживает распределённую трассировку:

- **При публикации** (`publish()`): текущий trace context захватывается и сохраняется в БД вместе с сообщением
- **При обработке через scheduler**: context восстанавливается перед вызовом обработчика — span видно как дочерний к
  исходному запросу
- **При немедленной обработке через executor**: context не восстанавливается вручную — executor делает это автоматически
  через механизм context propagation

Если Micrometer Tracing не подключён, функция просто не активируется.

## Метрики

При наличии Micrometer в classpath стартер автоматически регистрирует счётчики в `MeterRegistry`:

| Метрика                     | Тег            | Описание                                            |
|-----------------------------|----------------|-----------------------------------------------------|
| `outbox.messages.published` | `handler_type` | Количество опубликованных сообщений                 |
| `outbox.messages.processed` | `handler_type` | Количество успешно обработанных сообщений           |
| `outbox.messages.error`     | `handler_type` | Количество сообщений, переведённых в статус `ERROR` |

Если Micrometer не подключён, метрики просто не собираются — никаких дополнительных настроек не требуется.

## Отключение стартера

```yaml
outbox:
  enabled: false
```
