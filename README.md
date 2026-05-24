# outbox-spring-boot-starter

Spring Boot стартер, реализующий паттерн [Transactional Outbox](https://microservices.io/patterns/data/transactional-outbox.html) поверх PostgreSQL.

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

1. `OutboxService.publish()` сохраняет сообщение в таблицу `transaction_outbox` **в рамках текущей транзакции** вместе с бизнес-логикой — атомарно
2. После коммита транзакции сообщение немедленно отправляется на обработку в пул worker-ов
3. Периодический poller подхватывает сообщения, которые не были обработаны сразу (сбой worker-а, рестарт приложения и т.д.)
4. При временных ошибках сообщение повторяется до исчерпания лимита попыток, после чего переводится в статус `ERROR`

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

`publish()` необходимо вызывать **внутри транзакции** — это обязательное условие паттерна Outbox.

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

Если обработчик выбрасывает `TemporaryFailureException`, сообщение будет повторено позже (в рамках настроенного лимита попыток). Любое другое исключение переводит сообщение сразу в статус `ERROR`.

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

Комбинация `(handlerType, payloadKey)` должна быть уникальной среди необработанных сообщений. Попытка опубликовать сообщение
с уже существующим ключом приведёт к исключению. Это намеренное ограничение, предотвращающее дублирующиеся события для одного
и того же объекта.

## Конфигурация

```yaml
outbox:
  enabled: true                   # включить/выключить стартер (по умолчанию: true)
  
  poll-interval: 1m               # интервал poller-а (по умолчанию: 1m)

  worker:
    thread-type: PLATFORM         # PLATFORM или VIRTUAL (по умолчанию: PLATFORM)
    thread-name-prefix: outbox-worker-
    core-size: 2
    max-size: 2
    queue-capacity: 10
    await-termination: true
    termination-timeout: 30s

  poller:
    thread-name-prefix: outbox-poll-
    await-termination: true
    termination-timeout: 30s

  defaults:                       # настройки по умолчанию для всех обработчиков
    max-retries: 3                # максимальное число попыток при TemporaryFailureException
    timeout: 30s                  # таймаут обработки (по истечении сообщение возвращается в NEW)

  handlers:                       # переопределение настроек для конкретного обработчика
    order.created:
      max-retries: 5
      timeout: 1m
```

## Tracing

При наличии Micrometer Tracing в classpath стартер автоматически поддерживает распределённую трассировку:

- **При публикации** (`publish()`): текущий trace context захватывается и сохраняется в БД вместе с сообщением
- **При обработке через poller**: context восстанавливается перед вызовом обработчика — span видно как дочерний к исходному запросу
- **При немедленной обработке через executor**: context не восстанавливается вручную — executor делает это автоматически через механизм context propagation

Если Micrometer Tracing не подключён, функция просто не активируется.

## Метрики

При наличии Micrometer в classpath стартер автоматически регистрирует счётчики в `MeterRegistry`:

| Метрика                    | Тег           | Описание                                               |
|----------------------------|---------------|--------------------------------------------------------|
| `outbox.messages.published` | `handler_type` | Количество опубликованных сообщений                    |
| `outbox.messages.processed` | `handler_type` | Количество успешно обработанных сообщений              |
| `outbox.messages.error`     | `handler_type` | Количество сообщений, переведённых в статус `ERROR`    |

Если Micrometer не подключён, метрики просто не собираются — никаких дополнительных настроек не требуется.

## Отключение стартера

```yaml
outbox:
  enabled: false
```
