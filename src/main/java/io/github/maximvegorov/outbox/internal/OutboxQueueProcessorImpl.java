package io.github.maximvegorov.outbox.internal;

import io.github.maximvegorov.outbox.OutboxProperties;
import io.github.maximvegorov.outbox.TemporaryFailureException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.TaskRejectedException;

import java.time.Instant;
import java.util.Optional;

@NullUnmarked
@Slf4j
@RequiredArgsConstructor
public class OutboxQueueProcessorImpl implements OutboxQueueProcessor, InitializingBean {
    private final OutboxProperties properties;
    private final AsyncTaskExecutor taskExecutor;
    private final OutboxHandlerInvoker invoker;
    private final OutboxRepository repository;
    private final OutboxObservability observability;

    @Override
    public void afterPropertiesSet() {
        repository.moveErrorToNew();
    }

    @NonNull
    @Override
    public Optional<@NonNull OutboxMessage> enqueue(String handlerType, String payloadKey, Object payload) {
        var payloadJson = invoker.toJson(payloadKey, payload);

        var tracingContext = observability.captureTracingContext();

        var message = repository.save(handlerType, payloadKey, payloadJson, Instant.now(), tracingContext);
        if (message.isPresent()) {
            observability.recordPublished(handlerType);
        }
        return message;
    }

    @NonNull
    @Override
    public Optional<@NonNull OutboxMessage> reenqueue(String handlerType, String payloadKey) {
        var result = repository.moveToNew(handlerType, payloadKey);
        if (result.isPresent()) {
            observability.recordPublished(handlerType);
        }
        return result;
    }

    @Override
    public void tryProcessAsync(OutboxMessage message) {
        try {
            taskExecutor.submitCompletable(() -> process(message))
                    .exceptionally(ex -> {
                        log.error("Error while processing message with key {}", message.payloadKey(), ex);
                        return null;
                    });
        } catch (TaskRejectedException e) {
            log.warn("Unable to submit message with key {}. Will be processed by poller", message.payloadKey(), e);
        }
    }

    @Override
    public Long tryProcessFirst(Instant now) {
        repository.moveExpiredToNew(now);

        return repository.fetchFirstReadyToProcess()
                .map(message -> {
                    var cleanup = observability.restoreTracingContext(message.tracingContext());
                    try {
                        process(message);

                        return message.id();
                    } finally {
                        cleanup.run();
                    }
                })
                .orElse(null);
    }

    @Override
    public Long tryProcessNext(Long prevId) {
        return repository.fetchNextReadyToProcess(prevId)
                .map(message -> {
                    var cleanup = observability.restoreTracingContext(message.tracingContext());
                    try {
                        process(message);

                        return message.id();
                    } finally {
                        cleanup.run();
                    }
                })
                .orElse(null);
    }

    private void process(OutboxMessage message) {
        try {
            var expiredAt = properties.expiredAtFor(message.handlerType(), Instant.now(), message.failedAttempts());

            if (!repository.tryMoveToInProgress(message.id(), message.version(), expiredAt)) {
                log.warn("Message with key {} will be skipped", message.payloadKey());
                return;
            }

            invoker.invoke(message.handlerType(), message.payloadKey(), message.payload());

            repository.moveToDone(message.id(), Instant.now());

            observability.recordProcessed(message.handlerType());
        } catch (TemporaryFailureException e) {
            log.error("Temporary failure while processing message with key {}", message.payloadKey(), e);
            processTemporaryFailure(message);
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.error("Unexpected error while processing message with key {}", message.payloadKey(), e);
            moveToError(message);
        }
    }

    private void processTemporaryFailure(OutboxMessage message) {
        var maxAttempts = properties.maxAttemptsFor(message.handlerType());
        if (message.failedAttempts() + 1 < maxAttempts) {
            log.info("Try again later for message with key {}. Remain {} attempts", message.payloadKey(), maxAttempts - message.failedAttempts() - 1);
        } else {
            log.error("Retry count for message with key {} was exceeded. Will be moved to error", message.payloadKey());
            moveToError(message);
        }
    }

    private void moveToError(OutboxMessage message) {
        observability.recordError(message.handlerType());
        if (!repository.moveToError(message.id(), message.version() + 1)) {
            log.warn("Could not move to error message with key {}", message.payloadKey());
        }
    }
}
