package io.github.maximvegorov.outbox.internal;

import io.github.maximvegorov.outbox.OutboxService;
import lombok.RequiredArgsConstructor;
import org.jspecify.annotations.NullUnmarked;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@NullUnmarked
@RequiredArgsConstructor
public class DefaultOutboxService implements OutboxService {
    private final OutboxQueueProcessor queueProcessor;

    @Override
    public boolean publish(String handlerType, String payloadKey, Object payload) {
        return queueProcessor.enqueue(handlerType, payloadKey, payload)
                .map(message -> {
                    triggerAfterCommit(message);
                    return true;
                })
                .orElse(false);

    }

    @Override
    public boolean republish(String handlerType, String payloadKey) {
        return queueProcessor.reenqueue(handlerType, payloadKey)
                .map(message -> {
                    triggerAfterCommit(message);
                    return true;
                })
                .orElse(false);
    }

    private void triggerAfterCommit(OutboxMessage message) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    queueProcessor.tryProcessAsync(message);
                }
            });
        } else {
            queueProcessor.tryProcessAsync(message);
        }
    }
}
