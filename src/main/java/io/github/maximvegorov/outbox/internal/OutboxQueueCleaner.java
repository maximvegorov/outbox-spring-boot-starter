package io.github.maximvegorov.outbox.internal;

import io.github.maximvegorov.outbox.OutboxProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;

@Slf4j
@RequiredArgsConstructor
public class OutboxQueueCleaner {
    private final OutboxProperties properties;
    private final OutboxRepository repository;

    public void clean() {
        log.info("Start cleaning outdated messages");
        try {
            var config = properties.getCleaner();
            var processedBefore = Instant.now().minus(config.getRetentionPeriod());
            var batchSize = config.getBatchSize();
            var totalDeleted = 0L;
            int deleted;
            do {
                if (Thread.currentThread().isInterrupted()) {
                    break;
                }
                deleted = repository.deleteDoneBefore(processedBefore, batchSize);
                totalDeleted += deleted;
            } while (deleted == batchSize);
            log.info("Deleted {} outdated messages processed before {}", totalDeleted, processedBefore);
        } catch (RuntimeException e) {
            log.error("Unexpected error while cleaning outdated messages", e);
        }
    }
}
