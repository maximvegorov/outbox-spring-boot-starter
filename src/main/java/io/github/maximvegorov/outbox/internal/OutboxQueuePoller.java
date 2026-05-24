package io.github.maximvegorov.outbox.internal;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;

@Slf4j
@RequiredArgsConstructor
public class OutboxQueuePoller {
    private final OutboxQueueProcessor processor;

    public void poll() {
        log.info("Start processing messages");
        try {
            var prevPos = processor.tryProcessFirst(Instant.now());
            while (!Thread.currentThread().isInterrupted() && prevPos != null) {
                log.info("Processing next message after {}", prevPos);
                prevPos = processor.tryProcessNext(prevPos);
            }
            log.info("Messages were processed");
        } catch (RuntimeException e) {
            log.error("Error while processing messages", e);
        }
    }
}
