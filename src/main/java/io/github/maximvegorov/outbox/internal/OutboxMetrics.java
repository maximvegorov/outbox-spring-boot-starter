package io.github.maximvegorov.outbox.internal;

import org.jspecify.annotations.NullUnmarked;

@NullUnmarked
public interface OutboxMetrics {
    void recordPublished(String handlerType);

    void recordProcessed(String handlerType);

    void recordError(String handlerType);

    OutboxMetrics NOOP = new OutboxMetrics() {
        public void recordPublished(String handlerType) {
        }

        public void recordProcessed(String handlerType) {
        }

        public void recordError(String handlerType) {
        }
    };
}
