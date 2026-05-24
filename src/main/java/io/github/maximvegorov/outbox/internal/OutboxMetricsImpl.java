package io.github.maximvegorov.outbox.internal;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import org.jspecify.annotations.NullUnmarked;

@NullUnmarked
@RequiredArgsConstructor
public class OutboxMetricsImpl implements OutboxMetrics {
    private static final String HANDLER_TYPE_TAG = "handler_type";

    private final MeterRegistry meterRegistry;

    @Override
    public void recordPublished(String handlerType) {
        counter("outbox.messages.published", handlerType).increment();
    }

    @Override
    public void recordProcessed(String handlerType) {
        counter("outbox.messages.processed", handlerType).increment();
    }

    @Override
    public void recordError(String handlerType) {
        counter("outbox.messages.error", handlerType).increment();
    }

    private Counter counter(String name, String handlerType) {
        return Counter.builder(name)
                .tag(HANDLER_TYPE_TAG, handlerType)
                .register(meterRegistry);
    }
}
