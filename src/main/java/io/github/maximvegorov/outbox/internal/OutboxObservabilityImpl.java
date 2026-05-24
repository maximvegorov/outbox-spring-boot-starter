package io.github.maximvegorov.outbox.internal;

import lombok.RequiredArgsConstructor;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.jspecify.annotations.NullUnmarked;

@NullUnmarked
@RequiredArgsConstructor
public class OutboxObservabilityImpl implements OutboxObservability {
    private final OutboxMetrics metrics;
    private final OutboxTracing tracing;

    @Override
    public void recordPublished(String handlerType) {
        metrics.recordPublished(handlerType);
    }

    @Override
    public void recordProcessed(String handlerType) {
        metrics.recordProcessed(handlerType);
    }

    @Override
    public void recordError(String handlerType) {
        metrics.recordError(handlerType);
    }

    @Nullable
    @Override
    public String captureTracingContext() {
        return tracing.captureContext();
    }

    @NonNull
    @Override
    public Runnable restoreTracingContext(@Nullable String tracingContext) {
        return tracing.restoreContext(tracingContext);
    }
}
