package io.github.maximvegorov.outbox.internal;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;

@NullUnmarked
public interface OutboxObservability {
    void recordPublished(String handlerType);

    void recordProcessed(String handlerType);

    void recordError(String handlerType);

    @Nullable
    String captureTracingContext();

    @NonNull
    Runnable restoreTracingContext(@Nullable String tracingContext);
}
