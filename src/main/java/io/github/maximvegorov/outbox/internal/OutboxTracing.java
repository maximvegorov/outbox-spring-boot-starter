package io.github.maximvegorov.outbox.internal;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

public interface OutboxTracing {
    @Nullable String captureContext();

    @NonNull
    Runnable restoreContext(@Nullable String tracingContext);

    Runnable NOOP_CLEANUP = () -> {};

    OutboxTracing NOOP = new OutboxTracing() {
        public String captureContext() { return null; }
        @NonNull
        public Runnable restoreContext(String ctx) { return NOOP_CLEANUP; }
    };
}
