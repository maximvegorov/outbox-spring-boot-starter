package io.github.maximvegorov.outbox.internal;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;

import java.time.Instant;

@NullUnmarked
public interface OutboxQueueProcessor {
    @NonNull
    OutboxMessage enqueue(String handlerType, String payloadKey, Object payload);

    void tryProcessAsync(OutboxMessage message);

    Long tryProcessFirst(Instant now);

    Long tryProcessNext(Long prevId);
}
