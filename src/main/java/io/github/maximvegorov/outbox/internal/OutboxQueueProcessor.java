package io.github.maximvegorov.outbox.internal;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Optional;

@NullUnmarked
public interface OutboxQueueProcessor {
    @NonNull
    Optional<@NonNull OutboxMessage> enqueue(String handlerType, String payloadKey, Object payload);

    @NonNull
    Optional<@NonNull OutboxMessage> reenqueue(String handlerType, String payloadKey);

    void tryProcessAsync(OutboxMessage message);

    Long tryProcessFirst(Instant now);

    Long tryProcessNext(Long prevId);
}
