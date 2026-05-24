package io.github.maximvegorov.outbox.internal;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.jspecify.annotations.NullUnmarked;

import java.time.Instant;
import java.util.Optional;

@NullUnmarked
public interface OutboxRepository {
    void moveErrorToNew();

    @NonNull
    Optional<@NonNull OutboxMessage> save(
            String handlerType,
            String payloadKey,
            String payloadJson,
            Instant createdAt,
            @Nullable String tracingContext);

    @NonNull
    Optional<@NonNull OutboxMessage> moveToNew(String handlerType, String payloadKey);

    boolean tryMoveToInProgress(Long id, long expectedVersion, Instant expiredAt);

    void moveToDone(Long id, Instant processedAt);

    boolean moveToError(Long id, long expectedVersion);

    void moveExpiredToNew(Instant now);

    Optional<@NonNull OutboxMessage> fetchFirstReadyToProcess();

    Optional<@NonNull OutboxMessage> fetchNextReadyToProcess(Long prevId);

    int deleteDoneBefore(Instant processedBefore, int batchSize);
}
