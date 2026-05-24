package io.github.maximvegorov.outbox.internal;

import io.github.maximvegorov.outbox.OutboxStatus;
import lombok.Builder;
import lombok.NonNull;

import java.time.Instant;

@Builder
public record OutboxMessage(
        Long id,
        @NonNull
        String handlerType,
        @NonNull
        String payloadKey,
        @NonNull
        String payload,
        int retryCount,
        @NonNull
        Instant createdAt,
        Instant expiredAt,
        Instant processedAt,
        String tracingContext,
        @NonNull
        OutboxStatus status,
        long version
) {
}
