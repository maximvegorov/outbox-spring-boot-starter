package io.github.maximvegorov.outbox.internal;

import io.github.maximvegorov.outbox.OutboxStatus;
import lombok.RequiredArgsConstructor;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SqlTypeValue;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.sql.JDBCType;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.Optional;

@NullUnmarked
@RequiredArgsConstructor
public class OutboxRepositoryImpl implements OutboxRepository {
    private final JdbcClient jdbcClient;

    @Override
    public void moveErrorToNew() {
        jdbcClient.sql("""
                        update transaction_outbox
                        set status = :to, version = version + 1, failed_attempts = 0
                        where status = :from
                        """)
                .param("to", OutboxStatus.NEW.name())
                .param("from", OutboxStatus.ERROR.name())
                .update();
    }

    @NonNull
    @Override
    public Optional<@NonNull OutboxMessage> save(
            String handlerType,
            String payloadKey,
            String payloadJson,
            Instant createdAt,
            @Nullable String tracingContext) {
        return jdbcClient.sql("""
                        insert into transaction_outbox (handler_type, payload_key, payload, status, version, failed_attempts, created_at, tracing_context)
                        values (:handlerType, :payloadKey, cast(:payload AS jsonb), :status, 1, 0, :createdAt, :tracingContext)
                        on conflict (handler_type, payload_key) do nothing
                        returning id
                        """)
                .param("handlerType", handlerType)
                .param("payloadKey", payloadKey)
                .param("payload", payloadJson)
                .param("status", OutboxStatus.NEW.name())
                .param("createdAt", Timestamp.from(createdAt))
                .param("tracingContext", tracingContext, Types.VARCHAR)
                .query(Long.class)
                .optional()
                .map(id -> OutboxMessage.builder()
                        .id(id)
                        .handlerType(handlerType)
                        .payloadKey(payloadKey)
                        .payload(payloadJson)
                        .status(OutboxStatus.NEW)
                        .version(1)
                        .createdAt(createdAt)
                        .tracingContext(tracingContext)
                        .build());
    }

    @NonNull
    @Override
    public Optional<@NonNull OutboxMessage> moveToNew(String handlerType, String payloadKey) {
        return jdbcClient.sql("""
                        update transaction_outbox
                        set status = :newStatus, version = version + 1, failed_attempts = 0, expired_at = null
                        where handler_type = :handlerType and payload_key = :payloadKey and status = :errorStatus
                        returning *
                        """)
                .param("newStatus", OutboxStatus.NEW.name())
                .param("handlerType", handlerType)
                .param("payloadKey", payloadKey)
                .param("errorStatus", OutboxStatus.ERROR.name())
                .query(Mappers.ROW_MAPPER)
                .optional();
    }

    @Override
    public boolean tryMoveToInProgress(Long id, long expectedVersion, Instant expiredAt) {
        return jdbcClient.sql("""
                        update transaction_outbox
                        set status = :status, version = version + 1, failed_attempts = failed_attempts + 1, expired_at = :expiredAt
                        where id = :id and version = :version
                        """)
                .param("status", OutboxStatus.IN_PROGRESS.name())
                .param("expiredAt", Timestamp.from(expiredAt))
                .param("id", id)
                .param("version", expectedVersion)
                .update() == 1;
    }

    @Override
    public void moveToDone(Long id, Instant processedAt) {
        jdbcClient.sql("""
                        update transaction_outbox
                        set status = :status, version = version + 1, processed_at = :processedAt
                        where id = :id and status <> :status
                        """)
                .param("status", OutboxStatus.DONE.name())
                .param("processedAt", Timestamp.from(processedAt))
                .param("id", id)
                .update();
    }

    @Override
    public boolean moveToError(Long id, long expectedVersion) {
        return jdbcClient.sql("""
                        update transaction_outbox
                        set status = :status, version = version + 1
                        where id = :id and version = :version
                        """)
                .param("status", OutboxStatus.ERROR.name())
                .param("id", id)
                .param("version", expectedVersion)
                .update() == 1;
    }

    @Override
    public void moveExpiredToNew(Instant now) {
        jdbcClient.sql("""
                        update transaction_outbox
                        set status = :to, version = version + 1, expired_at = null
                        where status = :from and expired_at <= :now
                        """)
                .param("to", OutboxStatus.NEW.name())
                .param("from", OutboxStatus.IN_PROGRESS.name())
                .param("now", Timestamp.from(now))
                .update();
    }

    @Override
    public Optional<@NonNull OutboxMessage> fetchFirstReadyToProcess() {
        return jdbcClient.sql("""
                        select * from transaction_outbox where status = :status order by id limit 1
                        """)
                .param("status", OutboxStatus.NEW.name())
                .query(Mappers.ROW_MAPPER)
                .optional();
    }

    @Override
    public Optional<@NonNull OutboxMessage> fetchNextReadyToProcess(Long prevId) {
        return jdbcClient.sql("""
                        select * from transaction_outbox where status = :status and id > :prevId order by id limit 1
                        """)
                .param("status", OutboxStatus.NEW.name())
                .param("prevId", prevId)
                .query(Mappers.ROW_MAPPER)
                .optional();
    }

    @Override
    public int deleteDoneBefore(Instant processedBefore, int batchSize) {
        return jdbcClient.sql("""
                        delete from transaction_outbox
                        where id in (
                            select id from transaction_outbox
                            where status = :status and processed_at < :processedBefore
                            order by id
                            limit :batchSize
                        )
                        """)
                .param("status", OutboxStatus.DONE.name())
                .param("processedBefore", Timestamp.from(processedBefore))
                .param("batchSize", batchSize)
                .update();
    }

    private static final class Mappers {
        public static final RowMapper<OutboxMessage> ROW_MAPPER = (rs, rowNum) -> {
            var expiredAt = rs.getTimestamp("expired_at");
            var processedAt = rs.getTimestamp("processed_at");

            return OutboxMessage.builder()
                    .id(rs.getLong("id"))
                    .handlerType(rs.getString("handler_type"))
                    .payloadKey(rs.getString("payload_key"))
                    .payload(rs.getString("payload"))
                    .failedAttempts(rs.getInt("failed_attempts"))
                    .createdAt(rs.getTimestamp("created_at").toInstant())
                    .expiredAt(expiredAt != null ? expiredAt.toInstant() : null)
                    .processedAt(processedAt != null ? processedAt.toInstant() : null)
                    .tracingContext(rs.getString("tracing_context"))
                    .status(OutboxStatus.valueOf(rs.getString("status")))
                    .version(rs.getLong("version"))
                    .build();
        };
    }
}
