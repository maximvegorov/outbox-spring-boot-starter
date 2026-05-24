CREATE TABLE IF NOT EXISTS transaction_outbox
(
    id              BIGSERIAL                NOT NULL,
    handler_type    TEXT                     NOT NULL,
    payload_key     TEXT                     NOT NULL,
    payload         JSONB                    NOT NULL,
    status          TEXT                     NOT NULL,
    version         BIGINT                   NOT NULL,
    failed_attempts INTEGER                  NOT NULL,
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL,
    expired_at      TIMESTAMP WITH TIME ZONE,
    processed_at    TIMESTAMP WITH TIME ZONE,
    tracing_context TEXT,

    CONSTRAINT pk_transaction_outbox PRIMARY KEY (id),
    CONSTRAINT ak_transaction_outbox UNIQUE (handler_type, payload_key)
);

CREATE INDEX IF NOT EXISTS idx_transaction_outbox_status_id ON transaction_outbox (status, id);
CREATE INDEX IF NOT EXISTS idx_transaction_outbox_status_processed_at ON transaction_outbox (status, processed_at);
