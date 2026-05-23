CREATE TABLE IF NOT EXISTS transaction_outbox
(
    id            BIGSERIAL                NOT NULL,
    handler_type  TEXT                     NOT NULL,
    payload_key   TEXT                     NOT NULL,
    payload       JSONB                    NOT NULL,
    status        TEXT                     NOT NULL,
    version       BIGINT                   NOT NULL,
    retry_count   INTEGER                  NOT NULL,
    created_at    TIMESTAMP WITH TIME ZONE NOT NULL,
    expired_at    TIMESTAMP WITH TIME ZONE,
    processed_at  TIMESTAMP WITH TIME ZONE,

    CONSTRAINT pk_transaction_outbox PRIMARY KEY (id),
    CONSTRAINT ak_transaction_outbox UNIQUE (handler_type, payload_key)
);
