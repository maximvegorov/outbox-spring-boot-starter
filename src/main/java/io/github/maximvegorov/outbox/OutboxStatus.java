package io.github.maximvegorov.outbox;

public enum OutboxStatus {
    NEW,
    IN_PROGRESS,
    DONE,
    ERROR;
}
