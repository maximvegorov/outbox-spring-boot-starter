package io.github.maximvegorov.outbox;

public class TemporaryFailureException extends RuntimeException {
    public TemporaryFailureException(String message) {
        super(message);
    }
}
