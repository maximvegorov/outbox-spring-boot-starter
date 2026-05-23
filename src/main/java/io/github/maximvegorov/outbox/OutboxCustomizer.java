package io.github.maximvegorov.outbox;

@FunctionalInterface
public interface OutboxCustomizer {
    void customize(OutboxHandlerRegistry registry);
}
