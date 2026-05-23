package io.github.maximvegorov.outbox;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class OutboxHandlerRegistry {
    private final Map<String, OutboxHandler<?>> handlers = new HashMap<>();

    public <T> void register(OutboxHandler<T> handler) {
        handlers.put(handler.getType(), handler);
    }

    public Optional<OutboxHandler<?>> getHandler(String handlerType) {
        return Optional.ofNullable(handlers.get(handlerType));
    }
}
