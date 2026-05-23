package io.github.maximvegorov.outbox;

import org.jspecify.annotations.NullUnmarked;

@NullUnmarked
public interface OutboxService {
    void publish(String handlerType, String payloadKey, Object payload);
}
