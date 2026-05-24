package io.github.maximvegorov.outbox;

import org.jspecify.annotations.NullUnmarked;

@NullUnmarked
public interface OutboxService {
    boolean publish(String handlerType, String payloadKey, Object payload);

    boolean republish(String handlerType, String payloadKey);
}
