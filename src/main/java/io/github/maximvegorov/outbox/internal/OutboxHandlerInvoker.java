package io.github.maximvegorov.outbox.internal;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;

@NullUnmarked
public interface OutboxHandlerInvoker {
    void invoke(String handlerType, String payloadKey, String payloadJson) throws Exception;

    @NonNull
    String toJson(String payloadKey, Object payload);
}
