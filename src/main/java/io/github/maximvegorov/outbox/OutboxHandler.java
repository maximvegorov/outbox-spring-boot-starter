package io.github.maximvegorov.outbox;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;

@NullUnmarked
public interface OutboxHandler<T> {
    @NonNull
    String getType();

    @NonNull
    Class<T> getPayloadType();

    void handle(String key, T payload) throws Exception;
}
