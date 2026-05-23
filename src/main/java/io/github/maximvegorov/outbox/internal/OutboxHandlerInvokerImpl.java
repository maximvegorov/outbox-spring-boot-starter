package io.github.maximvegorov.outbox.internal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.maximvegorov.outbox.OutboxException;
import io.github.maximvegorov.outbox.OutboxHandler;
import io.github.maximvegorov.outbox.OutboxHandlerRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;

@NullUnmarked
@Slf4j
@RequiredArgsConstructor
public class OutboxHandlerInvokerImpl implements OutboxHandlerInvoker {
    private final OutboxHandlerRegistry registry;
    private final ObjectMapper objectMapper;

    @Override
    public void invoke(String handlerType, String payloadKey, String payloadJson) throws Exception {
        var handler = getHandler(handlerType, payloadKey);

        var payload = fromJson(payloadKey, payloadJson, handler.getPayloadType());

        handler.handle(payloadKey, payload);
    }

    @NonNull
    @Override
    public String toJson(String payloadKey, Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new OutboxException("Failed to serialize payload for message with key %s".formatted(payloadKey), e);
        }
    }

    @SuppressWarnings("unchecked")
    private OutboxHandler<Object> getHandler(String handlerType, String payloadKey) {
        return (OutboxHandler<Object>) registry.getHandler(handlerType)
                .orElseThrow(() -> new OutboxException("No handler for message with key %s".formatted(payloadKey)));
    }

    private Object fromJson(String payloadKey, String payload, Class<?> clazz) {
        try {
            return objectMapper.readValue(payload, clazz);
        } catch (JsonProcessingException e) {
            throw new OutboxException("Failed to deserialize payload for message with key %s".formatted(payloadKey), e);
        }
    }
}
