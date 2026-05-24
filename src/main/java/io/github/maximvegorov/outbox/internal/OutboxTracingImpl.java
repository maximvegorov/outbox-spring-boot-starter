package io.github.maximvegorov.outbox.internal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

@Slf4j
@RequiredArgsConstructor
public class OutboxTracingImpl implements OutboxTracing {
    private final Tracer tracer;
    private final Propagator propagator;
    private final ObjectMapper objectMapper;

    @Nullable
    @Override
    public String captureContext() {
        var context = tracer.currentTraceContext().context();
        if (context == null) {
            return null;
        }

        var carrier = new HashMap<String, String>();
        propagator.inject(context, carrier, Map::put);
        if (carrier.isEmpty()) {
            return null;
        }

        return toJson(carrier);
    }

    @NonNull
    @Override
    public Runnable restoreContext(@Nullable String tracingContext) {
        if (tracingContext == null) {
            return () -> {
            };
        }

        var carrier = fromJson(tracingContext);
        if (carrier.isEmpty()) {
            return () -> {};
        }

        try {
            var span = propagator.extract(carrier, Map::get)
                    .name("outbox.process")
                    .start();

            var scope = tracer.withSpan(span);

            return () -> {
                scope.close();
                span.end();
            };
        } catch (RuntimeException e) {
            log.warn("Failed to restore tracing context", e);

            return () -> {
            };
        }
    }

    private @Nullable String toJson(HashMap<String, String> carrier) {
        try {
            return objectMapper.writeValueAsString(new TracingContext(carrier));
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize tracing context", e);
            return null;
        }
    }

    private Map<String, String> fromJson(String tracingContext) {
        try {
            return objectMapper.readValue(tracingContext, TracingContext.class)
                    .carrier();
        } catch (JsonProcessingException e) {
            log.error("Failed to parse tracing context", e);
            return emptyMap();
        }
    }

    record TracingContext(@NonNull Map<String, String> carrier) {
    }
}
