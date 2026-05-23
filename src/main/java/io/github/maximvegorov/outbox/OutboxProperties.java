package io.github.maximvegorov.outbox;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Data;
import org.springframework.boot.autoconfigure.thread.Threading;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@ConfigurationProperties("outbox")
@Data
@Validated
public class OutboxProperties {
    private boolean enabled = true;

    @NotNull
    private Duration pollInterval = Duration.ofMinutes(1);

    @NotNull
    @Valid
    private PollerConfig poller = new PollerConfig();
    @NotNull
    @Valid
    private WorkerConfig worker = new WorkerConfig();
    @NotNull
    @Valid
    private HandlerConfig defaults = new HandlerConfig();
    @NotNull
    @Valid
    private Map<@NotEmpty String, @NotNull @Valid HandlerConfig> handlers = new HashMap<>();

    public int maxRetriesFor(String handlerType) {
        return Optional.ofNullable(handlers.get(handlerType))
                .map(HandlerConfig::getMaxRetries)
                .orElse(defaults.getMaxRetries());
    }

    public Instant expiredAtFor(String handlerType, Instant now) {
        var timeout = Optional.ofNullable(handlers.get(handlerType))
                .map(HandlerConfig::getTimeout)
                .orElse(defaults.getTimeout());
        return now.plus(timeout);
    }

    @Data
    public static class PollerConfig {
        @NotEmpty
        private String threadNamePrefix = "outbox-poll-";
        private boolean awaitTermination = true;
        @NotNull
        private Duration terminationTimeout = Duration.ofSeconds(30);
    }

    @Data
    public static class WorkerConfig {
        @NotNull
        private Threading threadType = Threading.PLATFORM;
        @NotEmpty
        private String threadNamePrefix = "outbox-worker-";
        private boolean allowCoreThreadTimeout;
        @Positive
        private int coreSize = 2;
        @Positive
        private int maxSize = 2;
        @Positive
        private int queueCapacity = 10;
        private boolean awaitTermination = true;
        @NotNull
        private Duration terminationTimeout = Duration.ofSeconds(30);
    }

    @Data
    public static class HandlerConfig {
        @Positive
        private int maxRetries = 3;
        @NotNull
        private Duration timeout = Duration.ofSeconds(30);
    }
}
