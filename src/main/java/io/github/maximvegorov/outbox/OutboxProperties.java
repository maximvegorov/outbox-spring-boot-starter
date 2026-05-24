package io.github.maximvegorov.outbox;

import io.github.maximvegorov.outbox.utils.BackoffTimeoutCalculator;
import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
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
    private SchedulerConfig scheduler = new SchedulerConfig();
    @NotNull
    @Valid
    private WorkerConfig worker = new WorkerConfig();
    @NotNull
    @Valid
    private CleanerConfig cleaner = new CleanerConfig();
    @NotNull
    @Valid
    private HandlerConfig defaults = new HandlerConfig();
    @NotNull
    @Valid
    private Map<@NotEmpty String, @NotNull @Valid HandlerConfig> handlers = new HashMap<>();

    public int maxAttemptsFor(String handlerType) {
        return Optional.ofNullable(handlers.get(handlerType))
                .map(HandlerConfig::getMaxAttempts)
                .orElse(defaults.getMaxAttempts());
    }

    public Instant expiredAtFor(String handlerType, Instant now, int failedAttempts) {
        var handlerConfig = Optional.ofNullable(handlers.get(handlerType))
                .orElse(defaults);
        var maxTimeout = handlerConfig.getMaxTimeout();
        var timeout0 = handlerConfig.getTimeout();
        var multiplier = handlerConfig.getMultiplier();
        var timeout = BackoffTimeoutCalculator.calc(timeout0, failedAttempts, multiplier, maxTimeout);
        return now.plus(timeout);
    }

    @Data
    public static class SchedulerConfig {
        @NotEmpty
        private String threadNamePrefix = "outbox-scheduler-";
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
    public static class CleanerConfig {
        private boolean enabled = false;
        @NotNull
        private Duration retentionPeriod = Duration.ofDays(7);
        @Positive
        private int batchSize = 10_000;
        @NotNull
        private Duration runInterval = Duration.ofDays(1);
    }

    @Data
    public static class HandlerConfig {
        @Positive
        private int maxAttempts = 5;
        @NotNull
        private Duration timeout = Duration.ofSeconds(30);
        @Positive
        @DecimalMin("1.0")
        private double multiplier = 1.5;
        @NotNull
        private Duration maxTimeout = Duration.ofMinutes(5);
    }
}
