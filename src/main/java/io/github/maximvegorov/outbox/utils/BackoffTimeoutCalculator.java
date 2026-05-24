package io.github.maximvegorov.outbox.utils;

import lombok.experimental.UtilityClass;

import java.time.Duration;

@UtilityClass
public final class BackoffTimeoutCalculator {
    public static Duration calc(Duration timeout0, int failedAttempts, double multiplier, Duration maxTimeout) {
        if (failedAttempts == 0) {
            return timeout0;
        }
        var result = timeout0.toNanos() * Math.pow(multiplier, failedAttempts);
        var timeout = Math.min((long) result, maxTimeout.toNanos());
        return Duration.ofNanos(timeout);
    }
}
