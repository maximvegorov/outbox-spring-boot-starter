package io.github.maximvegorov.outbox.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.maximvegorov.outbox.OutboxCustomizer;
import io.github.maximvegorov.outbox.OutboxHandlerRegistry;
import io.github.maximvegorov.outbox.OutboxProperties;
import io.github.maximvegorov.outbox.OutboxService;
import io.github.maximvegorov.outbox.internal.*;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.task.TaskExecutionAutoConfiguration;
import org.springframework.boot.autoconfigure.task.TaskSchedulingAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.task.ThreadPoolTaskExecutorBuilder;
import org.springframework.boot.task.ThreadPoolTaskSchedulerBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.VirtualThreadTaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

@AutoConfiguration(after = {TaskExecutionAutoConfiguration.class, TaskSchedulingAutoConfiguration.class})
@ConditionalOnProperty(prefix = "outbox", name = "enabled", matchIfMissing = true)
@EnableConfigurationProperties(OutboxProperties.class)
public class OutboxAutoConfiguration {
    private static final String SCHEMA_INITIALIZER_BEAN_NAME = "outboxSchemaInitializer";

    public static final String WORKER_EXECUTOR_BEAN_NAME = "outboxWorkerExecutor";
    public static final String OUTBOX_SCHEDULER_BEAN_NAME = "outboxScheduler";

    @Bean
    public OutboxService outboxService(OutboxQueueProcessor processor) {
        return new DefaultOutboxService(processor);
    }

    @Bean
    public OutboxQueueProcessor outboxQueueProcessor(
            OutboxProperties properties,
            @Qualifier(WORKER_EXECUTOR_BEAN_NAME) AsyncTaskExecutor taskExecutor,
            OutboxHandlerInvoker invoker,
            OutboxRepository repository,
            OutboxObservability observability) {
        return new OutboxQueueProcessorImpl(properties, taskExecutor, invoker, repository, observability);
    }

    @Bean(WORKER_EXECUTOR_BEAN_NAME)
    @ConditionalOnMissingBean(name = WORKER_EXECUTOR_BEAN_NAME)
    @DependsOn(TaskExecutionAutoConfiguration.APPLICATION_TASK_EXECUTOR_BEAN_NAME)
    public AsyncTaskExecutor outboxWorkerExecutor(
            OutboxProperties properties,
            ThreadPoolTaskExecutorBuilder builder) {
        var worker = properties.getWorker();

        return switch (worker.getThreadType()) {
            case VIRTUAL -> new VirtualThreadTaskExecutor(worker.getThreadNamePrefix());
            case PLATFORM -> builder.threadNamePrefix(worker.getThreadNamePrefix())
                    .allowCoreThreadTimeOut(worker.isAllowCoreThreadTimeout())
                    .corePoolSize(worker.getCoreSize())
                    .maxPoolSize(worker.getMaxSize())
                    .queueCapacity(worker.getQueueCapacity())
                    .acceptTasksAfterContextClose(false)
                    .awaitTermination(worker.isAwaitTermination())
                    .awaitTerminationPeriod(worker.getTerminationTimeout())
                    .build();
        };
    }

    @Bean
    public OutboxHandlerInvoker outboxHandlerInvoker(ObjectProvider<OutboxCustomizer> customizers, ObjectMapper objectMapper) {
        var registry = new OutboxHandlerRegistry();
        customizers.forEach(c -> c.customize(registry));
        return new OutboxHandlerInvokerImpl(registry, objectMapper);
    }

    @Bean
    @DependsOn(SCHEMA_INITIALIZER_BEAN_NAME)
    public OutboxRepository outboxRepository(JdbcClient jdbcClient) {
        return new OutboxRepositoryImpl(jdbcClient);
    }

    @Bean
    public OutboxObservability outboxObservability(
            ObjectProvider<OutboxMetrics> metrics,
            ObjectProvider<OutboxTracing> tracing) {
        return new OutboxObservabilityImpl(
                metrics.getIfAvailable(() -> OutboxMetrics.NOOP),
                tracing.getIfAvailable(() -> OutboxTracing.NOOP));
    }

    @Bean
    @ConditionalOnClass(MeterRegistry.class)
    @ConditionalOnBean(MeterRegistry.class)
    public OutboxMetrics outboxMetrics(MeterRegistry meterRegistry) {
        return new OutboxMetricsImpl(meterRegistry);
    }

    @Bean
    @ConditionalOnClass(Tracer.class)
    @ConditionalOnBean(Tracer.class)
    public OutboxTracing outboxTracing(Tracer tracer, Propagator propagator, ObjectMapper objectMapper) {
        return new OutboxTracingImpl(tracer, propagator, objectMapper);
    }

    @Bean(SCHEMA_INITIALIZER_BEAN_NAME)
    public OutboxSchemaInitializer outboxSchemaInitializer(JdbcTemplate jdbcTemplate) {
        return new OutboxSchemaInitializer(jdbcTemplate);
    }

    @Bean(OUTBOX_SCHEDULER_BEAN_NAME)
    @ConditionalOnMissingBean(name = OUTBOX_SCHEDULER_BEAN_NAME)
    @DependsOn("taskScheduler")
    public ThreadPoolTaskScheduler outboxScheduler(
            OutboxProperties outboxProperties,
            ThreadPoolTaskSchedulerBuilder builder) {
        var poller = outboxProperties.getScheduler();

        return builder.threadNamePrefix(poller.getThreadNamePrefix())
                .poolSize(1)
                .awaitTermination(poller.isAwaitTermination())
                .awaitTerminationPeriod(poller.getTerminationTimeout())
                .build();
    }

    @Bean
    public OutboxQueuePoller outboxPoller(OutboxQueueProcessor processor) {
        return new OutboxQueuePoller(processor);
    }

    @Bean
    public SmartInitializingSingleton outboxPollerScheduler(
            @Qualifier(OUTBOX_SCHEDULER_BEAN_NAME) ThreadPoolTaskScheduler scheduler,
            OutboxProperties properties,
            OutboxQueuePoller poller) {
        return () -> {
            var pollInterval = properties.getPollInterval();
            var randomShift = ThreadLocalRandom.current().nextLong(0, pollInterval.toNanos());
            var startTime = Instant.now().plusNanos(randomShift);
            scheduler.scheduleWithFixedDelay(poller::poll, startTime, properties.getPollInterval());
        };
    }

    @Bean
    @ConditionalOnProperty(prefix = "outbox.cleaner", name = "enabled", havingValue = "true")
    public OutboxQueueCleaner outboxQueueCleaner(OutboxProperties properties, OutboxRepository repository) {
        return new OutboxQueueCleaner(properties, repository);
    }

    @Bean
    @ConditionalOnBean(OutboxQueueCleaner.class)
    public SmartInitializingSingleton outboxCleanerScheduler(
            @Qualifier(OUTBOX_SCHEDULER_BEAN_NAME) ThreadPoolTaskScheduler scheduler,
            OutboxProperties properties,
            OutboxQueueCleaner cleaner) {
        return () -> {
            var runInterval = properties.getCleaner().getRunInterval();
            scheduler.scheduleWithFixedDelay(cleaner::clean, runInterval);
        };
    }
}
