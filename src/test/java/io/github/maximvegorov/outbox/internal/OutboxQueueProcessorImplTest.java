package io.github.maximvegorov.outbox.internal;

import io.github.maximvegorov.outbox.OutboxProperties;
import io.github.maximvegorov.outbox.OutboxStatus;
import io.github.maximvegorov.outbox.TemporaryFailureException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.TaskRejectedException;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.github.maximvegorov.outbox.internal.OutboxQueueProcessorImplTest.TestData.testMessage;
import static io.github.maximvegorov.outbox.internal.OutboxTracing.NOOP_CLEANUP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OutboxQueueProcessorImplTest {
    @Mock
    OutboxProperties properties;
    @Mock
    AsyncTaskExecutor taskExecutor;
    @Mock
    OutboxHandlerInvoker invoker;
    @Mock
    OutboxRepository repository;
    @Mock
    OutboxObservability observability;
    @InjectMocks
    OutboxQueueProcessorImpl processor;

    @BeforeEach
    void setUp() {
        lenient().when(observability.restoreTracingContext(any())).thenReturn(NOOP_CLEANUP);
    }

    @Test
    void afterPropertiesSet_shouldMoveErrorToNew() {
        processor.afterPropertiesSet();

        verify(repository).moveErrorToNew();
    }

    @Test
    void enqueue_shouldRecordPublishedCaptureTracingAndSave() {
        when(invoker.toJson(any(), any())).thenReturn(TestData.PAYLOAD_JSON);
        when(observability.captureTracingContext()).thenReturn(TestData.TRACING_CONTEXT);
        when(repository.save(any(), any(), any(), any(), any())).thenReturn(testMessage());

        processor.enqueue(TestData.HANDLER_TYPE, TestData.PAYLOAD_KEY, new Object());

        verify(observability).recordPublished(TestData.HANDLER_TYPE);
        verify(observability).captureTracingContext();
        verify(repository).save(eq(TestData.HANDLER_TYPE), eq(TestData.PAYLOAD_KEY), eq(TestData.PAYLOAD_JSON), any(), eq(TestData.TRACING_CONTEXT));
    }

    @Test
    void tryProcessAsync_shouldSubmitToExecutor() {
        var message = testMessage();
        when(taskExecutor.submitCompletable(any(Runnable.class))).thenReturn(CompletableFuture.completedFuture(null));

        processor.tryProcessAsync(message);

        verify(taskExecutor).submitCompletable(any(Runnable.class));
    }

    @Test
    void tryProcessAsync_whenTaskRejected_shouldNotThrow() {
        var message = testMessage();
        when(taskExecutor.submitCompletable(any(Runnable.class))).thenThrow(new TaskRejectedException("full"));

        processor.tryProcessAsync(message);

        verify(taskExecutor).submitCompletable(any(Runnable.class));
    }

    @Test
    void tryProcessFirst_whenQueueEmpty_shouldReturnNull() {
        when(repository.fetchFirstReadyToProcess()).thenReturn(Optional.empty());

        var result = processor.tryProcessFirst(Instant.now());

        assertThat(result).isNull();
        verify(repository).moveExpiredToNew(any());
    }

    @Test
    void tryProcessFirst_shouldReturnMessageId() {
        var message = testMessage();
        when(repository.fetchFirstReadyToProcess()).thenReturn(Optional.of(message));
        when(repository.tryMoveToInProgress(any(), anyLong(), any())).thenReturn(true);
        when(properties.expiredAtFor(any(), any(), anyInt())).thenReturn(Instant.now().plusSeconds(30));

        var result = processor.tryProcessFirst(Instant.now());

        assertThat(result).isEqualTo(message.id());
    }

    @Test
    void tryProcessFirst_shouldCleanupTracingContextEvenOnError() throws Exception {
        var message = testMessage();
        var cleanup = mock(Runnable.class);
        when(repository.fetchFirstReadyToProcess()).thenReturn(Optional.of(message));
        when(observability.restoreTracingContext(any())).thenReturn(cleanup);
        when(repository.tryMoveToInProgress(any(), anyLong(), any())).thenReturn(true);
        when(properties.expiredAtFor(any(), any(), anyInt())).thenReturn(Instant.now().plusSeconds(30));
        doThrow(new RuntimeException("boom")).when(invoker).invoke(any(), any(), any());

        processor.tryProcessFirst(Instant.now());

        verify(cleanup).run();
    }

    @Test
    void tryProcessNext_whenQueueEmpty_shouldReturnNull() {
        when(repository.fetchNextReadyToProcess(TestData.MESSAGE_ID)).thenReturn(Optional.empty());

        var result = processor.tryProcessNext(TestData.MESSAGE_ID);

        assertThat(result).isNull();
    }

    @Test
    void tryProcessNext_shouldReturnNextMessageId() {
        var message = testMessage(TestData.NEXT_MESSAGE_ID);
        when(repository.fetchNextReadyToProcess(TestData.MESSAGE_ID)).thenReturn(Optional.of(message));
        when(repository.tryMoveToInProgress(any(), anyLong(), any())).thenReturn(true);
        when(properties.expiredAtFor(any(), any(), anyInt())).thenReturn(Instant.now().plusSeconds(30));

        var result = processor.tryProcessNext(TestData.MESSAGE_ID);

        assertThat(result).isEqualTo(TestData.NEXT_MESSAGE_ID);
    }

    @Test
    void process_onSuccess_shouldMoveToDoneAndRecordProcessed() throws Exception {
        var message = testMessage();
        when(repository.fetchFirstReadyToProcess()).thenReturn(Optional.of(message));
        when(repository.tryMoveToInProgress(any(), anyLong(), any())).thenReturn(true);
        when(properties.expiredAtFor(any(), any(), anyInt())).thenReturn(Instant.now().plusSeconds(30));

        processor.tryProcessFirst(Instant.now());

        verify(invoker).invoke(TestData.HANDLER_TYPE, TestData.PAYLOAD_KEY, TestData.PAYLOAD_JSON);
        verify(repository).moveToDone(eq(message.id()), any());
        verify(observability).recordProcessed(TestData.HANDLER_TYPE);
    }

    @Test
    void process_whenOptimisticLockFails_shouldSkipMessage() throws Exception {
        var message = testMessage();
        when(repository.fetchFirstReadyToProcess()).thenReturn(Optional.of(message));
        when(repository.tryMoveToInProgress(any(), anyLong(), any())).thenReturn(false);
        when(properties.expiredAtFor(any(), any(), anyInt())).thenReturn(Instant.now().plusSeconds(30));

        processor.tryProcessFirst(Instant.now());

        verify(invoker, never()).invoke(any(), any(), any());
        verify(repository, never()).moveToDone(any(), any());
    }

    @Test
    void process_whenTemporaryFailureWithRemainingRetries_shouldNotMoveToError() throws Exception {
        var message = testMessage();
        when(repository.fetchFirstReadyToProcess()).thenReturn(Optional.of(message));
        when(repository.tryMoveToInProgress(any(), anyLong(), any())).thenReturn(true);
        when(properties.expiredAtFor(any(), any(), anyInt())).thenReturn(Instant.now().plusSeconds(30));
        when(properties.maxAttemptsFor(TestData.HANDLER_TYPE)).thenReturn(TestData.MAX_RETRIES);
        doThrow(new TemporaryFailureException("timeout")).when(invoker).invoke(any(), any(), any());

        processor.tryProcessFirst(Instant.now());

        verify(repository, never()).moveToError(any(), anyLong());
        verify(observability, never()).recordError(any());
    }

    @Test
    void process_whenTemporaryFailureExhausted_shouldMoveToError() throws Exception {
        var message = testMessage(TestData.MESSAGE_ID, TestData.LAST_RETRY_COUNT);
        when(repository.fetchFirstReadyToProcess()).thenReturn(Optional.of(message));
        when(repository.tryMoveToInProgress(any(), anyLong(), any())).thenReturn(true);
        when(properties.expiredAtFor(any(), any(), anyInt())).thenReturn(Instant.now().plusSeconds(30));
        when(properties.maxAttemptsFor(TestData.HANDLER_TYPE)).thenReturn(TestData.MAX_RETRIES);
        when(repository.moveToError(any(), anyLong())).thenReturn(true);
        doThrow(new TemporaryFailureException("timeout")).when(invoker).invoke(any(), any(), any());

        processor.tryProcessFirst(Instant.now());

        verify(repository).moveToError(eq(message.id()), eq(message.version() + 1));
        verify(observability).recordError(TestData.HANDLER_TYPE);
    }

    @Test
    void process_whenUnexpectedException_shouldMoveToError() throws Exception {
        var message = testMessage();
        when(repository.fetchFirstReadyToProcess()).thenReturn(Optional.of(message));
        when(repository.tryMoveToInProgress(any(), anyLong(), any())).thenReturn(true);
        when(properties.expiredAtFor(any(), any(), anyInt())).thenReturn(Instant.now().plusSeconds(30));
        when(repository.moveToError(any(), anyLong())).thenReturn(true);
        doThrow(new RuntimeException("unexpected")).when(invoker).invoke(any(), any(), any());

        processor.tryProcessFirst(Instant.now());

        verify(repository).moveToError(eq(message.id()), eq(message.version() + 1));
        verify(observability).recordError(TestData.HANDLER_TYPE);
    }

    @Test
    void process_whenInterruptedException_shouldRestoreInterruptFlag() throws Exception {
        var message = testMessage();
        when(repository.fetchFirstReadyToProcess()).thenReturn(Optional.of(message));
        when(repository.tryMoveToInProgress(any(), anyLong(), any())).thenReturn(true);
        when(properties.expiredAtFor(any(), any(), anyInt())).thenReturn(Instant.now().plusSeconds(30));
        when(repository.moveToError(any(), anyLong())).thenReturn(true);
        doThrow(new InterruptedException()).when(invoker).invoke(any(), any(), any());

        processor.tryProcessFirst(Instant.now());

        assertThat(Thread.interrupted()).isTrue();
    }

    static class TestData {
        static final long MESSAGE_ID = 1L;
        static final long NEXT_MESSAGE_ID = 2L;
        static final String HANDLER_TYPE = "order.created";
        static final String PAYLOAD_KEY = "order-1";
        static final String PAYLOAD_JSON = "{}";
        static final long VERSION = 1L;
        static final String TRACING_CONTEXT = "{}";
        static final int MAX_RETRIES = 3;
        static final int LAST_RETRY_COUNT = 2;

        public static OutboxMessage testMessage() {
            return testMessage(TestData.MESSAGE_ID, 0);
        }

        public static OutboxMessage testMessage(Long id) {
            return testMessage(id, 0);
        }

        public static OutboxMessage testMessage(Long id, int retryCount) {
            return OutboxMessage.builder()
                    .id(id)
                    .handlerType(TestData.HANDLER_TYPE)
                    .payloadKey(TestData.PAYLOAD_KEY)
                    .payload(TestData.PAYLOAD_JSON)
                    .status(OutboxStatus.NEW)
                    .version(TestData.VERSION)
                    .failedAttempts(retryCount)
                    .createdAt(Instant.now())
                    .build();
        }
    }
}
