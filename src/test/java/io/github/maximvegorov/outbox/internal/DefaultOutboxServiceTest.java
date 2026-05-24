package io.github.maximvegorov.outbox.internal;

import io.github.maximvegorov.outbox.OutboxStatus;
import lombok.experimental.UtilityClass;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Instant;

import static io.github.maximvegorov.outbox.internal.DefaultOutboxServiceTest.TestData.testMessage;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DefaultOutboxServiceTest {
    @Mock
    OutboxQueueProcessor queueProcessor;
    @InjectMocks
    DefaultOutboxService service;

    @AfterEach
    void tearDown() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    @Test
    void publish_whenNoActiveTransaction_shouldProcessAsyncImmediately() {
        var message = testMessage();
        when(queueProcessor.enqueue(TestData.HANDLER_TYPE, TestData.PAYLOAD_KEY, TestData.PAYLOAD)).thenReturn(message);

        service.publish(TestData.HANDLER_TYPE, TestData.PAYLOAD_KEY, TestData.PAYLOAD);

        verify(queueProcessor).enqueue(TestData.HANDLER_TYPE, TestData.PAYLOAD_KEY, TestData.PAYLOAD);
        verify(queueProcessor).tryProcessAsync(message);
    }

    @Test
    void publish_whenActiveTransaction_shouldNotProcessBeforeCommit() {
        TransactionSynchronizationManager.initSynchronization();
        var message = testMessage();
        when(queueProcessor.enqueue(any(), any(), any())).thenReturn(message);

        service.publish(TestData.HANDLER_TYPE, TestData.PAYLOAD_KEY, TestData.PAYLOAD);

        verify(queueProcessor).enqueue(TestData.HANDLER_TYPE, TestData.PAYLOAD_KEY, TestData.PAYLOAD);
        verify(queueProcessor, never()).tryProcessAsync(any());
    }

    @Test
    void publish_whenActiveTransaction_shouldProcessAsyncAfterCommit() {
        TransactionSynchronizationManager.initSynchronization();
        var message = testMessage();
        when(queueProcessor.enqueue(any(), any(), any())).thenReturn(message);

        service.publish(TestData.HANDLER_TYPE, TestData.PAYLOAD_KEY, TestData.PAYLOAD);

        var syncCaptor = ArgumentCaptor.forClass(TransactionSynchronization.class);
        verify(queueProcessor, never()).tryProcessAsync(any());

        syncCaptor.getAllValues(); // just to confirm no premature calls
        TransactionSynchronizationManager.getSynchronizations()
                .forEach(TransactionSynchronization::afterCommit);

        verify(queueProcessor).tryProcessAsync(message);
    }

    @UtilityClass
    static class TestData {
        static final String HANDLER_TYPE = "order.created";
        static final String PAYLOAD_KEY = "order-1";
        static final Object PAYLOAD = "payload";
        static final String PAYLOAD_JSON = "{}";
        static final long MESSAGE_ID = 1L;
        static final long VERSION = 1L;

        public static OutboxMessage testMessage() {
            return OutboxMessage.builder()
                    .id(TestData.MESSAGE_ID)
                    .handlerType(TestData.HANDLER_TYPE)
                    .payloadKey(TestData.PAYLOAD_KEY)
                    .payload(TestData.PAYLOAD_JSON)
                    .status(OutboxStatus.NEW)
                    .version(TestData.VERSION)
                    .retryCount(0)
                    .createdAt(Instant.now())
                    .build();
        }
    }
}
