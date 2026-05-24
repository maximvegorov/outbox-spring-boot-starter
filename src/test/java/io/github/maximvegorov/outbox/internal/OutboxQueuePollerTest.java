package io.github.maximvegorov.outbox.internal;

import lombok.experimental.UtilityClass;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OutboxQueuePollerTest {
    @Mock
    OutboxQueueProcessor processor;
    @InjectMocks
    OutboxQueuePoller poller;

    @Test
    void poll_whenQueueEmpty_shouldCallTryProcessFirstOnce() {
        when(processor.tryProcessFirst(any())).thenReturn(null);

        poller.poll();

        verify(processor).tryProcessFirst(any(Instant.class));
        verify(processor, never()).tryProcessNext(any());
    }

    @Test
    void poll_whenOneMessage_shouldProcessItAndStop() {
        when(processor.tryProcessFirst(any())).thenReturn(TestData.FIRST_ID);
        when(processor.tryProcessNext(TestData.FIRST_ID)).thenReturn(null);

        poller.poll();

        verify(processor).tryProcessFirst(any());
        verify(processor).tryProcessNext(TestData.FIRST_ID);
    }

    @Test
    void poll_whenMultipleMessages_shouldProcessAllSequentially() {
        when(processor.tryProcessFirst(any())).thenReturn(TestData.FIRST_ID);
        when(processor.tryProcessNext(TestData.FIRST_ID)).thenReturn(TestData.SECOND_ID);
        when(processor.tryProcessNext(TestData.SECOND_ID)).thenReturn(TestData.THIRD_ID);
        when(processor.tryProcessNext(TestData.THIRD_ID)).thenReturn(null);

        poller.poll();

        verify(processor).tryProcessFirst(any());
        verify(processor).tryProcessNext(TestData.FIRST_ID);
        verify(processor).tryProcessNext(TestData.SECOND_ID);
        verify(processor).tryProcessNext(TestData.THIRD_ID);
    }

    @Test
    void poll_whenRuntimeExceptionThrown_shouldCatchAndNotPropagate() {
        when(processor.tryProcessFirst(any())).thenThrow(new RuntimeException("some error"));

        poller.poll();

        verify(processor).tryProcessFirst(any());
    }

    @Test
    void poll_whenThreadInterrupted_shouldStopLoop() {
        when(processor.tryProcessFirst(any())).thenReturn(TestData.FIRST_ID);
        when(processor.tryProcessNext(TestData.FIRST_ID)).thenAnswer(inv -> {
            Thread.currentThread().interrupt();
            return TestData.SECOND_ID;
        });

        poller.poll();

        verify(processor).tryProcessNext(TestData.FIRST_ID);
        verify(processor, never()).tryProcessNext(TestData.SECOND_ID);

        Thread.interrupted(); // clear interrupt flag
    }

    @UtilityClass
    static class TestData {
        static final Long FIRST_ID = 1L;
        static final Long SECOND_ID = 2L;
        static final Long THIRD_ID = 3L;
    }
}
