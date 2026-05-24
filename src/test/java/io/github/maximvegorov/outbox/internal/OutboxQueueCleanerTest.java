package io.github.maximvegorov.outbox.internal;

import io.github.maximvegorov.outbox.OutboxProperties;
import lombok.experimental.UtilityClass;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OutboxQueueCleanerTest {
    @Mock
    OutboxProperties properties;
    @Mock
    OutboxRepository repository;
    @InjectMocks
    OutboxQueueCleaner cleaner;

    @BeforeEach
    void setUp() {
        when(properties.getCleaner()).thenReturn(TestData.config());
    }

    @Test
    void clean_whenNothingToDelete_shouldCallRepositoryOnce() {
        when(repository.deleteDoneBefore(any(), anyInt())).thenReturn(0);

        cleaner.clean();

        verify(repository).deleteDoneBefore(any(), eq(TestData.BATCH_SIZE));
    }

    @Test
    void clean_whenPartialBatch_shouldCallRepositoryOnceAndStop() {
        when(repository.deleteDoneBefore(any(), anyInt())).thenReturn(TestData.BATCH_SIZE - 1);

        cleaner.clean();

        verify(repository, times(1)).deleteDoneBefore(any(), anyInt());
    }

    @Test
    void clean_whenFullBatchFollowedByPartial_shouldRepeatUntilDone() {
        when(repository.deleteDoneBefore(any(), anyInt()))
                .thenReturn(TestData.BATCH_SIZE)
                .thenReturn(TestData.BATCH_SIZE)
                .thenReturn(1);

        cleaner.clean();

        verify(repository, times(3)).deleteDoneBefore(any(), anyInt());
    }

    @Test
    void clean_whenExactlyFullBatchDeleted_shouldCallRepositoryAgain() {
        when(repository.deleteDoneBefore(any(), anyInt()))
                .thenReturn(TestData.BATCH_SIZE)
                .thenReturn(0);

        cleaner.clean();

        verify(repository, times(2)).deleteDoneBefore(any(), anyInt());
    }

    @Test
    void clean_whenThreadInterrupted_shouldStopAfterCurrentBatch() {
        when(repository.deleteDoneBefore(any(), anyInt())).thenAnswer(inv -> {
            Thread.currentThread().interrupt();
            return TestData.BATCH_SIZE;
        });

        cleaner.clean();

        verify(repository, times(1)).deleteDoneBefore(any(), anyInt());
        Thread.interrupted(); // сбрасываем флаг после теста
    }

    @Test
    void clean_whenRuntimeExceptionThrown_shouldCatchAndNotPropagate() {
        when(repository.deleteDoneBefore(any(), anyInt())).thenThrow(new RuntimeException("db error"));

        cleaner.clean();

        verify(repository).deleteDoneBefore(any(), anyInt());
    }

    @UtilityClass
    static class TestData {
        static final int BATCH_SIZE = 10;

        static OutboxProperties.CleanerConfig config() {
            var config = new OutboxProperties.CleanerConfig();
            config.setBatchSize(BATCH_SIZE);
            return config;
        }
    }
}
