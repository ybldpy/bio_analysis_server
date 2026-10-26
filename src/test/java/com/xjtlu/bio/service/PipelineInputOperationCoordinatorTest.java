package com.xjtlu.bio.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.xjtlu.bio.service.PipelineInputOperationCoordinator.Acquisition;
import com.xjtlu.bio.service.PipelineInputOperationCoordinator.Operation;

class PipelineInputOperationCoordinatorTest {

    private final PipelineInputOperationCoordinator coordinator = new PipelineInputOperationCoordinator();

    @Test
    void singleUploadsShareOneEntryAndLastCloseReclaimsIt() {
        Acquisition first = coordinator.tryAcquire(1L, Operation.SINGLE_UPLOAD);
        Acquisition second = coordinator.tryAcquire(1L, Operation.SINGLE_UPLOAD);
        assertTrue(first.acquired());
        assertTrue(second.acquired());
        assertNull(first.blockedBy());
        assertEquals(1, coordinator.activePipelineCount());

        first.permit().close();
        assertEquals(1, coordinator.activePipelineCount());
        assertBlocked(1L, Operation.START, Operation.SINGLE_UPLOAD);
        assertBlocked(1L, Operation.BATCH_UPLOAD, Operation.SINGLE_UPLOAD);
        second.permit().close();
        assertEquals(0, coordinator.activePipelineCount());
        assertCanAcquireAndClose(1L, Operation.BATCH_UPLOAD);
    }

    @ParameterizedTest
    @EnumSource(value = Operation.class, names = { "BATCH_UPLOAD", "START" })
    void exclusiveOperationsRejectEveryOtherOperationWithoutChangingTheirHolder(Operation holder) {
        Acquisition held = coordinator.tryAcquire(1L, holder);
        assertTrue(held.acquired());
        for (Operation attempt : Operation.values()) {
            assertBlocked(1L, attempt, holder);
        }
        assertEquals(1, coordinator.activePipelineCount());
        held.permit().close();
        assertEquals(0, coordinator.activePipelineCount());
        for (Operation attempt : Operation.values()) {
            assertCanAcquireAndClose(1L, attempt);
        }
    }

    @Test
    void operationsOnDifferentPipelinesDoNotBlockOneAnother() {
        try (var first = coordinator.tryAcquire(1L, Operation.BATCH_UPLOAD).permit();
                var second = coordinator.tryAcquire(2L, Operation.START).permit();
                var third = coordinator.tryAcquire(3L, Operation.SINGLE_UPLOAD).permit()) {
            assertEquals(3, coordinator.activePipelineCount());
        }
        assertEquals(0, coordinator.activePipelineCount());
    }

    @Test
    void repeatedCloseCannotRemoveANewEntryForTheSamePipeline() {
        var oldPermit = coordinator.tryAcquire(1L, Operation.START).permit();
        oldPermit.close();
        var newPermit = coordinator.tryAcquire(1L, Operation.BATCH_UPLOAD).permit();
        oldPermit.close();
        assertBlocked(1L, Operation.START, Operation.BATCH_UPLOAD);
        assertEquals(1, coordinator.activePipelineCount());
        newPermit.close();
        assertEquals(0, coordinator.activePipelineCount());
    }

    @Test
    void rejectedAcquisitionHasNoPermitAndDoesNotReleaseTheHolder() {
        try (var held = coordinator.tryAcquire(1L, Operation.START).permit()) {
            Acquisition rejected = coordinator.tryAcquire(1L, Operation.SINGLE_UPLOAD);
            assertFalse(rejected.acquired());
            assertNull(rejected.permit());
            assertEquals(Operation.START, rejected.blockedBy());
            assertBlocked(1L, Operation.BATCH_UPLOAD, Operation.START);
        }
        assertEquals(0, coordinator.activePipelineCount());
    }

    @Test
    void failedWorkStillReleasesItsPermit() {
        assertThrows(IllegalStateException.class, () -> {
            try (var held = coordinator.tryAcquire(1L, Operation.BATCH_UPLOAD).permit()) {
                throw new IllegalStateException("test work failure");
            }
        });
        assertCanAcquireAndClose(1L, Operation.START);
        assertEquals(0, coordinator.activePipelineCount());
    }

    @Test
    void completedPipelineIdsDoNotAccumulateInRegistry() {
        for (long pipelineId = 1; pipelineId <= 5_000; pipelineId++) {
            assertCanAcquireAndClose(pipelineId, Operation.SINGLE_UPLOAD);
        }
        assertEquals(0, coordinator.activePipelineCount());
    }

    @Test
    void nullOperationIsRejectedWithoutCreatingAnEntry() {
        assertThrows(NullPointerException.class, () -> coordinator.tryAcquire(1L, null));
        assertEquals(0, coordinator.activePipelineCount());
    }

    @Test
    @Timeout(15)
    void concurrentAdmissionAndEntryReclamationNeverSplitOwnership() throws Exception {
        ExecutorService workers = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger readers = new AtomicInteger();
        AtomicBoolean writer = new AtomicBoolean();
        AtomicInteger completed = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int worker = 0; worker < 8; worker++) {
                final int workerIndex = worker;
                futures.add(workers.submit(() -> {
                    start.await();
                    for (int iteration = 0; iteration < 1_000; iteration++) {
                        Operation operation = Operation.values()[(iteration + workerIndex) % 3];
                        Acquisition acquisition = coordinator.tryAcquire(1L, operation);
                        if (!acquisition.acquired()) {
                            continue;
                        }
                        try (var held = acquisition.permit()) {
                            if (operation == Operation.SINGLE_UPLOAD) {
                                readers.incrementAndGet();
                                try {
                                    assertFalse(writer.get());
                                    Thread.yield();
                                    assertFalse(writer.get());
                                } finally {
                                    readers.decrementAndGet();
                                }
                            } else {
                                assertTrue(writer.compareAndSet(false, true));
                                try {
                                    assertEquals(0, readers.get());
                                    Thread.yield();
                                    assertEquals(0, readers.get());
                                } finally {
                                    writer.set(false);
                                }
                            }
                            completed.incrementAndGet();
                        }
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }
        } finally {
            start.countDown();
            workers.shutdownNow();
            assertTrue(workers.awaitTermination(3, TimeUnit.SECONDS));
        }
        assertTrue(completed.get() > 0);
        assertEquals(0, readers.get());
        assertFalse(writer.get());
        assertEquals(0, coordinator.activePipelineCount());
    }

    private void assertBlocked(long pipelineId, Operation attempt, Operation expectedHolder) {
        Acquisition acquisition = coordinator.tryAcquire(pipelineId, attempt);
        assertFalse(acquisition.acquired());
        assertNull(acquisition.permit());
        assertEquals(expectedHolder, acquisition.blockedBy());
    }

    private void assertCanAcquireAndClose(long pipelineId, Operation operation) {
        Acquisition acquisition = coordinator.tryAcquire(pipelineId, operation);
        assertTrue(acquisition.acquired());
        acquisition.permit().close();
    }
}
