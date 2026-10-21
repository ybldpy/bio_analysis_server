package com.xjtlu.bio.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.test.util.ReflectionTestUtils;

import com.xjtlu.bio.entity.BioPipelineStage;

class PipelineStageTaskDispatcherTest {

    @Test
    void repeatedSubmissionOfSameStageIsIdempotent() {
        PipelineStageTaskDispatcher dispatcher = new PipelineStageTaskDispatcher();
        BlockingQueue<BioPipelineStage> queue = new LinkedBlockingQueue<>(1);
        ReflectionTestUtils.setField(dispatcher, "stageBuffer", queue);
        BioPipelineStage original = stage(1L);

        assertTrue(dispatcher.addTask(original));
        assertTrue(dispatcher.addTask(stage(1L)));

        assertTrue(dispatcher.isStageIn(1L));
        assertEquals(1, queue.size());
        assertSame(original, queue.peek());
    }

    @Test
    void fullQueueClearsReservationAndAllowsRetryAfterCapacityIsAvailable() {
        PipelineStageTaskDispatcher dispatcher = new PipelineStageTaskDispatcher();
        BlockingQueue<BioPipelineStage> queue = new LinkedBlockingQueue<>(1);
        ReflectionTestUtils.setField(dispatcher, "stageBuffer", queue);
        BioPipelineStage queued = stage(1L);
        BioPipelineStage retry = stage(2L);

        assertTrue(dispatcher.addTask(queued));
        assertFalse(dispatcher.addTask(retry));
        assertFalse(dispatcher.isStageIn(2L));
        assertSame(queued, queue.poll());

        assertTrue(dispatcher.addTask(retry));
        assertTrue(dispatcher.isStageIn(2L));
        assertEquals(1, queue.size());
        assertSame(retry, queue.peek());
    }

    @Test
    @Timeout(10)
    void simultaneousSameStageSubmissionWaitsForFailedOfferThenActuallyRetries() throws Exception {
        PipelineStageTaskDispatcher dispatcher = new PipelineStageTaskDispatcher();
        FailFirstOfferQueue queue = new FailFirstOfferQueue();
        ReflectionTestUtils.setField(dispatcher, "stageBuffer", queue);
        BioPipelineStage original = stage(1L);
        BioPipelineStage retry = stage(1L);
        FutureTask<Boolean> first = new FutureTask<>(() -> dispatcher.addTask(original));
        FutureTask<Boolean> second = new FutureTask<>(() -> dispatcher.addTask(retry));
        Thread firstSubmitter = new Thread(first, "first-stage-submitter");
        Thread secondSubmitter = new Thread(second, "second-stage-submitter");

        try {
            firstSubmitter.start();
            assertTrue(queue.firstOfferStarted.await(2, TimeUnit.SECONDS));
            secondSubmitter.start();

            // The first task is only provisionally reserved and will fail its offer.
            // The second request must not treat that reservation as a successful enqueue.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (secondSubmitter.getState() != Thread.State.BLOCKED
                    && !second.isDone() && System.nanoTime() < deadline) {
                secondSubmitter.join(10);
            }
            assertEquals(Thread.State.BLOCKED, secondSubmitter.getState());
            assertFalse(second.isDone());
            assertTrue(queue.isEmpty());
            assertEquals(1, queue.offerAttempts.get());

            queue.allowFirstOfferFailure.countDown();

            assertFalse(first.get(2, TimeUnit.SECONDS));
            assertTrue(second.get(2, TimeUnit.SECONDS));
            assertEquals(2, queue.offerAttempts.get());
            assertTrue(dispatcher.isStageIn(1L));
            assertEquals(1, queue.size());
            assertSame(retry, queue.peek());
        } finally {
            queue.allowFirstOfferFailure.countDown();
            firstSubmitter.interrupt();
            secondSubmitter.interrupt();
            firstSubmitter.join(2000);
            secondSubmitter.join(2000);
        }
    }

    private static BioPipelineStage stage(long stageId) {
        BioPipelineStage stage = new BioPipelineStage();
        stage.setStageId(stageId);
        return stage;
    }

    private static final class FailFirstOfferQueue extends LinkedBlockingQueue<BioPipelineStage> {

        private static final long serialVersionUID = 1L;
        private final CountDownLatch firstOfferStarted = new CountDownLatch(1);
        private final CountDownLatch allowFirstOfferFailure = new CountDownLatch(1);
        private final AtomicInteger offerAttempts = new AtomicInteger();

        @Override
        public boolean offer(BioPipelineStage stage) {
            if (offerAttempts.incrementAndGet() == 1) {
                firstOfferStarted.countDown();
                try {
                    if (!allowFirstOfferFailure.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("First offer failure was not released");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("First offer was interrupted", e);
                }
                return false;
            }
            return super.offer(stage);
        }
    }
}
