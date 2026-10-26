package com.xjtlu.bio.service;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Non-blocking, single-instance coordination of input changes and pipeline start.
 * Existing single-file uploads may share a permit; batch upload and start
 * require exclusive access. Entries exist only while a permit is held.
 */
public final class PipelineInputOperationCoordinator {

    public enum Operation {
        SINGLE_UPLOAD,
        BATCH_UPLOAD,
        START
    }

    public interface Permit extends AutoCloseable {
        @Override
        void close();
    }

    /** A rejected acquisition has no permit; blockedBy identifies its holder. */
    public record Acquisition(boolean acquired, Permit permit, Operation blockedBy) {
    }

    private static final class Entry {
        private long readers;
        private Operation exclusive;
    }

    private final ConcurrentHashMap<Long, Entry> entries = new ConcurrentHashMap<>();

    public Acquisition tryAcquire(long pipelineId, Operation operation) {
        Objects.requireNonNull(operation, "operation");
        AtomicReference<Acquisition> result = new AtomicReference<>();
        entries.compute(pipelineId, (id, current) -> {
            Entry entry = current == null ? new Entry() : current;
            Operation blocker = entry.exclusive;
            if (blocker == null && operation != Operation.SINGLE_UPLOAD && entry.readers != 0) {
                blocker = Operation.SINGLE_UPLOAD;
            }
            if (blocker != null) {
                result.set(new Acquisition(false, null, blocker));
                return entry;
            }

            if (operation == Operation.SINGLE_UPLOAD) {
                entry.readers++;
            } else {
                entry.exclusive = operation;
            }
            result.set(new Acquisition(true, new HeldPermit(pipelineId, entry, operation), null));
            return entry;
        });
        return result.get();
    }

    // Package-private for lifecycle regression tests; not a cache of past IDs.
    int activePipelineCount() {
        return entries.size();
    }

    private final class HeldPermit implements Permit {
        private final long pipelineId;
        private final Entry entry;
        private final Operation operation;
        private final AtomicBoolean closed = new AtomicBoolean();

        private HeldPermit(long pipelineId, Entry entry, Operation operation) {
            this.pipelineId = pipelineId;
            this.entry = entry;
            this.operation = operation;
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            entries.compute(pipelineId, (id, current) -> {
                if (current != entry) {
                    throw new IllegalStateException("Pipeline input permit does not own the active entry");
                }
                if (operation == Operation.SINGLE_UPLOAD) {
                    if (entry.readers <= 0) {
                        throw new IllegalStateException("Pipeline input shared permit already released");
                    }
                    entry.readers--;
                } else {
                    if (entry.exclusive != operation) {
                        throw new IllegalStateException("Pipeline input exclusive permit already released");
                    }
                    entry.exclusive = null;
                }
                // Admission and removal use the same map computation. A new
                // caller can never retain an unregistered, obsolete entry.
                return entry.readers == 0 && entry.exclusive == null ? null : entry;
            });
        }
    }
}
