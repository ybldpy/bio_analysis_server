package com.xjtlu.bio.analysisPipeline.taskrunner;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

/** Tests only harmless operating-system processes; no bioinformatics programs are invoked. */
@EnabledOnOs(OS.LINUX)
@Timeout(10)
class CovidWastewaterProcessTest {

    @TempDir
    Path tempDir;

    @Test
    void successfulProcessAppendsBothOutputStreamsToTheExecutionLog() throws Exception {
        Path log = tempDir.resolve("execution.log");
        Files.writeString(log, "existing log\n");

        new ProcessExecutor().run(List.of("/bin/sh", "-c",
                "printf 'stdout-marker\\n'; printf 'stderr-marker\\n' >&2"), tempDir, log,
                deadline(Duration.ofSeconds(3)));

        String content = Files.readString(log);
        assertTrue(content.startsWith("existing log\n"));
        assertTrue(content.contains("stdout-marker"));
        assertTrue(content.contains("stderr-marker"));
    }

    @Test
    void nonzeroExitIncludesTheExitCode() {
        IOException error = assertThrows(IOException.class, () -> new ProcessExecutor().run(
                List.of("/bin/sh", "-c", "exit 7"), tempDir, tempDir.resolve("execution.log"),
                deadline(Duration.ofSeconds(3))));

        assertTrue(error.getMessage().contains("code 7"));
    }

    @Test
    void expiredDeadlineDoesNotAttemptToStartTheCommand() {
        IOException error = assertThrows(IOException.class, () -> new ProcessExecutor().run(
                List.of("/intentionally-nonexistent-fixture-program"), tempDir, tempDir.resolve("execution.log"),
                System.nanoTime() - 1L));

        assertTrue(error.getMessage().contains("timed out"));
        assertFalse(Files.exists(tempDir.resolve("execution.log")));
    }

    @Test
    void timeoutTerminatesTheOwnedProcess() throws Exception {
        Path log = tempDir.resolve("timeout.log");
        IOException error = assertThrows(IOException.class, () -> new ProcessExecutor().run(
                sleepingProcess(), tempDir, log, deadline(Duration.ofMillis(350))));

        assertTrue(error.getMessage().contains("timed out"));
        long pid = processId(log);
        try { awaitStopped(pid); }
        finally { terminateIfAlive(pid); }
    }

    @Test
    void interruptionPropagatesAndTerminatesTheOwnedProcess() throws Exception {
        Path log = tempDir.resolve("interrupt.log");
        AtomicReference<Throwable> outcome = new AtomicReference<>();
        Thread worker = new Thread(() -> {
            try {
                new ProcessExecutor().run(sleepingProcess(), tempDir, log, deadline(Duration.ofSeconds(5)));
            } catch (Throwable e) { outcome.set(e); }
        }, "wastewater-process-fixture");
        worker.start();
        long pid = -1L;
        try {
            long readyDeadline = deadline(Duration.ofSeconds(3));
            while ((!Files.exists(log) || Files.size(log) == 0L) && System.nanoTime() < readyDeadline) {
                Thread.sleep(10L);
            }
            pid = processId(log);
            worker.interrupt();
            worker.join(2000L);
            assertFalse(worker.isAlive());
            assertInstanceOf(InterruptedException.class, outcome.get());
            awaitStopped(pid);
        } finally {
            worker.interrupt();
            worker.join(2000L);
            if (pid > 0) { terminateIfAlive(pid); }
        }
    }

    private static List<String> sleepingProcess() {
        // Print only this fixture process's PID, then replace it with a harmless sleeper.
        return List.of("/bin/sh", "-c", "echo $$; exec /bin/sleep 30");
    }

    private static long processId(Path log) throws IOException {
        return Long.parseLong(Files.readString(log).strip().split("\\s+", 2)[0]);
    }

    private static void awaitStopped(long pid) throws InterruptedException {
        long until = deadline(Duration.ofSeconds(2));
        while (ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false) && System.nanoTime() < until) {
            Thread.sleep(10L);
        }
        assertFalse(ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false), "Owned fixture process still alive");
    }

    private static void terminateIfAlive(long pid) {
        ProcessHandle.of(pid).filter(ProcessHandle::isAlive).ifPresent(ProcessHandle::destroyForcibly);
    }

    private static long deadline(Duration duration) {
        return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(duration.toMillis());
    }

    private static final class ProcessExecutor extends CovidWastewaterStageExecutor {
        private void run(List<String> command, Path workDir, Path log, long deadline)
                throws IOException, InterruptedException {
            runProcess(command, workDir, log, deadline);
        }
    }
}
