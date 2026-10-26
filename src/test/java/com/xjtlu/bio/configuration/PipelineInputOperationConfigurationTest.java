package com.xjtlu.bio.configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.aop.config.AopConfigUtils;
import org.springframework.aop.framework.autoproxy.InfrastructureAdvisorAutoProxyCreator;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.AnnotationConfigUtils;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.MapPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.SimpleTransactionStatus;

import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.service.PipelineInputOperationCoordinator;
import com.xjtlu.bio.service.PipelineInputOperationCoordinator.Acquisition;
import com.xjtlu.bio.service.PipelineInputOperationCoordinator.Operation;
import com.xjtlu.bio.service.PipelineInputService;
import com.xjtlu.bio.service.PipelineService;

/** Real Spring registration/auto-proxy tests, without a database or Mockito. */
class PipelineInputOperationConfigurationTest {

    private AnnotationConfigApplicationContext context;
    private PipelineInputOperationCoordinator coordinator;
    private RecordingInputService inputTarget;
    private RecordingPipelineService pipelineTarget;
    private PipelineInputService inputs;
    private PipelineService pipelines;
    private RecordingTransactionManager transactions;

    @BeforeEach
    void openSpringContext() {
        inputTarget = new RecordingInputService();
        pipelineTarget = new RecordingPipelineService();
        transactions = new RecordingTransactionManager();
        context = new AnnotationConfigApplicationContext();
        context.getEnvironment().getPropertySources().addFirst(new MapPropertySource("test", Map.of(
                "analysis-pipeline.covid2RefSeq.accession", "TEST_ONLY",
                "analysis-pipeline.covid19-gff3", "TEST_ONLY")));
        context.register(PipelineInputOperationConfiguration.class, TransactionConfiguration.class);
        // These fixtures override the entry points and deliberately have no
        // legacy persistence dependencies. Keep real Spring AOP/transactions,
        // but do not inject unused @Resource fields inherited from services.
        if (context.containsBeanDefinition(AnnotationConfigUtils.COMMON_ANNOTATION_PROCESSOR_BEAN_NAME)) {
            context.removeBeanDefinition(AnnotationConfigUtils.COMMON_ANNOTATION_PROCESSOR_BEAN_NAME);
        }
        context.registerBean("pipelineInputService", PipelineInputService.class, () -> inputTarget);
        context.registerBean("pipelineService", PipelineService.class, () -> pipelineTarget);
        context.registerBean("transactionManager", RecordingTransactionManager.class, () -> transactions);
        context.refresh();
        coordinator = context.getBean(PipelineInputOperationCoordinator.class);
        inputs = context.getBean(PipelineInputService.class);
        pipelines = context.getBean(PipelineService.class);
    }

    @AfterEach
    void closeSpringContext() {
        if (context != null) {
            context.close();
        }
    }

    @Test
    void infrastructureTransactionAutoProxyCreatorRecognizesTheCoordinatorAdvisor() {
        assertTrue(AopUtils.isAopProxy(inputs));
        assertTrue(AopUtils.isAopProxy(pipelines));
        assertTrue(context.getBean(AopConfigUtils.AUTO_PROXY_CREATOR_BEAN_NAME)
                instanceof InfrastructureAdvisorAutoProxyCreator);
        assertEquals(BeanDefinition.ROLE_INFRASTRUCTURE,
                context.getBeanFactory().getBeanDefinition("pipelineInputOperationAdvisor").getRole());
    }

    @Test
    void startHeldRejectsOldUploadWithItsOriginal201ContractAndClosesStream() {
        TrackedInputStream stream = new TrackedInputStream();
        try (var held = coordinator.tryAcquire(1L, Operation.START).permit()) {
            Result<Long> result = upload(1L, stream);
            assertEquals(PipelineInputService.RESPONSE_CODE_PIPELINE_ALREADY_START, result.getStatus());
            assertEquals(-1L, result.getData());
            assertEquals(0, inputTarget.calls);
            assertEquals(0, transactions.begins);
            assertEquals(1, stream.closes);
            assertBlocked(1L, Operation.BATCH_UPLOAD, Operation.START);
        }
        assertCanAcquireAndClose(1L, Operation.BATCH_UPLOAD);
    }

    @Test
    void batchHeldRejectsOldUploadWithBusyCodeAndClosesStream() {
        TrackedInputStream stream = new TrackedInputStream();
        try (var held = coordinator.tryAcquire(1L, Operation.BATCH_UPLOAD).permit()) {
            Result<Long> result = upload(1L, stream);
            assertEquals(Result.DUPLICATE_OPERATION, result.getStatus());
            assertEquals(-1L, result.getData());
            assertEquals(0, inputTarget.calls);
            assertEquals(1, stream.closes);
            assertBlocked(1L, Operation.START, Operation.BATCH_UPLOAD);
        }
    }

    @Test
    void failureClosingRejectedStreamDoesNotChangeBusyResultOrReleaseHolder() {
        TrackedInputStream stream = new TrackedInputStream();
        stream.failClose = true;
        try (var held = coordinator.tryAcquire(1L, Operation.START).permit()) {
            Result<Long> result = upload(1L, stream);
            assertEquals(PipelineInputService.RESPONSE_CODE_PIPELINE_ALREADY_START, result.getStatus());
            assertEquals(1, stream.closes);
            assertBlocked(1L, Operation.SINGLE_UPLOAD, Operation.START);
        }
    }

    @Test
    void sharedUploadPermitKeepsExistingConcurrentUploadCapability() {
        inputTarget.duringCall = () -> {
            assertBlocked(1L, Operation.START, Operation.SINGLE_UPLOAD);
            assertBlocked(1L, Operation.BATCH_UPLOAD, Operation.SINGLE_UPLOAD);
        };
        try (var otherUpload = coordinator.tryAcquire(1L, Operation.SINGLE_UPLOAD).permit()) {
            assertEquals(Result.SUCCESS, upload(1L, new TrackedInputStream()).getStatus());
            assertEquals(1, inputTarget.calls);
            assertBlocked(1L, Operation.START, Operation.SINGLE_UPLOAD);
        }
        assertCanAcquireAndClose(1L, Operation.START);
    }

    @Test
    void successfulUploadPermitIncludesMethodFinallyAndTransactionCommit() {
        inputTarget.duringCall = () -> assertBlocked(1L, Operation.START, Operation.SINGLE_UPLOAD);
        inputTarget.duringFinally = () -> assertBlocked(1L, Operation.START, Operation.SINGLE_UPLOAD);
        transactions.onBegin = () -> assertBlocked(1L, Operation.START, Operation.SINGLE_UPLOAD);
        transactions.onCommit = () -> assertBlocked(1L, Operation.START, Operation.SINGLE_UPLOAD);

        TrackedInputStream stream = new TrackedInputStream();
        Result<Long> result = upload(1L, stream);
        assertSame(inputTarget.response, result);
        assertEquals(1, stream.closes);
        assertEquals(1, transactions.commits);
        assertCanAcquireAndClose(1L, Operation.START);
    }

    @Test
    void thrownUploadFailureReleasesOnlyAfterFinallyAndRollback() {
        RuntimeException failure = new IllegalStateException("fixture upload failure");
        inputTarget.failure = failure;
        inputTarget.duringFinally = () -> assertBlocked(1L, Operation.START, Operation.SINGLE_UPLOAD);
        transactions.onRollback = () -> assertBlocked(1L, Operation.START, Operation.SINGLE_UPLOAD);

        TrackedInputStream stream = new TrackedInputStream();
        assertSame(failure, assertThrows(IllegalStateException.class, () -> upload(1L, stream)));
        assertEquals(1, stream.closes);
        assertEquals(1, transactions.rollbacks);
        assertCanAcquireAndClose(1L, Operation.BATCH_UPLOAD);
    }

    @Test
    void singleUploadRejectsStartWithoutCallingOriginalMethod() {
        try (var held = coordinator.tryAcquire(1L, Operation.SINGLE_UPLOAD).permit()) {
            Result<?> result = pipelines.startPipeline(1L);
            assertEquals(Result.DUPLICATE_OPERATION, result.getStatus());
            assertNull(result.getData());
            assertEquals(0, pipelineTarget.calls);
            assertBlocked(1L, Operation.BATCH_UPLOAD, Operation.SINGLE_UPLOAD);
        }
    }

    @Test
    void batchRejectsStartWithoutReleasingBatchOwnership() {
        try (var held = coordinator.tryAcquire(1L, Operation.BATCH_UPLOAD).permit()) {
            assertEquals(Result.DUPLICATE_OPERATION, pipelines.startPipeline(1L).getStatus());
            assertEquals(0, pipelineTarget.calls);
            assertBlocked(1L, Operation.SINGLE_UPLOAD, Operation.BATCH_UPLOAD);
        }
    }

    @Test
    void runningStartRejectsAnotherStart() {
        try (var held = coordinator.tryAcquire(1L, Operation.START).permit()) {
            Result<?> result = pipelines.startPipeline(1L);
            assertEquals(Result.DUPLICATE_OPERATION, result.getStatus());
            assertEquals("分析任务正在启动", result.getFailMsg());
            assertEquals(0, pipelineTarget.calls);
        }
    }

    @Test
    void successfulStartIsExclusiveThroughItsFinallyAndCommit() {
        pipelineTarget.duringCall = () -> assertBlocked(1L, Operation.SINGLE_UPLOAD, Operation.START);
        pipelineTarget.duringFinally = () -> assertBlocked(1L, Operation.BATCH_UPLOAD, Operation.START);
        transactions.onCommit = () -> assertBlocked(1L, Operation.SINGLE_UPLOAD, Operation.START);

        assertSame(pipelineTarget.response, pipelines.startPipeline(1L));
        assertEquals(1, pipelineTarget.calls);
        assertCanAcquireAndClose(1L, Operation.BATCH_UPLOAD);
    }

    @Test
    void startFailureReleasesItsExclusivePermit() {
        RuntimeException failure = new IllegalStateException("fixture start failure");
        pipelineTarget.failure = failure;
        pipelineTarget.duringFinally = () -> assertBlocked(1L, Operation.SINGLE_UPLOAD, Operation.START);
        transactions.onRollback = () -> assertBlocked(1L, Operation.BATCH_UPLOAD, Operation.START);

        assertSame(failure, assertThrows(IllegalStateException.class, () -> pipelines.startPipeline(1L)));
        assertEquals(1, transactions.rollbacks);
        assertCanAcquireAndClose(1L, Operation.SINGLE_UPLOAD);
    }

    @Test
    void anotherPipelineMayRunWhileBatchIsHeld() {
        try (var held = coordinator.tryAcquire(1L, Operation.BATCH_UPLOAD).permit()) {
            assertEquals(Result.SUCCESS, upload(2L, new TrackedInputStream()).getStatus());
            assertEquals(Result.SUCCESS, pipelines.startPipeline(3L).getStatus());
            assertBlocked(1L, Operation.START, Operation.BATCH_UPLOAD);
        }
    }

    private Result<Long> upload(long pipelineId, InputStream stream) {
        return inputs.createInputs(pipelineId, "fixture.fastq", 10, "sample/0/0", stream);
    }

    private void assertBlocked(long pipelineId, Operation attempt, Operation expectedHolder) {
        Acquisition acquisition = coordinator.tryAcquire(pipelineId, attempt);
        // Clean up even if an assertion exposes an incorrectly missing proxy.
        try (var unexpectedPermit = acquisition.permit()) {
            assertFalse(acquisition.acquired());
            assertEquals(expectedHolder, acquisition.blockedBy());
        }
    }

    private void assertCanAcquireAndClose(long pipelineId, Operation operation) {
        Acquisition acquisition = coordinator.tryAcquire(pipelineId, operation);
        try (var permit = acquisition.permit()) {
            assertTrue(acquisition.acquired());
            assertNotNull(permit);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableTransactionManagement(proxyTargetClass = true)
    static class TransactionConfiguration {
    }

    public static class RecordingInputService extends PipelineInputService {
        int calls;
        RuntimeException failure;
        Runnable duringCall = () -> { };
        Runnable duringFinally = () -> { };
        final Result<Long> response = new Result<>(Result.SUCCESS, 42L, null);

        @Override
        @Transactional
        public Result<Long> createInputs(long pipelineId, String fileName, int inputType,
                String inputKey, InputStream stream) {
            calls++;
            try (stream) {
                duringCall.run();
                if (failure != null) {
                    throw failure;
                }
                return response;
            } catch (IOException exception) {
                throw new IllegalStateException(exception);
            } finally {
                duringFinally.run();
            }
        }
    }

    public static class RecordingPipelineService extends PipelineService {
        int calls;
        RuntimeException failure;
        Runnable duringCall = () -> { };
        Runnable duringFinally = () -> { };
        final Result<Boolean> response = new Result<>(Result.SUCCESS, true, null);

        @Override
        @Transactional
        public Result<Boolean> startPipeline(long pipelineId) {
            calls++;
            try {
                duringCall.run();
                if (failure != null) {
                    throw failure;
                }
                return response;
            } finally {
                duringFinally.run();
            }
        }
    }

    public static class RecordingTransactionManager implements PlatformTransactionManager {
        int begins;
        int commits;
        int rollbacks;
        Runnable onBegin = () -> { };
        Runnable onCommit = () -> { };
        Runnable onRollback = () -> { };

        @Override
        public TransactionStatus getTransaction(TransactionDefinition definition) {
            begins++;
            onBegin.run();
            return new SimpleTransactionStatus();
        }

        @Override
        public void commit(TransactionStatus status) {
            onCommit.run();
            commits++;
        }

        @Override
        public void rollback(TransactionStatus status) {
            onRollback.run();
            rollbacks++;
        }
    }

    private static class TrackedInputStream extends ByteArrayInputStream {
        int closes;
        boolean failClose;

        TrackedInputStream() {
            super(new byte[] { 1 });
        }

        @Override
        public void close() throws IOException {
            closes++;
            if (failClose) {
                throw new IOException("fixture close failure");
            }
            super.close();
        }
    }
}
