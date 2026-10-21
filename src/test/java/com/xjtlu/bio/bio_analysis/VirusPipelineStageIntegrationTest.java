package com.xjtlu.bio.bio_analysis;

import static com.xjtlu.bio.analysisPipeline.Constants.PipelineType.PIPELINE_VIRUS;
import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.SEQUENCE_LEVEL_READ;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FINISHED;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_NOT_APPLICABLE;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_RUNNING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_ASSEMBLY;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_CONSENSUS;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_MAPPING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_QC;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_READ_INSPECT;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_REFERENCE_SELECTION;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_SNP_ANNOTATION;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_VARIANT_CALL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.bean.override.convention.TestBean;
import org.springframework.test.context.bean.override.BeanOverrideTestExecutionListener;
import org.springframework.test.context.support.CommonCachesTestExecutionListener;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.support.DirtiesContextBeforeModesTestExecutionListener;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.util.ReflectionTestUtils;

import com.xjtlu.bio.analysisPipeline.stageMetrics.ConsensusResultMetrics;
import com.xjtlu.bio.analysisPipeline.stageMetrics.MappingResultMetrics;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceCandidateScore;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceGenome;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSequence;
import com.xjtlu.bio.analysisPipeline.stageDoneHandler.StageDoneHandler;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.AssemblyInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ConsensusStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.MappingInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.QcStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReferenceSelectionStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.VarientCallInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.MappingParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ConsensusStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.SNPAnnotationStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.VarientCallParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.SequenceMeta;
import com.xjtlu.bio.analysisPipeline.stageResult.AssemblyResult;
import com.xjtlu.bio.analysisPipeline.stageResult.ConsensusStageResult;
import com.xjtlu.bio.analysisPipeline.stageResult.MappingResult;
import com.xjtlu.bio.analysisPipeline.stageResult.QcResult;
import com.xjtlu.bio.analysisPipeline.stageResult.ReadInspectStageResult;
import com.xjtlu.bio.analysisPipeline.stageResult.ReferenceSelectionStageResult;
import com.xjtlu.bio.analysisPipeline.stageResult.VarientCallStageResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.PipelineStageExecutor;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.workflow.AnalysisPipelineStagesBuilder;
import com.xjtlu.bio.analysisPipeline.workflow.AnalysisPipelineStagesBuilder.PipelineConfigurations;
import com.xjtlu.bio.analysisPipeline.workflow.AnalysisPipelineStagesBuilder.PipelineSampleInput;
import com.xjtlu.bio.analysisPipeline.workflow.StageOrchestrator;
import com.xjtlu.bio.analysisPipeline.workflow.StageOrchestrator.OrchestratePlan;
import com.xjtlu.bio.analysisPipeline.stageMetrics.VariantCallResultMetrics;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.entity.BioReferenceSequence;
import com.xjtlu.bio.mapper.BioReferenceSequenceMapper;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.service.PipelineStagePoller;
import com.xjtlu.bio.service.PipelineStageTaskDispatcher;
import com.xjtlu.bio.service.StorageService;
import com.xjtlu.bio.service.command.UpdateStageCommand;
import com.xjtlu.bio.utils.JsonUtil;

import jakarta.annotation.Resource;

/**
 * Runs the read-level virus DAG through the real stage executors and done
 * handlers without using the real PipelineService persistence/scheduling path.
 */
@SpringBootTest(properties = {
        "localstorageService.baseDir=/home/jcy/bioTest",
        "analysis-pipeline.stage.baseWorkDir=/home/jcy/bioTest/workDir/virusPipelineTest",
        "analysis-pipeline.stage.baseInputDir=/home/jcy/bioTest/inputDir/virusPipelineTest"
})
@ActiveProfiles("dev")
@TestExecutionListeners(
        listeners = {
                DirtiesContextBeforeModesTestExecutionListener.class,
                BeanOverrideTestExecutionListener.class,
                DependencyInjectionTestExecutionListener.class,
                DirtiesContextTestExecutionListener.class,
                CommonCachesTestExecutionListener.class
        },
        mergeMode = TestExecutionListeners.MergeMode.REPLACE_DEFAULTS)
public class VirusPipelineStageIntegrationTest {

    private static final Path TEST_STORAGE_ROOT = Path.of("/home/jcy/bioTest");

    private static final int INFLUENZA_A_SPECIES_TAX_ID = 11_320;
    private static final int ROTAVIRUS_A_TAX_ID = 28_875;
    private static final int MONKEYPOX_TAX_ID = 10_244;

    // Two complete H1N1 genomes (8 segments each):
    // A/California/07/2009 and A/Puerto Rico/8/1934.
    private static final List<Long> INFLUENZA_REFERENCE_IDS = List.of(
            25065L, 25068L, 25066L, 25070L,
            25067L, 25069L, 25072L, 25071L,
            19203L, 19205L, 19204L, 19209L,
            19207L, 19208L, 19210L, 19206L);

    private static final List<Long> ROTAVIRUS_A_REFERENCE_IDS = List.of(
            20679L, 20680L, 20678L, 20676L, 20686L, 20677L,
            20685L, 20684L, 20683L, 20682L, 20681L);

    private static final List<Long> MONKEYPOX_REFERENCE_IDS = List.of(
            18982L, 33044L);

    private static final VirusPipelineTestCase INFLUENZA_H1N1_CASE =
            new VirusPipelineTestCase(
                    "Influenza A H1N1 SRR7775427",
                    "inputDir/virusSraTest/SRR7775427/SRR7775427_1.fastq.gz",
                    "inputDir/virusSraTest/SRR7775427/SRR7775427_2.fastq.gz",
                    7_775_427L,
                    7_775_427_000L,
                    INFLUENZA_A_SPECIES_TAX_ID,
                    8,
                    List.of(),
                    INFLUENZA_REFERENCE_IDS);

    private static final VirusPipelineTestCase INFLUENZA_H1N1_PARTIAL_CASE =
            new VirusPipelineTestCase(
                    "Influenza A H1N1 SRR7643018 partial segments",
                    "inputDir/virusSraTest/SRR7643018/SRR7643018_1.fastq.gz",
                    "inputDir/virusSraTest/SRR7643018/SRR7643018_2.fastq.gz",
                    7_643_018L,
                    7_643_018_000L,
                    INFLUENZA_A_SPECIES_TAX_ID,
                    6,
                    List.of("SEGMENT_4", "SEGMENT_8"),
                    INFLUENZA_REFERENCE_IDS);

    private static final VirusPipelineTestCase ROTAVIRUS_A_CASE =
            new VirusPipelineTestCase(
                    "Rotavirus A synthetic 11-segment genome",
                    "inputDir/virusSyntheticTest/rotavirusA/rotavirusA_synthetic_R1.fastq.gz",
                    "inputDir/virusSyntheticTest/rotavirusA/rotavirusA_synthetic_R2.fastq.gz",
                    28_875L,
                    28_875_000L,
                    ROTAVIRUS_A_TAX_ID,
                    11,
                    List.of(),
                    ROTAVIRUS_A_REFERENCE_IDS);

    private static final VirusPipelineTestCase MONKEYPOX_CASE =
            new VirusPipelineTestCase(
                    "Monkeypox synthetic unsegmented genome",
                    "inputDir/virusSyntheticTest/monkeypox/monkeypox_synthetic_R1.fastq.gz",
                    "inputDir/virusSyntheticTest/monkeypox/monkeypox_synthetic_R2.fastq.gz",
                    10_244L,
                    10_244_000L,
                    MONKEYPOX_TAX_ID,
                    1,
                    List.of(),
                    MONKEYPOX_REFERENCE_IDS);

    private static final List<Integer> EXPECTED_EXECUTION_ORDER = List.of(
            PIPELINE_STAGE_READ_INSPECT,
            PIPELINE_STAGE_QC,
            PIPELINE_STAGE_ASSEMBLY,
            PIPELINE_STAGE_REFERENCE_SELECTION,
            PIPELINE_STAGE_MAPPING,
            PIPELINE_STAGE_VARIANT_CALL,
            PIPELINE_STAGE_CONSENSUS);

    @Resource(name = "stageExecutorMap")
    private Map<Integer, PipelineStageExecutor> stageExecutorMap;

    @Resource(name = "stageDoneHandlerMap")
    private Map<Integer, StageDoneHandler> stageDoneHandlerMap;

    @Resource
    private StageOrchestrator stageOrchestrator;

    @Resource
    private StorageService storageService;

    @Resource
    private BioReferenceSequenceMapper bioReferenceSequenceMapper;

    @TestBean(methodName = "createInMemoryPipelineService", enforceOverride = true)
    private PipelineService pipelineService;

    // Prevent the production dispatcher from starting its permanent worker
    // threads. Stages are executed synchronously by this test.
    @TestBean(methodName = "createSynchronousDispatcher", enforceOverride = true)
    private PipelineStageTaskDispatcher pipelineStageTaskDispatcher;

    // Prevent scheduled recovery scans from touching the mocked service during
    // this relatively long-running integration test.
    @TestBean(methodName = "createNoOpPoller", enforceOverride = true)
    private PipelineStagePoller pipelineStagePoller;

    private final Map<Long, BioPipelineStage> stagesById = new HashMap<>();

    @BeforeEach
    void configureInMemoryPipelineService() {
        stagesById.clear();
        ((InMemoryPipelineService) pipelineService).useStages(stagesById);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.MINUTES)
    void runCompleteReadLevelInfluenzaVirusPipeline() throws Exception {
        runCompleteReadLevelVirusPipeline(INFLUENZA_H1N1_CASE);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.MINUTES)
    void runPartialReadLevelInfluenzaVirusPipeline() throws Exception {
        runCompleteReadLevelVirusPipeline(INFLUENZA_H1N1_PARTIAL_CASE);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.MINUTES)
    void runCompleteReadLevelRotavirusPipeline() throws Exception {
        runCompleteReadLevelVirusPipeline(ROTAVIRUS_A_CASE);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.MINUTES)
    void runCompleteReadLevelMonkeypoxPipeline() throws Exception {
        runCompleteReadLevelVirusPipeline(MONKEYPOX_CASE);
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.MINUTES)
    void runPostAssemblyInfluenzaVirusPipeline() throws Exception {
        runPostAssemblyReadLevelVirusPipeline(INFLUENZA_H1N1_CASE);
    }

    /**
     * Opt-in complete production DAG for a frozen external benchmark manifest.
     * All artifacts stay in a fresh directory under the supplied benchmark root;
     * only service persistence and the scheduler are replaced by this test harness.
     * No precomputed QC, assembly, BAM, variants or consensus are injected.
     */
    @Test
    @EnabledIfSystemProperty(named = "benchmark.dataset", matches = ".+")
    @Timeout(value = 30, unit = TimeUnit.MINUTES)
    void runExternalBenchmarkCompletePipeline() throws Exception {
        Path manifestPath = Path.of(System.getProperty("benchmark.dataset")).toAbsolutePath();
        Map<String, Object> manifest = JsonUtil.toMap(Files.readString(manifestPath));
        Path parent = Path.of(System.getProperty("benchmark.root")).toAbsolutePath();
        Files.createDirectories(parent);
        Path root = Files.createTempDirectory(parent, "project-full-");
        Path storageRoot = Files.createDirectories(root.resolve("storage"));
        Path inputObjects = Files.createDirectories(storageRoot.resolve("benchmarkInput"));
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("scope", "Real builder, orchestrator, executors and done handlers; in-memory persistence/scheduling");
        report.put("datasetManifest", manifestPath.toString());
        report.put("sample", manifest.get("sample"));
        report.put("storageRoot", storageRoot.toString());
        report.put("startedAt", java.time.Instant.now().toString());
        System.out.println("Complete project benchmark artifacts: " + root);
        Object directories = ReflectionTestUtils.getField(stageExecutorMap.get(PIPELINE_STAGE_ASSEMBLY), "bioStageUtil");
        assertNotNull(directories);
        Object originalStorageBase = ReflectionTestUtils.getField(storageService, "base");
        Object originalWorkBase = ReflectionTestUtils.getField(directories, "stageWorkDirPath");
        Object originalInputBase = ReflectionTestUtils.getField(directories, "stageInputDirPath");
        try {
            for (String key : List.of("r1", "r2", "reference")) {
                Path source = Path.of(Objects.toString(manifest.get(key), ""));
                assertTrue(Files.isRegularFile(source), "Missing benchmark input " + key + ": " + source);
                String name = key.equals("reference") ? "reference.fasta" : key + ".fastq.gz";
                Files.copy(source, inputObjects.resolve(name));
            }
            assertEquals(1L, countFastaRecords(inputObjects.resolve("reference.fasta")),
                    "This benchmark controls a single-record reference shared with the external workflow");
            ReflectionTestUtils.setField(storageService, "base", storageRoot.toString());
            ReflectionTestUtils.setField(directories, "stageWorkDirPath", root.resolve("work").toString());
            ReflectionTestUtils.setField(directories, "stageInputDirPath", root.resolve("input").toString());

            ReferenceSequence candidate = new ReferenceSequence();
            candidate.setAccession(Objects.toString(manifest.get("accession")));
            candidate.setPath("benchmarkInput/reference.fasta");
            candidate.setOrganismName(Objects.toString(manifest.get("organism"), "Public benchmark control"));
            candidate.setIsAnnotated(false);
            candidate.setCompleteness("COMPLETE");
            if (manifest.get("taxId") instanceof Number taxId) candidate.setTaxId(taxId.intValue());
            PipelineSampleInput input = new PipelineSampleInput("benchmarkInput/r1.fastq.gz", "benchmarkInput/r2.fastq.gz");
            input.setSequenceLevel(SEQUENCE_LEVEL_READ);
            PipelineConfigurations configuration = new PipelineConfigurations();
            configuration.setCandidateReferenceSequences(List.of(candidate));
            List<BioPipelineStage> stages = AnalysisPipelineStagesBuilder.buildVirusStages(input, configuration);
            initializeStages(stages, new VirusPipelineTestCase("External benchmark", "benchmarkInput/r1.fastq.gz",
                    "benchmarkInput/r2.fastq.gz", 0L, 1L, candidate.getTaxId() == null ? 0 : candidate.getTaxId(),
                    1, List.of(), List.of()));
            report.put("stages", stages);
            List<Integer> order = new ArrayList<>();
            List<Map<String, Object>> trace = new ArrayList<>();
            report.put("trace", trace);
            BioPipelineStage entry = stages.stream().filter(s -> s.getStageIndex() == 0).findFirst().orElseThrow();
            Deque<BioPipelineStage> ready = new ArrayDeque<>(stageOrchestrator.makePlan(stages, entry.getStageId()).getRunStages());
            Set<Long> executed = new HashSet<>();
            while (!ready.isEmpty()) {
                BioPipelineStage stage = ready.removeFirst();
                assertTrue(executed.add(stage.getStageId()), "Duplicate execution: " + stage.getStageId());
                long start = System.nanoTime();
                executeStage(stage);
                trace.add(Map.of("stageType", stage.getStageType(), "name", stage.getStageName(),
                        "elapsedSeconds", (System.nanoTime() - start) / 1_000_000_000.0));
                order.add(stage.getStageType());
                OrchestratePlan downstream = stageOrchestrator.makeDownstreamPlan(stage, stages, PIPELINE_VIRUS);
                ready.addAll(downstream.getRunStages());
                Files.writeString(root.resolve("report.json"), JsonUtil.toJson(report));
            }
            assertEquals(EXPECTED_EXECUTION_ORDER, order);
            assertFinalStageStates(stages);
            ReferenceSelectionStageResult selected = JsonUtil.toObject(
                    findStage(stages, PIPELINE_STAGE_REFERENCE_SELECTION).getOutputInline(), ReferenceSelectionStageResult.class);
            assertEquals(List.of(candidate.getAccession()), accessions(selected.getSelectedReference()));
            report.put("reference", Map.of("fasta", inputObjects.resolve("reference.fasta").toString(),
                    "accession", candidate.getAccession()));
            QcResult qc = JsonUtil.toObject(findStage(stages, PIPELINE_STAGE_QC).getOutputUrl(), QcResult.class);
            report.put("qc", Map.of("reportJSON", storageRoot.resolve(qc.getReportJSON()).toString(),
                    "cleanedR1", storageRoot.resolve(qc.getCleanedR1()).toString(),
                    "cleanedR2", storageRoot.resolve(qc.getCleanedR2()).toString()));
            MappingResult mapped = JsonUtil.toObject(findStage(stages, PIPELINE_STAGE_MAPPING).getOutputUrl(), MappingResult.class);
            report.put("mapping", Map.of("bam", storageRoot.resolve(mapped.getBamUrl()).toString(),
                    "depth", storageRoot.resolve(mapped.getCoverageDepthUrl()).toString(),
                    "metrics", storageRoot.resolve(mapped.getMetricsUrl()).toString()));
            VarientCallStageResult variants = JsonUtil.toObject(findStage(stages, PIPELINE_STAGE_VARIANT_CALL).getOutputUrl(), VarientCallStageResult.class);
            report.put("variants", Map.of("vcf", storageRoot.resolve(variants.getVcfGzUrl()).toString(),
                    "rawVcf", storageRoot.resolve(variants.getRawVcfGzUrl()).toString(),
                    "metrics", storageRoot.resolve(variants.getMetricsUrl()).toString()));
            ConsensusStageResult consensus = JsonUtil.toObject(findStage(stages, PIPELINE_STAGE_CONSENSUS).getOutputUrl(), ConsensusStageResult.class);
            report.put("consensus", Map.of("fasta", storageRoot.resolve(consensus.getConsensusUrl()).toString(),
                    "mask", storageRoot.resolve(consensus.getMaskBedUrl()).toString(),
                    "metrics", storageRoot.resolve(consensus.getMetricsUrl()).toString()));
            report.put("executionOrder", order);
            report.put("status", "PASS");
        } catch (Exception | AssertionError failure) {
            report.put("status", "FAIL");
            report.put("failure", failure.toString());
            throw failure;
        } finally {
            ReflectionTestUtils.setField(storageService, "base", originalStorageBase);
            ReflectionTestUtils.setField(directories, "stageWorkDirPath", originalWorkBase);
            ReflectionTestUtils.setField(directories, "stageInputDirPath", originalInputBase);
            report.put("finishedAt", java.time.Instant.now().toString());
            Files.writeString(root.resolve("report.json"), JsonUtil.toJson(report));
            System.out.println("Complete project benchmark report: " + root.resolve("report.json"));
        }
    }

    private void runCompleteReadLevelVirusPipeline(
            VirusPipelineTestCase testCase) throws Exception {
        List<ReferenceSequence> candidateReferences = loadReferenceSequences(
                testCase.candidateReferenceIds(), testCase.targetTaxId());
        assertEquals(testCase.candidateReferenceIds().size(),
                candidateReferences.size());
        assertStored(testCase.read1ObjectName());
        assertStored(testCase.read2ObjectName());

        PipelineSampleInput pipelineInput = new PipelineSampleInput(
                testCase.read1ObjectName(), testCase.read2ObjectName());
        pipelineInput.setSequenceLevel(SEQUENCE_LEVEL_READ);

        PipelineConfigurations pipelineConfigurations = new PipelineConfigurations();
        pipelineConfigurations.setCandidateReferenceSequences(candidateReferences);

        List<BioPipelineStage> stages = AnalysisPipelineStagesBuilder.buildVirusStages(
                pipelineInput, pipelineConfigurations);
        initializeStages(stages, testCase);

        assertEquals(8, stages.size());

        List<Integer> executionOrder = executePipeline(stages);

        assertEquals(EXPECTED_EXECUTION_ORDER, executionOrder);
        assertFinalStageStates(stages);
        assertDataFlowAndArtifacts(stages, candidateReferences, testCase);
    }

    /**
     * Re-runs the production stages after assembly using artifacts from a
     * previously successful assembly. This keeps selection/mapping/variant/
     * consensus regression coverage available when the external SPAdes
     * process is unavailable or stalls in the test environment.
     */
    private void runPostAssemblyReadLevelVirusPipeline(
            VirusPipelineTestCase testCase) throws Exception {
        List<ReferenceSequence> candidateReferences = loadReferenceSequences(
                testCase.candidateReferenceIds(), testCase.targetTaxId());
        assertEquals(testCase.candidateReferenceIds().size(),
                candidateReferences.size());

        PipelineSampleInput pipelineInput = new PipelineSampleInput(
                testCase.read1ObjectName(), testCase.read2ObjectName());
        pipelineInput.setSequenceLevel(SEQUENCE_LEVEL_READ);

        PipelineConfigurations pipelineConfigurations = new PipelineConfigurations();
        pipelineConfigurations.setCandidateReferenceSequences(candidateReferences);

        List<BioPipelineStage> stages = AnalysisPipelineStagesBuilder.buildVirusStages(
                pipelineInput, pipelineConfigurations);
        initializeStages(stages, testCase);
        preloadCompletedReadQcAndAssembly(stages, testCase);

        List<Integer> executionOrder = executePipelineFrom(
                stages, PIPELINE_STAGE_REFERENCE_SELECTION);

        assertEquals(List.of(
                PIPELINE_STAGE_REFERENCE_SELECTION,
                PIPELINE_STAGE_MAPPING,
                PIPELINE_STAGE_VARIANT_CALL,
                PIPELINE_STAGE_CONSENSUS), executionOrder);
        assertPostAssemblyStageStates(stages);
        assertDataFlowAndArtifacts(stages, candidateReferences, testCase);
    }

    private List<Integer> executePipeline(List<BioPipelineStage> stages) throws Exception {
        BioPipelineStage entryStage = stages.stream()
                .filter(stage -> stage.getStageIndex() == 0)
                .findFirst()
                .orElseThrow();

        return executePipelineFrom(stages, entryStage.getStageType());
    }

    private List<Integer> executePipelineFrom(
            List<BioPipelineStage> stages,
            int startStageType) throws Exception {
        BioPipelineStage entryStage = findStage(stages, startStageType);

        OrchestratePlan initialPlan = stageOrchestrator.makePlan(
                stages, entryStage.getStageId());
        Deque<BioPipelineStage> readyStages = new ArrayDeque<>(
                initialPlan.getRunStages());

        Set<Long> executedStageIds = new HashSet<>();
        List<Integer> executionOrder = new ArrayList<>();

        while (!readyStages.isEmpty()) {
            BioPipelineStage stage = readyStages.removeFirst();
            assertTrue(executedStageIds.add(stage.getStageId()),
                    "Stage was scheduled more than once: " + stage.getStageId());

            executeStage(stage);
            executionOrder.add(stage.getStageType());

            OrchestratePlan downstreamPlan = stageOrchestrator.makeDownstreamPlan(
                    stage, stages, PIPELINE_VIRUS);
            assertNotNull(downstreamPlan,
                    "No downstream plan for stage type " + stage.getStageType());
            readyStages.addAll(downstreamPlan.getRunStages());
        }

        return executionOrder;
    }

    private void preloadCompletedReadQcAndAssembly(
            List<BioPipelineStage> stages,
            VirusPipelineTestCase testCase) throws Exception {
        BioPipelineStage readInspectStage = findStage(
                stages, PIPELINE_STAGE_READ_INSPECT);
        BioPipelineStage qcStage = findStage(stages, PIPELINE_STAGE_QC);
        BioPipelineStage assemblyStage = findStage(stages, PIPELINE_STAGE_ASSEMBLY);

        String qcOutputPrefix = "stageOutput/" + qcStage.getStageId() + "/";
        String assemblyOutputPrefix = "stageOutput/"
                + assemblyStage.getStageId() + "/";

        ReadInspectStageResult readInspectResult = new ReadInspectStageResult();
        readInspectResult.setR1Url(testCase.read1ObjectName());
        readInspectResult.setR2Url(testCase.read2ObjectName());
        SequenceMeta readMeta = new SequenceMeta();
        readMeta.setSequenceLevel(SEQUENCE_LEVEL_READ);
        readMeta.setQualityEncoding(33);
        readInspectResult.setReadMeta(readMeta);

        QcStageInputUrls qcInput = new QcStageInputUrls();
        qcInput.setRead1(testCase.read1ObjectName());
        qcInput.setRead2(testCase.read2ObjectName());
        QcResult qcResult = new QcResult(
                qcOutputPrefix + "trimmed_r1.fastq.gz",
                qcOutputPrefix + "trimmed_r2.fastq.gz",
                qcOutputPrefix + "cleaned.html",
                qcOutputPrefix + "cleaned.json");

        AssemblyInputUrls assemblyInput = new AssemblyInputUrls();
        assemblyInput.setRead1Url(qcResult.getCleanedR1());
        assemblyInput.setRead2Url(qcResult.getCleanedR2());
        AssemblyResult assemblyResult = new AssemblyResult(
                assemblyOutputPrefix + "contigs.fasta",
                assemblyOutputPrefix + "scaffolds.fasta");

        readInspectStage.setOutputUrl(JsonUtil.toJson(readInspectResult));
        qcStage.setInputUrl(JsonUtil.toJson(qcInput));
        qcStage.setOutputUrl(JsonUtil.toJson(qcResult));
        assemblyStage.setInputUrl(JsonUtil.toJson(assemblyInput));
        assemblyStage.setOutputUrl(JsonUtil.toJson(assemblyResult));

        for (BioPipelineStage completedStage : List.of(
                readInspectStage, qcStage, assemblyStage)) {
            completedStage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
            completedStage.setVersion(3);
        }

        assertStored(testCase.read1ObjectName());
        assertStored(testCase.read2ObjectName());
        assertStored(qcResult.getCleanedR1());
        assertStored(qcResult.getCleanedR2());
        assertStored(assemblyResult.getContigsUrl());
        assertStored(assemblyResult.getScaffoldUrl());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void executeStage(BioPipelineStage stage) {
        // Equivalent to PipelineService.startStageExecute(), but entirely
        // in-memory.
        stage.setStatus(PIPELINE_STAGE_STATUS_RUNNING);
        stage.setVersion(stage.getVersion() + 1);

        PipelineStageExecutor executor = stageExecutorMap.get(stage.getStageType());
        assertNotNull(executor,
                "No executor registered for stage type " + stage.getStageType());

        StageRunResult stageRunResult = executor.execute(stage);
        assertNotNull(stageRunResult);
        assertTrue(stageRunResult.isSuccess(), stageRunResult.getErrorLog());
        assertNotNull(stageRunResult.getStageOutput());

        StageDoneHandler doneHandler = stageDoneHandlerMap.get(stage.getStageType());
        assertNotNull(doneHandler,
                "No done handler registered for stage type " + stage.getStageType());

        // The real handler uploads artifacts and constructs the exact
        // production StageResult. Its PipelineService calls are intercepted by
        // configureInMemoryPipelineService().
        doneHandler.handleStageDone(stageRunResult);

        assertEquals(PIPELINE_STAGE_STATUS_FINISHED, stage.getStatus(),
                "Done handler did not finish stage type " + stage.getStageType());
        assertStoredStageResult(stage);
    }

    private static void assertStoredStageResult(BioPipelineStage stage) {
        if (stage.getStageType() == PIPELINE_STAGE_REFERENCE_SELECTION) {
            assertNotNull(stage.getOutputInline(), "Selection result must be stored inline");
            assertNull(stage.getOutputUrl(), "Selection must not store its result in output_url");
        } else {
            assertNotNull(stage.getOutputUrl(),
                    "Done handler did not write a result for stage type " + stage.getStageType());
        }
    }

    private static int applyStageUpdate(
            Map<Long, BioPipelineStage> stagesById,
            UpdateStageCommand command) {
        BioPipelineStage target = stagesById.get(command.getStageId());
        if (target == null
                || !Objects.equals(target.getVersion(), command.getCurrentVersion())) {
            return 0;
        }

        BioPipelineStage patch = command.getUpdateStage();
        if (patch.getInputUrl() != null) {
            target.setInputUrl(patch.getInputUrl());
        }
        if (patch.getOutputUrl() != null) {
            target.setOutputUrl(patch.getOutputUrl());
        }
        if (patch.getParameters() != null) {
            target.setParameters(patch.getParameters());
        }
        if (patch.getOutputInline() != null) {
            target.setOutputInline(patch.getOutputInline());
        }
        if (patch.getStatus() != null) {
            target.setStatus(patch.getStatus());
        }
        if (patch.getStartTime() != null) {
            target.setStartTime(patch.getStartTime());
        }
        if (patch.getEndTime() != null) {
            target.setEndTime(patch.getEndTime());
        }

        // PipelineService.updateStageFromVersion() always advances the stored
        // version from the command's optimistic-lock version.
        target.setVersion(command.getCurrentVersion() + 1);
        return 1;
    }

    private static PipelineService createInMemoryPipelineService() {
        return new InMemoryPipelineService();
    }

    private static PipelineStageTaskDispatcher createSynchronousDispatcher() {
        return new SynchronousTestDispatcher();
    }

    private static PipelineStagePoller createNoOpPoller() {
        return new NoOpPipelineStagePoller();
    }

    private static final class InMemoryPipelineService extends PipelineService {
        private Map<Long, BioPipelineStage> stages = Map.of();

        @Override
        public void recordStageFailure(com.xjtlu.bio.analysisPipeline.context.runtime.StageContext context,
                String message) {
            // Failure details are retained in the test report; no database writes.
        }

        private void useStages(Map<Long, BioPipelineStage> stages) {
            this.stages = stages;
        }

        @Override
        public BioPipelineStage queryStageById(long stageId) {
            return stages.get(stageId);
        }

        @Override
        public int updateStageFromVersion(UpdateStageCommand command) {
            return applyStageUpdate(stages, command);
        }

        @Override
        public void pipelineStageDone(long stageId, boolean success) {
            // The test drives StageOrchestrator synchronously after the real
            // DoneHandler returns.
        }
    }

    private static final class SynchronousTestDispatcher
            extends PipelineStageTaskDispatcher {
        @Override
        public void init() {
            // Do not create production worker threads in this test.
        }

        @Override
        public void run() {
            // Stages are executed directly by executePipeline().
        }

        @Override
        public boolean addTask(BioPipelineStage stage) {
            throw new AssertionError(
                    "The direct-stage integration test must not use the dispatcher");
        }
    }

    private static final class NoOpPipelineStagePoller
            extends PipelineStagePoller {
        @Override
        public void dispatchQueuingStages() {
            // Scheduling recovery is outside this direct-stage test.
        }

        @Override
        public void dispatchPendingStages() {
            // Scheduling recovery is outside this direct-stage test.
        }

        @Override
        public void reconcileCompletedPipelines() {
            // Completion reconciliation is driven by the test's in-memory service.
        }
    }

    private void initializeStages(
            List<BioPipelineStage> stages,
            VirusPipelineTestCase testCase) {
        long stageId = testCase.firstStageId();
        for (BioPipelineStage stage : stages) {
            stage.setPipelineId(testCase.pipelineId());
            stage.setStageId(stageId++);
            stage.setVersion(0);
            BioPipelineStage previous = stagesById.put(stage.getStageId(), stage);
            assertTrue(previous == null, "Duplicate stage id: " + stage.getStageId());
        }
    }

    private void assertFinalStageStates(List<BioPipelineStage> stages) {
        for (int stageType : EXPECTED_EXECUTION_ORDER) {
            BioPipelineStage stage = findStage(stages, stageType);
            assertEquals(PIPELINE_STAGE_STATUS_FINISHED, stage.getStatus(),
                    "Unexpected final status for stage type " + stageType);
            assertEquals(3, stage.getVersion(),
                    "Unexpected lifecycle version for stage type " + stageType);
            assertStoredStageResult(stage);
            assertNotNull(stage.getEndTime());
        }

        BioPipelineStage snpAnnotationStage = findStage(
                stages, PIPELINE_STAGE_SNP_ANNOTATION);
        assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE,
                snpAnnotationStage.getStatus());
        assertEquals(1, snpAnnotationStage.getVersion());
    }

    private void assertPostAssemblyStageStates(List<BioPipelineStage> stages) {
        for (int stageType : List.of(
                PIPELINE_STAGE_REFERENCE_SELECTION,
                PIPELINE_STAGE_MAPPING,
                PIPELINE_STAGE_VARIANT_CALL,
                PIPELINE_STAGE_CONSENSUS)) {
            BioPipelineStage stage = findStage(stages, stageType);
            assertEquals(PIPELINE_STAGE_STATUS_FINISHED, stage.getStatus(),
                    "Unexpected final status for stage type " + stageType);
            assertEquals(3, stage.getVersion(),
                    "Unexpected lifecycle version for stage type " + stageType);
            assertStoredStageResult(stage);
            assertNotNull(stage.getEndTime());
        }

        BioPipelineStage snpAnnotationStage = findStage(
                stages, PIPELINE_STAGE_SNP_ANNOTATION);
        assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE,
                snpAnnotationStage.getStatus());
        assertEquals(1, snpAnnotationStage.getVersion());
    }

    private void assertDataFlowAndArtifacts(
            List<BioPipelineStage> stages,
            List<ReferenceSequence> candidateReferences,
            VirusPipelineTestCase testCase) throws Exception {

        BioPipelineStage readInspectStage = findStage(
                stages, PIPELINE_STAGE_READ_INSPECT);
        BioPipelineStage qcStage = findStage(stages, PIPELINE_STAGE_QC);
        BioPipelineStage assemblyStage = findStage(stages, PIPELINE_STAGE_ASSEMBLY);
        BioPipelineStage selectionStage = findStage(
                stages, PIPELINE_STAGE_REFERENCE_SELECTION);
        BioPipelineStage mappingStage = findStage(stages, PIPELINE_STAGE_MAPPING);
        BioPipelineStage variantStage = findStage(
                stages, PIPELINE_STAGE_VARIANT_CALL);
        BioPipelineStage consensusStage = findStage(
                stages, PIPELINE_STAGE_CONSENSUS);

        ReadInspectStageResult readInspectResult = JsonUtil.toObject(
                readInspectStage.getOutputUrl(), ReadInspectStageResult.class);
        QcStageInputUrls qcInput = JsonUtil.toObject(
                qcStage.getInputUrl(), QcStageInputUrls.class);
        assertEquals(readInspectResult.getR1Url(), qcInput.getRead1());
        assertEquals(readInspectResult.getR2Url(), qcInput.getRead2());
        assertStored(readInspectResult.getR1Url());
        assertStored(readInspectResult.getR2Url());

        QcResult qcResult = JsonUtil.toObject(qcStage.getOutputUrl(), QcResult.class);
        AssemblyInputUrls assemblyInput = JsonUtil.toObject(
                assemblyStage.getInputUrl(), AssemblyInputUrls.class);
        assertEquals(qcResult.getCleanedR1(), assemblyInput.getRead1Url());
        assertEquals(qcResult.getCleanedR2(), assemblyInput.getRead2Url());
        assertStored(qcResult.getCleanedR1());
        assertStored(qcResult.getCleanedR2());
        assertStored(qcResult.getReportHTML());
        assertStored(qcResult.getReportJSON());

        AssemblyResult assemblyResult = JsonUtil.toObject(
                assemblyStage.getOutputUrl(), AssemblyResult.class);
        ReferenceSelectionStageInputUrls selectionInput = JsonUtil.toObject(
                selectionStage.getInputUrl(), ReferenceSelectionStageInputUrls.class);
        assertEquals(assemblyResult.getContigsUrl(), selectionInput.getContigsUrl());
        assertStored(assemblyResult.getContigsUrl());
        assertStored(assemblyResult.getScaffoldUrl());

        ReferenceSelectionStageResult selectionResult = JsonUtil.toObject(
                selectionStage.getOutputInline(), ReferenceSelectionStageResult.class);
        assertNotNull(selectionResult);
        ReferenceGenome selectedReference = selectionResult.getSelectedReference();
        assertNotNull(selectedReference);
        assertNotNull(selectedReference.getSequences());
        assertEquals(testCase.expectedSelectedSequenceCount(),
                selectedReference.getSequences().size());
        assertEquals(testCase.expectedSelectedSequenceCount(),
                selectedReference.getSequences().stream()
                        .map(ReferenceSequence::getSegmentKey)
                        .distinct()
                        .count());

        List<ReferenceCandidateScore> candidateScores =
                selectionResult.getCandidateScores();
        assertNotNull(candidateScores);
        assertEquals(testCase.candidateReferenceIds().size(),
                candidateScores.size());
        printSelectionResult(testCase.name(), selectionResult);
        assertSelectionCompleteness(testCase, selectedReference, candidateScores,
                selectionResult);

        Set<String> candidateAccessions = candidateReferences.stream()
                .map(ReferenceSequence::getAccession)
                .collect(java.util.stream.Collectors.toSet());
        for (ReferenceSequence selectedSequence : selectedReference.getSequences()) {
            assertTrue(candidateAccessions.contains(selectedSequence.getAccession()),
                    "Selected sequence was not a candidate: "
                            + selectedSequence.getAccession());

            ReferenceCandidateScore selectedScore = candidateScores.stream()
                    .filter(score -> selectedSequence.getAccession().equals(
                            score.getReferenceAccession()))
                    .findFirst()
                    .orElseThrow();
            String segmentKey = selectedSequence.getSegmentKey();
            double bestSegmentScore;
            if (segmentKey == null || segmentKey.isBlank()) {
                bestSegmentScore = candidateScores.stream()
                        .mapToDouble(ReferenceCandidateScore::getScore)
                        .max()
                        .orElseThrow();
            } else {
                bestSegmentScore = candidateScores.stream()
                        .filter(score -> segmentKey.equals(score.getSegmentKey()))
                        .mapToDouble(ReferenceCandidateScore::getScore)
                        .max()
                        .orElseThrow();
            }

            assertEquals(bestSegmentScore, selectedScore.getScore(), 0.0d);
            assertTrue(selectedScore.getAlignedReferenceBases() > 0L);
            assertTrue(selectedScore.getReferenceCoverage() > 0.0d);
            assertTrue(selectedScore.getSequenceIdentity() > 0.0d);
            assertTrue(selectedScore.getScore() > 0.0d);
        }

        List<String> expectedAccessions = accessions(selectedReference);

        MappingInputUrls mappingInput = JsonUtil.toObject(
                mappingStage.getInputUrl(), MappingInputUrls.class);
        MappingParameters mappingParameters = JsonUtil.toObject(
                mappingStage.getParameters(), MappingParameters.class);
        assertEquals(qcResult.getCleanedR1(), mappingInput.getR1Url());
        assertEquals(qcResult.getCleanedR2(), mappingInput.getR2Url());
        assertEquals(expectedAccessions,
                accessions(mappingParameters.getReferenceGenome()));

        MappingResult mappingResult = JsonUtil.toObject(
                mappingStage.getOutputUrl(), MappingResult.class);
        assertCompactUrlResult(mappingStage);
        assertStored(mappingResult.getBamUrl());
        assertStored(mappingResult.getBamIndexUrl());
        assertStored(mappingResult.getCoverageDepthUrl());
        assertStored(mappingResult.getMetricsUrl());

        MappingResultMetrics mappingMetrics = JsonUtil.toObject(
                Files.readString(TEST_STORAGE_ROOT.resolve(mappingResult.getMetricsUrl())),
                MappingResultMetrics.class);
        assertNotNull(mappingMetrics);
        assertEquals(expectedAccessions,
                mappingMetrics.getReferences().stream()
                        .map(MappingResultMetrics.ReferenceMetrics::getReferenceName)
                        .toList());
        assertTrue(mappingMetrics.getTotalReferenceBases() > 0L);
        assertEquals(mappingMetrics.getTotalReferenceBases(),
                mappingMetrics.getCoveredBases() + mappingMetrics.getZeroDepthBases());
        assertEquals(mappingMetrics.getTotalReadSegments(),
                mappingMetrics.getMappedReadSegments()
                        + mappingMetrics.getUnmappedReadSegments());
        assertTrue(mappingMetrics.getMappedReadSegments() > 0L);
        assertTrue(mappingMetrics.getMappingFraction() >= 0.0d
                && mappingMetrics.getMappingFraction() <= 1.0d);
        assertEquals(mappingMetrics.getTotalReferenceBases(),
                mappingMetrics.getReferences().stream()
                        .mapToLong(MappingResultMetrics.ReferenceMetrics::getReferenceLength)
                        .sum());
        assertEquals(mappingMetrics.getCoveredBases(),
                mappingMetrics.getReferences().stream()
                        .mapToLong(MappingResultMetrics.ReferenceMetrics::getCoveredBases)
                        .sum());
        assertEquals(mappingMetrics.getDepthSum(),
                mappingMetrics.getReferences().stream()
                        .mapToLong(MappingResultMetrics.ReferenceMetrics::getDepthSum)
                        .sum());

        VarientCallInputUrls variantInput = JsonUtil.toObject(
                variantStage.getInputUrl(), VarientCallInputUrls.class);
        VarientCallParameters variantParameters = JsonUtil.toObject(
                variantStage.getParameters(), VarientCallParameters.class);
        assertEquals(mappingResult.getBamUrl(), variantInput.getBamUrl());
        assertEquals(mappingResult.getBamIndexUrl(), variantInput.getBamIndexUrl());
        assertEquals(expectedAccessions,
                accessions(variantParameters.getReferenceGenome()));
        assertEquals(VarientCallParameters.DEFAULT_THREADS,
                variantParameters.getThreads());
        assertEquals(VarientCallParameters.DEFAULT_MINIMUM_MAPPING_QUALITY,
                variantParameters.getMinimumMappingQuality());
        assertEquals(VarientCallParameters.DEFAULT_MINIMUM_BASE_QUALITY,
                variantParameters.getMinimumBaseQuality());
        assertEquals(VarientCallParameters.DEFAULT_MINIMUM_VARIANT_QUALITY,
                variantParameters.getMinimumVariantQuality());
        assertEquals(VarientCallParameters.DEFAULT_MINIMUM_VARIANT_DEPTH,
                variantParameters.getMinimumVariantDepth());
        assertEquals(VarientCallParameters.DEFAULT_MINIMUM_ALTERNATE_DEPTH,
                variantParameters.getMinimumAlternateDepth());
        assertEquals(
                VarientCallParameters.DEFAULT_MINIMUM_ALTERNATE_ALLELE_FRACTION,
                variantParameters.getMinimumAlternateAlleleFraction());

        VarientCallStageResult variantResult = JsonUtil.toObject(
                variantStage.getOutputUrl(), VarientCallStageResult.class);
        assertCompactUrlResult(variantStage);
        assertStored(variantResult.getRawVcfGzUrl());
        assertStored(variantResult.getRawVcfTbiUrl());
        assertStored(variantResult.getVcfGzUrl());
        assertStored(variantResult.getVcfTbiUrl());
        assertStored(variantResult.getMetricsUrl());

        VariantCallResultMetrics variantMetrics = JsonUtil.toObject(
                Files.readString(TEST_STORAGE_ROOT.resolve(variantResult.getMetricsUrl())),
                VariantCallResultMetrics.class);
        assertNotNull(variantMetrics);
        assertEquals(expectedAccessions,
                variantMetrics.getReferences().stream()
                        .map(VariantCallResultMetrics.ReferenceMetrics::getReferenceName)
                        .toList());
        Map<String, Long> mappedReferenceLengths = mappingMetrics.getReferences().stream()
                .collect(java.util.stream.Collectors.toMap(
                        MappingResultMetrics.ReferenceMetrics::getReferenceName,
                        MappingResultMetrics.ReferenceMetrics::getReferenceLength));
        for (VariantCallResultMetrics.ReferenceMetrics referenceMetrics
                : variantMetrics.getReferences()) {
            assertNotNull(referenceMetrics.getReferenceLength());
            assertEquals(mappedReferenceLengths.get(referenceMetrics.getReferenceName()),
                    referenceMetrics.getReferenceLength());
        }
        assertTrue(variantMetrics.getTotalVcfRecords() > 0L);
        assertTrue(variantMetrics.getVariantRecords()
                <= variantMetrics.getTotalVcfRecords());
        assertEquals(variantMetrics.getVariantRecords(),
                variantMetrics.getSnpCount()
                        + variantMetrics.getIndelCount()
                        + variantMetrics.getOtherCount());
        assertEquals(variantMetrics.getVariantRecords(),
                variantMetrics.getExplicitPassCount()
                        + variantMetrics.getFilteredCount()
                        + variantMetrics.getUnfilteredCount());
        assertEquals(0L, variantMetrics.getUnfilteredCount(),
                "Every called variant must be assigned PASS or LowConfidence");
        assertEquals(variantMetrics.getTotalVcfRecords(),
                variantMetrics.getReferences().stream()
                        .mapToLong(VariantCallResultMetrics.ReferenceMetrics::getTotalVcfRecords)
                        .sum());
        assertEquals(variantMetrics.getVariantRecords(),
                variantMetrics.getReferences().stream()
                        .mapToLong(VariantCallResultMetrics.ReferenceMetrics::getVariantRecords)
                        .sum());

        ConsensusStageInputUrls consensusInput = JsonUtil.toObject(
                consensusStage.getInputUrl(), ConsensusStageInputUrls.class);
        assertEquals(variantResult.getVcfGzUrl(), consensusInput.getVcfGz());
        assertEquals(variantResult.getVcfTbiUrl(), consensusInput.getVcfTbi());
        assertEquals(mappingResult.getCoverageDepthUrl(),
                consensusInput.getCoverageDepthUrl());

        ConsensusStageParameters consensusParameters = JsonUtil.toObject(
                consensusStage.getParameters(), ConsensusStageParameters.class);
        assertEquals(ConsensusStageParameters.DEFAULT_MINIMUM_CONSENSUS_DEPTH,
                consensusParameters.getMinimumConsensusDepth());
        assertEquals(
                ConsensusStageParameters.DEFAULT_APPLY_ONLY_PASSING_VARIANTS,
                consensusParameters.isApplyOnlyPassingVariants());

        ConsensusStageResult consensusResult = JsonUtil.toObject(
                consensusStage.getOutputUrl(), ConsensusStageResult.class);
        assertCompactUrlResult(consensusStage);
        assertStored(consensusResult.getConsensusUrl());
        assertStoredAllowEmpty(consensusResult.getMaskBedUrl());
        assertStored(consensusResult.getMetricsUrl());

        ConsensusResultMetrics consensusMetrics = JsonUtil.toObject(
                Files.readString(TEST_STORAGE_ROOT.resolve(consensusResult.getMetricsUrl())),
                ConsensusResultMetrics.class);
        assertNotNull(consensusMetrics);
        assertEquals(consensusParameters.getMinimumConsensusDepth(),
                consensusMetrics.getMinimumDepth());
        assertTrue(consensusMetrics.getTotalBases() > 0L);
        assertEquals(consensusMetrics.getTotalBases(),
                consensusMetrics.getUnmaskedBases() + consensusMetrics.getMaskedBases());
        assertEquals(testCase.expectedSelectedSequenceCount(),
                consensusMetrics.getSequences().size());
        assertEquals(expectedAccessions,
                consensusMetrics.getSequences().stream()
                        .map(ConsensusResultMetrics.SequenceMetrics::getSequenceName)
                        .toList());

        Path consensusPath = TEST_STORAGE_ROOT.resolve(
                consensusResult.getConsensusUrl());
        assertEquals(testCase.expectedSelectedSequenceCount(), countFastaRecords(
                consensusPath));
        assertTrue(countFastaNs(consensusPath) >= consensusMetrics.getMaskedBases(),
                "Consensus must contain every low-depth masked base as N");

        BioPipelineStage snpAnnotationStage = findStage(
                stages, PIPELINE_STAGE_SNP_ANNOTATION);
        SNPAnnotationStageParameters snpParameters = JsonUtil.toObject(
                snpAnnotationStage.getParameters(), SNPAnnotationStageParameters.class);
        assertEquals(expectedAccessions,
                accessions(snpParameters.getReferenceGenome()));
    }

    private static void assertCompactUrlResult(BioPipelineStage stage) throws Exception {
        String outputJson = stage.getOutputUrl();
        assertNotNull(outputJson);
        assertTrue(outputJson.length() <= 500,
                "URL result exceeds output_url capacity for stage " + stage.getStageId()
                        + ": " + outputJson.length() + " characters");
        Map<String, Object> result = JsonUtil.toMap(outputJson);
        assertFalse(result.containsKey("metrics"), "Detailed metrics must remain in the metrics file");
        assertFalse(result.containsKey("qcMetrics"), "Legacy embedded metrics must not be written");
    }

    private static void assertSelectionCompleteness(
            VirusPipelineTestCase testCase,
            ReferenceGenome selectedReference,
            List<ReferenceCandidateScore> candidateScores,
            ReferenceSelectionStageResult selectionResult) {
        assertEquals(testCase.expectedUnmatchedSegmentKeys(),
                selectionResult.getUnmatchedSegmentKeys());
        assertEquals(!testCase.expectedUnmatchedSegmentKeys().isEmpty(),
                selectionResult.isPartial());

        Set<String> selectedSegmentKeys = selectedReference.getSequences().stream()
                .map(ReferenceSequence::getSegmentKey)
                .collect(java.util.stream.Collectors.toSet());
        for (String unmatchedSegmentKey : testCase.expectedUnmatchedSegmentKeys()) {
            assertTrue(!selectedSegmentKeys.contains(unmatchedSegmentKey),
                    "Unmatched segment must not be selected: "
                            + unmatchedSegmentKey);

            List<ReferenceCandidateScore> unmatchedScores = candidateScores.stream()
                    .filter(score -> unmatchedSegmentKey.equals(
                            score.getSegmentKey()))
                    .toList();
            assertTrue(!unmatchedScores.isEmpty(),
                    "Expected unmatched candidate scores for "
                            + unmatchedSegmentKey);
            for (ReferenceCandidateScore score : unmatchedScores) {
                assertEquals(0L, score.getAlignedReferenceBases(),
                        "Unmatched candidate must not have aligned bases: "
                                + score.getReferenceAccession());
                assertEquals(0.0d, score.getScore(), 0.0d,
                        "Unmatched candidate must have zero score: "
                                + score.getReferenceAccession());
            }
        }
    }

    private void assertStored(String objectName) throws IOException {
        assertNotNull(objectName);
        assertTrue(storageService.exists(objectName),
                "Stored object does not exist: " + objectName);

        Path path = TEST_STORAGE_ROOT.resolve(objectName);
        assertTrue(Files.isRegularFile(path),
                "Stored object is not a regular file: " + path);
        assertTrue(Files.size(path) > 0L,
                "Stored object is empty: " + path);
    }

    private void assertStoredAllowEmpty(String objectName) throws IOException {
        assertNotNull(objectName);
        assertTrue(storageService.exists(objectName),
                "Stored object does not exist: " + objectName);

        Path path = TEST_STORAGE_ROOT.resolve(objectName);
        assertTrue(Files.isRegularFile(path),
                "Stored object is not a regular file: " + path);
    }

    private static long countFastaRecords(Path fastaPath) throws IOException {
        try (var lines = Files.lines(fastaPath, StandardCharsets.UTF_8)) {
            return lines.filter(line -> line.startsWith(">"))
                    .count();
        }
    }

    private static long countFastaNs(Path fastaPath) throws IOException {
        try (var lines = Files.lines(fastaPath, StandardCharsets.UTF_8)) {
            return lines.filter(line -> !line.startsWith(">"))
                    .flatMapToInt(String::chars)
                    .filter(base -> base == 'N' || base == 'n')
                    .count();
        }
    }

    private static List<String> accessions(ReferenceGenome referenceGenome) {
        assertNotNull(referenceGenome);
        assertNotNull(referenceGenome.getSequences());
        return referenceGenome.getSequences().stream()
                .map(ReferenceSequence::getAccession)
                .toList();
    }

    private static void printSelectionResult(
            String testCaseName,
            ReferenceSelectionStageResult selectionResult) {
        ReferenceGenome selectedReference = selectionResult.getSelectedReference();
        Map<String, ReferenceCandidateScore> scoreByAccession = new HashMap<>();
        for (ReferenceCandidateScore score : selectionResult.getCandidateScores()) {
            scoreByAccession.put(score.getReferenceAccession(), score);
        }

        System.out.println();
        System.out.println(
                "================ Virus reference selection segments ================");
        System.out.println("case: " + testCaseName);
        System.out.println("partial: " + selectionResult.isPartial()
                + ", unmatchedSegmentKeys="
                + selectionResult.getUnmatchedSegmentKeys());
        System.out.printf(
                "%-10s %-8s %-14s %-12s %-11s %-11s %-14s %s%n",
                "segmentKey",
                "segment",
                "accession",
                "referenceId",
                "identity(%)",
                "coverage(%)",
                "score",
                "path");

        selectedReference.getSequences().stream()
                .sorted((left, right) -> {
                    Integer leftOrdinal = left.getSegmentOrdinal();
                    Integer rightOrdinal = right.getSegmentOrdinal();
                    if (leftOrdinal != null && rightOrdinal != null) {
                        return leftOrdinal.compareTo(rightOrdinal);
                    }
                    if (leftOrdinal != null) {
                        return -1;
                    }
                    if (rightOrdinal != null) {
                        return 1;
                    }
                    return Objects.toString(left.getSegmentKey(), "")
                            .compareTo(Objects.toString(
                                    right.getSegmentKey(), ""));
                })
                .forEach(sequence -> {
                    ReferenceCandidateScore score = scoreByAccession.get(
                            sequence.getAccession());
                    System.out.printf(
                            "%-10s %-8s %-14s %-12d %-11.4f %-11.4f %-14.6f %s%n",
                            sequence.getSegmentKey(),
                            sequence.getSegment(),
                            sequence.getAccession(),
                            sequence.getReferenceId(),
                            score == null ? 0.0d : score.getSequenceIdentity(),
                            score == null ? 0.0d
                                    : score.getReferenceCoverage() * 100.0d,
                            score == null ? 0.0d : score.getScore(),
                            sequence.getPath());
                });
        System.out.println(
                "====================================================================");
    }

    private static BioPipelineStage findStage(
            List<BioPipelineStage> stages,
            int stageType) {
        return stages.stream()
                .filter(stage -> stage.getStageType() == stageType)
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "Stage does not exist, type=" + stageType));
    }

    private List<ReferenceSequence> loadReferenceSequences(
            List<Long> referenceIds,
            int targetTaxId) {
        return referenceIds.stream()
                .map(referenceId -> loadReferenceSequence(referenceId, targetTaxId))
                .toList();
    }

    private ReferenceSequence loadReferenceSequence(
            Long referenceId,
            int targetTaxId) {
        BioReferenceSequence source = bioReferenceSequenceMapper.selectByPrimaryKey(
                referenceId);
        assertNotNull(source,
                "Reference sequence does not exist: " + referenceId);
        return new ReferenceSequence(
                source.getReferenceId(),
                source.getAccession(),
                source.getSourceDb(),
                targetTaxId,
                source.getOrganismName(),
                source.getGenomeLength(),
                source.getCompleteness(),
                source.getIsAnnotated(),
                source.getGeneCount(),
                source.getProteinCount(),
                source.getSegment(),
                source.getBioproject(),
                source.getReleaseDate(),
                source.getUpdateDate(),
                source.getOrgType(),
                source.getPath(),
                source.getAnnotationFile(),
                source.getRawMetadata());
    }

    private record VirusPipelineTestCase(
            String name,
            String read1ObjectName,
            String read2ObjectName,
            long pipelineId,
            long firstStageId,
            int targetTaxId,
            int expectedSelectedSequenceCount,
            List<String> expectedUnmatchedSegmentKeys,
            List<Long> candidateReferenceIds) {
    }

}
