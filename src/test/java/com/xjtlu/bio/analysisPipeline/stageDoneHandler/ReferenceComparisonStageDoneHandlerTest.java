package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FAIL;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FINISHED;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_REFERENCE_COMPARISON;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.xjtlu.bio.analysisPipeline.BioStageUtil;
import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.stageMetrics.ReferenceComparisonMetrics;
import com.xjtlu.bio.analysisPipeline.stageResult.ReferenceComparisonResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReferenceComparisonStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReferenceComparisonStageOutput.CandidateComparison;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.service.StorageService;
import com.xjtlu.bio.service.command.UpdateStageCommand;
import com.xjtlu.bio.utils.JsonUtil;

class ReferenceComparisonStageDoneHandlerTest {

    private static final JsonMapper JSON = new JsonMapper();
    private static final String PREFIX = "stageOutput/42/";

    @TempDir
    Path tempDir;

    private ReferenceComparisonStageDoneHandler handler;
    private PipelineService pipelineService;
    private StorageService storageService;
    private final Map<String, byte[]> uploaded = new LinkedHashMap<>();

    @BeforeEach
    void setUp() {
        pipelineService = mock(PipelineService.class);
        storageService = mock(StorageService.class);
        when(pipelineService.updateStageFromVersion(any(UpdateStageCommand.class))).thenReturn(1);
        when(storageService.putObject(anyString(), any(InputStream.class))).thenAnswer(invocation -> {
            uploaded.put(invocation.getArgument(0), invocation.<InputStream>getArgument(1).readAllBytes());
            return new StorageService.PutResult(true, null);
        });
        handler = new ReferenceComparisonStageDoneHandler();
        handler.pipelineService = pipelineService;
        handler.storageService = storageService;
        handler.bioStageUtil = new BioStageUtil();
    }

    @Test
    void uploadsEveryCandidateAndRewritesManifestPathsBeforeFinishing() throws Exception {
        StageRunResult<ReferenceComparisonStageOutput> run = runResult("MATCHED", "NO_MATCH");

        handler.handleStageDone(run);

        UpdateStageCommand command = capturedUpdate();
        BioPipelineStage patch = command.getUpdateStage();
        assertEquals(42L, command.getStageId());
        assertEquals(3, command.getCurrentVersion());
        assertEquals(4, patch.getVersion());
        assertEquals(PIPELINE_STAGE_STATUS_FINISHED, patch.getStatus());
        assertNotNull(patch.getEndTime());
        assertNull(patch.getOutputInline());

        ReferenceComparisonResult result = JsonUtil.toObject(patch.getOutputUrl(), ReferenceComparisonResult.class);
        assertEquals(2, result.getSchemaVersion());
        assertEquals(2, result.getCandidateCount());
        assertEquals(1, result.getMatchedCandidateCount());
        assertEquals(1, result.getUnmatchedCandidateCount());
        assertEquals(PREFIX + "reference_comparison.json", result.getComparisonJsonUrl());
        assertEquals(PREFIX + "reference_comparison.tsv", result.getComparisonTsvUrl());
        assertNull(result.getPafPath());
        assertNull(result.getDifferenceTsv());
        assertFalse(patch.getOutputUrl().contains("comparisons"));
        assertTrue(patch.getOutputUrl().length() <= 500);

        assertEquals(6, uploaded.size());
        JsonNode manifest = uploadedJson(result.getComparisonJsonUrl());
        assertEquals(2, manifest.path("schemaVersion").asInt());
        assertEquals(2, manifest.path("candidateCount").asInt());
        assertEquals(1, manifest.path("matchedCandidateCount").asInt());
        assertEquals(1, manifest.path("unmatchedCandidateCount").asInt());
        JsonNode comparisons = manifest.path("comparisons");
        assertEquals(2, comparisons.size());
        JsonNode matched = comparisons.get(0);
        assertEquals(101L, matched.path("referenceId").asLong());
        assertEquals("DUPLICATE_ACCESSION", matched.path("referenceAccession").asText());
        assertEquals("USER_UPLOAD", matched.path("sourceDb").asText());
        assertEquals(123, matched.path("taxId").asInt());
        assertEquals("Synthetic fixture", matched.path("organismName").asText());
        assertEquals("RNA 1", matched.path("segment").asText());
        assertEquals("SEGMENT_1", matched.path("segmentKey").asText());
        assertEquals("MATCHED", matched.path("status").asText());
        assertEquals(98.75d, matched.path("sequenceIdentity").asDouble());
        assertEquals(0.8d, matched.path("referenceCoverage").asDouble());
        assertEquals(80L, matched.path("alignedReferenceBases").asLong());
        assertEquals(100L, matched.path("referenceBases").asLong());
        assertEquals(79L, matched.path("matchingBases").asLong());
        assertEquals(80L, matched.path("alignmentBlockBases").asLong());
        assertEquals(1, matched.path("alignmentCount").asInt());

        JsonNode unmatched = comparisons.get(1);
        assertEquals("DUPLICATE_ACCESSION", unmatched.path("referenceAccession").asText());
        assertTrue(unmatched.path("sourceDb").isNull());
        assertTrue(unmatched.path("segment").isNull());
        assertEquals("NO_MATCH", unmatched.path("status").asText());
        assertEquals(0.0d, unmatched.path("sequenceIdentity").asDouble());
        assertEquals(0.0d, unmatched.path("referenceCoverage").asDouble());
        assertEquals(0, unmatched.path("alignmentCount").asInt());
        for (int index = 0; index < comparisons.size(); index++) {
            String candidatePrefix = PREFIX + "candidates/000" + (index + 1) + "/";
            JsonNode candidate = comparisons.get(index);
            assertEquals(candidatePrefix + "alignment.paf", candidate.path("alignmentPafUrl").asText());
            assertEquals(candidatePrefix + "reference_difference.tsv", candidate.path("differenceTsvUrl").asText());
            assertTrue(uploaded.containsKey(candidate.path("alignmentPafUrl").asText()));
            assertTrue(uploaded.containsKey(candidate.path("differenceTsvUrl").asText()));
            assertFalse(candidate.has("alignmentPafPath"));
            assertFalse(candidate.has("differenceTsvPath"));
        }
        assertEquals("paf fixture 1\n", uploadedText(PREFIX + "candidates/0001/alignment.paf"));
        assertEquals("", uploadedText(PREFIX + "candidates/0002/alignment.paf"));
        assertEquals("referenceAccession\tstatus\n", uploadedText(result.getComparisonTsvUrl()));
        String report = uploadedText(result.getComparisonJsonUrl());
        assertFalse(report.contains(tempDir.toString()));
        assertFalse(report.contains("selectedReference"));
        assertFalse(report.contains("bestReference"));
        assertFalse(Files.exists(run.getWorkDir()));
        verify(pipelineService).pipelineStageDone(42L, true);
    }

    @Test
    void allNoMatchIsACompletedReportNotAStageFailure() throws Exception {
        handler.handleStageDone(runResult("NO_MATCH", "NO_MATCH"));

        BioPipelineStage patch = capturedUpdate().getUpdateStage();
        ReferenceComparisonResult result = JsonUtil.toObject(patch.getOutputUrl(), ReferenceComparisonResult.class);
        assertEquals(PIPELINE_STAGE_STATUS_FINISHED, patch.getStatus());
        assertEquals(2, result.getCandidateCount());
        assertEquals(0, result.getMatchedCandidateCount());
        assertEquals(2, result.getUnmatchedCandidateCount());
        assertEquals(2, uploadedJson(result.getComparisonJsonUrl()).path("comparisons").size());
        verify(pipelineService).pipelineStageDone(42L, true);
    }

    @Test
    void largeCandidateReportDoesNotExpandDatabaseOutput() throws Exception {
        String[] statuses = new String[120];
        java.util.Arrays.fill(statuses, "MATCHED");

        handler.handleStageDone(runResult(statuses));

        BioPipelineStage patch = capturedUpdate().getUpdateStage();
        ReferenceComparisonResult result = JsonUtil.toObject(patch.getOutputUrl(), ReferenceComparisonResult.class);
        assertTrue(patch.getOutputUrl().length() <= 500);
        assertNull(patch.getOutputInline());
        assertEquals(120, result.getCandidateCount());
        assertEquals(120, result.getMatchedCandidateCount());
        assertEquals(0, result.getUnmatchedCandidateCount());
        assertEquals(242, uploaded.size());
        assertTrue(uploadedText(result.getComparisonJsonUrl()).length() > 10_000);
        assertEquals(120, uploadedJson(result.getComparisonJsonUrl()).path("comparisons").size());
    }

    @Test
    void legacyOutputStillPublishesAndReadsOriginalTwoFileContract() throws Exception {
        Path workDir = Files.createDirectory(tempDir.resolve("legacy"));
        Path paf = Files.writeString(workDir.resolve("alignment.paf"), "legacy paf\n");
        Path difference = Files.writeString(workDir.resolve("reference_difference.tsv"), "legacy tsv\n");

        handler.handleStageDone(runResult(new ReferenceComparisonStageOutput(paf, difference), workDir));

        BioPipelineStage patch = capturedUpdate().getUpdateStage();
        assertEquals(PIPELINE_STAGE_STATUS_FINISHED, patch.getStatus());
        assertNull(patch.getOutputInline());
        JsonNode output = JSON.readTree(patch.getOutputUrl());
        assertEquals(2, output.size());
        assertEquals(PREFIX + "alignment.paf", output.path("pafPath").asText());
        assertEquals(PREFIX + "reference_difference.tsv", output.path("differenceTsv").asText());
        assertEquals(2, uploaded.size());
        assertEquals("legacy paf\n", uploadedText(PREFIX + "alignment.paf"));
        assertEquals("legacy tsv\n", uploadedText(PREFIX + "reference_difference.tsv"));
        ReferenceComparisonResult old = JsonUtil.toObject(
                "{\"pafPath\":\"old/alignment.paf\",\"differenceTsv\":\"old/difference.tsv\"}",
                ReferenceComparisonResult.class);
        assertEquals("old/alignment.paf", old.getPafPath());
        assertEquals("old/difference.tsv", old.getDifferenceTsv());
        assertNull(old.getSchemaVersion());
        assertNull(old.getCandidateCount());
        assertNull(old.getComparisonJsonUrl());
        verify(pipelineService).pipelineStageDone(42L, true);
    }

    @Test
    void uploadFailureDoesNotPersistACompletedComparisonIndex() throws Exception {
        doReturn(new StorageService.PutResult(false, null)).when(storageService)
                .putObject(anyString(), any(InputStream.class));

        handler.handleStageDone(runResult("MATCHED"));

        BioPipelineStage patch = capturedUpdate().getUpdateStage();
        assertEquals(PIPELINE_STAGE_STATUS_FAIL, patch.getStatus());
        assertNull(patch.getOutputUrl());
        assertNull(patch.getOutputInline());
        verify(pipelineService).pipelineStageDone(42L, false);
    }

    @Test
    void manifestIoFailureIsUncheckedAndHandledByCompletionBoundary() throws Exception {
        StageRunResult<ReferenceComparisonStageOutput> valid = runResult("MATCHED");
        ReferenceComparisonStageOutput output = new ReferenceComparisonStageOutput(
                valid.getStageOutput().getComparisons(),
                valid.getWorkDir().resolve("missing-parent/reference_comparison.tsv"));
        StageRunResult<ReferenceComparisonStageOutput> run = runResult(output, valid.getWorkDir());
        assertThrows(UncheckedIOException.class, () -> handler.buildUploadConfigAndOutputUrlMap(run));

        handler.handleStageDone(run);

        assertEquals(PIPELINE_STAGE_STATUS_FAIL, capturedUpdate().getUpdateStage().getStatus());
        verifyNoInteractions(storageService);
        verify(pipelineService).pipelineStageDone(42L, false);
    }

    private StageRunResult<ReferenceComparisonStageOutput> runResult(String... statuses) throws Exception {
        Path workDir = Files.createDirectory(tempDir.resolve("work"));
        Path summary = Files.writeString(workDir.resolve("reference_comparison.tsv"), "referenceAccession\tstatus\n");
        List<CandidateComparison> comparisons = new ArrayList<>();
        for (int index = 0; index < statuses.length; index++) {
            boolean matched = "MATCHED".equals(statuses[index]);
            Path candidateDir = Files.createDirectory(workDir.resolve("local-" + index));
            Path paf = Files.writeString(candidateDir.resolve("alignment.paf"),
                    matched ? "paf fixture " + (index + 1) + "\n" : "");
            Path difference = Files.writeString(candidateDir.resolve("reference_difference.tsv"), "type\tposition\n");
            ReferenceComparisonMetrics metrics = new ReferenceComparisonMetrics(
                    101L + index, "DUPLICATE_ACCESSION", index == 0 ? "USER_UPLOAD" : null,
                    123, "Synthetic fixture", index == 0 ? "RNA 1" : null,
                    index == 0 ? "SEGMENT_1" : "UNSEGMENTED", statuses[index],
                    matched ? 98.75d : 0.0d, matched ? 0.8d : 0.0d,
                    matched ? 80L : 0L, 100L, matched ? 79L : 0L,
                    matched ? 80L : 0L, matched ? 1 : 0);
            comparisons.add(new CandidateComparison(metrics, paf, difference));
        }
        return runResult(new ReferenceComparisonStageOutput(comparisons, summary), workDir);
    }

    private StageRunResult<ReferenceComparisonStageOutput> runResult(
            ReferenceComparisonStageOutput output, Path workDir) {
        return StageRunResult.OK(output, new StageContext(42L, 3, PIPELINE_STAGE_REFERENCE_COMPARISON, 7), workDir);
    }

    private UpdateStageCommand capturedUpdate() {
        ArgumentCaptor<UpdateStageCommand> captor = ArgumentCaptor.forClass(UpdateStageCommand.class);
        verify(pipelineService).updateStageFromVersion(captor.capture());
        return captor.getValue();
    }

    private JsonNode uploadedJson(String key) throws Exception {
        return JSON.readTree(uploaded.get(key));
    }

    private String uploadedText(String key) {
        return new String(uploaded.get(key), StandardCharsets.UTF_8);
    }
}
