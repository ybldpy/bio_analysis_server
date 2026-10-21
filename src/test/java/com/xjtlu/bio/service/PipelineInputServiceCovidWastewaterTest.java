package com.xjtlu.bio.service;

import static com.xjtlu.bio.analysisPipeline.Constants.PipelineType.PIPELINE_VIRUS_COVID_WASTEWATER;
import static com.xjtlu.bio.service.PipelineInputService.PIPELINE_INPUT_TYPE_REFSEQ;
import static com.xjtlu.bio.service.PipelineInputService.PIPELINE_INPUT_TYPE_SEQUENCE_ASSEMBLY;
import static com.xjtlu.bio.service.PipelineInputService.PIPELINE_INPUT_TYPE_SEQUENCE_READ;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioAnalysisPipeline;
import com.xjtlu.bio.entity.BioPipelineInputFile;
import com.xjtlu.bio.mapper.BioPipelineInputFileMapper;

/** Upload contract only: storage, pipeline lookup and persistence are mocks. */
class PipelineInputServiceCovidWastewaterTest {

    private PipelineInputService service;
    private BioPipelineInputFileMapper inputMapper;
    private StorageService storage;
    private BioAnalysisPipeline pipeline;

    @BeforeEach
    void setUp() {
        service = new PipelineInputService();
        inputMapper = mock(BioPipelineInputFileMapper.class);
        storage = mock(StorageService.class);
        PipelineService pipelineService = mock(PipelineService.class);
        ReflectionTestUtils.setField(service, "bioPipelineInputFileMapper", inputMapper);
        ReflectionTestUtils.setField(service, "storageService", storage);
        ReflectionTestUtils.setField(service, "pipelineService", pipelineService);

        pipeline = new BioAnalysisPipeline();
        pipeline.setPipelineId(102L);
        pipeline.setPipelineType(PIPELINE_VIRUS_COVID_WASTEWATER);
        pipeline.setStatus(PipelineService.PIPELINE_STATUS_PENDING);
        when(pipelineService.queryPipelines(any())).thenReturn(new Result<>(Result.SUCCESS, List.of(pipeline), null));
        when(inputMapper.selectByExample(any())).thenReturn(List.of());
        when(inputMapper.insertSelective(any())).thenAnswer(invocation -> {
            BioPipelineInputFile input = invocation.getArgument(0);
            input.setInputFileId(1000L);
            return 1;
        });
        when(storage.putObject(anyString(), any(InputStream.class))).thenReturn(new StorageService.PutResult(true, null));
    }

    @ParameterizedTest
    @ValueSource(strings = {"sample.fastq", "sample.fq", "sample.fastq.gz", "sample.fq.gz", "sample.fastq.gzip",
            "sample.fq.gzip", "sample.fastq.zip", "sample.fq.zip", "SAMPLE.FASTQ.GZ", "SAMPLE.FQ.ZIP"})
    void acceptsDeclaredReadFastqNamesIncludingExistingCompressionFormats(String fileName) {
        Result<Long> result = upload(fileName, PIPELINE_INPUT_TYPE_SEQUENCE_READ, "sample/0/0");

        assertEquals(Result.SUCCESS, result.getStatus());
        assertEquals(1000L, result.getData());
        ArgumentCaptor<BioPipelineInputFile> saved = ArgumentCaptor.forClass(BioPipelineInputFile.class);
        verify(inputMapper).insertSelective(saved.capture());
        assertEquals(fileName, saved.getValue().getFileName());
        assertEquals(PIPELINE_INPUT_TYPE_SEQUENCE_READ, saved.getValue().getFileRole());
        verify(storage).putObject(anyString(), any(InputStream.class));
    }

    @Test
    void preservesOptionalR2Slot() {
        Result<Long> result = upload("sample_R2.fastq.gz", PIPELINE_INPUT_TYPE_SEQUENCE_READ, "sample/0/1");

        assertEquals(Result.SUCCESS, result.getStatus());
        ArgumentCaptor<BioPipelineInputFile> saved = ArgumentCaptor.forClass(BioPipelineInputFile.class);
        verify(inputMapper).insertSelective(saved.capture());
        assertEquals("sample/0/1", saved.getValue().getInputKey());
    }

    @ParameterizedTest
    @ValueSource(ints = {PIPELINE_INPUT_TYPE_SEQUENCE_ASSEMBLY, PIPELINE_INPUT_TYPE_REFSEQ, 999})
    void rejectsNonReadRolesBeforeObjectStorage(int role) {
        Result<Long> result = upload("sample.fastq", role, "sample/0/0");

        assertRejectedBeforeUpload(result);
        assertTrue(result.getFailMsg().contains("READ"));
        assertTrue(result.getFailMsg().contains("新冠扩增子"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"sample.fasta", "sample.fa.gz", "sample.fna", "sample.txt", "sample.zip",
            "sample.fasta.zip", "sample.fastq.tar.gz", "sample.fastq.gz.zip", ""})
    void rejectsNonFastqFileNamesWithoutClaimingLibraryPreparationWasVerified(String fileName) {
        Result<Long> result = upload(fileName, PIPELINE_INPUT_TYPE_SEQUENCE_READ, "sample/0/0");

        assertRejectedBeforeUpload(result);
        assertTrue(result.getFailMsg().contains("FASTQ"));
        assertTrue(result.getFailMsg().contains("不能确认扩增子建库类型"));
    }

    @Test
    void stillRejectsMultipleSamplesUnderExistingUploadKeyRules() {
        Result<Long> result = upload("sample.fastq", PIPELINE_INPUT_TYPE_SEQUENCE_READ, "sample/1/0");

        assertEquals(PipelineInputService.RESPONSE_CODE_INVALID_KEY, result.getStatus());
        verifyNoInteractions(storage);
        verify(inputMapper, never()).insertSelective(any());
    }

    @ParameterizedTest
    @ValueSource(ints = {100, 101, 200, 300})
    void existingPipelineTypesStillAcceptAssembly(int pipelineType) {
        pipeline.setPipelineType(pipelineType);

        Result<Long> result = upload("contigs.fasta", PIPELINE_INPUT_TYPE_SEQUENCE_ASSEMBLY, "sample/0/0");

        assertEquals(Result.SUCCESS, result.getStatus());
    }

    @ParameterizedTest
    @ValueSource(ints = {100, 101, 200, 300})
    void existingPipelineTypesStillAcceptReferenceFiles(int pipelineType) {
        pipeline.setPipelineType(pipelineType);

        Result<Long> result = upload("reference.fasta", PIPELINE_INPUT_TYPE_REFSEQ, "reference/0");

        assertEquals(Result.SUCCESS, result.getStatus());
    }

    @Test
    void fastqWhitelistDoesNotChangeLegacyReadFilenamePolicy() {
        pipeline.setPipelineType(200);

        Result<Long> result = upload("legacy.txt", PIPELINE_INPUT_TYPE_SEQUENCE_READ, "sample/0/0");

        assertEquals(Result.SUCCESS, result.getStatus());
    }

    private Result<Long> upload(String fileName, int role, String key) {
        return service.createInputs(102L, fileName, role, key, InputStream.nullInputStream());
    }

    private void assertRejectedBeforeUpload(Result<Long> result) {
        assertEquals(Result.BUSINESS_FAIL, result.getStatus());
        verifyNoInteractions(storage);
        verify(inputMapper, never()).insertSelective(any());
    }
}
