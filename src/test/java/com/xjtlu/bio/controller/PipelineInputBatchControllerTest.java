package com.xjtlu.bio.controller;

import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.mock.web.MockPart;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.util.MultiValueMap;
import org.springframework.web.multipart.MultipartFile;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.requestParameters.BatchPipelineInputUploadRequest;
import com.xjtlu.bio.response.BatchPipelineInputUploadResponse;
import com.xjtlu.bio.service.PipelineInputBatchService;

/** Multipart protocol tests only; no pipeline execution, database or storage. */
class PipelineInputBatchControllerTest {
    private static final String URL = "/pipelineInput/uploadBatch";
    private static final String SINGLE = """
            {"pipelineId":123,"sequenceLevel":0,"inputs":[{"inputKey":"sample/0/0","filePart":"reads"}]}
            """;
    private PipelineInputBatchService service;
    private ObjectMapper globalMapper;
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        service = mock(PipelineInputBatchService.class);
        globalMapper = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        mvc = MockMvcBuilders.standaloneSetup(new PipelineInputBatchController(service, globalMapper)).build();
        when(service.uploadBatch(any(), any())).thenReturn(new Result<>(Result.SUCCESS, response(), null));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void browserBlobManifestAndNamedFilesArePassedAsOneCompleteBatch() throws Exception {
        String paired = """
                {"pipelineId":123,"sequenceLevel":0,"inputs":[
                  {"inputKey":"sample/0/0","filePart":"r1"},
                  {"inputKey":"sample/0/1","filePart":"r2"}]}
                """;
        MockMultipartFile r1 = file("r1", "reads_R1.fastq.gz");
        MockMultipartFile r2 = file("r2", "reads_R2.fastq.gz");
        mvc.perform(multipart(URL).file(r2).file(manifest(paired)).file(r1))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value(Result.SUCCESS))
                .andExpect(jsonPath("$.data.sequenceLevel").value(0))
                .andExpect(jsonPath("$.data.inputs[0].inputFileId").value(31))
                .andExpect(jsonPath("$.data.inputs[0].fileType").doesNotExist())
                .andExpect(jsonPath("$.data.inputs[0].filePath").doesNotExist())
                .andExpect(jsonPath("$.data.removedInputFileIds").doesNotExist());

        ArgumentCaptor<BatchPipelineInputUploadRequest> dto = ArgumentCaptor.forClass(BatchPipelineInputUploadRequest.class);
        ArgumentCaptor<MultiValueMap<String, MultipartFile>> files = ArgumentCaptor.forClass((Class) MultiValueMap.class);
        verify(service).uploadBatch(dto.capture(), files.capture());
        assertEquals(123L, dto.getValue().getPipelineId());
        assertEquals(0, dto.getValue().getSequenceLevel());
        assertEquals(List.of("r1", "r2"), dto.getValue().getInputs().stream()
                .map(BatchPipelineInputUploadRequest.Input::getFilePart).toList());
        assertEquals(2, files.getValue().size());
        assertFalse(files.getValue().containsKey("manifest"));
        assertSame(r1, files.getValue().getFirst("r1"));
        assertSame(r2, files.getValue().getFirst("r2"));
    }

    @Test
    void jsonPartWithoutFilenameIsAlsoSupported() throws Exception {
        mvc.perform(multipart(URL).part(jsonPart(SINGLE)).file(file("reads", "reads.fastq")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value(Result.SUCCESS));
        verify(service).uploadBatch(any(), any());
    }

    @Test
    void servletFileManifestExposedAsPartAndMultipartFileIsCountedOnlyOnce() throws Exception {
        MockPart part = new MockPart("manifest", "blob", SINGLE.getBytes(StandardCharsets.UTF_8));
        part.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        mvc.perform(multipart(URL).part(part).file(file("reads", "reads.fastq")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value(Result.SUCCESS));
    }

    @Test
    void assemblySequenceLevelIsPassedThroughWithoutConvertingItToAFileRole() throws Exception {
        mvc.perform(multipart(URL).file(manifest(SINGLE.replace("\"sequenceLevel\":0", "\"sequenceLevel\":1")))
                        .file(file("reads", "assembly.fasta")))
                .andExpect(status().isOk());
        ArgumentCaptor<BatchPipelineInputUploadRequest> dto = ArgumentCaptor.forClass(BatchPipelineInputUploadRequest.class);
        verify(service).uploadBatch(dto.capture(), any());
        assertEquals(1, dto.getValue().getSequenceLevel());
    }

    @ParameterizedTest
    @ValueSource(strings = {"\"0\"", "0.0", "0.5", "true", "[]", "{}", "2147483648"})
    void sequenceLevelMustBeAnActualIntegerWithoutJacksonCoercion(String invalidLevel) throws Exception {
        mvc.perform(multipart(URL).file(manifest(SINGLE.replace("\"sequenceLevel\":0", "\"sequenceLevel\":" + invalidLevel)))
                        .file(file("reads", "reads.fastq")))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(Result.PARAMETER_NOT_VALID))
                .andExpect(jsonPath("$.data").value(nullValue()));
        verifyNoInteractions(service);
    }

    @ParameterizedTest
    @ValueSource(strings = {"\"sequenceLevel\":null,", ""})
    void missingOrNullSequenceLevelRemainsNullForRequiredFieldValidation(String levelField) throws Exception {
        when(service.uploadBatch(any(), any())).thenReturn(new Result<>(Result.PARAMETER_NOT_VALID, null, "sequenceLevel 必填"));
        mvc.perform(multipart(URL).file(manifest(SINGLE.replace("\"sequenceLevel\":0,", levelField)))
                        .file(file("reads", "reads.fastq")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value(Result.PARAMETER_NOT_VALID));
        ArgumentCaptor<BatchPipelineInputUploadRequest> dto = ArgumentCaptor.forClass(BatchPipelineInputUploadRequest.class);
        verify(service).uploadBatch(dto.capture(), any());
        assertNull(dto.getValue().getSequenceLevel());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"pipelineId\":123,\"sequenceLevel\":0,\"fileType\":10,\"inputs\":[]}",
            "{\"pipelineId\":123,\"sequenceLevel\":0,\"inputs\":[{\"inputKey\":\"sample/0/0\",\"filePart\":\"reads\",\"fileType\":10}]}",
            "{\"pipelineId\":123,\"sequenceLevel\":0,\"inputs\":[{\"inputKey\":\"sample/0/0\",\"filePart\":\"reads\",\"inputFileId\":1}]}",
            "{\"pipelineId\":123,\"sequenceLevel\":0,\"inputs\":[{\"filePart\":\"reads\",\"filePath\":\"/tmp/client\"}]}",
            "{\"pipelineId\":\"123\",\"sequenceLevel\":0,\"inputs\":[]}",
            "{\"pipelineId\":123,\"sequenceLevel\":0,\"inputs\":[{\"inputKey\":123,\"filePart\":\"reads\"}]}",
            "{\"pipelineId\":123,\"sequenceLevel\":0,\"inputs\":[{\"inputKey\":\"sample/0/0\",\"filePart\":42}]}"
    })
    void rejectsLegacyFieldsAndUnexpectedScalarTypes(String json) throws Exception {
        mvc.perform(multipart(URL).file(manifest(json)).file(file("reads", "reads.fastq")))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(Result.PARAMETER_NOT_VALID));
        verifyNoInteractions(service);
        assertFalse(globalMapper.isEnabled(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "{", "{}{}", "[]", "{\"sequenceLevel\":0,\"sequenceLevel\":1}"})
    void rejectsMalformedAmbiguousOrNonObjectJson(String json) throws Exception {
        mvc.perform(multipart(URL).file(manifest(json)).file(file("reads", "reads.fastq")))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(Result.PARAMETER_NOT_VALID));
        verifyNoInteractions(service);
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"text/plain", "application/octet-stream", "application/problem+json"})
    void manifestRequiresApplicationJsonContentType(String contentType) throws Exception {
        MockMultipartFile invalidManifest = new MockMultipartFile("manifest", "blob", contentType,
                SINGLE.getBytes(StandardCharsets.UTF_8));
        mvc.perform(multipart(URL).file(invalidManifest).file(file("reads", "reads.fastq")))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @Test
    void jsonCharsetParameterIsAllowed() throws Exception {
        MockMultipartFile manifest = new MockMultipartFile("manifest", "manifest.json", "application/json;charset=UTF-8",
                SINGLE.getBytes(StandardCharsets.UTF_8));
        mvc.perform(multipart(URL).file(manifest).file(file("reads", "reads.fastq")))
                .andExpect(status().isOk());
    }

    @Test
    void missingManifestIsRejected() throws Exception {
        mvc.perform(multipart(URL).file(file("reads", "reads.fastq")))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @Test
    void duplicateManifestFilesAreRejected() throws Exception {
        mvc.perform(multipart(URL).file(manifest(SINGLE)).file(manifest(SINGLE)).file(file("reads", "reads.fastq")))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @Test
    void duplicatePlainJsonManifestPartsAreRejected() throws Exception {
        mvc.perform(multipart(URL).part(jsonPart(SINGLE)).part(jsonPart(SINGLE)).file(file("reads", "reads.fastq")))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @Test
    void plainJsonAndBlobManifestsCannotBeCombined() throws Exception {
        mvc.perform(multipart(URL).part(jsonPart(SINGLE)).file(manifest(SINGLE)).file(file("reads", "reads.fastq")))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @Test
    void separateTextFieldsCannotOverrideManifestFields() throws Exception {
        mvc.perform(multipart(URL).file(manifest(SINGLE)).file(file("reads", "reads.fastq"))
                        .param("sequenceLevel", "1"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @Test
    void extraPlainPartIsRejectedEvenWithoutARequestParameterEntry() throws Exception {
        mvc.perform(multipart(URL).file(manifest(SINGLE)).file(file("reads", "reads.fastq"))
                        .part(new MockPart("unexpected", "value".getBytes(StandardCharsets.UTF_8))))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @Test
    void ordinaryManifestFormParameterCannotReplaceAJsonPart() throws Exception {
        mvc.perform(multipart(URL).file(file("reads", "reads.fastq")).param("manifest", SINGLE))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void duplicateFilesAreNotSilentlyCollapsedBeforeServiceValidation() throws Exception {
        when(service.uploadBatch(any(), any())).thenReturn(new Result<>(Result.PARAMETER_NOT_VALID, null, "重复文件 Part"));
        mvc.perform(multipart(URL).file(manifest(SINGLE)).file(file("reads", "one.fastq")).file(file("reads", "two.fastq")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value(Result.PARAMETER_NOT_VALID));
        ArgumentCaptor<MultiValueMap<String, MultipartFile>> files = ArgumentCaptor.forClass((Class) MultiValueMap.class);
        verify(service).uploadBatch(any(), files.capture());
        assertEquals(2, files.getValue().get("reads").size());
    }

    @ParameterizedTest
    @ValueSource(ints = {Result.SUCCESS, Result.BUSINESS_FAIL, Result.DUPLICATE_OPERATION, Result.PARAMETER_NOT_VALID})
    void businessResultsKeepHttp200(int resultStatus) throws Exception {
        when(service.uploadBatch(any(), any())).thenReturn(new Result<>(resultStatus, null, "service message"));
        mvc.perform(multipart(URL).file(manifest(SINGLE)).file(file("reads", "reads.fastq")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value(resultStatus))
                .andExpect(jsonPath("$.failMsg").value("service message"));
    }

    @Test
    void internalFailureReturnsHttp500WithResultBodyRatherThanPlainText() throws Exception {
        when(service.uploadBatch(any(), any())).thenReturn(new Result<>(Result.INTERNAL_FAIL, null, "整批上传失败"));
        mvc.perform(multipart(URL).file(manifest(SINGLE)).file(file("reads", "reads.fastq")))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.status").value(Result.INTERNAL_FAIL))
                .andExpect(jsonPath("$.data").value(nullValue()))
                .andExpect(jsonPath("$.failMsg").value("整批上传失败"));
    }

    @Test
    void oversizedMetadataIsRejectedWithoutCallingTheService() throws Exception {
        mvc.perform(multipart(URL).file(manifest(" ".repeat(1024 * 1024 + 1))).file(file("reads", "reads.fastq")))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @Test
    void endpointDoesNotAcceptAJsonOnlyRequest() throws Exception {
        mvc.perform(post(URL).contentType(MediaType.APPLICATION_JSON).content(SINGLE))
                .andExpect(status().isUnsupportedMediaType());
        verifyNoInteractions(service);
    }

    private static MockMultipartFile manifest(String json) {
        return new MockMultipartFile("manifest", "blob", MediaType.APPLICATION_JSON_VALUE,
                json.getBytes(StandardCharsets.UTF_8));
    }

    private static MockPart jsonPart(String json) {
        MockPart part = new MockPart("manifest", json.getBytes(StandardCharsets.UTF_8));
        part.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        return part;
    }

    private static MockMultipartFile file(String partName, String fileName) {
        return new MockMultipartFile(partName, fileName, MediaType.APPLICATION_OCTET_STREAM_VALUE,
                "test upload bytes".getBytes(StandardCharsets.UTF_8));
    }

    private static BatchPipelineInputUploadResponse response() {
        BatchPipelineInputUploadResponse response = new BatchPipelineInputUploadResponse();
        response.setPipelineId(123L);
        response.setSequenceLevel(0);
        BatchPipelineInputUploadResponse.Input input = new BatchPipelineInputUploadResponse.Input();
        input.setInputFileId(31L);
        input.setInputKey("sample/0/0");
        input.setFileName("reads_R1.fastq.gz");
        response.setInputs(List.of(input));
        return response;
    }
}
