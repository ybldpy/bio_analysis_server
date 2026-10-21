package com.xjtlu.bio.controller;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.requestParameters.ReferenceLibraryQuery;
import com.xjtlu.bio.requestParameters.UploadReferenceRequest;
import com.xjtlu.bio.response.ReferenceLibraryItem;
import com.xjtlu.bio.response.ReferenceLibraryPage;
import com.xjtlu.bio.service.ReferenceLibraryService;

/** Standalone HTTP contract tests; no security context, storage, live database or analysis. */
class ReferenceLibraryControllerTest {
    private static final String BASE = "/referenceGenome/library";
    private ReferenceLibraryService service;
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        service = mock(ReferenceLibraryService.class);
        mvc = MockMvcBuilders.standaloneSetup(new ReferenceLibraryController(service)).build();
    }

    @Test
    void queryBindsAllFiltersAndSerializesManagementPage() throws Exception {
        when(service.query(any())).thenReturn(new Result<>(Result.SUCCESS,
                new ReferenceLibraryPage(9, 2, 3, List.of(item(false))), null));

        mvc.perform(post(BASE + "/query").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"start\":2,\"pageSize\":3,\"taxId\":101,\"orgType\":2,\"enabled\":false,\"accession\":\"SYNTHETIC_001\"}"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.status").value(Result.SUCCESS))
                .andExpect(jsonPath("$.data.total").value(9))
                .andExpect(jsonPath("$.data.start").value(2))
                .andExpect(jsonPath("$.data.pageSize").value(3))
                .andExpect(jsonPath("$.data.items[0].referenceId").value(42))
                .andExpect(jsonPath("$.data.items[0].enabled").value(false))
                .andExpect(jsonPath("$.data.items[0].fastaObjectName").value("refseq/custom/synthetic/reference.fna"))
                .andExpect(jsonPath("$.data.items[0].path").doesNotExist())
                .andExpect(jsonPath("$.data.items[0].rawMetadata").doesNotExist())
                .andExpect(jsonPath("$.data.items[0].annotationFile").doesNotExist());

        ArgumentCaptor<ReferenceLibraryQuery> query = ArgumentCaptor.forClass(ReferenceLibraryQuery.class);
        verify(service).query(query.capture());
        assertEquals(2, query.getValue().getStart());
        assertEquals(3, query.getValue().getPageSize());
        assertEquals(101, query.getValue().getTaxId());
        assertEquals(2, query.getValue().getOrgType());
        assertFalse(query.getValue().getEnabled());
        assertEquals("SYNTHETIC_001", query.getValue().getAccession());
    }

    @Test
    void malformedQueryJsonOrNumberReturnsControlledHttp400WithoutCallingService() throws Exception {
        for (String json : List.of("{", "{\"start\":\"invalid\"}")) {
            mvc.perform(post(BASE + "/query").contentType(MediaType.APPLICATION_JSON).content(json))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.status").value(Result.PARAMETER_NOT_VALID))
                    .andExpect(jsonPath("$.data").value(nullValue()))
                    .andExpect(content().string(not(containsString("Exception"))));
        }
        verifyNoInteractions(service);
    }

    @Test
    void detailReturnsDisabledItemWithoutFilteringItOut() throws Exception {
        when(service.detail(42)).thenReturn(new Result<>(Result.SUCCESS, item(false), null));

        mvc.perform(get(BASE + "/42"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value(Result.SUCCESS))
                .andExpect(jsonPath("$.data.referenceId").value(42))
                .andExpect(jsonPath("$.data.enabled").value(false));

        verify(service).detail(42);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void enabledRequestBindsExplicitBoolean(boolean enabled) throws Exception {
        when(service.setEnabled(42, enabled)).thenReturn(new Result<>(Result.SUCCESS, item(enabled), null));

        mvc.perform(post(BASE + "/42/enabled").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"enabled\":" + enabled + "}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.enabled").value(enabled));

        verify(service).setEnabled(42, enabled);
    }

    @Test
    void missingEnabledFlagReachesServiceAsNullNotImplicitFalse() throws Exception {
        when(service.setEnabled(42, null)).thenReturn(new Result<>(Result.PARAMETER_NOT_VALID, null, "enabled 必填"));

        mvc.perform(post(BASE + "/42/enabled").contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value(Result.PARAMETER_NOT_VALID));

        verify(service).setEnabled(42, null);
    }

    @Test
    void malformedEnabledBooleanReturnsHttp400() throws Exception {
        mvc.perform(post(BASE + "/42/enabled").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"enabled\":\"invalid\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(Result.PARAMETER_NOT_VALID));
        verifyNoInteractions(service);
    }

    @Test
    void multipartBindsSupportedMetadataAndIgnoresServerOwnedFields() throws Exception {
        MockMultipartFile file = syntheticFile();
        when(service.upload(any(), any())).thenReturn(new Result<>(Result.SUCCESS, item(false), null));

        mvc.perform(multipart(BASE + "/upload").file(file)
                        .param("accession", "SYNTHETIC_001").param("taxId", "101").param("orgType", "2")
                        .param("organismName", "synthetic organism").param("segment", "fixture segment")
                        .param("completeness", "fixture status").param("enabled", "false")
                        .param("referenceId", "not-a-number").param("path", "/private/client-path.fna")
                        .param("annotationFile", "/private/client-annotation.gff").param("sourceDb", "CLIENT_SOURCE")
                        .param("rawMetadata", "client raw metadata").param("genomeLength", "999")
                        .param("isAnnotated", "true"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value(Result.SUCCESS))
                .andExpect(jsonPath("$.data.enabled").value(false))
                .andExpect(jsonPath("$.data.sourceDb").value("USER_UPLOAD"))
                .andExpect(jsonPath("$.data.rawMetadata").doesNotExist())
                .andExpect(content().string(not(containsString("client-path"))));

        ArgumentCaptor<UploadReferenceRequest> metadata = ArgumentCaptor.forClass(UploadReferenceRequest.class);
        verify(service).upload(metadata.capture(), same(file));
        assertEquals("SYNTHETIC_001", metadata.getValue().getAccession());
        assertEquals(101, metadata.getValue().getTaxId());
        assertEquals(2, metadata.getValue().getOrgType());
        assertEquals("synthetic organism", metadata.getValue().getOrganismName());
        assertEquals("fixture segment", metadata.getValue().getSegment());
        assertEquals("fixture status", metadata.getValue().getCompleteness());
        assertFalse(metadata.getValue().getEnabled());
    }

    @Test
    void multipartOmittedEnabledKeepsTrueDefault() throws Exception {
        when(service.upload(any(), any())).thenReturn(new Result<>(Result.SUCCESS, item(true), null));

        mvc.perform(multipart(BASE + "/upload").file(syntheticFile()).param("taxId", "101").param("orgType", "2"))
                .andExpect(status().isOk());

        ArgumentCaptor<UploadReferenceRequest> metadata = ArgumentCaptor.forClass(UploadReferenceRequest.class);
        verify(service).upload(metadata.capture(), any());
        assertTrue(metadata.getValue().getEnabled());
        assertNull(metadata.getValue().getAccession());
    }

    @Test
    void multipartNumberAndBooleanBindingErrorsReturnHttp400WithoutImport() throws Exception {
        for (String field : List.of("taxId", "enabled")) {
            mvc.perform(multipart(BASE + "/upload").file(syntheticFile()).param(field, "invalid"))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.status").value(Result.PARAMETER_NOT_VALID))
                    .andExpect(jsonPath("$.failMsg").value("上传元数据字段类型不正确"));
        }
        verifyNoInteractions(service);
    }

    @Test
    void missingMultipartFileReturnsControlledHttp400() throws Exception {
        mvc.perform(multipart(BASE + "/upload").param("taxId", "101").param("orgType", "2"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(Result.PARAMETER_NOT_VALID));
        verifyNoInteractions(service);
    }

    @Test
    void uploadPendingConfirmationErrorUsesHttp500AndKeepsPublicMessage() throws Exception {
        String message = "导入提交状态待确认：SYNTHETIC_001";
        when(service.upload(any(), any())).thenReturn(new Result<>(Result.INTERNAL_FAIL, null, message));

        mvc.perform(multipart(BASE + "/upload").file(syntheticFile()).param("taxId", "101").param("orgType", "2"))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.status").value(Result.INTERNAL_FAIL))
                .andExpect(jsonPath("$.failMsg").value(message));
    }

    private static MockMultipartFile syntheticFile() {
        return new MockMultipartFile("file", "synthetic.fa", "application/octet-stream",
                ">synthetic_fixture\nACGT\n".getBytes(StandardCharsets.UTF_8));
    }

    private static ReferenceLibraryItem item(boolean enabled) {
        return new ReferenceLibraryItem(42L, "SYNTHETIC_001", "USER_UPLOAD", 101, 2,
                "synthetic organism", 4, null, null, false, enabled,
                "refseq/custom/synthetic/reference.fna");
    }
}
