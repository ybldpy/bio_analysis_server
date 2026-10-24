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
import com.xjtlu.bio.response.ReferenceMembershipConfirmationResult;
import com.xjtlu.bio.service.ReferenceLibraryService;

/** Standalone target-context HTTP contract tests; no database, network or storage. */
class ReferenceLibraryTargetControllerTest {
    private static final String BASE = "/referenceGenome/library";
    private ReferenceLibraryService service;
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        service = mock(ReferenceLibraryService.class);
        mvc = MockMvcBuilders.standaloneSetup(new ReferenceLibraryController(service)).build();
    }

    @Test
    void queryBindsTargetContextSeparatelyFromTheActualTaxIdFilter() throws Exception {
        when(service.query(any())).thenReturn(new Result<>(Result.SUCCESS,
                new ReferenceLibraryPage(1, 0, 20, List.of(item())), null));

        mvc.perform(post(BASE + "/query").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"targetTaxId\":900,\"pipelineType\":100,\"taxId\":101,\"orgType\":1,\"enabled\":false}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value(Result.SUCCESS))
                .andExpect(jsonPath("$.data.items[0].taxId").value(101))
                .andExpect(jsonPath("$.data.items[0].enabled").value(false))
                .andExpect(jsonPath("$.confirmation").doesNotExist());

        ArgumentCaptor<ReferenceLibraryQuery> query = ArgumentCaptor.forClass(ReferenceLibraryQuery.class);
        verify(service).query(query.capture());
        assertEquals(900, query.getValue().getTargetTaxId());
        assertEquals(100, query.getValue().getPipelineType());
        assertEquals(101, query.getValue().getTaxId());
        assertEquals(1, query.getValue().getOrgType());
        assertFalse(query.getValue().getEnabled());
    }

    @Test
    void malformedQueryTargetContextReturnsControlledHttp400WithoutCallingService() throws Exception {
        for (String field : List.of("targetTaxId", "pipelineType")) {
            mvc.perform(post(BASE + "/query").contentType(MediaType.APPLICATION_JSON)
                            .content("{\"" + field + "\":\"not-an-integer\"}"))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.status").value(Result.PARAMETER_NOT_VALID))
                    .andExpect(jsonPath("$.data").value(nullValue()))
                    .andExpect(content().string(not(containsString("Exception"))));
        }
        verifyNoInteractions(service);
    }

    @Test
    void multipartAllowsTargetContextAndExplicitConfirmationButKeepsActualTaxIdIndependent() throws Exception {
        MockMultipartFile file = syntheticFile();
        when(service.upload(any(), any())).thenReturn(new Result<>(Result.SUCCESS, item(), null));

        mvc.perform(multipart(BASE + "/upload").file(file)
                        .param("targetTaxId", "900").param("pipelineType", "100")
                        .param("taxId", "101").param("orgType", "1").param("confirmAddTaxonomyMember", "true")
                        .param("accession", "SYNTHETIC_SCOPE_001").param("enabled", "false")
                        .param("memberTaxIds", "777").param("referenceId", "not-an-integer")
                        .param("path", "/private/client-path.fna").param("sourceDb", "CLIENT_SOURCE"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value(Result.SUCCESS))
                .andExpect(jsonPath("$.data.taxId").value(101))
                .andExpect(jsonPath("$.data.sourceDb").value("USER_UPLOAD"))
                .andExpect(jsonPath("$.code").doesNotExist())
                .andExpect(jsonPath("$.confirmation").doesNotExist())
                .andExpect(content().string(not(containsString("client-path"))));

        ArgumentCaptor<UploadReferenceRequest> request = ArgumentCaptor.forClass(UploadReferenceRequest.class);
        verify(service).upload(request.capture(), same(file));
        assertEquals(900, request.getValue().getTargetTaxId());
        assertEquals(100, request.getValue().getPipelineType());
        assertEquals(101, request.getValue().getTaxId());
        assertEquals(1, request.getValue().getOrgType());
        assertTrue(request.getValue().getConfirmAddTaxonomyMember());
        assertFalse(request.getValue().getEnabled());
    }

    @Test
    void omittedMultipartConfirmationDefaultsToFalseWithoutInventingTargetContext() throws Exception {
        when(service.upload(any(), any())).thenReturn(new Result<>(Result.SUCCESS, item(), null));

        mvc.perform(multipart(BASE + "/upload").file(syntheticFile()).param("taxId", "101").param("orgType", "1"))
                .andExpect(status().isOk());

        ArgumentCaptor<UploadReferenceRequest> request = ArgumentCaptor.forClass(UploadReferenceRequest.class);
        verify(service).upload(request.capture(), any());
        assertFalse(request.getValue().getConfirmAddTaxonomyMember());
        assertNull(request.getValue().getTargetTaxId());
        assertNull(request.getValue().getPipelineType());
    }

    @ParameterizedTest
    @ValueSource(strings = {"targetTaxId", "pipelineType", "confirmAddTaxonomyMember"})
    void malformedMultipartTargetContextCannotReachUpload(String field) throws Exception {
        mvc.perform(multipart(BASE + "/upload").file(syntheticFile()).param(field, "invalid"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(Result.PARAMETER_NOT_VALID))
                .andExpect(jsonPath("$.failMsg").value("上传元数据字段类型不正确"));

        verifyNoInteractions(service);
    }

    @Test
    void confirmationResponseUsesStableCodeAndContextWithoutPretendingAnItemWasImported() throws Exception {
        when(service.upload(any(), any())).thenReturn(new ReferenceMembershipConfirmationResult(900, 100, 101));

        mvc.perform(multipart(BASE + "/upload").file(syntheticFile())
                        .param("targetTaxId", "900").param("pipelineType", "100")
                        .param("taxId", "101").param("orgType", "1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value(Result.BUSINESS_FAIL))
                .andExpect(jsonPath("$.data").value(nullValue()))
                .andExpect(jsonPath("$.code").value("REFERENCE_TARGET_MEMBERSHIP_CONFIRMATION_REQUIRED"))
                .andExpect(jsonPath("$.confirmation.targetTaxId").value(900))
                .andExpect(jsonPath("$.confirmation.pipelineType").value(100))
                .andExpect(jsonPath("$.confirmation.taxId").value(101))
                .andExpect(jsonPath("$.confirmation.referenceId").doesNotExist());
    }

    @Test
    void partialTargetContextPreservesTheServiceValidationError() throws Exception {
        when(service.query(any())).thenReturn(new Result<>(Result.PARAMETER_NOT_VALID, null, "target context required"));

        mvc.perform(post(BASE + "/query").contentType(MediaType.APPLICATION_JSON).content("{\"targetTaxId\":900}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value(Result.PARAMETER_NOT_VALID))
                .andExpect(jsonPath("$.failMsg").value("target context required"))
                .andExpect(jsonPath("$.confirmation").doesNotExist());

        ArgumentCaptor<ReferenceLibraryQuery> query = ArgumentCaptor.forClass(ReferenceLibraryQuery.class);
        verify(service).query(query.capture());
        assertEquals(900, query.getValue().getTargetTaxId());
        assertNull(query.getValue().getPipelineType());
    }

    @Test
    void uncertainCommitIsHttp500WithoutMembershipConfirmationPayload() throws Exception {
        when(service.upload(any(), any())).thenReturn(new Result<>(Result.INTERNAL_FAIL, null,
                "导入提交状态待确认，请按 accession 查询后再重试：SYNTHETIC_SCOPE_001"));

        mvc.perform(multipart(BASE + "/upload").file(syntheticFile())
                        .param("targetTaxId", "900").param("pipelineType", "100")
                        .param("taxId", "101").param("orgType", "1").param("confirmAddTaxonomyMember", "true"))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.status").value(Result.INTERNAL_FAIL))
                .andExpect(jsonPath("$.data").value(nullValue()))
                .andExpect(jsonPath("$.failMsg").value(containsString("SYNTHETIC_SCOPE_001")))
                .andExpect(jsonPath("$.code").doesNotExist())
                .andExpect(jsonPath("$.confirmation").doesNotExist());
    }

    private static MockMultipartFile syntheticFile() {
        return new MockMultipartFile("file", "synthetic.fa", "application/octet-stream",
                ">synthetic_scope_fixture\nACGT\n".getBytes(StandardCharsets.UTF_8));
    }

    private static ReferenceLibraryItem item() {
        return new ReferenceLibraryItem(42L, "SYNTHETIC_SCOPE_001", "USER_UPLOAD", 101, 1,
                "synthetic fixture", 4, null, null, false, false,
                "refseq/custom/synthetic/reference.fna");
    }
}
