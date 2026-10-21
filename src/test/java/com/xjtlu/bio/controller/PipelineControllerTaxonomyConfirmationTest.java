package com.xjtlu.bio.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.http.MediaType;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.requestParameters.ConfirmTaxonomyRequest;
import com.xjtlu.bio.response.TaxonomyConfirmationResponse;
import com.xjtlu.bio.service.PipelineService;

class PipelineControllerTaxonomyConfirmationTest {

    private static final String ENDPOINT = "/pipeline/stage/taxonomy/confirm";
    private static final String VALID_REQUEST =
            "{\"stageId\":123,\"taxId\":562,\"reason\":\"independent review\"}";

    private PipelineService service;
    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        service = mock(PipelineService.class);
        PipelineController controller = new PipelineController();
        ReflectionTestUtils.setField(controller, "pipelineService", service);
        LocalValidatorFactoryBean validator = new LocalValidatorFactoryBean();
        validator.afterPropertiesSet();
        mockMvc = MockMvcBuilders.standaloneSetup(controller).setValidator(validator).build();
    }

    @Test
    void bindsRequestAndUsesAuthenticatedPrincipalAsConfirmationIdentity() throws Exception {
        TaxonomyConfirmationResponse response = new TaxonomyConfirmationResponse(123L, 562, 5, true, false);
        when(service.confirmTaxonomy(any(ConfirmTaxonomyRequest.class), eq("reviewer")))
                .thenReturn(new Result<>(Result.SUCCESS, response, null));

        mockMvc.perform(post(ENDPOINT)
                        .principal(() -> "reviewer")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_REQUEST))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value(Result.SUCCESS))
                .andExpect(jsonPath("$.data.stageId").value(123))
                .andExpect(jsonPath("$.data.taxId").value(562))
                .andExpect(jsonPath("$.data.version").value(5))
                .andExpect(jsonPath("$.data.downstreamScheduled").value(true))
                .andExpect(jsonPath("$.data.alreadyConfirmed").value(false));

        ArgumentCaptor<ConfirmTaxonomyRequest> request = ArgumentCaptor.forClass(ConfirmTaxonomyRequest.class);
        verify(service).confirmTaxonomy(request.capture(), eq("reviewer"));
        assertEquals(123L, request.getValue().getStageId());
        assertEquals(562, request.getValue().getTaxId());
        assertEquals("independent review", request.getValue().getReason());
    }

    @Test
    void acceptsAbsentPrincipalAndOptionalReason() throws Exception {
        when(service.confirmTaxonomy(any(ConfirmTaxonomyRequest.class), isNull()))
                .thenReturn(new Result<>(Result.SUCCESS,
                        new TaxonomyConfirmationResponse(123L, 562, 1, true, false), null));

        mockMvc.perform(post(ENDPOINT)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"stageId\":123,\"taxId\":562}"))
                .andExpect(status().isOk());

        verify(service).confirmTaxonomy(any(ConfirmTaxonomyRequest.class), isNull());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"stageId\":123,\"taxId\":0}",
            "{\"stageId\":123,\"taxId\":-1}",
            "{\"stageId\":123}",
            "{\"stageId\":0,\"taxId\":562}",
            "{\"taxId\":562}"
    })
    void rejectsInvalidIdentifiersWithoutCallingService(String request) throws Exception {
        mockMvc.perform(post(ENDPOINT)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request))
                .andExpect(status().isBadRequest());

        verifyNoInteractions(service);
    }

    @Test
    void rejectsOverlongConfirmationReason() throws Exception {
        String request = "{\"stageId\":123,\"taxId\":562,\"reason\":\""
                + "x".repeat(2001) + "\"}";

        mockMvc.perform(post(ENDPOINT)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request))
                .andExpect(status().isBadRequest());

        verifyNoInteractions(service);
    }

    @Test
    void returnsInternalFailureAsHttp500() throws Exception {
        when(service.confirmTaxonomy(any(ConfirmTaxonomyRequest.class), isNull()))
                .thenReturn(new Result<>(Result.INTERNAL_FAIL, null, "confirmation unavailable"));

        mockMvc.perform(post(ENDPOINT)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_REQUEST))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.status").value(Result.INTERNAL_FAIL))
                .andExpect(jsonPath("$.failMsg").value("confirmation unavailable"));
    }

    @Test
    void keepsCommittedConfirmationDataWhenDownstreamSchedulingFails() throws Exception {
        TaxonomyConfirmationResponse response = new TaxonomyConfirmationResponse(123L, 562, 5, false, false);
        when(service.confirmTaxonomy(any(ConfirmTaxonomyRequest.class), isNull()))
                .thenReturn(new Result<>(Result.INTERNAL_FAIL, response, "downstream scheduling failed"));

        mockMvc.perform(post(ENDPOINT)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_REQUEST))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.status").value(Result.INTERNAL_FAIL))
                .andExpect(jsonPath("$.failMsg").value("downstream scheduling failed"))
                .andExpect(jsonPath("$.data.stageId").value(123))
                .andExpect(jsonPath("$.data.taxId").value(562))
                .andExpect(jsonPath("$.data.version").value(5))
                .andExpect(jsonPath("$.data.downstreamScheduled").value(false))
                .andExpect(jsonPath("$.data.alreadyConfirmed").value(false));
    }

    @Test
    void keepsBusinessFailureInResultEnvelopeWithHttp200() throws Exception {
        when(service.confirmTaxonomy(any(ConfirmTaxonomyRequest.class), isNull()))
                .thenReturn(new Result<>(Result.BUSINESS_FAIL, null, "version conflict"));

        mockMvc.perform(post(ENDPOINT)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_REQUEST))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value(Result.BUSINESS_FAIL))
                .andExpect(jsonPath("$.failMsg").value("version conflict"));
    }
}
