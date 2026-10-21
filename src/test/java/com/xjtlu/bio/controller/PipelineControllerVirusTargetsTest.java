package com.xjtlu.bio.controller;

import static com.xjtlu.bio.analysisPipeline.Constants.PipelineType.PIPELINE_VIRUS;
import static com.xjtlu.bio.analysisPipeline.Constants.PipelineType.PIPELINE_VIRUS_COVID;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.http.MediaType;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioPipelineTarget;
import com.xjtlu.bio.mapper.BioPipelineTargetMapper;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.service.PipelineTargetService;

/** Standalone MVC plus the real catalog service, with only the database boundary mocked. */
class PipelineControllerVirusTargetsTest {

    private static final String ENDPOINT = "/pipeline/virusTargets";

    private BioPipelineTargetMapper mapper;
    private PipelineService pipelineService;
    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mapper = mock(BioPipelineTargetMapper.class);
        pipelineService = mock(PipelineService.class);
        PipelineController controller = new PipelineController();
        ReflectionTestUtils.setField(controller, "pipelineTargetService", new PipelineTargetService(mapper));
        ReflectionTestUtils.setField(controller, "pipelineService", pipelineService);
        mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    void getReturnsTheOrderedFlatPublicOptionsWithoutReferenceMembershipOrDatabaseMetadata() throws Exception {
        when(mapper.selectEnabledVirusTargets(PIPELINE_VIRUS, PIPELINE_VIRUS_COVID))
                .thenReturn(List.of(target("测试选项 B", 202, PIPELINE_VIRUS_COVID),
                        target("测试选项 A", 101, PIPELINE_VIRUS)));

        mockMvc.perform(get(ENDPOINT).accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.status").value(Result.SUCCESS))
                .andExpect(jsonPath("$.failMsg").value(nullValue()))
                .andExpect(jsonPath("$.data.length()").value(2))
                .andExpect(jsonPath("$.data[0]").value(aMapWithSize(3)))
                .andExpect(jsonPath("$.data[0].label").value("测试选项 B"))
                .andExpect(jsonPath("$.data[0].taxId").value(202))
                .andExpect(jsonPath("$.data[0].pipelineType").value(PIPELINE_VIRUS_COVID))
                .andExpect(jsonPath("$.data[1]").value(aMapWithSize(3)))
                .andExpect(jsonPath("$.data[1].label").value("测试选项 A"))
                .andExpect(jsonPath("$.data[1].taxId").value(101))
                .andExpect(jsonPath("$.data[1].pipelineType").value(PIPELINE_VIRUS))
                .andExpect(jsonPath("$.data[0].memberTaxIds").doesNotExist())
                .andExpect(jsonPath("$.data[0].id").doesNotExist())
                .andExpect(jsonPath("$.data[0].enabled").doesNotExist())
                .andExpect(jsonPath("$.data[0].sortOrder").doesNotExist())
                .andExpect(jsonPath("$.data[0].createdAt").doesNotExist())
                .andExpect(jsonPath("$.data[0].updatedAt").doesNotExist());

        verify(mapper).selectEnabledVirusTargets(PIPELINE_VIRUS, PIPELINE_VIRUS_COVID);
        verifyNoInteractions(pipelineService);
    }

    @Test
    void emptyDatabaseResultRemainsHttp200WithAnEmptyArray() throws Exception {
        when(mapper.selectEnabledVirusTargets(PIPELINE_VIRUS, PIPELINE_VIRUS_COVID))
                .thenReturn(List.of());

        mockMvc.perform(get(ENDPOINT))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value(Result.SUCCESS))
                .andExpect(jsonPath("$.data").isArray())
                .andExpect(jsonPath("$.data").isEmpty())
                .andExpect(jsonPath("$.failMsg").value(nullValue()));

        verifyNoInteractions(pipelineService);
    }

    @Test
    void databaseFailureReturnsHttp500WithAJsonEnvelopeAndNoPrivateSqlDetails() throws Exception {
        when(mapper.selectEnabledVirusTargets(PIPELINE_VIRUS, PIPELINE_VIRUS_COVID))
                .thenThrow(new DataAccessResourceFailureException(
                        "SELECT label FROM bio_pipeline_target; database=db.internal password=secret"));

        mockMvc.perform(get(ENDPOINT))
                .andExpect(status().isInternalServerError())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.status").value(Result.INTERNAL_FAIL))
                .andExpect(jsonPath("$.data").value(nullValue()))
                .andExpect(jsonPath("$.failMsg").isNotEmpty())
                .andExpect(content().string(not(containsString("SELECT"))))
                .andExpect(content().string(not(containsString("bio_pipeline_target"))))
                .andExpect(content().string(not(containsString("db.internal"))))
                .andExpect(content().string(not(containsString("secret"))));

        verifyNoInteractions(pipelineService);
    }

    @Test
    void nextGetReflectsEditedRowsAndThenAnEmptyCatalog() throws Exception {
        when(mapper.selectEnabledVirusTargets(PIPELINE_VIRUS, PIPELINE_VIRUS_COVID))
                .thenReturn(List.of(target("原始名称", 101, PIPELINE_VIRUS)),
                        List.of(target("修改后的名称", 202, PIPELINE_VIRUS_COVID)),
                        List.of());

        mockMvc.perform(get(ENDPOINT))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data[0].label").value("原始名称"));
        mockMvc.perform(get(ENDPOINT))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.length()").value(1))
                .andExpect(jsonPath("$.data[0].label").value("修改后的名称"))
                .andExpect(jsonPath("$.data[0].taxId").value(202))
                .andExpect(jsonPath("$.data[0].pipelineType").value(PIPELINE_VIRUS_COVID));
        mockMvc.perform(get(ENDPOINT))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data").isEmpty());

        verify(mapper, times(3)).selectEnabledVirusTargets(PIPELINE_VIRUS, PIPELINE_VIRUS_COVID);
        verifyNoInteractions(pipelineService);
    }

    @Test
    void endpointIsGetOnlyAndDoesNotRunPipelines() throws Exception {
        mockMvc.perform(post(ENDPOINT))
                .andExpect(status().isMethodNotAllowed());

        verifyNoInteractions(mapper, pipelineService);
    }

    private static BioPipelineTarget target(String label, int taxId, int pipelineType) {
        BioPipelineTarget target = new BioPipelineTarget();
        target.setId(42L);
        target.setLabel(label);
        target.setTaxId(taxId);
        target.setPipelineType(pipelineType);
        target.setEnabled(true);
        target.setSortOrder(0);
        return target;
    }
}
