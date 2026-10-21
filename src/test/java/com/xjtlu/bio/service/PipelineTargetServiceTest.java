package com.xjtlu.bio.service;

import static com.xjtlu.bio.analysisPipeline.Constants.PipelineType.PIPELINE_VIRUS;
import static com.xjtlu.bio.analysisPipeline.Constants.PipelineType.PIPELINE_VIRUS_COVID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioPipelineTarget;
import com.xjtlu.bio.mapper.BioPipelineTargetMapper;
import com.xjtlu.bio.response.VirusTargetOption;

/** Isolated catalog reads: no Spring application, database, or pipeline execution. */
class PipelineTargetServiceTest {

    private BioPipelineTargetMapper mapper;
    private PipelineTargetService service;

    @BeforeEach
    void setUp() {
        mapper = mock(BioPipelineTargetMapper.class);
        service = new PipelineTargetService(mapper);
    }

    @Test
    void constructionDoesNotPreloadTheCatalog() {
        verifyNoInteractions(mapper);
    }

    @Test
    void projectsOnlyPublicFieldsAndPreservesDatabaseOrder() {
        BioPipelineTarget first = target(2L, "第二项", 202, PIPELINE_VIRUS, 10);
        BioPipelineTarget second = target(1L, "第一项", 101, PIPELINE_VIRUS_COVID, 20);
        when(mapper.selectEnabledVirusTargets(PIPELINE_VIRUS, PIPELINE_VIRUS_COVID))
                .thenReturn(List.of(first, second));

        Result<List<VirusTargetOption>> result = service.listVirusTargets();

        assertEquals(Result.SUCCESS, result.getStatus());
        assertNull(result.getFailMsg());
        assertEquals(List.of("第二项", "第一项"), labels(result));
        assertEquals(List.of(202, 101), result.getData().stream().map(VirusTargetOption::getTaxId).toList());
        assertEquals(List.of(PIPELINE_VIRUS, PIPELINE_VIRUS_COVID),
                result.getData().stream().map(VirusTargetOption::getPipelineType).toList());

        JsonNode json = new ObjectMapper().valueToTree(result);
        for (JsonNode option : json.path("data")) {
            Set<String> fields = new HashSet<>();
            option.fieldNames().forEachRemaining(fields::add);
            assertEquals(Set.of("label", "taxId", "pipelineType"), fields);
            assertTrue(option.path("taxId").isIntegralNumber());
            assertTrue(option.path("pipelineType").isIntegralNumber());
            assertFalse(option.has("memberTaxIds"));
        }
        verify(mapper).selectEnabledVirusTargets(PIPELINE_VIRUS, PIPELINE_VIRUS_COVID);
        verifyNoMoreInteractions(mapper);
    }

    @Test
    void noEnabledTargetsReturnsAnEmptySuccessfulArray() {
        when(mapper.selectEnabledVirusTargets(PIPELINE_VIRUS, PIPELINE_VIRUS_COVID))
                .thenReturn(List.of());

        Result<List<VirusTargetOption>> result = service.listVirusTargets();

        assertEquals(Result.SUCCESS, result.getStatus());
        assertNotNull(result.getData());
        assertTrue(result.getData().isEmpty());
        assertNull(result.getFailMsg());
        verify(mapper).selectEnabledVirusTargets(PIPELINE_VIRUS, PIPELINE_VIRUS_COVID);
    }

    @Test
    void readsTheLatestRowsOnEveryCallIncludingAfterAllTargetsAreDisabled() {
        BioPipelineTarget original = target(1L, "原始名称", 101, PIPELINE_VIRUS, 0);
        BioPipelineTarget edited = target(1L, "修改后的名称", 101, PIPELINE_VIRUS_COVID, 0);
        BioPipelineTarget added = target(2L, "新增项", 202, PIPELINE_VIRUS, 1);
        when(mapper.selectEnabledVirusTargets(PIPELINE_VIRUS, PIPELINE_VIRUS_COVID))
                .thenReturn(List.of(original), List.of(edited, added), List.of(), List.of(added));

        Result<List<VirusTargetOption>> first = service.listVirusTargets();
        assertEquals(List.of("原始名称"), labels(first));
        Result<List<VirusTargetOption>> second = service.listVirusTargets();
        assertEquals(List.of("修改后的名称", "新增项"), labels(second));
        assertEquals(PIPELINE_VIRUS_COVID, second.getData().get(0).getPipelineType());
        assertTrue(service.listVirusTargets().getData().isEmpty());
        assertEquals(List.of("新增项"), labels(service.listVirusTargets()));
        assertEquals(List.of("原始名称"), labels(first));

        verify(mapper, times(4)).selectEnabledVirusTargets(PIPELINE_VIRUS, PIPELINE_VIRUS_COVID);
        verifyNoMoreInteractions(mapper);
    }

    @Test
    void responseDoesNotExposeMutableDatabaseEntities() {
        BioPipelineTarget row = target(1L, "原始名称", 101, PIPELINE_VIRUS, 0);
        when(mapper.selectEnabledVirusTargets(PIPELINE_VIRUS, PIPELINE_VIRUS_COVID))
                .thenReturn(List.of(row));
        Result<List<VirusTargetOption>> result = service.listVirusTargets();

        row.setLabel("数据库对象已修改");
        row.setTaxId(999);
        row.setPipelineType(PIPELINE_VIRUS_COVID);

        assertEquals("原始名称", result.getData().get(0).getLabel());
        assertEquals(101, result.getData().get(0).getTaxId());
        assertEquals(PIPELINE_VIRUS, result.getData().get(0).getPipelineType());
    }

    @Test
    void databaseFailureIsSanitizedAndNeverReturnsEmptySuccessOrStaleData() {
        String privateDetails = "SELECT * FROM bio_pipeline_target at db.internal password=secret";
        when(mapper.selectEnabledVirusTargets(PIPELINE_VIRUS, PIPELINE_VIRUS_COVID))
                .thenReturn(List.of(target(1L, "旧配置", 101, PIPELINE_VIRUS, 0)))
                .thenThrow(new DataAccessResourceFailureException(privateDetails))
                .thenReturn(List.of(target(2L, "恢复后的配置", 202, PIPELINE_VIRUS_COVID, 0)));

        assertEquals(List.of("旧配置"), labels(service.listVirusTargets()));
        Result<List<VirusTargetOption>> failed = service.listVirusTargets();

        assertEquals(Result.INTERNAL_FAIL, failed.getStatus());
        assertNull(failed.getData());
        assertNotNull(failed.getFailMsg());
        assertFalse(failed.getFailMsg().isBlank());
        assertFalse(failed.getFailMsg().contains(privateDetails));
        assertFalse(failed.getFailMsg().contains("bio_pipeline_target"));
        assertFalse(failed.getFailMsg().contains("db.internal"));
        assertFalse(failed.getFailMsg().contains("secret"));
        assertEquals(List.of("恢复后的配置"), labels(service.listVirusTargets()));
        verify(mapper, times(3)).selectEnabledVirusTargets(PIPELINE_VIRUS, PIPELINE_VIRUS_COVID);
        verifyNoMoreInteractions(mapper);
    }

    @Test
    void unexpectedMapperRuntimeFailureUsesTheSameInternalFailureEnvelope() {
        when(mapper.selectEnabledVirusTargets(PIPELINE_VIRUS, PIPELINE_VIRUS_COVID))
                .thenThrow(new IllegalStateException("private mapper configuration"));

        Result<List<VirusTargetOption>> result = service.listVirusTargets();

        assertEquals(Result.INTERNAL_FAIL, result.getStatus());
        assertNull(result.getData());
        assertNotNull(result.getFailMsg());
        assertFalse(result.getFailMsg().contains("private mapper configuration"));
    }

    private static List<String> labels(Result<List<VirusTargetOption>> result) {
        return result.getData().stream().map(VirusTargetOption::getLabel).toList();
    }

    private static BioPipelineTarget target(long id, String label, int taxId, int pipelineType, int sortOrder) {
        BioPipelineTarget target = new BioPipelineTarget();
        target.setId(id);
        target.setLabel(label);
        target.setTaxId(taxId);
        target.setPipelineType(pipelineType);
        target.setEnabled(true);
        target.setSortOrder(sortOrder);
        target.setCreatedAt(new Date(1000));
        target.setUpdatedAt(new Date(2000));
        return target;
    }
}
