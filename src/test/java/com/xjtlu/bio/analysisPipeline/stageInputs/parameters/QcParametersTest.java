package com.xjtlu.bio.analysisPipeline.stageInputs.parameters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.SequenceMeta;
import com.xjtlu.bio.utils.JsonUtil;

class QcParametersTest {

    @Test
    void freshParametersHaveSeparateToolDefaults() {
        assertDefaults(new QcParameters());
    }

    @ParameterizedTest
    @MethodSource("compatibleJson")
    void missingOrExplicitlyNullFiltersKeepDefaults(String json) throws Exception {
        assertDefaults(JsonUtil.toObject(json, QcParameters.class));
    }

    private static Stream<String> compatibleJson() {
        return Stream.of(
                "{}",
                "{\"readMeta\":{\"readLenType\":1}}",
                "{\"shortReadFilters\":{},\"longReadFilters\":{}}",
                "{\"shortReadFilters\":null,\"longReadFilters\":null}",
                """
                {
                  "shortReadFilters": {
                    "qualifiedQualityPhred": null, "unqualifiedPercentLimit": null,
                    "meanQuality": null, "minimumLength": null,
                    "maximumLength": null, "nBaseLimit": null
                  },
                  "longReadFilters": {
                    "qualifiedQualityPhred": null, "unqualifiedPercentLimit": null,
                    "meanQuality": null, "minimumLength": null,
                    "maximumLength": null, "nBaseLimit": null, "nPercentLimit": null
                  }
                }
                """);
    }

    @Test
    void partialConfigurationPreservesOtherDefaultsAndOppositeReadType() throws Exception {
        QcParameters parameters = JsonUtil.toObject("""
                {"longReadFilters":{"minimumLength":123,"meanQuality":9}}
                """, QcParameters.class);

        assertReadDefaults(parameters.getShortReadFilters(), 15, 5);
        assertEquals(123, parameters.getLongReadFilters().getMinimumLength());
        assertEquals(9, parameters.getLongReadFilters().getMeanQuality());
        assertEquals(15, parameters.getLongReadFilters().getQualifiedQualityPhred());
        assertEquals(40, parameters.getLongReadFilters().getUnqualifiedPercentLimit());
        assertEquals(0, parameters.getLongReadFilters().getMaximumLength());
        assertEquals(1_000_000, parameters.getLongReadFilters().getNBaseLimit());
        assertEquals(10, parameters.getLongReadFilters().getNPercentLimit());
    }

    @Test
    void mutableFilterObjectsAreNotSharedBetweenInstancesOrReadTypes() {
        QcParameters first = new QcParameters();
        QcParameters second = new QcParameters();

        assertNotSame(first.getShortReadFilters(), second.getShortReadFilters());
        assertNotSame(first.getLongReadFilters(), second.getLongReadFilters());
        assertNotSame(first.getShortReadFilters(), first.getLongReadFilters());
        first.getShortReadFilters().setMinimumLength(123);
        first.getLongReadFilters().setQualifiedQualityPhred(19);

        assertEquals(20, first.getLongReadFilters().getMinimumLength());
        assertEquals(15, first.getShortReadFilters().getQualifiedQualityPhred());
        assertDefaults(second);
    }

    @Test
    void nullFilterGroupsDoNotBreakDefaultParameters() {
        QcParameters parameters = new QcParameters();
        parameters.setShortReadFilters(null);
        parameters.setLongReadFilters(null);

        assertDefaults(parameters);
    }

    @Test
    void jsonSnapshotRecordsDefaultsAndPreservesExplicitSettings() throws Exception {
        QcParameters parameters = new QcParameters();
        SequenceMeta meta = new SequenceMeta();
        meta.setReadLenType(Constants.SequenceInput.READ_LEN_TYPE_LONG);
        parameters.setReadMeta(meta);
        parameters.getShortReadFilters().setNBaseLimit(8);
        parameters.getLongReadFilters().setMinimumLength(345);
        parameters.getLongReadFilters().setNPercentLimit(6);

        String json = JsonUtil.toJson(parameters);
        QcParameters restored = JsonUtil.toObject(json, QcParameters.class);

        assertTrue(json.contains("\"shortReadFilters\""));
        assertTrue(json.contains("\"longReadFilters\""));
        assertTrue(json.contains("\"qualifiedQualityPhred\":15"));
        assertTrue(json.contains("\"unqualifiedPercentLimit\":40"));
        assertEquals(8, restored.getShortReadFilters().getNBaseLimit());
        assertEquals(15, restored.getShortReadFilters().getMinimumLength());
        assertEquals(345, restored.getLongReadFilters().getMinimumLength());
        assertEquals(6, restored.getLongReadFilters().getNPercentLimit());
        assertEquals(1_000_000, restored.getLongReadFilters().getNBaseLimit());
        assertEquals(Constants.SequenceInput.READ_LEN_TYPE_LONG, restored.getReadMeta().getReadLenType());
    }

    private static void assertDefaults(QcParameters parameters) {
        assertNotNull(parameters);
        assertReadDefaults(parameters.getShortReadFilters(), 15, 5);
        assertReadDefaults(parameters.getLongReadFilters(), 20, 1_000_000);
        assertEquals(10, parameters.getLongReadFilters().getNPercentLimit());
    }

    private static void assertReadDefaults(QcParameters.ReadFilters filters, int minimumLength, int nBaseLimit) {
        assertNotNull(filters);
        assertEquals(15, filters.getQualifiedQualityPhred());
        assertEquals(40, filters.getUnqualifiedPercentLimit());
        assertEquals(0, filters.getMeanQuality());
        assertEquals(minimumLength, filters.getMinimumLength());
        assertEquals(0, filters.getMaximumLength());
        assertEquals(nBaseLimit, filters.getNBaseLimit());
    }
}
