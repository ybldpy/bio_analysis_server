package com.xjtlu.bio.analysisPipeline.stageResult;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.xjtlu.bio.utils.JsonUtil;

class VFResultTest {

    @Test
    void serializesOnlyVfTsvProperty() throws Exception {
        VFResult result = new VFResult("stored/vf.tsv");

        Map<String, Object> properties = JsonUtil.toMap(JsonUtil.toJson(result));

        assertEquals(Map.of("vfTsv", "stored/vf.tsv"), properties);
        assertFalse(properties.containsKey("amrTsv"));
    }

    @Test
    void newResultRoundTripsUsingVfTsv() throws Exception {
        VFResult result = new VFResult();
        result.setVfTsv("stored/vf.tsv");

        String json = JsonUtil.toJson(result);
        VFResult restored = JsonUtil.toObject(json, VFResult.class);

        assertEquals("stored/vf.tsv", result.getVfTsv());
        assertEquals("stored/vf.tsv", restored.getVfTsv());
        assertEquals(json, JsonUtil.toJson(restored));
    }

    @Test
    void readsLegacyPropertyWithoutRenamingHistoricalArtifactUrl() throws Exception {
        VFResult result = JsonUtil.toObject("""
                {"amrTsv":"stored/historical/amr.tsv"}
                """, VFResult.class);

        assertEquals("stored/historical/amr.tsv", result.getVfTsv());
        assertEquals(Map.of("vfTsv", "stored/historical/amr.tsv"),
                JsonUtil.toMap(JsonUtil.toJson(result)));
    }

    @Test
    void readsNullLegacyProperty() throws Exception {
        VFResult result = JsonUtil.toObject("""
                {"amrTsv":null}
                """, VFResult.class);

        assertNull(result.getVfTsv());
        Map<String, Object> properties = JsonUtil.toMap(JsonUtil.toJson(result));
        assertTrue(properties.containsKey("vfTsv"));
        assertNull(properties.get("vfTsv"));
        assertFalse(properties.containsKey("amrTsv"));
    }

    @Test
    void readsEmptyLegacyPropertyWithoutChangingItsValue() throws Exception {
        VFResult result = JsonUtil.toObject("""
                {"amrTsv":""}
                """, VFResult.class);

        assertEquals("", result.getVfTsv());
        assertEquals(Map.of("vfTsv", ""), JsonUtil.toMap(JsonUtil.toJson(result)));
    }
}
