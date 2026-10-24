package com.xjtlu.bio.requestParameters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.requestParameters.CreateAnalysisPipelineRequest.PipelineStageParameters;
import com.xjtlu.bio.utils.JsonUtil;

/** Field-local JSON contracts, including the untyped maps used by batch creation. */
class EnabledReferenceIdsJsonTest {

    @ParameterizedTest
    @MethodSource("parameterTypes")
    void orderedLongIdsRoundTripWithoutNarrowing(Class<?> parameterType) throws Exception {
        Object parameters = JsonUtil.toObject(
                "{\"enabledReferenceIds\":[9223372036854775807,1,2147483648]}", parameterType);

        assertEquals(List.of(Long.MAX_VALUE, 1L, 2147483648L), ids(parameters));
        Object restored = JsonUtil.toObject(JsonUtil.toJson(parameters), parameterType);
        assertEquals(ids(parameters), ids(restored));
    }

    @ParameterizedTest
    @ValueSource(strings = {"{}", "{\"enabledReferenceIds\":null}"})
    void absentAndNullFieldsRemainUnspecifiedAcrossBothDtos(String json) throws Exception {
        for (Class<?> parameterType : parameterTypes().toList()) {
            Object parameters = JsonUtil.toObject(json, parameterType);

            assertNull(ids(parameters), parameterType.getSimpleName());
            assertNull(ids(JsonUtil.toObject(JsonUtil.toJson(parameters), parameterType)));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "\"901\"", "901", "true", "{}",
            "[\"901\"]", "[1.0]", "[1.5]", "[1e3]", "[true]", "[null]", "[[]]", "[{}]",
            "[901,\"902\"]", "[9223372036854775808]", "[-9223372036854775809]"
    })
    void bothDtosRejectNonIntegerArraysAndOutOfRangeNumbers(String value) {
        String json = "{\"enabledReferenceIds\":" + value + "}";
        for (Class<?> parameterType : parameterTypes().toList()) {
            assertThrows(JsonProcessingException.class, () -> JsonUtil.toObject(json, parameterType),
                    parameterType.getSimpleName() + " must reject " + value);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"[]", "[0]", "[-1]", "[901,901]", "[-9223372036854775808]"})
    void integerShapeRemainsAvailableForServiceLevelSemanticValidation(String value) throws Exception {
        String json = "{\"enabledReferenceIds\":" + value + "}";
        for (Class<?> parameterType : parameterTypes().toList()) {
            Object parameters = JsonUtil.toObject(json, parameterType);
            Map<String, Object> serialized = JsonUtil.toMap(JsonUtil.toJson(parameters));

            assertEquals(JsonUtil.toMap(json).get("enabledReferenceIds"), serialized.get("enabledReferenceIds"),
                    "The JSON reader must not remove duplicates, empty lists, or nonpositive IDs");
        }
    }

    @Test
    void nestedCreateRequestRoundTripsThroughStoredParametersDto() throws Exception {
        CreateAnalysisPipelineRequest request = JsonUtil.toObject("""
                {"projectId":1,"pipelineType":100,"analysisName":"json-fixture",
                 "pipelineStageParameters":{"taxId":700001,"enabledReferenceIds":[2147483648,901]}}
                """, CreateAnalysisPipelineRequest.class);

        AnalysisPipelineParameters stored = JsonUtil.toObject(
                JsonUtil.toJson(request.getPipelineStageParameters()), AnalysisPipelineParameters.class);

        assertEquals(List.of(2147483648L, 901L), stored.getEnabledReferenceIds());
        assertEquals(700001, stored.getTaxId());
    }

    @Test
    void batchMapToPojoPreservesIntegerIdsAndLongBoundaryForBothDtos() throws Exception {
        Map<String, Object> parameters = batchParameters("[9223372036854775807,901,2147483648]");

        for (Class<?> parameterType : parameterTypes().toList()) {
            Object restored = JsonUtil.mapToPojo(parameters, parameterType);

            assertEquals(List.of(Long.MAX_VALUE, 901L, 2147483648L), ids(restored));
            assertEquals(ids(restored), ids(JsonUtil.toObject(JsonUtil.toJson(restored), parameterType)));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"omitted", "null"})
    void batchMapToPojoKeepsUnspecifiedSelection(String value) throws Exception {
        Map<String, Object> parameters = "omitted".equals(value)
                ? Map.of("taxId", 700001) : batchParameters(value);

        for (Class<?> parameterType : parameterTypes().toList()) {
            assertNull(ids(JsonUtil.mapToPojo(parameters, parameterType)));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "\"901\"", "901", "true", "{}",
            "[\"901\"]", "[1.0]", "[1.5]", "[1e3]", "[true]", "[null]", "[[]]", "[{}]",
            "[901,\"902\"]", "[9223372036854775808]", "[-9223372036854775809]"
    })
    void batchMapToPojoCannotBypassStrictIntegerArrayContract(String value) throws Exception {
        Map<String, Object> parameters = batchParameters(value);

        for (Class<?> parameterType : parameterTypes().toList()) {
            assertThrows(IllegalArgumentException.class, () -> JsonUtil.mapToPojo(parameters, parameterType),
                    parameterType.getSimpleName() + " must reject batch value " + value);
        }
    }

    private static Stream<Class<?>> parameterTypes() {
        return Stream.of(PipelineStageParameters.class, AnalysisPipelineParameters.class);
    }

    private static List<Long> ids(Object parameters) {
        if (parameters instanceof PipelineStageParameters request) {
            return request.getEnabledReferenceIds();
        }
        return ((AnalysisPipelineParameters) parameters).getEnabledReferenceIds();
    }

    private static Map<String, Object> batchParameters(String value) throws Exception {
        BatchCreateAnalysisPipelineRequest batch = JsonUtil.toObject("""
                {"projectId":1,"pipelineType":100,"pipelines":[
                  {"pipelineName":"batch-json-fixture","pipelineParameters":{
                    "taxId":700001,"enabledReferenceIds":%s}}
                ]}
                """.formatted(value), BatchCreateAnalysisPipelineRequest.class);
        return batch.getPipelines().get(0).getPipelineParameters();
    }
}
