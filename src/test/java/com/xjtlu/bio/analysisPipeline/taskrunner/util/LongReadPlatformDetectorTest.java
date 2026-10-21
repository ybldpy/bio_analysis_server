package com.xjtlu.bio.analysisPipeline.taskrunner.util;

import static com.xjtlu.bio.analysisPipeline.Constants.SequencingPlatform.ONT;
import static com.xjtlu.bio.analysisPipeline.Constants.SequencingPlatform.PACBIO;
import static com.xjtlu.bio.analysisPipeline.Constants.SequencingPlatform.UNKNOWN;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

/** Header-string tests only; no files, sequencing data, external tools or database. */
class LongReadPlatformDetectorTest {

    private static final String READ_ID = "550e8400-e29b-41d4-a716-446655440000";
    private static final String RUN_UUID = "6d171fc3-bc98-4e63-86e1-0ef1e9402500";
    private static final String RUN_HEX = "0123456789abcdef0123456789abcdef01234567";
    private static final String ONT_HEADER = "@" + READ_ID + " runid=" + RUN_HEX + " ch=123";
    private static final String PACBIO_HEADER = "@m64011_190830_220126/100/ccs";

    @ParameterizedTest
    @MethodSource("ontHeaders")
    void recognizesSupportedOntHeaderStructures(String header) {
        assertEquals(ONT, LongReadPlatformDetector.detect(List.of(header)));
    }

    private static Stream<String> ontHeaders() {
        return Stream.of(
                ONT_HEADER,
                "@" + READ_ID + " runid=" + RUN_UUID + " ch=123",
                "@" + READ_ID + " runid=" + RUN_HEX + " read=987 ch=123 start_time=2024-02-29T10:00:00Z",
                "@" + READ_ID + " runid=" + RUN_UUID + " start_time=2024-02-29T10:00:00.125+01:00",
                "@" + READ_ID + " runid=" + RUN_HEX
                        + " flow_cell_id=FLO12345 basecall_model_version_id=dna_r10.4.1_e8.2_400bps_hac@v4.3.0",
                "@" + READ_ID + " runid=" + RUN_HEX
                        + " flow_cell_id=FLO12345 basecall_model_version_id=rna004_130bps_sup@v5.0.0",
                "@" + READ_ID + " runid=" + RUN_HEX
                        + " flow_cell_id=FLO12345 basecall_model_version_id=rna004_130bps_fast@v5.1.0",
                "@" + READ_ID + " runid=" + RUN_HEX
                        + " flow_cell_id=FLO12345 basecall_model_version_id=2021-05-17_dna_r9.4.1_minion_384_d37a2ab9",
                "@" + READ_ID.toUpperCase() + " ch=1\trunid=" + RUN_HEX.toUpperCase() + "\t");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "@m64011_190830_220126/100/ccs",
            "@m64011_190830_220126/100/0_12345",
            "@m64011_190830_220126_s1_p0/100/500_12345",
            "@m84039e_240229_235959_s1/0/ccs",
            "@m140905_042212_sidney_c100_s1_p0/100/0_12345",
            "@m150404_101626_42267_s1_p0/100/0_12345",
            "@m150404_101626_123456_s1_p0/100/ccs",
            "@m64011_190830_220126/100/ccs/fwd",
            "@m64011_190830_220126/100/ccs/rev",
            "@m64011_190830_220126/100/ccs/0_12345",
            "@m64011_190830_220126/100/ccs/fwd/0_12345",
            "@m64011_190830_220126/100/ccs/rev/500_12345",
            "@m64011_190830_220126/100/ccs rq=0.999"
    })
    void recognizesSupportedPacBioMovieAndReadStructures(String header) {
        assertEquals(PACBIO, LongReadPlatformDetector.detect(List.of(header)));
    }

    @Test
    void consistentSamplesCanUseDifferentSupportedConventions() {
        assertEquals(ONT, LongReadPlatformDetector.detect(ontHeaders().toList()));
        assertEquals(PACBIO, LongReadPlatformDetector.detect(List.of(
                PACBIO_HEADER, "@m140905_042212_sidney_c100_s1_p0/100/0_12345")));
    }

    @Test
    void nullOrEmptySamplesAreUnknown() {
        assertEquals(UNKNOWN, LongReadPlatformDetector.detect(null));
        assertEquals(UNKNOWN, LongReadPlatformDetector.detect(List.of()));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {
            " ", "@", "@ ", ">m64011_190830_220126/100/ccs", "m64011_190830_220126/100/ccs",
            "@SRR123456.1", "@S1_1", "@contig1", "@read-ONT", "@PACBIO-read", "@sample-MINION-001",
            "@sample/1/ccs", "@sample/1/0_100", "@mystery/1/ccs", "@m1234_190830/1/ccs",
            "@m64011_991332_250000/100/ccs", "@m64011_240230_120000/100/ccs",
            "@m64011_190830_220126/not-a-number/ccs", "@m64011_190830_220126/-1/ccs",
            "@m64011_190830_220126/100/ccs/other", "@m64011_190830_220126/100/0_0",
            "@m64011_190830_220126/100/20_10", "@m64011_190830_220126/100/-1_10",
            "@m64011_190830_220126/100/0_99999999999999999999999",
            "@m64011_190830_220126/100/ccs/fwd/100_20", "@m64011_190830_220126/100/ccs/",
            "@550e8400-e29b-41d4-a716-446655440000",
            "@550e8400-e29b-41d4-a716-446655440000 ch=1 start_time=2024-01-01T00:00:00Z",
            "@550e8400-e29b-41d4-a716-446655440000 runid=invalid ch=1"
    })
    void weakRenamedOrMalformedHeadersDoNotBecomePlatformEvidence(String header) {
        assertEquals(UNKNOWN, LongReadPlatformDetector.detect(Arrays.asList(header)));
    }

    @ParameterizedTest
    @MethodSource("weakOntHeaders")
    void ontRequiresMoreThanUuidAndRunId(String header) {
        assertEquals(UNKNOWN, LongReadPlatformDetector.detect(List.of(header)));
    }

    private static Stream<String> weakOntHeaders() {
        String prefix = "@" + READ_ID + " runid=" + RUN_HEX;
        return Stream.of(
                prefix,
                prefix + " read=100",
                prefix + " ch=0",
                prefix + " ch=abc",
                prefix + " start_time=yesterday",
                prefix + " flow_cell_id=FLO12345",
                prefix + " basecall_model_version_id=dna_r10.4.1_hac@v4.3.0",
                prefix + " flow_cell_id=FLO12345 basecall_model_version_id=arbitrary-word",
                prefix + " ch=1 ch=2",
                prefix + " ch=1 runid=" + RUN_UUID);
    }

    @Test
    void mixedPlatformsDoNotProduceAMajorityGuess() {
        assertEquals(UNKNOWN, LongReadPlatformDetector.detect(List.of(ONT_HEADER, PACBIO_HEADER)));
        assertEquals(UNKNOWN, LongReadPlatformDetector.detect(List.of(PACBIO_HEADER, ONT_HEADER)));
        assertEquals(UNKNOWN,
                LongReadPlatformDetector.detect(List.of(ONT_HEADER, ONT_HEADER, PACBIO_HEADER)));
    }

    @Test
    void unrecognizedHeadersMakeTheWholeSampleInconclusive() {
        assertEquals(UNKNOWN, LongReadPlatformDetector.detect(List.of(ONT_HEADER, "@renamed-read")));
        assertEquals(UNKNOWN, LongReadPlatformDetector.detect(List.of("@renamed-read", PACBIO_HEADER)));
        assertEquals(UNKNOWN, LongReadPlatformDetector.detect(Arrays.asList(ONT_HEADER, null)));
        assertEquals(UNKNOWN, LongReadPlatformDetector.detect(List.of(PACBIO_HEADER, "")));
    }
}
