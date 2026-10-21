package com.xjtlu.bio.analysisPipeline.workflow;

import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FINISHED;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_PENDING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_QUEUING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_ASSEMBLY;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_CONSENSUS;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_MAPPING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_QC;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_READ_INSPECT;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_REFERENCE_COMPARISON;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_REFERENCE_SELECTION;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_SNP_ANNOTATION;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_VARIANT_CALL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceGenome;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSequence;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ConsensusStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.MappingParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ReferenceComparisonStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.SNPAnnotationStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.VarientCallParameters;
import com.xjtlu.bio.analysisPipeline.stageResult.MappingResult;
import com.xjtlu.bio.analysisPipeline.stageResult.QcResult;
import com.xjtlu.bio.analysisPipeline.stageResult.ReadInspectStageResult;
import com.xjtlu.bio.analysisPipeline.stageResult.ReferenceSelectionStageResult;
import com.xjtlu.bio.analysisPipeline.stageResult.VarientCallStageResult;
import com.xjtlu.bio.analysisPipeline.workflow.StageOrchestrator.MissingUpstreamException;
import com.xjtlu.bio.analysisPipeline.workflow.StageOrchestrator.OrchestratePlan;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.command.UpdateStageCommand;
import com.xjtlu.bio.utils.JsonUtil;

class StageOrchestratorReferenceSelectionResultTest {

    private final StageOrchestrator orchestrator = new StageOrchestrator();

    @ParameterizedTest
    @EnumSource(Consumer.class)
    void readsInlineSelectionForEveryConsumer(Consumer consumer) throws Exception {
        Fixture fixture = fixture(consumer);
        fixture.selection().setOutputInline(selectionJson("INLINE"));

        assertScheduledReference(consumer, fixture, "INLINE");
    }

    @ParameterizedTest
    @EnumSource(Consumer.class)
    void prefersInlineSelectionOverLegacyUrl(Consumer consumer) throws Exception {
        Fixture fixture = fixture(consumer);
        fixture.selection().setOutputInline(selectionJson("INLINE"));
        fixture.selection().setOutputUrl(selectionJson("LEGACY"));

        assertScheduledReference(consumer, fixture, "INLINE");
    }

    @ParameterizedTest
    @MethodSource("consumersWithBlankInline")
    void fallsBackToLegacyUrlOnlyWhenInlineIsBlank(Consumer consumer, String inline) throws Exception {
        Fixture fixture = fixture(consumer);
        fixture.selection().setOutputInline(inline);
        fixture.selection().setOutputUrl(selectionJson("LEGACY"));

        assertScheduledReference(consumer, fixture, "LEGACY");
    }

    @ParameterizedTest
    @EnumSource(Consumer.class)
    void rejectsMissingSelectionResult(Consumer consumer) throws Exception {
        Fixture fixture = fixture(consumer);
        fixture.selection().setOutputInline(" \t\n");
        fixture.selection().setOutputUrl(" ");

        MissingUpstreamException exception = assertThrows(MissingUpstreamException.class,
                () -> orchestrator.makePlan(fixture.stages(), fixture.runStage().getStageId()));

        assertEquals("Reference selection result does not exist", exception.getDesc());
        assertEquals(PIPELINE_STAGE_STATUS_PENDING, fixture.runStage().getStatus());
        assertEquals(3, fixture.runStage().getVersion());
    }

    @ParameterizedTest
    @EnumSource(Consumer.class)
    void rejectsMissingSelectionStage(Consumer consumer) throws Exception {
        Fixture fixture = fixture(consumer);
        fixture.stages().remove(fixture.selection());

        MissingUpstreamException exception = assertThrows(MissingUpstreamException.class,
                () -> orchestrator.makePlan(fixture.stages(), fixture.runStage().getStageId()));

        assertEquals("Reference selection result does not exist", exception.getDesc());
    }

    @ParameterizedTest
    @EnumSource(Consumer.class)
    void rejectsNullSelectedReferenceInsteadOfUsingLegacyUrl(Consumer consumer) throws Exception {
        Fixture fixture = fixture(consumer);
        fixture.selection().setOutputInline(JsonUtil.toJson(new ReferenceSelectionStageResult()));
        fixture.selection().setOutputUrl(selectionJson("LEGACY"));

        MissingUpstreamException exception = assertThrows(MissingUpstreamException.class,
                () -> orchestrator.makePlan(fixture.stages(), fixture.runStage().getStageId()));

        assertEquals("Selected reference genome does not exist", exception.getDesc());
        assertEquals(PIPELINE_STAGE_STATUS_PENDING, fixture.runStage().getStatus());
    }

    @ParameterizedTest
    @EnumSource(Consumer.class)
    void rejectsMalformedInlineInsteadOfUsingLegacyUrl(Consumer consumer) throws Exception {
        Fixture fixture = fixture(consumer);
        fixture.selection().setOutputInline("{broken json");
        fixture.selection().setOutputUrl(selectionJson("LEGACY"));

        assertThrows(JsonProcessingException.class,
                () -> orchestrator.makePlan(fixture.stages(), fixture.runStage().getStageId()));
        assertEquals(PIPELINE_STAGE_STATUS_PENDING, fixture.runStage().getStatus());
    }

    private void assertScheduledReference(Consumer consumer, Fixture fixture, String accession) throws Exception {
        OrchestratePlan plan = orchestrator.makePlan(fixture.stages(), fixture.runStage().getStageId());

        assertEquals(1, plan.getRunStages().size());
        assertSame(fixture.runStage(), plan.getRunStages().get(0));
        assertEquals(PIPELINE_STAGE_STATUS_QUEUING, fixture.runStage().getStatus());
        assertEquals(1, plan.getUpdateStageCommands().size());
        UpdateStageCommand update = plan.getUpdateStageCommands().get(0);
        assertEquals(fixture.runStage().getStageId().longValue(), update.getStageId());
        assertEquals(3, update.getCurrentVersion());
        assertEquals(4, update.getUpdateStage().getVersion());

        assertReference(consumer, update.getUpdateStage().getParameters(), accession);
        assertReference(consumer, fixture.runStage().getParameters(), accession);
    }

    private static void assertReference(Consumer consumer, String parametersJson, String accession)
            throws Exception {
        BaseStageParams parameters = JsonUtil.toObject(parametersJson, consumer.parametersType);
        ReferenceGenome reference = parameters.getReferenceGenome();
        assertNotNull(reference);
        assertEquals(1234, reference.getTaxId());
        assertEquals(1, reference.getSequences().size());
        ReferenceSequence sequence = reference.getSequences().get(0);
        assertEquals(accession, sequence.getAccession());
        assertEquals("references/" + accession + ".fasta", sequence.getPath());
        assertEquals("references/" + accession + ".gff", sequence.getAnnotationFile());
        assertEquals("x".repeat(5_000), sequence.getRawMetadata());
    }

    private static Stream<Arguments> consumersWithBlankInline() {
        return Stream.of(Consumer.values()).flatMap(consumer ->
                Stream.of((String) null, "", " \t\n")
                        .map(inline -> Arguments.of(consumer, inline)));
    }

    private static String selectionJson(String accession) throws Exception {
        ReferenceSequence sequence = new ReferenceSequence();
        sequence.setAccession(accession);
        sequence.setTaxId(1234);
        sequence.setOrganismName("Test virus");
        sequence.setCompleteness("COMPLETE");
        sequence.setPath("references/" + accession + ".fasta");
        sequence.setAnnotationFile("references/" + accession + ".gff");
        sequence.setRawMetadata("x".repeat(5_000));
        ReferenceGenome reference = ReferenceGenome.fromSingleSequence(sequence);
        return JsonUtil.toJson(new ReferenceSelectionStageResult(reference, List.of()));
    }

    private static Fixture fixture(Consumer consumer) throws Exception {
        List<BioPipelineStage> stages = new ArrayList<>();
        stages.add(stage(PIPELINE_STAGE_READ_INSPECT,
                JsonUtil.toJson(new ReadInspectStageResult())));
        stages.add(stage(PIPELINE_STAGE_QC,
                JsonUtil.toJson(new QcResult("cleaned-r1.fq", "cleaned-r2.fq", null, null))));
        stages.add(stage(PIPELINE_STAGE_ASSEMBLY, null));
        BioPipelineStage selection = stage(PIPELINE_STAGE_REFERENCE_SELECTION, null);
        stages.add(selection);
        stages.add(stage(PIPELINE_STAGE_MAPPING,
                JsonUtil.toJson(new MappingResult("mapping.bam", "mapping.bam.bai", "depth.tsv"))));
        stages.add(stage(PIPELINE_STAGE_VARIANT_CALL,
                JsonUtil.toJson(new VarientCallStageResult("variants.vcf.gz", "variants.vcf.gz.tbi"))));
        stages.add(stage(PIPELINE_STAGE_CONSENSUS, null));
        stages.add(stage(PIPELINE_STAGE_REFERENCE_COMPARISON, null));
        stages.add(stage(PIPELINE_STAGE_SNP_ANNOTATION, null));

        BioPipelineStage runStage = stages.stream()
                .filter(stage -> stage.getStageType() == consumer.stageType)
                .findFirst().orElseThrow();
        runStage.setStatus(PIPELINE_STAGE_STATUS_PENDING);
        return new Fixture(stages, selection, runStage);
    }

    private static BioPipelineStage stage(int stageType, String outputUrl) {
        BioPipelineStage stage = new BioPipelineStage();
        stage.setStageId((long) stageType);
        stage.setPipelineId(1L);
        stage.setStageType(stageType);
        stage.setStageIndex(-1);
        stage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        stage.setVersion(3);
        stage.setOutputUrl(outputUrl);
        return stage;
    }

    private record Fixture(List<BioPipelineStage> stages, BioPipelineStage selection,
            BioPipelineStage runStage) {
    }

    private enum Consumer {
        MAPPING(PIPELINE_STAGE_MAPPING, MappingParameters.class),
        VARIANT_CALL(PIPELINE_STAGE_VARIANT_CALL, VarientCallParameters.class),
        CONSENSUS(PIPELINE_STAGE_CONSENSUS, ConsensusStageParameters.class),
        REFERENCE_COMPARISON(PIPELINE_STAGE_REFERENCE_COMPARISON, ReferenceComparisonStageParameters.class),
        SNP_ANNOTATION(PIPELINE_STAGE_SNP_ANNOTATION, SNPAnnotationStageParameters.class);

        private final int stageType;
        private final Class<? extends BaseStageParams> parametersType;

        Consumer(int stageType, Class<? extends BaseStageParams> parametersType) {
            this.stageType = stageType;
            this.parametersType = parametersType;
        }
    }
}
