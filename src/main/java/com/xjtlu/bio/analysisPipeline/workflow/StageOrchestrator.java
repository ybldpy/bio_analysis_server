package com.xjtlu.bio.analysisPipeline.workflow;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.mysql.cj.x.protobuf.MysqlxCrud.OrderOrBuilder;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.context.domain.TaxonomyContext;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceGenome;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.AMRInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.AssemblyInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ConsensusStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.MLSTStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.MappingInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.QcStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReferenceSelectionStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.SNPAnnotationInputs;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.SeroTypeStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.TaxonomyStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.VFStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.VarientCallInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.AMRParamters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ConsensusStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.MappingParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.QcParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ReferenceComparisonStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.SNPAnnotationStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.SeroTypingStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.VFParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.VarientCallParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.SequenceMeta;
import com.xjtlu.bio.analysisPipeline.stageResult.AssemblyResult;
import com.xjtlu.bio.analysisPipeline.stageResult.MappingResult;
import com.xjtlu.bio.analysisPipeline.stageResult.QcResult;
import com.xjtlu.bio.analysisPipeline.stageResult.ReadInspectStageResult;
import com.xjtlu.bio.analysisPipeline.stageResult.ReferenceSelectionStageResult;
import com.xjtlu.bio.analysisPipeline.stageResult.TaxonomyResult;
import com.xjtlu.bio.analysisPipeline.stageResult.VarientCallStageResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.SeroTypingStageExectuor;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.command.UpdateStageCommand;
import com.xjtlu.bio.utils.JsonUtil;

import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.*;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.*;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

@Component
public class StageOrchestrator {

    private static final Map<Integer, Set<Integer>> REQUIRES = Map.ofEntries(
            Map.entry(PIPELINE_STAGE_QC, Set.of(PIPELINE_STAGE_READ_INSPECT)),
            Map.entry(PIPELINE_STAGE_ASSEMBLY, Set.of(PIPELINE_STAGE_QC)),
            Map.entry(PIPELINE_STAGE_REFERENCE_SELECTION, Set.of(PIPELINE_STAGE_ASSEMBLY)),
            Map.entry(PIPELINE_STAGE_REFERENCE_COMPARISON, Set.of(PIPELINE_STAGE_REFERENCE_SELECTION)),
            Map.entry(PIPELINE_STAGE_MAPPING, Set.of(PIPELINE_STAGE_REFERENCE_SELECTION)),
            Map.entry(PIPELINE_STAGE_VARIANT_CALL, Set.of(PIPELINE_STAGE_MAPPING)),
            Map.entry(PIPELINE_STAGE_CONSENSUS, Set.of(PIPELINE_STAGE_VARIANT_CALL)),
            Map.entry(PIPELINE_STAGE_TAXONOMY, Set.of(PIPELINE_STAGE_ASSEMBLY)),
            Map.entry(PIPELINE_STAGE_MLST, Set.of(PIPELINE_STAGE_ASSEMBLY)),
            Map.entry(PIPELINE_STAGE_AMR, Set.of(PIPELINE_STAGE_ASSEMBLY)),
            Map.entry(PIPELINE_STAGE_VIRULENCE, Set.of(PIPELINE_STAGE_ASSEMBLY)),
            Map.entry(PIPELINE_STAGE_SNP_ANNOTATION, Set.of(PIPELINE_STAGE_VARIANT_CALL)),
            Map.entry(PIPELINE_STAGE_SEROTYPE, Set.of(PIPELINE_STAGE_ASSEMBLY, PIPELINE_STAGE_TAXONOMY)));

    public StageOrchestrator() {

    }

    public static class MissingUpstreamException extends Exception {

        private String desc;

        public MissingUpstreamException() {
            this("Upstream stage not finished yet");
        }

        public MissingUpstreamException(String desc) {
            this.desc = desc;
        }

        public String getDesc() {
            return desc;
        }

    }

    public static class OrchestratePlan {

        private final List<UpdateStageCommand> updateStageCommands;
        private final List<BioPipelineStage> runStages;
        private final boolean noNextStage;

        public final List<BioPipelineStage> getRunStages() {
            return runStages;
        }

        public OrchestratePlan() {
            this(false);
        }

        public OrchestratePlan(boolean noNextStage) {
            this.noNextStage = noNextStage;
            this.updateStageCommands = new ArrayList<>();
            this.runStages = new ArrayList<>();
        }

        public List<UpdateStageCommand> getUpdateStageCommands() {
            return updateStageCommands;
        }

        public boolean isNoNextStage() {
            return noNextStage;
        }

    }

    private void applyUpdatesToUpdateStage(BioPipelineStage patch, BioPipelineStage toUpdateStage, String inputUrl,
            String params, int status, int currentVersion) {
        boolean setCache = toUpdateStage != null;

        if (StringUtils.isNotBlank(inputUrl)) {
            patch.setInputUrl(inputUrl);
            if (setCache)
                toUpdateStage.setInputUrl(inputUrl);
        }
        if (StringUtils.isNotBlank(params)) {
            patch.setParameters(params);
            if (setCache)
                toUpdateStage.setParameters(params);
        }
        if (status >= 0) {
            patch.setStatus(status);
            if (setCache)
                toUpdateStage.setStatus(status);
        }

        patch.setVersion(currentVersion + 1);
        if (setCache)
            toUpdateStage.setVersion(currentVersion + 1);

    }

    private OrchestratePlan planDownstreamQc(List<BioPipelineStage> allStages)
            throws JsonProcessingException, MissingUpstreamException {
        BioPipelineStage assembly = findStageFromStages(allStages, PIPELINE_STAGE_ASSEMBLY);
        return makePlan(allStages, assembly.getStageId());
    }

    // 病原学特征分析
    private OrchestratePlan planBacteriaPathogenAnalysis(List<BioPipelineStage> allStages)
            throws MissingUpstreamException, JsonMappingException, JsonProcessingException {

        BioPipelineStage assembly = findStageFromStages(allStages, PIPELINE_STAGE_ASSEMBLY);
        BioPipelineStage taxonomy = findStageFromStages(allStages, PIPELINE_STAGE_TAXONOMY);
        if (assembly != null && assembly.getStatus() != PIPELINE_STAGE_STATUS_FINISHED) {
            return new OrchestratePlan();
        }

        BioPipelineStage amr = findStageFromStages(allStages, PIPELINE_STAGE_AMR);
        BioPipelineStage vf = findStageFromStages(allStages, PIPELINE_STAGE_VIRULENCE);
        BioPipelineStage mlst = findStageFromStages(allStages, PIPELINE_STAGE_MLST);

        OrchestratePlan plan = new OrchestratePlan();

        if (amr.getStatus() == PIPELINE_STAGE_STATUS_PENDING) {
            OrchestratePlan amrPlan = makePlan(allStages, amr.getStageId());
            plan.runStages.addAll(amrPlan.runStages);
            plan.updateStageCommands.addAll(amrPlan.updateStageCommands);
        }

        if (vf.getStatus() == PIPELINE_STAGE_STATUS_PENDING) {
            OrchestratePlan vfPlan = makePlan(allStages, vf.getStageId());
            plan.runStages.addAll(vfPlan.getRunStages());
            plan.updateStageCommands.addAll(vfPlan.getUpdateStageCommands());
        }
        if (taxonomy.getStatus() == PIPELINE_STAGE_STATUS_PENDING) {
            OrchestratePlan taxonomyPlan = makePlan(allStages, taxonomy.getStageId());
            plan.runStages.addAll(taxonomyPlan.getRunStages());
            plan.updateStageCommands.addAll(taxonomyPlan.getUpdateStageCommands());
        }

        if (mlst.getStatus() == PIPELINE_STAGE_STATUS_PENDING) {
            OrchestratePlan mlstPlan = makePlan(allStages, mlst.getStageId());
            plan.runStages.addAll(mlstPlan.getRunStages());
            plan.updateStageCommands.addAll(mlstPlan.getUpdateStageCommands());
        }

        // the result has been comfirmed
        // if (taxonomy.getStatus() == PIPELINE_STAGE_STATUS_FINISHED) {

        // BioPipelineStage mlst = findStageFromStages(allStages, PIPELINE_STAGE_MLST);
        // BioPipelineStage serotypeStage = findStageFromStages(allStages,
        // PIPELINE_STAGE_SEROTYPE);

        // OrchestratePlan mlstPlan = makePlan(allStages, mlst.getStageId());
        // OrchestratePlan seroTypePlan = makePlan(allStages,
        // serotypeStage.getStageId());

        // plan.runStages.addAll(mlstPlan.runStages);
        // plan.runStages.addAll(seroTypePlan.runStages);

        // plan.updateStageCommands.addAll(mlstPlan.updateStageCommands);
        // plan.updateStageCommands.addAll(seroTypePlan.updateStageCommands);
        // }

        return plan;

    }

    private OrchestratePlan planDownstreamAssembly(List<BioPipelineStage> allStages, BioPipelineStage assembly,
            int pipelineType)
            throws JsonProcessingException, MissingUpstreamException {

        if (pipelineType == Constants.PipelineType.PIPELINE_REGULAR_BACTERIA) {
            return planBacteriaPathogenAnalysis(allStages);
        }
        // should select an best candiate reference
        BioPipelineStage referenceSelectionStage = findStageFromStages(allStages, PIPELINE_STAGE_REFERENCE_SELECTION);
        return planForReferenceSelection(allStages, referenceSelectionStage);

    }

    public OrchestratePlan planDownstreamReferenceSelection(List<BioPipelineStage> allStages,
            BioPipelineStage referenceSelectionStage)
            throws JsonProcessingException, MissingUpstreamException {

        BioPipelineStage referenceComparisonStage = findStageFromStages(
                allStages, PIPELINE_STAGE_REFERENCE_COMPARISON);
        if (referenceComparisonStage != null) {
            return makePlan(allStages, referenceComparisonStage.getStageId());
        }

        BioPipelineStage mappingStage = findStageFromStages(allStages, PIPELINE_STAGE_MAPPING);
        return makePlan(allStages, mappingStage.getStageId());
    }

    // 病毒才做mapping后续阶段
    // 这边先顺序跑
    public OrchestratePlan planDownstreamMapping(List<BioPipelineStage> allStages, BioPipelineStage mappingStage)
            throws JsonMappingException, JsonProcessingException, MissingUpstreamException {

        BioPipelineStage vcStage = findStageFromStages(
                allStages,
                PIPELINE_STAGE_VARIANT_CALL);

        return makePlan(allStages, vcStage.getStageId());
    }

    public OrchestratePlan planDownstreamVarientCall(List<BioPipelineStage> allStages,
            BioPipelineStage varientCallStage)
            throws JsonMappingException, JsonProcessingException, MissingUpstreamException {

        BioPipelineStage consensusStage = findStageFromStages(allStages, PIPELINE_STAGE_CONSENSUS);
        BioPipelineStage snpAnnotationStage = findStageFromStages(allStages, PIPELINE_STAGE_SNP_ANNOTATION);

        if (consensusStage == null && snpAnnotationStage == null) {
            return noDownstreamPlan();
        }

        OrchestratePlan plan = new OrchestratePlan();

        OrchestratePlan consensusPlan = makePlan(allStages, consensusStage.getStageId());
        plan.runStages.addAll(consensusPlan.getRunStages());
        plan.updateStageCommands.addAll(consensusPlan.getUpdateStageCommands());

        if (snpAnnotationStage != null) {
            OrchestratePlan snpAnnotationPlan = makePlan(allStages, snpAnnotationStage.getStageId());
            plan.runStages.addAll(snpAnnotationPlan.getRunStages());
            plan.updateStageCommands.addAll(snpAnnotationPlan.getUpdateStageCommands());
        }

        return plan;
    }

    private void validateUpstreamStages(List<BioPipelineStage> allStages, long runStageId)
            throws MissingUpstreamException {

        BioPipelineStage runStage = allStages.stream().filter(s -> s.getStageId() == runStageId).findFirst()
                .orElse(null);

        Set<Integer> require = new HashSet<>(
                REQUIRES.getOrDefault(runStage.getStageType(), Set.of()));

        Set<Integer> allStageTypes = new HashSet<>();

        allStages.forEach(s -> {
            if (require.contains(s.getStageType()) && (s.getStatus() == PIPELINE_STAGE_STATUS_FINISHED)) {
                require.remove(s.getStageType());
            }
            allStageTypes.add(s.getStageType());
        });

        Set<Integer> nonExistType = new HashSet<>();
        for (Integer requiredType : require) {
            if (!allStageTypes.contains(requiredType)) {
                nonExistType.add(requiredType);
            }
        }
        require.removeAll(nonExistType);
        if (!require.isEmpty()) {
            throw new MissingUpstreamException();
        }

        if (runStage.getStageType() != PIPELINE_STAGE_MAPPING) {
            return;
        }

        BioPipelineStage assemblyStage = findStageFromStages(allStages, PIPELINE_STAGE_ASSEMBLY);

        if (assemblyStage != null && assemblyStage.getStatus() != PIPELINE_STAGE_STATUS_FINISHED) {
            throw new MissingUpstreamException();
        }

    }

    private OrchestratePlan planForAssembly(BioPipelineStage assebmlyStage, List<BioPipelineStage> upstreamStages)
            throws JsonMappingException, JsonProcessingException {

        OrchestratePlan plan = new OrchestratePlan();
        BioPipelineStage patch = new BioPipelineStage();
        String serializedParams = null;
        BioPipelineStage qcStage = upstreamStages.stream().filter(s -> s.getStageType() == PIPELINE_STAGE_QC)
                .findFirst().orElse(null);

        AssemblyInputUrls assemblyInputUrls = new AssemblyInputUrls();

        BioPipelineStage readInspectStage = findStageFromStages(upstreamStages, PIPELINE_STAGE_READ_INSPECT);
        ReadInspectStageResult readInspectStageResult = JsonUtil.toObject(readInspectStage.getOutputUrl(),
                ReadInspectStageResult.class);

        if (qcStage != null) {
            QcResult qcResult = JsonUtil.toObject(qcStage.getOutputUrl(), QcResult.class);
            assemblyInputUrls.setRead1Url(qcResult.getCleanedR1());
            assemblyInputUrls.setRead2Url(qcResult.getCleanedR2());
        } else {
            assemblyInputUrls.setRead1Url(readInspectStageResult.getR1Url());
            assemblyInputUrls.setRead2Url(readInspectStageResult.getR2Url());
        }

        String serializedInputMap = JsonUtil.toJson(assemblyInputUrls);

        this.applyUpdatesToUpdateStage(patch, assebmlyStage, serializedInputMap, serializedParams,
                PIPELINE_STAGE_STATUS_QUEUING, assebmlyStage.getVersion());

        plan.updateStageCommands
                .add(new UpdateStageCommand(patch, assebmlyStage.getStageId(), assebmlyStage.getVersion() - 1));
        plan.runStages.add(assebmlyStage);
        return plan;
    }

    private OrchestratePlan planForMapping(BioPipelineStage mappingStage, List<BioPipelineStage> allStages)
            throws JsonMappingException, JsonProcessingException, MissingUpstreamException {
        OrchestratePlan plan = new OrchestratePlan();
        BioPipelineStage patch = new BioPipelineStage();

        BioPipelineStage readInspectStage = findStageFromStages(allStages, PIPELINE_STAGE_READ_INSPECT);
        ReadInspectStageResult readInspectStageResult = JsonUtil.toObject(readInspectStage.getOutputUrl(),
                ReadInspectStageResult.class);

        BioPipelineStage qcStage = findStageFromStages(allStages, PIPELINE_STAGE_QC);
        QcResult qcResult = JsonUtil.toObject(qcStage.getOutputUrl(), QcResult.class);

        MappingParameters mappingParameters = Objects.requireNonNullElse(
                JsonUtil.toObject(mappingStage.getParameters(), MappingParameters.class),
                new MappingParameters());
        MappingInputUrls mappingInputUrls = new MappingInputUrls();

        mappingInputUrls.setR1Url(qcResult.getCleanedR1());
        mappingInputUrls.setR2Url(qcResult.getCleanedR2());
        mappingParameters.setReadMeta(readInspectStageResult.getReadMeta());
        mappingParameters.setReferenceGenome(selectedReferenceGenome(allStages));

        this.applyUpdatesToUpdateStage(patch, mappingStage, JsonUtil.toJson(mappingInputUrls),
                JsonUtil.toJson(mappingParameters), PIPELINE_STAGE_STATUS_QUEUING,
                mappingStage.getVersion());

        plan.updateStageCommands
                .add(new UpdateStageCommand(patch, mappingStage.getStageId(), mappingStage.getVersion() - 1));
        plan.runStages.add(mappingStage);
        return plan;

    }

    private OrchestratePlan planForVarientCall(BioPipelineStage varientCallStage,
            List<BioPipelineStage> upstreamStages)
            throws JsonMappingException, JsonProcessingException, MissingUpstreamException {

        OrchestratePlan plan = new OrchestratePlan();
        BioPipelineStage patch = new BioPipelineStage();

        BioPipelineStage mappingStage = upstreamStages.stream().filter(s -> s.getStageType() == PIPELINE_STAGE_MAPPING)
                .findFirst().orElse(null);

        VarientCallParameters varientCallParameters = Objects.requireNonNullElse(
                JsonUtil.toObject(varientCallStage.getParameters(), VarientCallParameters.class),
                new VarientCallParameters());

        varientCallParameters.setReferenceGenome(selectedReferenceGenome(upstreamStages));

        VarientCallInputUrls varientCallInputUrls = new VarientCallInputUrls();
        MappingResult mappingResult = JsonUtil.toObject(mappingStage.getOutputUrl(), MappingResult.class);

        varientCallInputUrls.setBamUrl(mappingResult.getBamUrl());
        varientCallInputUrls.setBamIndexUrl(mappingResult.getBamIndexUrl());

        this.applyUpdatesToUpdateStage(patch, varientCallStage, JsonUtil.toJson(varientCallInputUrls),
                JsonUtil.toJson(varientCallParameters), PIPELINE_STAGE_STATUS_QUEUING, varientCallStage.getVersion());

        plan.updateStageCommands
                .add(new UpdateStageCommand(patch, varientCallStage.getStageId(), varientCallStage.getVersion() - 1));
        plan.runStages.add(varientCallStage);
        return plan;

    }

    private OrchestratePlan planForConsensus(BioPipelineStage consensusStage, List<BioPipelineStage> upstreamStages)
            throws JsonMappingException, JsonProcessingException, MissingUpstreamException {
        // the final one

        OrchestratePlan plan = new OrchestratePlan();
        BioPipelineStage patch = new BioPipelineStage();

        BioPipelineStage varientStage = upstreamStages.stream()
                .filter(s -> s.getStageType() == PIPELINE_STAGE_VARIANT_CALL).findFirst().orElse(null);

        VarientCallStageResult varientCallStageResult = JsonUtil.toObject(varientStage.getOutputUrl(),
                VarientCallStageResult.class);

        ConsensusStageInputUrls consensusStageInputUrls = new ConsensusStageInputUrls();
        consensusStageInputUrls.setVcfGz(varientCallStageResult.getVcfGzUrl());
        consensusStageInputUrls.setVcfTbi(varientCallStageResult.getVcfTbiUrl());

        ConsensusStageParameters consensusStageParameters = JsonUtil.toObject(consensusStage.getParameters(),
                ConsensusStageParameters.class);
        consensusStageParameters.setReferenceGenome(selectedReferenceGenome(upstreamStages));

        this.applyUpdatesToUpdateStage(patch, consensusStage, JsonUtil.toJson(consensusStageInputUrls),
                JsonUtil.toJson(consensusStageParameters), PIPELINE_STAGE_STATUS_QUEUING, consensusStage.getVersion());

        plan.updateStageCommands
                .add(new UpdateStageCommand(patch, consensusStage.getStageId(), consensusStage.getVersion() - 1));
        plan.runStages.add(consensusStage);

        return plan;
    }

    // private static ReadMeta buildReadMeta(ReadInspectStageResult
    // readInspectStageResult) {
    // return new ReadMeta(readInspectStageResult.getQualityEncoding(),
    // readInspectStageResult.getReadLenType());
    // }

    private OrchestratePlan planForQc(BioPipelineStage qcStage, List<BioPipelineStage> pipelineStages)
            throws JsonMappingException, JsonProcessingException {

        BioPipelineStage readInspectStage = findStageFromStages(pipelineStages, PIPELINE_STAGE_READ_INSPECT);
        OrchestratePlan plan = new OrchestratePlan();
        BioPipelineStage patch = new BioPipelineStage();

        ReadInspectStageResult readInspectStageResult = JsonUtil.toObject(readInspectStage.getOutputUrl(),
                ReadInspectStageResult.class);

        String r1Url = readInspectStageResult.getR1Url();
        String r2Url = readInspectStageResult.getR2Url();

        SequenceMeta readMeta = readInspectStageResult.getReadMeta();

        QcParameters qcParameters = JsonUtil.toObject(qcStage.getParameters(), QcParameters.class);
        qcParameters.setReadMeta(readMeta);

        String serializedQcParameters = JsonUtil.toJson(qcParameters);
        QcStageInputUrls qcStageInputUrls = new QcStageInputUrls();
        qcStageInputUrls.setRead1(r1Url);
        qcStageInputUrls.setRead2(r2Url);

        String serializedInputUrls = JsonUtil.toJson(qcStageInputUrls);

        this.applyUpdatesToUpdateStage(patch, qcStage, serializedInputUrls, serializedQcParameters,
                PIPELINE_STAGE_STATUS_QUEUING,
                qcStage.getVersion());
        plan.updateStageCommands.add(new UpdateStageCommand(patch, qcStage.getStageId(), qcStage.getVersion() - 1));
        plan.runStages.add(qcStage);
        return plan;
    }


    private OrchestratePlan planForTaxonomy(List<BioPipelineStage> upstreamStages, BioPipelineStage taxStage)
            throws JsonMappingException, JsonProcessingException {

        OrchestratePlan plan = new OrchestratePlan();
        BioPipelineStage patch = new BioPipelineStage();
        TaxonomyStageInputUrls taxonomyStageInputUrls = new TaxonomyStageInputUrls();

        BioPipelineStage readInspect = findStageFromStages(upstreamStages, PIPELINE_STAGE_READ_INSPECT);
        ReadInspectStageResult readInspectStageResult = JsonUtil.toObject(readInspect.getOutputUrl(),
                ReadInspectStageResult.class);
        BioPipelineStage qc = upstreamStages.stream().filter(s -> s.getStageType() == PIPELINE_STAGE_QC).findFirst()
                .orElse(null);
        BioPipelineStage assembly = findStageFromStages(upstreamStages, PIPELINE_STAGE_ASSEMBLY);
        AssemblyResult assemblyResult = JsonUtil.toObject(assembly.getOutputUrl(), AssemblyResult.class);
        taxonomyStageInputUrls.setContigs(assemblyResult.getContigsUrl());

        if (qc != null) {
            QcResult qcResult = JsonUtil.toObject(qc.getOutputUrl(), QcResult.class);
            taxonomyStageInputUrls.setR1(qcResult.getCleanedR1());
            taxonomyStageInputUrls.setR2(qcResult.getCleanedR2());
        } else {
            taxonomyStageInputUrls.setR1(readInspectStageResult.getR1Url());
            taxonomyStageInputUrls.setR2(readInspectStageResult.getR2Url());
        }

        this.applyUpdatesToUpdateStage(patch, taxStage, JsonUtil.toJson(taxonomyStageInputUrls), null,
                PIPELINE_STAGE_STATUS_QUEUING, taxStage.getVersion());
        plan.runStages.add(taxStage);
        plan.updateStageCommands.add(new UpdateStageCommand(patch, taxStage.getStageId(), taxStage.getVersion() - 1));
        return plan;
    }

    private OrchestratePlan planForMLST(List<BioPipelineStage> upstreamStages, BioPipelineStage mlstStage)
            throws JsonMappingException, JsonProcessingException {
        OrchestratePlan plan = new OrchestratePlan();

        BioPipelineStage assembly = upstreamStages.stream().filter(s -> s.getStageType() == PIPELINE_STAGE_ASSEMBLY)
                .findFirst().orElse(null);

        AssemblyResult assemblyResult = JsonUtil.toObject(assembly.getOutputUrl(), AssemblyResult.class);

        MLSTStageInputUrls mlstStageInputUrls = new MLSTStageInputUrls(assemblyResult.getContigsUrl());
        BaseStageParams params = JsonUtil.toObject(mlstStage.getParameters(), BaseStageParams.class);

        String serializedInput = JsonUtil.toJson(mlstStageInputUrls);
        String serializedParams = JsonUtil.toJson(params);

        BioPipelineStage patch = new BioPipelineStage();
        this.applyUpdatesToUpdateStage(patch, mlstStage, serializedInput, serializedParams,
                PIPELINE_STAGE_STATUS_QUEUING, mlstStage.getVersion());
        plan.runStages.add(mlstStage);
        plan.updateStageCommands
                .add(new UpdateStageCommand(patch, mlstStage.getStageId(), mlstStage.getVersion() - 1));
        return plan;
    }

    private static BioPipelineStage findStageFromStages(List<BioPipelineStage> stages, int stageType) {
        return stages.stream().filter(s -> s.getStageType() == stageType).findFirst().orElse(null);
    }

    private ReferenceGenome selectedReferenceGenome(List<BioPipelineStage> stages)
            throws JsonProcessingException, MissingUpstreamException {

        BioPipelineStage referenceSelectionStage = findStageFromStages(
                stages, PIPELINE_STAGE_REFERENCE_SELECTION);
        if (referenceSelectionStage == null
                || StringUtils.isBlank(referenceSelectionStage.getOutputUrl())) {
            throw new MissingUpstreamException("Reference selection result does not exist");
        }

        ReferenceSelectionStageResult selectionResult = JsonUtil.toObject(
                referenceSelectionStage.getOutputUrl(), ReferenceSelectionStageResult.class);
        if (selectionResult == null || selectionResult.getSelectedReference() == null) {
            throw new MissingUpstreamException("Selected reference genome does not exist");
        }

        return selectionResult.getSelectedReference();
    }

    private String selectedReferenceAnnotationUrl(ReferenceGenome referenceGenome) {
        if (referenceGenome == null
                || referenceGenome.getSequences() == null
                || referenceGenome.getSequences().size() != 1
                || referenceGenome.getSequences().get(0) == null) {
            return null;
        }
        return referenceGenome.getSequences().get(0).getAnnotationFile();
    }

    private OrchestratePlan planDownstreamTaxonomy(List<BioPipelineStage> stages, BioPipelineStage taxonomyStage)
            throws JsonMappingException, JsonProcessingException, MissingUpstreamException {

        // return planBacteriaPathogenAnalysis(stages);

        BioPipelineStage serotypeStage = findStageFromStages(stages, PIPELINE_STAGE_SEROTYPE);
        if (serotypeStage.getStatus() == PIPELINE_STAGE_STATUS_PENDING) {
            return makePlan(stages, serotypeStage.getStageId());
        }

        return new OrchestratePlan();

    }

    private OrchestratePlan planForReferenceSelection(List<BioPipelineStage> stages,
            BioPipelineStage referenceSelectionStage) throws JsonMappingException, JsonProcessingException {

        ReferenceSelectionStageInputUrls referenceSelectionStageInputUrls = new ReferenceSelectionStageInputUrls();
        BioPipelineStage assemblyStage = stages.stream().filter(s -> s.getStageType() == PIPELINE_STAGE_ASSEMBLY)
                .findFirst().orElse(null);
        AssemblyResult assemblyResult = JsonUtil.toObject(assemblyStage.getOutputUrl(), AssemblyResult.class);
        referenceSelectionStageInputUrls.setContigsUrl(assemblyResult.getContigsUrl());

        BioPipelineStage patch = new BioPipelineStage();

        String serializedInputUrl = JsonUtil.toJson(referenceSelectionStageInputUrls);

        int curVersion = referenceSelectionStage.getVersion();
        applyUpdatesToUpdateStage(patch, referenceSelectionStage, serializedInputUrl, null,
                PIPELINE_STAGE_STATUS_QUEUING, curVersion);

        OrchestratePlan plan = new OrchestratePlan();
        plan.runStages.add(referenceSelectionStage);
        plan.updateStageCommands.add(new UpdateStageCommand(patch, referenceSelectionStage.getStageId(), curVersion));

        return plan;
    }

    private OrchestratePlan planForReferenceComparison(List<BioPipelineStage> stages,
            BioPipelineStage referenceComparisonStage)
            throws JsonProcessingException, MissingUpstreamException {

        ReferenceComparisonStageParameters parameters = Objects.requireNonNullElse(
                JsonUtil.toObject(referenceComparisonStage.getParameters(),
                        ReferenceComparisonStageParameters.class),
                new ReferenceComparisonStageParameters());
        parameters.setReferenceGenome(selectedReferenceGenome(stages));

        BioPipelineStage patch = new BioPipelineStage();
        int currentVersion = referenceComparisonStage.getVersion();
        applyUpdatesToUpdateStage(
                patch,
                referenceComparisonStage,
                null,
                JsonUtil.toJson(parameters),
                PIPELINE_STAGE_STATUS_QUEUING,
                currentVersion);

        OrchestratePlan plan = new OrchestratePlan();
        plan.runStages.add(referenceComparisonStage);
        plan.updateStageCommands.add(new UpdateStageCommand(
                patch, referenceComparisonStage.getStageId(), currentVersion));
        return plan;
    }

    private OrchestratePlan planForSeroType(List<BioPipelineStage> stages, BioPipelineStage seroTypeStage)
            throws JsonMappingException, JsonProcessingException {

        BioPipelineStage taxonomy = findStageFromStages(stages, PIPELINE_STAGE_TAXONOMY);
        TaxonomyResult taxonomyResult = JsonUtil.toObject(taxonomy.getOutputInline(), TaxonomyResult.class);

        TaxonomyContext taxonomyContext = TaxonomyContext.of(taxonomyResult);
        boolean canDoSeroType = SeroTypingStageExectuor.canDoSeroType(taxonomyContext);

        BioPipelineStage patch = new BioPipelineStage();

        if (!canDoSeroType) {
            patch.setStatus(PIPELINE_STAGE_STATUS_NOT_APPLICABLE);
            OrchestratePlan plan = new OrchestratePlan();
            plan.updateStageCommands
                    .add(new UpdateStageCommand(patch, seroTypeStage.getStageId(), seroTypeStage.getVersion()));
            return plan;
        }
        int inputType = SeroTypingStageExectuor.inputType(taxonomyContext);
        if (inputType == SeroTypingStageExectuor.INPUT_TYPE_READS) {
            BioPipelineStage qc = findStageFromStages(stages, PIPELINE_STAGE_QC);

            if (qc == null) {
                OrchestratePlan plan = new OrchestratePlan();
                patch.setStatus(PIPELINE_STAGE_STATUS_NOT_APPLICABLE);
                plan.updateStageCommands
                        .add(new UpdateStageCommand(patch, seroTypeStage.getStageId(), seroTypeStage.getVersion()));

                return plan;
            }
        }

        SeroTypeStageInputUrls seroTypeStageInputUrls = new SeroTypeStageInputUrls();
        TaxonomyStageInputUrls taxonomyStageInputUrls = JsonUtil.toObject(taxonomy.getInputUrl(),
                TaxonomyStageInputUrls.class);

        seroTypeStageInputUrls.setR1Url(taxonomyStageInputUrls.getR1());
        seroTypeStageInputUrls.setR2Url(taxonomyStageInputUrls.getR2());
        seroTypeStageInputUrls.setContigsUrl(taxonomyStageInputUrls.getContigs());

        String serializedInput = JsonUtil.toJson(seroTypeStageInputUrls);

        SeroTypingStageParameters seroTypingStageParameters = JsonUtil.toObject(seroTypeStage.getParameters(),
                SeroTypingStageParameters.class);
        seroTypingStageParameters.setTaxonomyContext(taxonomyContext);

        this.applyUpdatesToUpdateStage(patch, seroTypeStage, serializedInput,
                JsonUtil.toJson(seroTypingStageParameters), PIPELINE_STAGE_STATUS_QUEUING, seroTypeStage.getVersion());

        OrchestratePlan plan = new OrchestratePlan();
        plan.runStages.add(seroTypeStage);
        plan.updateStageCommands
                .add(new UpdateStageCommand(patch, seroTypeStage.getStageId(), seroTypeStage.getVersion() - 1));
        return plan;
    }

    private OrchestratePlan planForAMR(List<BioPipelineStage> upstreamStages, BioPipelineStage amrStage)
            throws JsonMappingException, JsonProcessingException {

        BioPipelineStage assembly = findStageFromStages(upstreamStages, PIPELINE_STAGE_ASSEMBLY);

        AssemblyResult assemblyResult = JsonUtil.toObject(assembly.getOutputUrl(), AssemblyResult.class);

        AMRInputUrls amrInputUrls = new AMRInputUrls();
        amrInputUrls.setContigsUrl(assemblyResult.getContigsUrl());

        AMRParamters params = JsonUtil.toObject(amrStage.getParameters(), AMRParamters.class);

        String serializedInput = JsonUtil.toJson(amrInputUrls);
        String serializedParams = JsonUtil.toJson(params);

        BioPipelineStage patch = new BioPipelineStage();
        this.applyUpdatesToUpdateStage(patch, amrStage, serializedInput, serializedParams,
                PIPELINE_STAGE_STATUS_QUEUING, amrStage.getVersion());

        OrchestratePlan plan = new OrchestratePlan();
        plan.runStages.add(amrStage);
        plan.updateStageCommands.add(new UpdateStageCommand(patch, amrStage.getStageId(), amrStage.getVersion() - 1));

        return plan;

    }

    private OrchestratePlan planForVirulenFactorStage(List<BioPipelineStage> upstreamStages, BioPipelineStage vfStage)
            throws JsonMappingException, JsonProcessingException {

        BioPipelineStage assembly = findStageFromStages(upstreamStages, PIPELINE_STAGE_ASSEMBLY);

        AssemblyResult assemblyResult = JsonUtil.toObject(assembly.getOutputUrl(), AssemblyResult.class);

        BioPipelineStage patch = new BioPipelineStage();

        VFParameters vfParameters = JsonUtil.toObject(vfStage.getParameters(), VFParameters.class);

        VFStageInputUrls vfStageInputUrls = new VFStageInputUrls(assemblyResult.getContigsUrl());

        String serializedInput = JsonUtil.toJson(vfStageInputUrls);
        String serializedParams = JsonUtil.toJson(vfParameters);

        this.applyUpdatesToUpdateStage(patch, vfStage, serializedInput, serializedParams, PIPELINE_STAGE_STATUS_QUEUING,
                vfStage.getVersion());

        OrchestratePlan plan = new OrchestratePlan();
        plan.runStages.add(vfStage);
        plan.updateStageCommands.add(new UpdateStageCommand(patch, vfStage.getStageId(), vfStage.getVersion() - 1));

        return plan;

    }

    private OrchestratePlan noDownstreamPlan() {
        return new OrchestratePlan(true);
    }

    private OrchestratePlan makePlanDownstreamAMR(List<BioPipelineStage> stages, BioPipelineStage stage) {
        return noDownstreamPlan();
    }

    private OrchestratePlan makePlanDownstreamMLST(List<BioPipelineStage> stages, BioPipelineStage stage) {
        return noDownstreamPlan();
    }

    private OrchestratePlan makePlanDownstreamVisurFactor(List<BioPipelineStage> stages, BioPipelineStage stage) {
        return noDownstreamPlan();
    }

    private OrchestratePlan makePlanDownstreamSerotype() {
        return noDownstreamPlan();
    }

    /*
     * For covid-19 snp annotation function
     */
    private OrchestratePlan planForSNPAnnotation(List<BioPipelineStage> stages)
            throws JsonMappingException, JsonProcessingException, MissingUpstreamException {

        BioPipelineStage vfStage = findStageFromStages(stages, PIPELINE_STAGE_VARIANT_CALL);
        BioPipelineStage snpAnnotationStage = findStageFromStages(stages, PIPELINE_STAGE_SNP_ANNOTATION);

        SNPAnnotationStageParameters snpAnnotationStageParameters = Objects.requireNonNullElse(
                JsonUtil.toObject(snpAnnotationStage.getParameters(), SNPAnnotationStageParameters.class),
                new SNPAnnotationStageParameters());
        ReferenceGenome referenceGenome = selectedReferenceGenome(stages);
        snpAnnotationStageParameters.setReferenceGenome(referenceGenome);

        BioPipelineStage snpStagePatch = new BioPipelineStage();
        OrchestratePlan plan = new OrchestratePlan();
        int currentVersion = snpAnnotationStage.getVersion();

        if (StringUtils.isBlank(selectedReferenceAnnotationUrl(referenceGenome))) {
            applyUpdatesToUpdateStage(
                    snpStagePatch,
                    snpAnnotationStage,
                    null,
                    JsonUtil.toJson(snpAnnotationStageParameters),
                    PIPELINE_STAGE_STATUS_NOT_APPLICABLE,
                    currentVersion);
            plan.updateStageCommands.add(new UpdateStageCommand(
                    snpStagePatch, snpAnnotationStage.getStageId(), currentVersion));
            return plan;
        }

        VarientCallStageResult varientCallStageResult = JsonUtil.toObject(vfStage.getOutputUrl(),
                VarientCallStageResult.class);

        // String serializedSnpAnnotationParameters =
        // JsonUtil.toJson(snpAnnotationStageParameters);
        SNPAnnotationInputs snpAnnotationInputs = new SNPAnnotationInputs();
        snpAnnotationInputs.setVcfUrl(varientCallStageResult.getVcfGzUrl());

        applyUpdatesToUpdateStage(snpStagePatch,
                snpAnnotationStage,
                JsonUtil.toJson(snpAnnotationInputs),
                JsonUtil.toJson(snpAnnotationStageParameters), PIPELINE_STAGE_STATUS_QUEUING,
                currentVersion);
        plan.runStages.add(snpAnnotationStage);
        plan.updateStageCommands.add(new UpdateStageCommand(snpStagePatch, snpAnnotationStage.getStageId(),
                currentVersion));
        return plan;
    }

    public OrchestratePlan makePlan(List<BioPipelineStage> stages, long runStageId)
            throws JsonMappingException, JsonProcessingException, MissingUpstreamException {

        BioPipelineStage runStage = null;
        for (BioPipelineStage stage : stages) {
            if (stage.getStageId() == runStageId) {
                runStage = stage;
                break;
            }
        }

        // pipeline entrance stage
        if (runStage.getStageIndex() == 0) {

            OrchestratePlan plan = new OrchestratePlan();
            BioPipelineStage patch = new BioPipelineStage();
            int currentVersion = runStage.getVersion();
            this.applyUpdatesToUpdateStage(patch, runStage, (String) null, (String) null, PIPELINE_STAGE_STATUS_QUEUING,
                    currentVersion);
            plan.updateStageCommands.add(new UpdateStageCommand(patch, runStageId, currentVersion));
            plan.runStages.add(runStage);
            return plan;
        }

        this.validateUpstreamStages(stages, runStageId);
        // prerequisize: cannot be null
        BioPipelineStage startStage = stages.stream().filter(s -> s.getStageId() == runStageId).findFirst()
                .orElse(null);
        // List<BioPipelineStage> upstreamStages = findUpstreamStages(stages,
        // startStage);

        if (startStage.getStageType() == PIPELINE_STAGE_ASSEMBLY) {
            return this.planForAssembly(startStage, stages);
        } else if (startStage.getStageType() == PIPELINE_STAGE_MAPPING) {
            return this.planForMapping(startStage, stages);
        } else if (startStage.getStageType() == PIPELINE_STAGE_VARIANT_CALL) {
            return this.planForVarientCall(startStage, stages);
        } else if (startStage.getStageType() == PIPELINE_STAGE_CONSENSUS) {
            return this.planForConsensus(startStage, stages);
        } else if (startStage.getStageType() == PIPELINE_STAGE_QC) {
            return this.planForQc(startStage, stages);
        } else if (startStage.getStageType() == PIPELINE_STAGE_TAXONOMY) {
            return this.planForTaxonomy(stages, startStage);
        } else if (startStage.getStageType() == PIPELINE_STAGE_MLST) {
            return this.planForMLST(stages, startStage);
        } else if (startStage.getStageType() == PIPELINE_STAGE_AMR) {
            return this.planForAMR(stages, startStage);
        } else if (startStage.getStageType() == PIPELINE_STAGE_SEROTYPE) {
            return this.planForSeroType(stages, startStage);
        } else if (startStage.getStageType() == PIPELINE_STAGE_VIRULENCE) {
            return this.planForVirulenFactorStage(stages, startStage);
        } else if (startStage.getStageType() == PIPELINE_STAGE_SNP_ANNOTATION) {
            return this.planForSNPAnnotation(stages);
        } else if (startStage.getStageType() == PIPELINE_STAGE_REFERENCE_SELECTION) {
            return this.planForReferenceSelection(stages, startStage);
        } else if (startStage.getStageType() == PIPELINE_STAGE_REFERENCE_COMPARISON) {
            return this.planForReferenceComparison(stages, startStage);
        }

        return null;

    }

    private OrchestratePlan makeDownstreamPlanConsensus(List<BioPipelineStage> allStages,
            BioPipelineStage consensusStage) {
        return noDownstreamPlan();
    }

    private OrchestratePlan makeDownstreamPlanReadInspect(List<BioPipelineStage> allStages, int pipelineType)
            throws JsonMappingException, JsonProcessingException, MissingUpstreamException {

        BioPipelineStage qcStage = findStageFromStages(allStages, PIPELINE_STAGE_QC);

        if (qcStage != null) {
            return makePlan(allStages, qcStage.getStageId());
        }
        BioPipelineStage assemblyStage = findStageFromStages(allStages, PIPELINE_STAGE_ASSEMBLY);

        return makePlan(allStages, assemblyStage.getStageId());

    }

    public OrchestratePlan makeDownstreamPlan(BioPipelineStage currentStage, List<BioPipelineStage> allStages,
            int pipelineType)
            throws InvocationTargetException, IllegalAccessException, NoSuchMethodException, JsonProcessingException,
            MissingUpstreamException {

        if (currentStage.getStageType() == PIPELINE_STAGE_QC) {
            return planDownstreamQc(allStages);
        } else if (currentStage.getStageType() == PIPELINE_STAGE_ASSEMBLY) {
            return planDownstreamAssembly(allStages, currentStage, pipelineType);
        } else if (currentStage.getStageType() == PIPELINE_STAGE_MAPPING) {
            return planDownstreamMapping(allStages, currentStage);
        } else if (currentStage.getStageType() == PIPELINE_STAGE_VARIANT_CALL) {
            return planDownstreamVarientCall(allStages, currentStage);
        } else if (currentStage.getStageType() == PIPELINE_STAGE_TAXONOMY) {
            return planDownstreamTaxonomy(allStages, currentStage);
        } else if (currentStage.getStageType() == PIPELINE_STAGE_AMR) {
            return this.makePlanDownstreamAMR(allStages, currentStage);
        } else if (currentStage.getStageType() == PIPELINE_STAGE_MLST) {
            return this.makePlanDownstreamMLST(allStages, currentStage);
        } else if (currentStage.getStageType() == PIPELINE_STAGE_VIRULENCE) {
            return this.makePlanDownstreamVisurFactor(allStages, currentStage);
        } else if (currentStage.getStageType() == PIPELINE_STAGE_CONSENSUS) {
            return this.makeDownstreamPlanConsensus(allStages, currentStage);
        } else if (currentStage.getStageType() == PIPELINE_STAGE_SEROTYPE) {
            return this.makePlanDownstreamSerotype();
        } else if (currentStage.getStageType() == PIPELINE_STAGE_READ_INSPECT) {
            return this.makeDownstreamPlanReadInspect(allStages, pipelineType);
        } else if (currentStage.getStageType() == PIPELINE_STAGE_REFERENCE_SELECTION) {
            return this.planDownstreamReferenceSelection(allStages, currentStage);
        } else if (currentStage.getStageType() == PIPELINE_STAGE_REFERENCE_COMPARISON) {
            return this.makeDownstreamPlanReferenceComparison();
        } else if (currentStage.getStageType() == PIPELINE_STAGE_SNP_ANNOTATION) {
            return noDownstreamPlan();
        }
        return null;
    }

    private OrchestratePlan makeDownstreamPlanReferenceComparison() {
        return new OrchestratePlan(true);
    }

    public OrchestratePlan makeDownstreamPlan(long finishedStageId, List<BioPipelineStage> allStages, int pipelineType)
            throws JsonProcessingException, InvocationTargetException, IllegalAccessException, NoSuchMethodException,
            MissingUpstreamException {

        BioPipelineStage finishedStage = allStages.stream().filter(s -> s.getStageId() == finishedStageId).findFirst()
                .orElse(null);
        return makeDownstreamPlan(finishedStage, allStages, pipelineType);

    }

}
