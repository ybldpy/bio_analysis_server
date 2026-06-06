package com.xjtlu.bio.analysisPipeline.workflow;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.ReadMeta;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.Constants.PipelineType;
import com.xjtlu.bio.analysisPipeline.context.domain.TaxonomyContext;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.AMRInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.AssemblyInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ConsensusStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.MLSTStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.MappingInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.QcStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.SeroTypeStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.TaxonomyStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.VFStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.VarientCallInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.AMRParamters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ConsensusStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.MappingParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.QcParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.SeroTypingStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.VFParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.VarientCallParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.RefSeqConfig;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.SequenceMeta;
import com.xjtlu.bio.analysisPipeline.stageResult.AssemblyResult;
import com.xjtlu.bio.analysisPipeline.stageResult.MappingResult;
import com.xjtlu.bio.analysisPipeline.stageResult.QcResult;
import com.xjtlu.bio.analysisPipeline.stageResult.ReadInspectStageResult;
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
            Map.entry(PIPELINE_STAGE_QC, Set.of()),
            Map.entry(PIPELINE_STAGE_ASSEMBLY, Set.of(PIPELINE_STAGE_QC)),
            Map.entry(PIPELINE_STAGE_MAPPING, Set.of(PIPELINE_STAGE_QC)),
            Map.entry(PIPELINE_STAGE_VARIANT_CALL, Set.of(PIPELINE_STAGE_MAPPING)),
            Map.entry(PIPELINE_STAGE_CONSENSUS, Set.of(PIPELINE_STAGE_VARIANT_CALL)),
            Map.entry(PIPELINE_STAGE_TAXONOMY, Set.of(PIPELINE_STAGE_QC)),
            Map.entry(PIPELINE_STAGE_MLST, Set.of(PIPELINE_STAGE_TAXONOMY, PIPELINE_STAGE_ASSEMBLY)),
            Map.entry(PIPELINE_STAGE_AMR, Set.of(PIPELINE_STAGE_ASSEMBLY)),
            Map.entry(PIPELINE_STAGE_VIRULENCE, Set.of(PIPELINE_STAGE_TAXONOMY, PIPELINE_STAGE_ASSEMBLY)),
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

    private void applyUpdatesToUpdateStage(BioPipelineStage updateStage, BioPipelineStage stageInCache, String inputUrl,
            String params, int status, int currentVersion) {
        boolean setCache = stageInCache != null;

        if (inputUrl != null) {
            updateStage.setInputUrl(inputUrl);
            if (setCache)
                stageInCache.setInputUrl(inputUrl);
        }
        if (params != null) {
            updateStage.setParameters(params);
            if (setCache)
                stageInCache.setParameters(params);
        }
        if (status >= 0) {
            updateStage.setStatus(status);
            if (setCache)
                stageInCache.setStatus(status);
        }

        updateStage.setVersion(currentVersion + 1);
        if (setCache)
            stageInCache.setVersion(currentVersion + 1);

    }

    private OrchestratePlan planDownstreamQc(List<BioPipelineStage> allStages, BioPipelineStage qcStage,
            int pipelineType)
            throws JsonProcessingException, MissingUpstreamException {

        if (pipelineType == Constants.PipelineType.PIPELINE_REGULAR_BACTERIA) {
            // taxonomy + assembly
            BioPipelineStage assembly = findStageFromStages(allStages, PIPELINE_STAGE_ASSEMBLY);
            BioPipelineStage taxonomy = findStageFromStages(allStages, PIPELINE_STAGE_TAXONOMY);

            OrchestratePlan nextRunPlan = new OrchestratePlan();
            if (assembly != null) {
                OrchestratePlan assemblyPlan = makePlan(allStages, assembly.getStageId());
                nextRunPlan.runStages.addAll(assemblyPlan.runStages);
                nextRunPlan.updateStageCommands.addAll(assemblyPlan.updateStageCommands);
            }

            if (taxonomy != null) {
                OrchestratePlan taxonomyPlan = makePlan(allStages, taxonomy.getStageId());
                nextRunPlan.runStages.addAll(taxonomyPlan.runStages);
                nextRunPlan.updateStageCommands.addAll(taxonomyPlan.updateStageCommands);
            }

            return nextRunPlan;

        } else if (pipelineType == Constants.PipelineType.PIPELINE_VIRUS
                || pipelineType == Constants.PipelineType.PIPELINE_VIRUS_COVID) {

            // BioPipelineStage assembly = findStageFromStages(allStages,
            // PIPELINE_STAGE_ASSEMBLY);
            // if (assembly != null) {
            // return makePlan(allStages, assembly.getStageId());
            // }

            BioPipelineStage mapping = findStageFromStages(allStages, PIPELINE_STAGE_MAPPING);

            return makePlan(allStages, mapping.getStageId());

        } else if (pipelineType == Constants.PipelineType.PIPELINE_SNP_SUB_ANALYSIS) {
            // TODO: implement later

            return null;
        }

        return null;
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

        // the result has been comfirmed
        if (taxonomy.getStatus() == PIPELINE_STAGE_STATUS_FINISHED) {

            BioPipelineStage mlst = findStageFromStages(allStages, PIPELINE_STAGE_MLST);
            BioPipelineStage serotypeStage = findStageFromStages(allStages, PIPELINE_STAGE_SEROTYPE);

            OrchestratePlan mlstPlan = makePlan(allStages, mlst.getStageId());
            OrchestratePlan seroTypePlan = makePlan(allStages, serotypeStage.getStageId());

            plan.runStages.addAll(mlstPlan.runStages);
            plan.runStages.addAll(seroTypePlan.runStages);

            plan.updateStageCommands.addAll(mlstPlan.updateStageCommands);
            plan.updateStageCommands.addAll(seroTypePlan.updateStageCommands);
        }

        return plan;

    }

    private OrchestratePlan planDownstreamAssembly(List<BioPipelineStage> allStages, BioPipelineStage assembly,
            int pipelineType)
            throws JsonProcessingException, MissingUpstreamException {

        if (pipelineType == Constants.PipelineType.PIPELINE_REGULAR_BACTERIA) {
            return planBacteriaPathogenAnalysis(allStages);
        }

        BioPipelineStage mappingStage = findStageFromStages(allStages, PIPELINE_STAGE_MAPPING);

        if (mappingStage != null) {
            return makePlan(allStages, mappingStage.getStageId());
        }

        return new OrchestratePlan();

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

        if (consensusStage == null) {
            return noDownstreamPlan();
        }

        return makePlan(allStages, consensusStage.getStageId());

    }

    private boolean getReadLenFromReadLenStage(BioPipelineStage readLenStage) {
        if (readLenStage == null) {
            return false;
        }
        String outputStr = readLenStage.getOutputUrl();
        if (StringUtils.isBlank(outputStr)) {
            return false;
        }
        return Boolean.parseBoolean(outputStr);
    }

    private void validateUpstreamStages(List<BioPipelineStage> allStages, long runStageId)
            throws MissingUpstreamException {

        BioPipelineStage runStage = allStages.stream().filter(s -> s.getStageId() == runStageId).findFirst()
                .orElse(null);

        Set<Integer> require = new HashSet<>(REQUIRES.get(runStage.getStageType()));

        allStages.forEach(s -> {
            if (require.contains(s.getStageType()) && s.getStatus() == PIPELINE_STAGE_STATUS_FINISHED) {
                require.remove(s.getStageType());
            }
        });

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

        QcResult qcResult = JsonUtil.toObject(qcStage.getOutputUrl(), QcResult.class);
        AssemblyInputUrls assemblyInputUrls = new AssemblyInputUrls();
        assemblyInputUrls.setRead1Url(qcResult.getCleanedR1());
        assemblyInputUrls.setRead2Url(qcResult.getCleanedR2());

        String serializedInputMap = JsonUtil.toJson(assemblyInputUrls);

        this.applyUpdatesToUpdateStage(patch, assebmlyStage, serializedInputMap, serializedParams,
                PIPELINE_STAGE_STATUS_QUEUING, assebmlyStage.getVersion());

        plan.updateStageCommands
                .add(new UpdateStageCommand(patch, assebmlyStage.getStageId(), assebmlyStage.getVersion() - 1));
        plan.runStages.add(assebmlyStage);
        return plan;
    }

    private OrchestratePlan planForMapping(BioPipelineStage mappingStage, List<BioPipelineStage> allStages)
            throws JsonMappingException, JsonProcessingException {
        OrchestratePlan plan = new OrchestratePlan();
        BioPipelineStage patch = new BioPipelineStage();
        BioPipelineStage qcStage = allStages.stream().filter(s->s.getStageType() == PIPELINE_STAGE_QC).findAny().orElse(null);

        MappingParameters mappingParameters = JsonUtil.toObject(mappingStage.getParameters(), MappingParameters.class);
        MappingInputUrls mappingInputUrls = new MappingInputUrls();



        QcResult qcResult = JsonUtil.toObject(qcStage.getOutputUrl(), QcResult.class);

        mappingInputUrls.setR1Url(qcResult.getCleanedR1());
        mappingInputUrls.setR2Url(qcResult.getCleanedR2());


        QcParameters qcParameters = JsonUtil.toObject(qcStage.getParameters(), QcParameters.class);
        mappingParameters.setRefSeqConfig(qcParameters.getRefSeqConfig());
        mappingParameters.setReadMeta(qcParameters.getSequenceMeta());

        this.applyUpdatesToUpdateStage(patch, mappingStage, JsonUtil.toJson(mappingInputUrls),
                JsonUtil.toJson(mappingParameters), PIPELINE_STAGE_STATUS_QUEUING,
                mappingStage.getVersion());

        plan.updateStageCommands
                .add(new UpdateStageCommand(patch, mappingStage.getStageId(), mappingStage.getVersion() - 1));
        plan.runStages.add(mappingStage);
        return plan;

    }

    private OrchestratePlan planForVarientCall(BioPipelineStage varientCallStage,
            List<BioPipelineStage> upstreamStages) throws JsonMappingException, JsonProcessingException {

        OrchestratePlan plan = new OrchestratePlan();
        BioPipelineStage patch = new BioPipelineStage();

        BioPipelineStage mappingStage = upstreamStages.stream().filter(s -> s.getStageType() == PIPELINE_STAGE_MAPPING)
                .findFirst().orElse(null);

        MappingParameters mappingParameters = JsonUtil.toObject(mappingStage.getParameters(), MappingParameters.class);
        VarientCallParameters varientCallParameters = JsonUtil.toObject(varientCallStage.getParameters(),
                VarientCallParameters.class);

        varientCallParameters.setRefSeqConfig(mappingParameters.getRefSeqConfig());

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
            throws JsonMappingException, JsonProcessingException {
        // the final one

        OrchestratePlan plan = new OrchestratePlan();
        BioPipelineStage patch = new BioPipelineStage();

        BioPipelineStage varientStage = upstreamStages.stream()
                .filter(s -> s.getStageType() == PIPELINE_STAGE_VARIANT_CALL).findFirst().orElse(null);

        VarientCallParameters varientCallParameters = JsonUtil.toObject(varientStage.getParameters(),
                VarientCallParameters.class);
        VarientCallStageResult varientCallStageResult = JsonUtil.toObject(varientStage.getOutputUrl(),
                VarientCallStageResult.class);

        ConsensusStageInputUrls consensusStageInputUrls = new ConsensusStageInputUrls();
        consensusStageInputUrls.setVcfGz(varientCallStageResult.getVcfGzUrl());
        consensusStageInputUrls.setVcfTbi(varientCallStageResult.getVcfTbiUrl());

        ConsensusStageParameters consensusStageParameters = JsonUtil.toObject(consensusStage.getParameters(),
                ConsensusStageParameters.class);
        consensusStageParameters.setRefSeqConfig(varientCallParameters.getRefSeqConfig());

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

    private OrchestratePlan planForReadLengDetect(BioPipelineStage readLengthDetectStage) {
        return null;
    }

    private OrchestratePlan planForTaxonomy(List<BioPipelineStage> upstreamStages, BioPipelineStage taxStage)
            throws JsonMappingException, JsonProcessingException {

        OrchestratePlan plan = new OrchestratePlan();
        BioPipelineStage patch = new BioPipelineStage();

        BioPipelineStage qc = upstreamStages.stream().filter(s -> s.getStageType() == PIPELINE_STAGE_QC).findFirst()
                .orElse(null);
        QcResult qcResult = JsonUtil.toObject(qc.getOutputUrl(), QcResult.class);

        TaxonomyStageInputUrls taxonomyStageInputUrls = new TaxonomyStageInputUrls();
        taxonomyStageInputUrls.setR1(qcResult.getCleanedR1());
        taxonomyStageInputUrls.setR2(qcResult.getCleanedR2());

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
        BioPipelineStage taxonomyStage = upstreamStages.stream()
                .filter(s -> s.getStageType() == PIPELINE_STAGE_TAXONOMY).findFirst().orElse(null);

        TaxonomyResult taxonomyResult = JsonUtil.toObject(taxonomyStage.getOutputInline(), TaxonomyResult.class);

        TaxonomyContext taxonomyContext = TaxonomyContext.of(taxonomyResult);
        AssemblyResult assemblyResult = JsonUtil.toObject(assembly.getOutputUrl(), AssemblyResult.class);

        MLSTStageInputUrls mlstStageInputUrls = new MLSTStageInputUrls(assemblyResult.getContigsUrl());
        BaseStageParams params = JsonUtil.toObject(taxonomyStage.getParameters(), BaseStageParams.class);
        params.setTaxonomyContext(taxonomyContext);

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

    private OrchestratePlan planDownstreamTaxonomy(List<BioPipelineStage> stages, BioPipelineStage taxonomyStage)
            throws JsonMappingException, JsonProcessingException, MissingUpstreamException {

        return planBacteriaPathogenAnalysis(stages);
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

        SeroTypeStageInputUrls seroTypeStageInputUrls = new SeroTypeStageInputUrls();

        int inputType = SeroTypingStageExectuor.inputType(taxonomyContext);

        if (inputType == SeroTypingStageExectuor.INPUT_TYPE_CONTIGS) {
            BioPipelineStage assembly = findStageFromStages(stages, PIPELINE_STAGE_ASSEMBLY);
            AssemblyResult assemblyResult = JsonUtil.toObject(assembly.getOutputUrl(), AssemblyResult.class);
            seroTypeStageInputUrls.setContigsUrl(assemblyResult.getContigsUrl());
        } else {
            BioPipelineStage qc = findStageFromStages(stages, PIPELINE_STAGE_QC);
            QcResult qcResult = JsonUtil.toObject(qc.getOutputUrl(), QcResult.class);

            seroTypeStageInputUrls.setR1Url(qcResult.getCleanedR1());
            seroTypeStageInputUrls
                    .setR2Url(StringUtils.isBlank(qcResult.getCleanedR2()) ? null : qcResult.getCleanedR2());
        }

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
        // TaxonomyResult taxonomyResult = JsonUtil.toObject(taxonomy.getOutputUrl(),
        // TaxonomyResult.class);

        AMRInputUrls amrInputUrls = new AMRInputUrls();
        amrInputUrls.setContigsUrl(assemblyResult.getContigsUrl());

        // TaxonomyContext taxonomyContext = TaxonomyContext.of(taxonomyResult);
        AMRParamters params = JsonUtil.toObject(amrStage.getParameters(), AMRParamters.class);
        // params.setTaxonomyContext(taxonomyContext);

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

        BioPipelineStage taxonomy = findStageFromStages(upstreamStages, PIPELINE_STAGE_TAXONOMY);
        BioPipelineStage assembly = findStageFromStages(upstreamStages, PIPELINE_STAGE_ASSEMBLY);

        AssemblyResult assemblyResult = JsonUtil.toObject(assembly.getOutputUrl(), AssemblyResult.class);
        TaxonomyResult taxonomyResult = JsonUtil.toObject(taxonomy.getOutputInline(), TaxonomyResult.class);

        TaxonomyContext taxonomyContext = TaxonomyContext.of(taxonomyResult);

        BioPipelineStage patch = new BioPipelineStage();

        VFParameters vfParameters = JsonUtil.toObject(vfStage.getParameters(), VFParameters.class);
        vfParameters.setTaxonomyContext(taxonomyContext);

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

    public OrchestratePlan makePlan(List<BioPipelineStage> stages, long runStageId)
            throws JsonMappingException, JsonProcessingException, MissingUpstreamException {

        BioPipelineStage runStage = null;
        for (BioPipelineStage stage : stages) {
            if (stage.getStageId() == runStageId) {
                runStage = stage;
                break;
            }
        }

        // start flag
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
        return makePlan(allStages, qcStage.getStageId());

    }

    public OrchestratePlan makeDownstreamPlan(BioPipelineStage currentStage, List<BioPipelineStage> allStages,
            int pipelineType)
            throws InvocationTargetException, IllegalAccessException, NoSuchMethodException, JsonProcessingException,
            MissingUpstreamException {

        if (currentStage.getStageType() == PIPELINE_STAGE_QC) {
            return planDownstreamQc(allStages, currentStage, pipelineType);
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
        }else if(currentStage.getStageType() == PIPELINE_STAGE_REFERENCE_COMPARISON){
            return this.makeDownstreamPlanReferenceComparison();
        }
        return null;
    }


    private OrchestratePlan makeDownstreamPlanReferenceComparison(){
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
