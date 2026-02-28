package com.xjtlu.bio.analysisPipeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.AMRInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.AssemblyInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ConsensusStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.MLSTStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.MappingInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.SeroTypeStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.TaxonomyStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.VFStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.VarientCallInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.AMRParamters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ConsensusStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.MappingParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.RefSeqConfig;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.SeroTypingStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.TaxonomyContext;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.VFParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.VarientCallParameters;
import com.xjtlu.bio.analysisPipeline.stageResult.AssemblyResult;
import com.xjtlu.bio.analysisPipeline.stageResult.MappingResult;
import com.xjtlu.bio.analysisPipeline.stageResult.QcResult;
import com.xjtlu.bio.analysisPipeline.stageResult.TaxonomyResult;
import com.xjtlu.bio.analysisPipeline.stageResult.VarientCallStageResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.SeroTypingStageExectuor;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.command.UpdateStageCommand;
import com.xjtlu.bio.utils.JsonUtil;

import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.*;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.*;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.*;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.*;

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
        Map.entry(PIPELINE_STAGE_AMR, Set.of(PIPELINE_STAGE_TAXONOMY, PIPELINE_STAGE_ASSEMBLY)), 
        Map.entry(PIPELINE_STAGE_VIRULENCE, Set.of(PIPELINE_STAGE_TAXONOMY, PIPELINE_STAGE_ASSEMBLY)),
        Map.entry(PIPELINE_STAGE_SEROTYPE, Set.of(PIPELINE_STAGE_ASSEMBLY, PIPELINE_STAGE_TAXONOMY))
    );


    

    public StageOrchestrator() {

    }



    public static class MissingUpstreamException extends Exception{

        private String desc;

        public MissingUpstreamException(){
            this("Upstream stage not finished yet");
        }

        public MissingUpstreamException(String desc){
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

    private void applyUpdatesToUpdateStage(BioPipelineStage updateStage, BioPipelineStage stageCache, Map inputUrlMap,
            Map params, int status, int currentVersion) throws JsonProcessingException {

        String serializedInputMap = inputUrlMap == null ? null : JsonUtil.toJson(inputUrlMap);
        String serializedParams = params == null ? null : JsonUtil.toJson(params);

        this.applyUpdatesToUpdateStage(updateStage, stageCache, serializedInputMap, serializedParams, status,
                currentVersion);
    }

    private OrchestratePlan planDownstreamQc(List<BioPipelineStage> allStages, BioPipelineStage qcStage)
            throws JsonProcessingException, MissingUpstreamException {

        BioPipelineStage assembly = findStageFromStages(allStages, PIPELINE_STAGE_ASSEMBLY);
        BioPipelineStage taxonomy = findStageFromStages(allStages, PIPELINE_STAGE_TAXONOMY);
        BioPipelineStage mapping = findStageFromStages(allStages, PIPELINE_STAGE_MAPPING);

        if (assembly != null) {
            OrchestratePlan assemblyPlan = makePlan(allStages,assembly.getStageId());
            if(taxonomy == null){return assemblyPlan;}
            OrchestratePlan taxonomyPlan = makePlan(allStages, taxonomy.getStageId());
            OrchestratePlan plan = new OrchestratePlan();
            plan.updateStageCommands.addAll(assemblyPlan.updateStageCommands);
            plan.updateStageCommands.addAll(taxonomyPlan.updateStageCommands);
            plan.runStages.addAll(assemblyPlan.runStages);
            plan.runStages.addAll(taxonomyPlan.runStages);

            return plan;
        }else {
            return makePlan(allStages, mapping.getStageId());
        }
    }

    


    private OrchestratePlan planForPathogenGenomicCharacterization(List<BioPipelineStage> allStages) throws JsonMappingException, JsonProcessingException, MissingUpstreamException{
        BioPipelineStage amr = findStageFromStages(allStages, PIPELINE_STAGE_AMR);
        BioPipelineStage mlst = findStageFromStages(allStages, PIPELINE_STAGE_MLST);
        BioPipelineStage seroType = findStageFromStages(allStages, PIPELINE_STAGE_SEROTYPE);
        BioPipelineStage vf = findStageFromStages(allStages, PIPELINE_STAGE_VIRULENCE);

        OrchestratePlan plan = new OrchestratePlan();
        OrchestratePlan amrPlan = makePlan(allStages, amr.getStageId());
        OrchestratePlan mlstPlan = makePlan(allStages, mlst.getStageId());
        OrchestratePlan serotypePlan = makePlan(allStages, seroType.getStageId());
        OrchestratePlan vfPlan = makePlan(allStages, vf.getStageId());

        plan.updateStageCommands.addAll(amrPlan.updateStageCommands);
        plan.updateStageCommands.addAll(mlstPlan.updateStageCommands);
        plan.updateStageCommands.addAll(serotypePlan.updateStageCommands);
        plan.updateStageCommands.addAll(vfPlan.updateStageCommands);

        plan.runStages.addAll(amrPlan.runStages);
        plan.runStages.addAll(mlstPlan.runStages);
        plan.runStages.addAll(serotypePlan.runStages);
        plan.runStages.addAll(vfPlan.runStages);
        return plan;
    }

    


    private OrchestratePlan planDownstreamAssembly(List<BioPipelineStage> allStages,BioPipelineStage assembly)
            throws JsonProcessingException, MissingUpstreamException {


        BioPipelineStage mappingStage = findStageFromStages(allStages, PIPELINE_STAGE_MAPPING);
        BioPipelineStage taxonomyStage = findStageFromStages(allStages, PIPELINE_STAGE_TAXONOMY);


        if(mappingStage!=null){return makePlan(allStages, mappingStage.getStageId());}

        if(taxonomyStage.getStatus()!=PIPELINE_STAGE_STATUS_FINISHED){
            return new OrchestratePlan();
        }
        return planForPathogenGenomicCharacterization(allStages);
    }

    // 病毒才做mapping后续阶段
    // 这边先顺序跑
    public OrchestratePlan planDownstreamMapping(List<BioPipelineStage> allStages, BioPipelineStage mappingStage)
            throws JsonMappingException, JsonProcessingException, MissingUpstreamException {

       


        BioPipelineStage vcStage = findStageFromStages(
            allStages,
            PIPELINE_STAGE_CONSENSUS
        );

        return makePlan(allStages, vcStage.getStageId());
    }

    public OrchestratePlan planDownstreamVarientCall(List<BioPipelineStage> allStages,
            BioPipelineStage varientCallStage) throws JsonMappingException, JsonProcessingException, MissingUpstreamException {

        BioPipelineStage consensusStage = findStageFromStages(allStages, PIPELINE_STAGE_CONSENSUS);
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


    private void validateUpstreamStages(List<BioPipelineStage> allStages, long runStageId) throws MissingUpstreamException{

        BioPipelineStage runStage = allStages.stream().filter(s->s.getStageId() == runStageId).findFirst().orElse(null);

        Set<Integer> require = new HashSet<>(REQUIRES.get(runStage.getStageType()));


        allStages.forEach(s->{
            if(require.contains(s.getStageType()) && s.getStatus() == PIPELINE_STAGE_STATUS_FINISHED){
                require.remove(s.getStageType());
            }
        });

        if(!require.isEmpty()){
            throw new MissingUpstreamException();
        }

        if(runStage.getStageType()!=PIPELINE_STAGE_MAPPING){return;}

        BioPipelineStage assemblyStage = findStageFromStages(allStages, PIPELINE_STAGE_ASSEMBLY);
        
        if(assemblyStage!=null && assemblyStage.getStatus()!=PIPELINE_STAGE_STATUS_FINISHED){throw new MissingUpstreamException();}





        // if(runStage.getStageType() == PIPELINE_STAGE_READ_LENGTH_DETECT){return;}

        // BioPipelineStage readLengStage = findStageFromStages(allStages, PIPELINE_STAGE_READ_LENGTH_DETECT);
        
        // if(runStage.getStageType() == PIPELINE_STAGE_QC){
        //     if(readLengStage!=null && readLengStage.getStatus()==PIPELINE_STAGE_STATUS_FINISHED){
        //         throw new MissingUpstreamException();
        //     }
        //     return;
        // }
        
        
        // BioPipelineStage qcStage = findStageFromStages(allStages, PIPELINE_STAGE_QC);
        // if(runStage.getStageType() == PIPELINE_STAGE_ASSEMBLY){
        //     if(qcStage.getStatus()!=PIPELINE_STAGE_STATUS_FINISHED){
        //         throw new MissingUpstreamException();
        //     }
        //     return;
        // }

        // if(runStage.getStageType() == PIPELINE_STAGE_VARIANT_CALL){
        //     if(findStageFromStages(allStages, PIPELINE_STAGE_MAPPING).getStatus()!=PIPELINE_STAGE_STATUS_FINISHED){
        //         throw new MissingUpstreamException();
        //     }
        //     return;
        // }

        // List<Integer> stagesWithassemblyAsUpstream = List.of(PIPELINE_STAGE_MAPPING, PIPELINE_STAGE_TAXONOMY);
                
        // if(stagesWithassemblyAsUpstream.contains((int)runStage.getStageType())){
        //     if(findStageFromStages(allStages, PIPELINE_STAGE_ASSEMBLY).getStatus()!=PIPELINE_STAGE_STATUS_FINISHED){
        //         throw new MissingUpstreamException();
        //     }
        //     return;
        // }

        // List<Integer> stagesWithTaxonomyAsUpstream = List.of(PIPELINE_STAGE_MLST, PIPELINE_STAGE_AMR, PIPELINE_STAGE_SEROTYPE);

        // if(stagesWithTaxonomyAsUpstream.contains((int)runStage.getStageType())){
        //     if(findStageFromStages(allStages, PIPELINE_STAGE_TAXONOMY).getStatus()!=PIPELINE_STAGE_STATUS_FINISHED){
        //         throw new MissingUpstreamException();
        //     }
        //     return;
        // }

    }

    private OrchestratePlan planForAssembly(BioPipelineStage assebmlyStage, List<BioPipelineStage> upstreamStages)
            throws JsonMappingException, JsonProcessingException {

        OrchestratePlan plan = new OrchestratePlan();
        BioPipelineStage patch = new BioPipelineStage();
        String serializedParams = null;
        BioPipelineStage qcStage = upstreamStages.stream().filter(s -> s.getStageType() == PIPELINE_STAGE_QC)
                .findFirst().orElse(null);
        BioPipelineStage readDetectLengthStage = upstreamStages.stream()
                .filter(s -> s.getStageType() == PIPELINE_STAGE_READ_LENGTH_DETECT).findFirst().orElse(null);



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

    private OrchestratePlan planForMapping(BioPipelineStage mappingStage, List<BioPipelineStage> upstreamStages)
            throws JsonMappingException, JsonProcessingException {
        OrchestratePlan plan = new OrchestratePlan();
        BioPipelineStage patch = new BioPipelineStage();

        List<BioPipelineStage> qcAndAssemblyAndReadLenStages = upstreamStages.stream()
                .filter(s -> s.getStageType() == PIPELINE_STAGE_READ_LENGTH_DETECT
                        || s.getStageType() == PIPELINE_STAGE_ASSEMBLY || s.getStageType() == PIPELINE_STAGE_QC)
                .toList();
        BioPipelineStage qcStage = qcAndAssemblyAndReadLenStages.stream()
                .filter(s -> s.getStageType() == PIPELINE_STAGE_QC).findFirst().orElse(null);
        BioPipelineStage assemblyStage = qcAndAssemblyAndReadLenStages.stream()
                .filter(s -> s.getStageType() == PIPELINE_STAGE_ASSEMBLY).findFirst().orElse(null);
        BioPipelineStage readLenStage = qcAndAssemblyAndReadLenStages.stream()
                .filter(s -> s.getStageType() == PIPELINE_STAGE_READ_LENGTH_DETECT).findFirst().orElse(null);
        boolean isLongRead = this.getReadLenFromReadLenStage(readLenStage);

        Map<String, Object> paramsMap = new HashMap<>();
        Map<String, String> inputMap = new HashMap<>();

        MappingParameters mappingParameters = JsonUtil.toObject(mappingStage.getParameters(), MappingParameters.class);
        MappingInputUrls mappingInputUrls = new MappingInputUrls();
        if(assemblyStage!=null){
            AssemblyResult assemblyResult = JsonUtil.toObject(assemblyStage.getOutputUrl(), AssemblyResult.class);
            RefSeqConfig refSeqConfig = new RefSeqConfig(assemblyResult.getContigsUrl());
            mappingParameters.setRefSeqConfig(refSeqConfig);
        }

        QcResult qcResult = JsonUtil.toObject(qcStage.getOutputUrl(), QcResult.class);

        mappingInputUrls.setR1Url(qcResult.getCleanedR1());
        mappingInputUrls.setR2Url(qcResult.getCleanedR2());



        this.applyUpdatesToUpdateStage(patch, mappingStage, JsonUtil.toJson(mappingInputUrls), JsonUtil.toJson(mappingParameters), PIPELINE_STAGE_STATUS_QUEUING,
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

        BioPipelineStage mappingStage = upstreamStages.stream().filter(s->s.getStageType() == PIPELINE_STAGE_MAPPING).findFirst().orElse(null);

        MappingParameters mappingParameters = JsonUtil.toObject(mappingStage.getParameters(), MappingParameters.class);
        VarientCallParameters varientCallParameters = JsonUtil.toObject(varientCallStage.getParameters(), VarientCallParameters.class);

        varientCallParameters.setRefSeqConfig(mappingParameters.getRefSeqConfig());

        VarientCallInputUrls varientCallInputUrls = new VarientCallInputUrls();
        MappingResult mappingResult = JsonUtil.toObject(mappingStage.getOutputUrl(), MappingResult.class);

        varientCallInputUrls.setBamUrl(mappingResult.getBamUrl());
        varientCallInputUrls.setBamIndexUrl(mappingResult.getBamIndexUrl());
        
        this.applyUpdatesToUpdateStage(patch, varientCallStage, JsonUtil.toJson(varientCallInputUrls), JsonUtil.toJson(varientCallParameters), PIPELINE_STAGE_STATUS_QUEUING, varientCallStage.getVersion());

        plan.updateStageCommands.add(new UpdateStageCommand(patch,varientCallStage.getStageId(), varientCallStage.getVersion()-1));
        plan.runStages.add(varientCallStage);
        return plan;
        
    }

    private OrchestratePlan planForConsensus(BioPipelineStage consensusStage, List<BioPipelineStage> upstreamStages) throws JsonMappingException, JsonProcessingException {
        // the final one

        OrchestratePlan plan = new OrchestratePlan();
        BioPipelineStage patch = new BioPipelineStage();

        BioPipelineStage varientStage = upstreamStages.stream().filter(s->s.getStageType() == PIPELINE_STAGE_VARIANT_CALL).findFirst().orElse(null);

        VarientCallParameters varientCallParameters = JsonUtil.toObject(varientStage.getParameters(), VarientCallParameters.class);
        VarientCallStageResult varientCallStageResult = JsonUtil.toObject(varientStage.getOutputUrl(), VarientCallStageResult.class);

        ConsensusStageInputUrls consensusStageInputUrls = new ConsensusStageInputUrls();
        consensusStageInputUrls.setVcfGz(varientCallStageResult.getVcfGzUrl());
        consensusStageInputUrls.setVcfTbi(varientCallStageResult.getVcfTbiUrl());

        ConsensusStageParameters consensusStageParameters = JsonUtil.toObject(consensusStage.getParameters(), ConsensusStageParameters.class);
        consensusStageParameters.setRefSeqConfig(varientCallParameters.getRefSeqConfig());

        this.applyUpdatesToUpdateStage(patch, consensusStage, JsonUtil.toJson(consensusStageInputUrls), JsonUtil.toJson(consensusStageParameters),PIPELINE_STAGE_STATUS_QUEUING, consensusStage.getVersion());

        plan.updateStageCommands.add(new UpdateStageCommand(patch, consensusStage.getStageId(), consensusStage.getVersion()-1));
        plan.runStages.add(consensusStage);
    
        return plan;


    }

    private OrchestratePlan planForQc(BioPipelineStage qcStage, List<BioPipelineStage> upstreamStages)
            throws JsonMappingException, JsonProcessingException {

        OrchestratePlan plan = new OrchestratePlan();
        BioPipelineStage patch = new BioPipelineStage();

        this.applyUpdatesToUpdateStage(patch, qcStage, (String) null, null, PIPELINE_STAGE_STATUS_QUEUING,
                qcStage.getVersion());
        plan.updateStageCommands.add(new UpdateStageCommand(patch, qcStage.getStageId(), qcStage.getVersion() - 1));
        plan.runStages.add(qcStage);
        return plan;
    }

    private OrchestratePlan planForReadLengDetect(BioPipelineStage readLengthDetectStage) {
        return null;
    }


    private List<BioPipelineStage> findUpstreamStages(List<BioPipelineStage> stages, BioPipelineStage stage){
        
        int stageType = stage.getStageType();
        switch (stageType) {
            case PIPELINE_STAGE_READ_LENGTH_DETECT:
                return Collections.emptyList();
            case PIPELINE_STAGE_QC:
                return stages.stream().filter(s->s.getStageType() == PIPELINE_STAGE_READ_LENGTH_DETECT).toList();
            case PIPELINE_STAGE_ASSEMBLY:
                return stages.stream().filter(s->s.getStageType() == PIPELINE_STAGE_QC).toList();
            case PIPELINE_STAGE_MAPPING:
                return stages.stream().filter(s->s.getStageType() == PIPELINE_STAGE_ASSEMBLY || s.getStageType() == PIPELINE_STAGE_QC).toList();
            case PIPELINE_STAGE_VARIANT_CALL:
                return stages.stream().filter(s->s.getStageType() == PIPELINE_STAGE_MAPPING || s.getStageType() == PIPELINE_STAGE_ASSEMBLY).toList();
            case PIPELINE_STAGE_CONSENSUS:
                return stages.stream().filter(s->s.getStageType() == PIPELINE_STAGE_VARIANT_CALL || s.getStageType() == PIPELINE_STAGE_ASSEMBLY).toList();
            case PIPELINE_STAGE_TAXONOMY:
                return stages.stream().filter(s->s.getStageType() == PIPELINE_STAGE_QC).toList();
            case PIPELINE_STAGE_AMR:
                return stages.stream().filter(s->s.getStageType() == PIPELINE_STAGE_TAXONOMY || s.getStageType() == PIPELINE_STAGE_ASSEMBLY).toList();
            case PIPELINE_STAGE_MLST:
                return stages.stream().filter(s->s.getStageType() == PIPELINE_STAGE_TAXONOMY || s.getStageType() == PIPELINE_STAGE_ASSEMBLY).toList();
            case PIPELINE_STAGE_SEROTYPE:
                return stages.stream().filter(s->s.getStageType() == PIPELINE_STAGE_TAXONOMY || s.getStageType() == PIPELINE_STAGE_ASSEMBLY || s.getStageType() == PIPELINE_STAGE_QC).toList();
            default:
                break;
        }

        return null;
    }


    private OrchestratePlan planForTaxonomy(List<BioPipelineStage> upstreamStages, BioPipelineStage taxStage) throws JsonMappingException, JsonProcessingException{


        OrchestratePlan plan = new OrchestratePlan();
        BioPipelineStage patch = new BioPipelineStage();

        BioPipelineStage qc = upstreamStages.stream().filter(s->s.getStageType() == PIPELINE_STAGE_QC).findFirst().orElse(null);
        QcResult qcResult = JsonUtil.toObject(qc.getOutputUrl(), QcResult.class);

        TaxonomyStageInputUrls taxonomyStageInputUrls = new TaxonomyStageInputUrls();
        taxonomyStageInputUrls.setR1(qcResult.getCleanedR1());
        taxonomyStageInputUrls.setR2(qcResult.getCleanedR2());

        this.applyUpdatesToUpdateStage(patch, taxStage, JsonUtil.toJson(taxonomyStageInputUrls), null, PIPELINE_STAGE_STATUS_QUEUING, taxStage.getVersion());
        plan.runStages.add(taxStage);
        plan.updateStageCommands.add(new UpdateStageCommand(patch, taxStage.getStageId(), taxStage.getVersion()-1));
        return plan;
    }


    private OrchestratePlan planForMLST(List<BioPipelineStage> upstreamStages, BioPipelineStage mlstStage) throws JsonMappingException, JsonProcessingException{
        OrchestratePlan plan = new OrchestratePlan();

        BioPipelineStage assembly = upstreamStages.stream().filter(s->s.getStageType() == PIPELINE_STAGE_ASSEMBLY).findFirst().orElse(null);
        BioPipelineStage taxonomyStage = upstreamStages.stream().filter(s->s.getStageType() == PIPELINE_STAGE_TAXONOMY).findFirst().orElse(null);

        TaxonomyResult taxonomyResult = JsonUtil.toObject(taxonomyStage.getOutputUrl(), TaxonomyResult.class);

        TaxonomyContext taxonomyContext = TaxonomyContext.of(taxonomyResult);
        AssemblyResult assemblyResult = JsonUtil.toObject(assembly.getOutputUrl(), AssemblyResult.class);

        
        MLSTStageInputUrls mlstStageInputUrls = new MLSTStageInputUrls(assemblyResult.getContigsUrl());
        BaseStageParams params = JsonUtil.toObject(taxonomyStage.getParameters(), BaseStageParams.class);
        params.setTaxonomyContext(taxonomyContext);

        String serializedInput = JsonUtil.toJson(mlstStageInputUrls);
        String serializedParams = JsonUtil.toJson(params);

        BioPipelineStage patch = new BioPipelineStage();
        this.applyUpdatesToUpdateStage(patch, mlstStage, serializedInput, serializedParams, PIPELINE_STAGE_STATUS_QUEUING, mlstStage.getVersion());
        plan.runStages.add(mlstStage);
        plan.updateStageCommands.add(new UpdateStageCommand(patch, taxonomyStage.getStageId(), taxonomyStage.getVersion()-1));
        return plan;
    }


    private static BioPipelineStage findStageFromStages(List<BioPipelineStage> stages, int stageType){
        return stages.stream().filter(s->s.getStageType() == stageType).findFirst().orElse(null);
    }

    private OrchestratePlan planDownstreamTaxonomy(List<BioPipelineStage> stages, BioPipelineStage taxonomyStage) throws JsonMappingException, JsonProcessingException, MissingUpstreamException{

        BioPipelineStage amr = findStageFromStages(stages, PIPELINE_STAGE_AMR);
        BioPipelineStage virusFactor = findStageFromStages(stages, PIPELINE_STAGE_VIRULENCE);
        BioPipelineStage MLST = findStageFromStages(stages, PIPELINE_STAGE_MLST);
        

        OrchestratePlan plan = new OrchestratePlan();

        OrchestratePlan amrPlan = this.makePlan(stages, amr.getStageId());
        OrchestratePlan virusFactorPlan = this.makePlan(stages, virusFactor.getStageId());
        OrchestratePlan MLSTPlan = this.makePlan(stages, MLST.getStageId());

        plan.runStages.addAll(amrPlan.getRunStages());
        plan.runStages.addAll(virusFactorPlan.getRunStages());
        plan.runStages.addAll(MLSTPlan.getRunStages());

        plan.updateStageCommands.addAll(amrPlan.getUpdateStageCommands());
        plan.updateStageCommands.addAll(virusFactorPlan.getUpdateStageCommands());
        plan.updateStageCommands.addAll(MLSTPlan.getUpdateStageCommands());
        return plan;
    }

    private OrchestratePlan planForSeroType(List<BioPipelineStage> stages, BioPipelineStage seroTypeStage) throws JsonMappingException, JsonProcessingException{

        BioPipelineStage taxonomy = findStageFromStages(stages, PIPELINE_STAGE_TAXONOMY);
        TaxonomyResult taxonomyResult = JsonUtil.toObject(taxonomy.getOutputUrl(), TaxonomyResult.class);

        TaxonomyContext taxonomyContext = TaxonomyContext.of(taxonomyResult);
        boolean canDoSeroType = SeroTypingStageExectuor.canDoSeroType(taxonomyContext);

        BioPipelineStage patch = new BioPipelineStage();
        if(!canDoSeroType){
            patch.setStatus(PIPELINE_STAGE_STATUS_NOT_APPLICABLE);
            OrchestratePlan plan = new OrchestratePlan();
            plan.updateStageCommands.add(new UpdateStageCommand(patch, seroTypeStage.getStageId(), seroTypeStage.getVersion()));
            return plan;
        }

        SeroTypeStageInputUrls seroTypeStageInputUrls = new SeroTypeStageInputUrls();

        int inputType = SeroTypingStageExectuor.inputType(taxonomyContext);

        if(inputType == SeroTypingStageExectuor.INPUT_TYPE_CONTIGS){
            BioPipelineStage assembly = findStageFromStages(stages, PIPELINE_STAGE_ASSEMBLY);
            AssemblyResult assemblyResult = JsonUtil.toObject(assembly.getOutputUrl(), AssemblyResult.class);
            seroTypeStageInputUrls.setContigsUrl(assemblyResult.getContigsUrl());
        }else {
            BioPipelineStage qc = findStageFromStages(stages, PIPELINE_STAGE_QC);
            QcResult qcResult = JsonUtil.toObject(qc.getOutputUrl(), QcResult.class);

            seroTypeStageInputUrls.setR1Url(qcResult.getCleanedR1());
            seroTypeStageInputUrls.setR2Url(StringUtils.isBlank(qcResult.getCleanedR2())?null:qcResult.getCleanedR2());
        }

        String serializedInput = JsonUtil.toJson(seroTypeStageInputUrls);
        
        SeroTypingStageParameters seroTypingStageParameters = JsonUtil.toObject(seroTypeStage.getParameters(), SeroTypingStageParameters.class);
        seroTypingStageParameters.setTaxonomyContext(taxonomyContext);


        this.applyUpdatesToUpdateStage(patch, seroTypeStage,serializedInput, JsonUtil.toJson(seroTypingStageParameters), PIPELINE_STAGE_STATUS_QUEUING, seroTypeStage.getVersion());

        OrchestratePlan plan = new OrchestratePlan();
        plan.runStages.add(seroTypeStage);
        plan.updateStageCommands.add(new UpdateStageCommand(patch, seroTypeStage.getStageId(), seroTypeStage.getVersion()-1));
        return plan;
    }


    

    private OrchestratePlan planForAMR(List<BioPipelineStage> upstreamStages, BioPipelineStage amrStage) throws JsonMappingException, JsonProcessingException{

        BioPipelineStage assembly = findStageFromStages(upstreamStages, PIPELINE_STAGE_ASSEMBLY);
        BioPipelineStage taxonomy = findStageFromStages(upstreamStages, PIPELINE_STAGE_TAXONOMY);

        AssemblyResult assemblyResult = JsonUtil.toObject(assembly.getOutputUrl(), AssemblyResult.class);
        TaxonomyResult taxonomyResult = JsonUtil.toObject(taxonomy.getOutputUrl(), TaxonomyResult.class);

        AMRInputUrls amrInputUrls = new AMRInputUrls();
        amrInputUrls.setContigsUrl(assemblyResult.getContigsUrl());

        TaxonomyContext taxonomyContext = TaxonomyContext.of(taxonomyResult);
        AMRParamters params = JsonUtil.toObject(amrStage.getParameters(), AMRParamters.class);
        params.setTaxonomyContext(taxonomyContext);

        String serializedInput = JsonUtil.toJson(amrInputUrls);
        String serializedParams = JsonUtil.toJson(params);

        BioPipelineStage patch = new BioPipelineStage();
        this.applyUpdatesToUpdateStage(patch, amrStage, serializedInput, serializedParams, PIPELINE_STAGE_STATUS_QUEUING, amrStage.getVersion());

        OrchestratePlan plan = new OrchestratePlan();
        plan.runStages.add(amrStage);
        plan.updateStageCommands.add(new UpdateStageCommand(patch, amrStage.getStageId(), amrStage.getVersion()-1));

        return plan;

    }


    private OrchestratePlan planForVirulenFactorStage(List<BioPipelineStage> upstreamStages, BioPipelineStage vfStage) throws JsonMappingException, JsonProcessingException{

        BioPipelineStage taxonomy = findStageFromStages(upstreamStages, PIPELINE_STAGE_TAXONOMY);
        BioPipelineStage assembly = findStageFromStages(upstreamStages, PIPELINE_STAGE_ASSEMBLY);

        AssemblyResult assemblyResult = JsonUtil.toObject(assembly.getOutputUrl(), AssemblyResult.class);
        TaxonomyResult taxonomyResult = JsonUtil.toObject(taxonomy.getOutputUrl(), TaxonomyResult.class);

        TaxonomyContext taxonomyContext = TaxonomyContext.of(taxonomyResult);

        BioPipelineStage patch = new BioPipelineStage();

        VFParameters vfParameters = JsonUtil.toObject(vfStage.getParameters(), VFParameters.class);
        vfParameters.setTaxonomyContext(taxonomyContext);

        VFStageInputUrls vfStageInputUrls = new VFStageInputUrls(assemblyResult.getContigsUrl());

        String serializedInput = JsonUtil.toJson(vfStageInputUrls);
        String serializedParams = JsonUtil.toJson(vfParameters);

        this.applyUpdatesToUpdateStage(patch, vfStage, serializedInput, serializedParams, PIPELINE_STAGE_STATUS_QUEUING, vfStage.getVersion());
        
        OrchestratePlan plan = new OrchestratePlan();
        plan.runStages.add(vfStage);
        plan.updateStageCommands.add(new UpdateStageCommand(patch, vfStage.getStageId(),vfStage.getVersion()-1));

        return plan;

    }


    private OrchestratePlan makePlanDownstreamTaxonomy(List<BioPipelineStage> allStages, BioPipelineStage taxonomyStage) throws JsonMappingException, JsonProcessingException, MissingUpstreamException{
        BioPipelineStage assemblyStage = findStageFromStages(allStages, PIPELINE_STAGE_ASSEMBLY);

        if(assemblyStage.getStatus()!=PIPELINE_STAGE_STATUS_FINISHED){
            return new OrchestratePlan();
        }

        return planForPathogenGenomicCharacterization(allStages);

    }


    private OrchestratePlan noDownstreamPlan(){
        return new OrchestratePlan(true);
    }

    private OrchestratePlan makePlanDownstreamAMR(List<BioPipelineStage> stages, BioPipelineStage stage){
        return noDownstreamPlan();
    }

    private OrchestratePlan makePlanDownstreamMLST(List<BioPipelineStage> stages, BioPipelineStage stage){
        return noDownstreamPlan();
    }

    private OrchestratePlan makePlanDownstreamVisurFactor(List<BioPipelineStage> stages, BioPipelineStage stage){
        return noDownstreamPlan();
    }
    private OrchestratePlan makePlanDownstreamSerotype(){
        return noDownstreamPlan();
    }

    public OrchestratePlan makePlan(List<BioPipelineStage> stages, long runStageId)
            throws JsonMappingException, JsonProcessingException, MissingUpstreamException {



        this.validateUpstreamStages(stages, runStageId);
        // prerequisize: cannot be null
        BioPipelineStage startStage = stages.stream().filter(s -> s.getStageId() == runStageId).findFirst()
                .orElse(null);
        List<BioPipelineStage> upstreamStages = findUpstreamStages(stages, startStage);

        if (startStage.getStageType() == PIPELINE_STAGE_ASSEMBLY) {
            return this.planForAssembly(startStage, upstreamStages);
        } else if (startStage.getStageType() == PIPELINE_STAGE_MAPPING) {
            return this.planForMapping(startStage, upstreamStages);
        } else if (startStage.getStageType() == PIPELINE_STAGE_VARIANT_CALL) {
            return this.planForVarientCall(startStage, upstreamStages);
        } else if (startStage.getStageType() == PIPELINE_STAGE_CONSENSUS) {
            return this.planForConsensus(startStage, upstreamStages);
        } else if (startStage.getStageType() == PIPELINE_STAGE_QC) {
            return this.planForQc(startStage, upstreamStages);
        } else if (startStage.getStageType() == PIPELINE_STAGE_READ_LENGTH_DETECT) {
            return this.planForReadLengDetect(startStage);
        } else if(startStage.getStageType() == PIPELINE_STAGE_TAXONOMY){
            return this.planForTaxonomy(upstreamStages, startStage);
        }else if (startStage.getStageType() == PIPELINE_STAGE_MLST) {
            return this.planForMLST(upstreamStages, startStage);
        }else if(startStage.getStageType() == PIPELINE_STAGE_AMR){
            return this.planForAMR(upstreamStages, startStage);
        }else if(startStage.getStageType() == PIPELINE_STAGE_SEROTYPE){
            return this.planForSeroType(upstreamStages, startStage);
        }
        return null;

    }

    public OrchestratePlan makeDownstreamPlan(BioPipelineStage currentStage, List<BioPipelineStage> allStages)
            throws InvocationTargetException, IllegalAccessException, NoSuchMethodException, JsonProcessingException, MissingUpstreamException {


        if (currentStage.getStageType() == PIPELINE_STAGE_QC) {
            return planDownstreamQc(allStages, currentStage);
        } else if (currentStage.getStageType() == PIPELINE_STAGE_ASSEMBLY) {
            return planDownstreamAssembly(allStages, currentStage);
        } else if (currentStage.getStageType() == PIPELINE_STAGE_MAPPING) {
            return planDownstreamMapping(allStages, currentStage);
        } else if(currentStage.getStageType() == PIPELINE_STAGE_VARIANT_CALL){
            return planDownstreamVarientCall(allStages, currentStage);
        }else if(currentStage.getStageType() == PIPELINE_STAGE_TAXONOMY){
            return planDownstreamTaxonomy(allStages, currentStage);
        }else if (currentStage.getStageType() == PIPELINE_STAGE_AMR) {
            return this.makePlanDownstreamAMR(allStages, currentStage);
        }else if (currentStage.getStageType() == PIPELINE_STAGE_MLST) {
            return this.makePlanDownstreamMLST(allStages, currentStage);
        }else if (currentStage.getStageType() == PIPELINE_STAGE_VIRULENCE) {
            return this.makePlanDownstreamVisurFactor(allStages, currentStage);
        }
        return null;

    }

    public OrchestratePlan makeDownstreamPlan(long finishedStageId, List<BioPipelineStage> allStages) throws JsonProcessingException, InvocationTargetException, IllegalAccessException, NoSuchMethodException, MissingUpstreamException{

        BioPipelineStage finishedStage = allStages.stream().filter(s->s.getStageId() == finishedStageId).findFirst().orElse(null);
        return makeDownstreamPlan(finishedStage, allStages);

    }

}
