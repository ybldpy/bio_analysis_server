package com.xjtlu.bio.utils;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.taskrunner.MappingStageExecutor;
import com.xjtlu.bio.taskrunner.parameters.RefSeqConfig;
import jakarta.annotation.Resource;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.logging.log4j.util.PropertiesUtil;
import org.springframework.beans.BeanUtils;
import org.springframework.boot.autoconfigure.service.connection.ConnectionDetails;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.lang.reflect.InvocationTargetException;
import java.util.*;


@Component
public class StageOrchestrator {






    public StageOrchestrator(ConnectionDetails connectionDetails, MappingStageExecutor mappingStageExecutor) {
        this.connectionDetails = connectionDetails;
        this.mappingStageExecutor = mappingStageExecutor;
    }

    public static class OrchestratePlan{
        private final List<BioPipelineStage> updateStages;
        private final List<Long> updateStageIds;
        public final List<BioPipelineStage> getRunStages() {
            return runStages;
        }

        public List<Long> getUpdateStageIds() {
            return updateStageIds;
        }

        public OrchestratePlan() {
            this.updateStages = new ArrayList<>();
            this.updateStageIds = new ArrayList<>();
            this.runStages = new ArrayList<>();
        }


        public List<BioPipelineStage> getUpdateStages() {
            return updateStages;
        }
        private List<BioPipelineStage> runStages;
    }

    private static void copy(BioPipelineStage src, BioPipelineStage target) throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {

        PropertyUtils.copyProperties(target, src);

    }

    private void applyUpdatesToUpdateStage(BioPipelineStage updateStage, BioPipelineStage stageInCache, String inputUrl, String params, int status, int currentVersion){
        boolean setCache = stageInCache!=null;

        if(inputUrl!=null){
            updateStage.setInputUrl(inputUrl);
            if(setCache) stageInCache.setInputUrl(inputUrl);
        }
        if(params!=null){
            updateStage.setParameters(params);
            if(setCache) stageInCache.setParameters(params);
        }
        if(status>=0){
            updateStage.setStatus(status);
            if(setCache) stageInCache.setStatus(status);
        }

        updateStage.setVersion(currentVersion+1);
        if(setCache) stageInCache.setVersion(currentVersion+1);

    }


    private OrchestratePlan planFollowingQc(BioPipelineStage qc, List<BioPipelineStage> followingStage) throws JsonProcessingException {



        OrchestratePlan plan = new OrchestratePlan();

        int index = qc.getStageIndex();
        List<BioPipelineStage> assemblyAndMappingStage = followingStage.stream().filter((s)->s.getStageType() == PipelineService.PIPELINE_STAGE_ASSEMBLY || s.getStageType() == PipelineService.PIPELINE_STAGE_MAPPING).toList();

        BioPipelineStage assembly = assemblyAndMappingStage.getFirst();
        BioPipelineStage mapping = assemblyAndMappingStage.getLast();

        if(assembly == mapping){
            assembly = null;
        }

        Map<String,String> qcOutputMap = JsonUtil.toMap(qc.getOutputUrl(), String.class);

        List<BioPipelineStage> updateStages = plan.getUpdateStages();
        List<Long> updateStagesIds = plan.getUpdateStageIds();
        List<BioPipelineStage> nextRunStages = plan.getRunStages();


        BioPipelineStage nextRunStage = null;
        BioPipelineStage updateStage = new BioPipelineStage();
        HashMap<String, String> nextInputMap = new HashMap<>();
        if(assembly!=null){
            nextInputMap.put(PipelineService.PIPELINE_STAGE_ASSEMBLY_INPUT_R1, qcOutputMap.get(PipelineService.PIPELINE_STAGE_QC_OUTPUT_R1));
            nextInputMap.put(PipelineService.PIPELINE_STAGE_ASSEMBLY_INPUT_R2, qcOutputMap.get(PipelineService.PIPELINE_STAGE_QC_OUTPUT_R2));
            String serializedInput = JsonUtil.toJson(nextInputMap);
            nextRunStage = assembly;
            this.applyUpdatesToUpdateStage(updateStage, nextRunStage, serializedInput, null, PipelineService.PIPELINE_STAGE_STATUS_QUEUING,nextRunStage.getVersion());
            nextInputMap.clear();
            updateStages.add(updateStage);
            nextRunStages.add(assembly);
            updateStagesIds.add(assembly.getStageId());
        }



        updateStage = new BioPipelineStage();
        nextInputMap.put(PipelineService.PIPELINE_STAGE_MAPPING_INPUT_R1, qcOutputMap.get(PipelineService.PIPELINE_STAGE_QC_OUTPUT_R1));
        nextInputMap.put(PipelineService.PIPELINE_STAGE_MAPPING_INPUT_R2, qcOutputMap.get(PipelineService.PIPELINE_STAGE_QC_OUTPUT_R2));
        String serializedInput = JsonUtil.toJson(nextInputMap);

        boolean isMappingNextRunStage = nextRunStages.isEmpty();
        this.applyUpdatesToUpdateStage(updateStage, isMappingNextRunStage?mapping:null, serializedInput, null, isMappingNextRunStage?PipelineService.PIPELINE_STAGE_STATUS_QUEUING:-1,mapping.getVersion());

        if(isMappingNextRunStage){
            nextRunStages.add(mapping);
        }
        updateStages.add(updateStage);
        updateStagesIds.add(mapping.getStageId());


        return plan;
    }


    private OrchestratePlan planBacteriaDownstreamAfterAssembly(BioPipelineStage assembly, List<BioPipelineStage> followingStages) throws JsonProcessingException {
        OrchestratePlan plan = new OrchestratePlan();
        Map<String,String> outputMap = JsonUtil.toMap(assembly.getOutputUrl(),String.class);
        String contigs = outputMap.get(PipelineService.PIPELINE_STAGE_ASSEMBLY_OUTPUT_CONTIGS_KEY);
        int nextRunIndex = assembly.getStageIndex()+1;
        for(BioPipelineStage followingStage:followingStages){
            //这边先顺序跑
            BioPipelineStage updateStage = new BioPipelineStage();
            Map<String,String> inputUrlMap = new HashMap<>();
            switch (followingStage.getStageType()){
                case PipelineService.PIPELINE_STAGE_MLST -> {
                    inputUrlMap.put(PipelineService.PIPELINE_STAGE_MLST_INPUT, contigs);
                    break;
                }
                case PipelineService.PIPELINE_STAGE_AMR -> {
                    inputUrlMap.put(PipelineService.PIPELINE_STAGE_AMR_INPUT_SAMPLE, contigs);
                    break;
                }
                case PipelineService.PIPELINE_STAGE_VIRULENCE -> {
                    inputUrlMap.put(PipelineService.PIPELINE_STAGE_VIRULENCE_FACTOR_INPUT, contigs);
                    break;
                }
                case PipelineService.PIPELINE_STAGE_SEROTYPE -> {
                    inputUrlMap.put(PipelineService.PIPELINE_STAGE_SEROTYPING_INPUT, contigs);
                    break;
                }
            }

            String serializedInputMap = JsonUtil.toJson(inputUrlMap);
            boolean isNextRunStage = followingStage.getStageIndex() == nextRunIndex;
            applyUpdatesToUpdateStage(updateStage,  isNextRunStage?followingStage: null,serializedInputMap, null, isNextRunStage? PipelineService.PIPELINE_STAGE_STATUS_QUEUING:-1, followingStage.getVersion());
            plan.getUpdateStages().add(updateStage);
            plan.getUpdateStageIds().add(followingStage.getStageId());
            if(isNextRunStage){
                plan.getRunStages().add(followingStage);
            }
        }

        return plan;
    }


    private OrchestratePlan planDownstreamAssembly(BioPipelineStage assembly, List<BioPipelineStage> followingStages) throws JsonProcessingException {


        BioPipelineStage mapping = followingStages.stream().filter(s->s.getStageType()==PipelineService.PIPELINE_STAGE_MAPPING).findAny().orElse(null);
        Map<String,String> assemblyOutputMap = JsonUtil.toMap(assembly.getOutputUrl(),String.class);


        if(mapping==null){
            return planBacteriaDownstreamAfterAssembly(assembly, followingStages);
        }

        OrchestratePlan plan = new OrchestratePlan();

        RefSeqConfig refSeqConfig = new RefSeqConfig();
        refSeqConfig.setRefseqObjectName(assemblyOutputMap.get(PipelineService.PIPELINE_STAGE_ASSEMBLY_OUTPUT_CONTIGS_KEY));
        refSeqConfig.setInnerRefSeq(false);
        refSeqConfig.setRefseqId(-1);

        for(BioPipelineStage followingStage: followingStages){

            BioPipelineStage updateStage = new BioPipelineStage();
            Map<String,Object> params = JsonUtil.toMap(followingStage.getParameters());
            params.put(PipelineService.PIPELINE_STAGE_PARAMETER_REFSEQ_CONFIG, refSeqConfig);
            String serializedParams = JsonUtil.toJson(params);
            boolean isMapping = followingStage.getStageType() == PipelineService.PIPELINE_STAGE_MAPPING;
            this.applyUpdatesToUpdateStage(updateStage, isMapping?followingStage:null, null, serializedParams, isMapping?PipelineService.PIPELINE_STAGE_STATUS_QUEUING:-1, followingStage.getVersion());
            if(isMapping){
                plan.getRunStages().add(followingStage);
            }
            plan.getUpdateStages().add(updateStage);
            plan.getUpdateStageIds().add(followingStage.getStageId());
        }
        return plan;
    }


    //病毒才做mapping后续阶段
    //这边先顺序跑
    public OrchestratePlan planDownstreamMapping(BioPipelineStage mapping, List<BioPipelineStage> followingStages) throws JsonMappingException, JsonProcessingException{

        OrchestratePlan plan = new OrchestratePlan();
        Map<String,String> outputMap = JsonUtil.toMap(mapping.getOutputUrl(),String.class);

        String bamUrl = outputMap.get(PipelineService.PIPELINE_STAGE_MAPPING_OUTPUT_BAM_KEY);
        String bamIndexUrl = outputMap.get(PipelineService.PIPELINE_STAGE_MAPPING_OUTPUT_BAM_INDEX_KEY);
        
        BioPipelineStage varientStage = followingStages.stream().filter((s)->s.getStageType() == PipelineService.PIPELINE_STAGE_VARIANT_CALL).findAny().orElse(null);
        Map<String,String> inputMap = new HashMap<>();
        inputMap.put(PipelineService.PIPELINE_STAGE_VARIENT_CALL_INPUT_BAM_KEY, bamUrl);
        inputMap.put(PipelineService.PIPELINE_STAGE_MAPPING_OUTPUT_BAM_INDEX_KEY, bamIndexUrl);

        BioPipelineStage updateVarientStage = new BioPipelineStage();
        String serializedInputMap = JsonUtil.toJson(inputMap);
        this.applyUpdatesToUpdateStage(updateVarientStage, varientStage, serializedInputMap, null, PipelineService.PIPELINE_STAGE_STATUS_QUEUING, varientStage.getVersion());
        
        plan.updateStageIds.add(varientStage.getStageId());
        plan.updateStages.add(updateVarientStage);
        plan.runStages.add(varientStage);
        return plan;
    }

    public OrchestratePlan planDownstreamVarientCall(BioPipelineStage varientCallStage, List<BioPipelineStage> followingStages) throws JsonMappingException, JsonProcessingException{

        OrchestratePlan plan = new OrchestratePlan();

        BioPipelineStage consensusStage = followingStages.stream().filter((s)->s.getStageType() == PipelineService.PIPELINE_STAGE_CONSENSUS).findAny().orElse(null);

        Map<String,String> outputMap = JsonUtil.toMap(varientCallStage.getInputUrl(),String.class);
        String vcfGz = outputMap.get(PipelineService.PIPELINE_STAGE_VARIENT_OUTPUT_VCF_GZ);
        String vcfTbi = outputMap.get(PipelineService.PIPELINE_STAGE_VARIENT_OUTPUT_VCF_TBI);

        Map<String,String> inputMap = new HashMap<>();
        inputMap.put(PipelineService.PIPELINE_STAGE_CONSENSUS_INPUT_VCFGZ, vcfGz);
        inputMap.put(PipelineService.PIPELINE_STAGE_CONSENSUS_INPUT_VCFGZ_TBI, vcfTbi);

        String serializedInputMap = JsonUtil.toJson(inputMap);
        
        BioPipelineStage updateConsensusStage = new BioPipelineStage();
        this.applyUpdatesToUpdateStage(updateConsensusStage, consensusStage, serializedInputMap, null, PipelineService.PIPELINE_STAGE_STATUS_QUEUING, consensusStage.getVersion());


        plan.runStages.add(consensusStage);
        plan.updateStageIds.add(consensusStage.getStageId());
        plan.updateStages.add(updateConsensusStage);
        return plan;

    }





    public OrchestratePlan makePlan(BioPipelineStage currentStage, List<BioPipelineStage> followingStages) throws InvocationTargetException, IllegalAccessException, NoSuchMethodException, JsonProcessingException {
        ArrayList<BioPipelineStage> copiesFollowingStage = new ArrayList<>(followingStages.size());
        for(BioPipelineStage originStage: followingStages){
            BioPipelineStage stageCopy = new BioPipelineStage();
            copy(originStage, stageCopy);
            copiesFollowingStage.add(stageCopy);
        }
        if(currentStage.getStageType() == PipelineService.PIPELINE_STAGE_QC){
            return planFollowingQc(currentStage, copiesFollowingStage);
        }else if(currentStage.getStageType() == PipelineService.PIPELINE_STAGE_ASSEMBLY){

        }

    }



}
