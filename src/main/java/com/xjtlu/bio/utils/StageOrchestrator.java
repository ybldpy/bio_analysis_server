package com.xjtlu.bio.utils;


import com.fasterxml.jackson.core.JsonProcessingException;
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

    private final ConnectionDetails connectionDetails;
    private final MappingStageExecutor mappingStageExecutor;

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


    }


    private OrchestratePlan planDownstreamAssembly(BioPipelineStage assembly, List<BioPipelineStage> followingStages) throws JsonProcessingException {


        BioPipelineStage mapping = followingStages.stream().filter(s->s.getStageType()==PipelineService.PIPELINE_STAGE_MAPPING).findAny().orElse(null);
        Map<String,String> assemblyOutputMap = JsonUtil.toMap(assembly.getOutputUrl(),String.class);


        if(mapping==null){
            return planBacteriaDownstreamAfterAssembly(assembly, followingStages);
        }




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


    public OrchestratePlan planFollowingMapping(BioPipelineStage mapping, List<BioPipelineStage> followingStages){




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
