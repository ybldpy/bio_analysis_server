package com.xjtlu.bio.analysisPipeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.AMRInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.MLSTStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.VFStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.AMRParamters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.RefSeqConfig;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.TaxonomyContext;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.VFParameters;
import com.xjtlu.bio.analysisPipeline.stageResult.AssemblyResult;
import com.xjtlu.bio.analysisPipeline.stageResult.TaxonomyResult;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.service.command.UpdateStageCommand;
import com.xjtlu.bio.utils.JsonUtil;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_AMR;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_ASSEMBLY;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_ASSEMBLY_INPUT_R1;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_ASSEMBLY_INPUT_R2;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_ASSEMBLY_OUTPUT_CONTIGS_KEY;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_CONSENSUS;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_CONSENSUS_INPUT_VCFGZ;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_CONSENSUS_INPUT_VCFGZ_TBI;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_MAPPING;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_MAPPING_INPUT_R1;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_MAPPING_INPUT_R2;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_MAPPING_OUTPUT_BAM_INDEX_KEY;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_MAPPING_OUTPUT_BAM_KEY;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_MLST;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_PARAMETERS_LONG_READ_KEY;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_PARAMETER_REFSEQ_CONFIG;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_QC;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_QC_OUTPUT_R1;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_QC_OUTPUT_R2;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_READ_LENGTH_DETECT;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_READ_LENGTH_DETECT_NAME;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_SEROTYPE;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_STATUS_FINISHED;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_STATUS_QUEUING;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_TAXONOMY;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_TAXONOMY_INPUT;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_VARIANT_CALL;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_VARIENT_CALL_INPUT_BAM_INDEX_KEY;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_VARIENT_CALL_INPUT_BAM_KEY;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_VARIENT_OUTPUT_VCF_GZ;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_VARIENT_OUTPUT_VCF_TBI;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_VIRULENCE;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

@Component
public class StageOrchestrator {

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

    private static void copy(BioPipelineStage src, BioPipelineStage target)
            throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        PropertyUtils.copyProperties(target, src);
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

    private OrchestratePlan planFollowingQc(List<BioPipelineStage> allStages, BioPipelineStage qcStage)
            throws JsonProcessingException, MissingUpstreamException {



        BioPipelineStage assembly = findStageFromStages(allStages, PIPELINE_STAGE_ASSEMBLY);
        if (assembly != null) {
            return makePlan(allStages, assembly.getStageId());
        }else {
            BioPipelineStage mapping = findStageFromStages(allStages, PIPELINE_STAGE_MAPPING);
            return makePlan(allStages, mapping.getStageId());
        }
        // OrchestratePlan plan = new OrchestratePlan();

        // int index = qc.getStageIndex();
        // List<BioPipelineStage> assemblyAndMappingStage = followingStage.stream()
        //         .filter((s) -> s.getStageType() == PipelineService.PIPELINE_STAGE_ASSEMBLY
        //                 || s.getStageType() == PipelineService.PIPELINE_STAGE_MAPPING)
        //         .toList();

        // // only one stage

        // BioPipelineStage assembly = assemblyAndMappingStage.get(0);
        // BioPipelineStage mapping = assemblyAndMappingStage.get(assemblyAndMappingStage.size() - 1);

        // if (assembly == mapping) {
        //     assembly = null;
        // }

        // Map<String, String> qcOutputMap = JsonUtil.toMap(qc.getOutputUrl(), String.class);

        // List<UpdateStageCommand> updateStages = plan.getUpdateStageCommands();
        // List<BioPipelineStage> nextRunStages = plan.getRunStages();

        // BioPipelineStage nextRunStage = null;
        // BioPipelineStage updateStage = new BioPipelineStage();
        // HashMap<String, String> nextInputMap = new HashMap<>();
        // if (assembly != null) {
        //     nextInputMap.put(PipelineService.PIPELINE_STAGE_ASSEMBLY_INPUT_R1,
        //             qcOutputMap.get(PipelineService.PIPELINE_STAGE_QC_OUTPUT_R1));
        //     nextInputMap.put(PipelineService.PIPELINE_STAGE_ASSEMBLY_INPUT_R2,
        //             qcOutputMap.get(PipelineService.PIPELINE_STAGE_QC_OUTPUT_R2));
        //     String serializedInput = JsonUtil.toJson(nextInputMap);
        //     nextRunStage = assembly;
        //     this.applyUpdatesToUpdateStage(updateStage, nextRunStage, serializedInput, null,
        //             PipelineService.PIPELINE_STAGE_STATUS_QUEUING, nextRunStage.getVersion());
        //     nextInputMap.clear();

        //     UpdateStageCommand assemblyUpdateCommand = new UpdateStageCommand(updateStage, nextRunStage.getStageId(),
        //             nextRunStage.getVersion());
        //     updateStages.add(assemblyUpdateCommand);
        //     nextRunStages.add(assembly);
        // }

        // updateStage = new BioPipelineStage();
        // nextInputMap.put(PipelineService.PIPELINE_STAGE_MAPPING_INPUT_R1,
        //         qcOutputMap.get(PipelineService.PIPELINE_STAGE_QC_OUTPUT_R1));
        // nextInputMap.put(PipelineService.PIPELINE_STAGE_MAPPING_INPUT_R2,
        //         qcOutputMap.get(PipelineService.PIPELINE_STAGE_QC_OUTPUT_R2));
        // String serializedInput = JsonUtil.toJson(nextInputMap);

        // boolean isMappingNextRunStage = nextRunStages.isEmpty();
        // this.applyUpdatesToUpdateStage(updateStage, isMappingNextRunStage ? mapping : null, serializedInput, null,
        //         isMappingNextRunStage ? PipelineService.PIPELINE_STAGE_STATUS_QUEUING : -1, mapping.getVersion());

        // if (isMappingNextRunStage) {
        //     nextRunStages.add(mapping);
        // }

        // updateStages.add(new UpdateStageCommand(updateStage, mapping.getStageId(), mapping.getVersion()));

        // return plan;
    }

    private OrchestratePlan planBacteriaDownstreamAssembly(BioPipelineStage assembly,
            List<BioPipelineStage> followingStages) throws JsonProcessingException {
        OrchestratePlan plan = new OrchestratePlan();
        Map<String, String> outputMap = JsonUtil.toMap(assembly.getOutputUrl(), String.class);
        String contigs = outputMap.get(PipelineService.PIPELINE_STAGE_ASSEMBLY_OUTPUT_CONTIGS_KEY);
        int nextRunIndex = assembly.getStageIndex() + 1;
        for (BioPipelineStage followingStage : followingStages) {
            // 这边先顺序跑
            BioPipelineStage updateStage = new BioPipelineStage();
            Map<String, String> inputUrlMap = new HashMap<>();
            switch (followingStage.getStageType()) {
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
            applyUpdatesToUpdateStage(updateStage, isNextRunStage ? followingStage : null, serializedInputMap, null,
                    isNextRunStage ? PipelineService.PIPELINE_STAGE_STATUS_QUEUING : -1, followingStage.getVersion());
            plan.getUpdateStageCommands()
                    .add(new UpdateStageCommand(updateStage, followingStage.getStageId(), followingStage.getVersion()));
            if (isNextRunStage) {
                plan.getRunStages().add(followingStage);
            }
        }

        return plan;
    }

    private OrchestratePlan planDownstreamAssembly(List<BioPipelineStage> allStages,BioPipelineStage assembly)
            throws JsonProcessingException, MissingUpstreamException {


        BioPipelineStage mappingStage = findStageFromStages(allStages, PIPELINE_STAGE_MAPPING);
        BioPipelineStage taxonomyStage = findStageFromStages(allStages, PIPELINE_STAGE_TAXONOMY);

        //if no mapping, then we will follow bacteria path.
        long runId = mappingStage == null?taxonomyStage.getStageId():mappingStage.getStageId();
        return makePlan(allStages, runId);

        // BioPipelineStage mapping = followingStages.stream()
        //         .filter(s -> s.getStageType() == PipelineService.PIPELINE_STAGE_MAPPING).findAny().orElse(null);
        // Map<String, String> assemblyOutputMap = JsonUtil.toMap(assembly.getOutputUrl(), String.class);

        // if (mapping == null) {
        //     return planBacteriaDownstreamAssembly(assembly, followingStages);
        // }

        // OrchestratePlan plan = new OrchestratePlan();

        // RefSeqConfig refSeqConfig = new RefSeqConfig();
        // refSeqConfig
        //         .setRefseqObjectName(assemblyOutputMap.get(PipelineService.PIPELINE_STAGE_ASSEMBLY_OUTPUT_CONTIGS_KEY));
        // refSeqConfig.setInnerRefSeq(false);
        // refSeqConfig.setRefseqId(-1);

        // for (BioPipelineStage followingStage : followingStages) {

        //     BioPipelineStage updateStage = new BioPipelineStage();
        //     Map<String, Object> params = JsonUtil.toMap(followingStage.getParameters());
        //     params.put(PipelineService.PIPELINE_STAGE_PARAMETER_REFSEQ_CONFIG, refSeqConfig);
        //     String serializedParams = JsonUtil.toJson(params);
        //     boolean isMapping = followingStage.getStageType() == PipelineService.PIPELINE_STAGE_MAPPING;
        //     this.applyUpdatesToUpdateStage(updateStage, isMapping ? followingStage : null, null, serializedParams,
        //             isMapping ? PipelineService.PIPELINE_STAGE_STATUS_QUEUING : -1, followingStage.getVersion());
        //     if (isMapping) {
        //         plan.getRunStages().add(followingStage);
        //     }

        //     plan.getUpdateStageCommands()
        //             .add(new UpdateStageCommand(updateStage, followingStage.getStageId(), followingStage.getVersion()));
        // }
        // return plan;
    }

    // 病毒才做mapping后续阶段
    // 这边先顺序跑
    public OrchestratePlan planDownstreamMapping(List<BioPipelineStage> allStages, BioPipelineStage mappingStage)
            throws JsonMappingException, JsonProcessingException, MissingUpstreamException {

        // OrchestratePlan plan = new OrchestratePlan();
        // Map<String, String> outputMap = JsonUtil.toMap(mapping.getOutputUrl(), String.class);

        // String bamUrl = outputMap.get(PipelineService.PIPELINE_STAGE_MAPPING_OUTPUT_BAM_KEY);
        // String bamIndexUrl = outputMap.get(PipelineService.PIPELINE_STAGE_MAPPING_OUTPUT_BAM_INDEX_KEY);

        // BioPipelineStage varientStage = followingStages.stream()
        //         .filter((s) -> s.getStageType() == PipelineService.PIPELINE_STAGE_VARIANT_CALL).findAny().orElse(null);
        // Map<String, String> inputMap = new HashMap<>();
        // inputMap.put(PipelineService.PIPELINE_STAGE_VARIENT_CALL_INPUT_BAM_KEY, bamUrl);
        // inputMap.put(PipelineService.PIPELINE_STAGE_MAPPING_OUTPUT_BAM_INDEX_KEY, bamIndexUrl);

        // BioPipelineStage updateVarientStage = new BioPipelineStage();
        // String serializedInputMap = JsonUtil.toJson(inputMap);
        // this.applyUpdatesToUpdateStage(updateVarientStage, varientStage, serializedInputMap, null,
        //         PipelineService.PIPELINE_STAGE_STATUS_QUEUING, varientStage.getVersion());
        // plan.getUpdateStageCommands()
        //         .add(new UpdateStageCommand(updateVarientStage, varientStage.getStageId(), varientStage.getVersion()));
        // plan.runStages.add(varientStage);
        // return plan;


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


    private void validateUpstreamStage(List<BioPipelineStage> allStages, long runStageId) throws MissingUpstreamException{

        BioPipelineStage runStage = allStages.stream().filter(s->s.getStageId() == runStageId).findFirst().orElse(null);

        if(runStage.getStageType() == PIPELINE_STAGE_READ_LENGTH_DETECT){return;}

        BioPipelineStage readLengStage = findStageFromStages(allStages, PIPELINE_STAGE_READ_LENGTH_DETECT);
        
        if(runStage.getStageType() == PIPELINE_STAGE_QC){
            if(readLengStage!=null && readLengStage.getStatus()==PIPELINE_STAGE_STATUS_FINISHED){
                throw new MissingUpstreamException();
            }
            return;
        }
        
        
        BioPipelineStage qcStage = findStageFromStages(allStages, PIPELINE_STAGE_QC);
        if(runStage.getStageType() == PIPELINE_STAGE_ASSEMBLY){
            if(qcStage.getStatus()!=PIPELINE_STAGE_STATUS_FINISHED){
                throw new MissingUpstreamException();
            }
            return;
        }

        if(runStage.getStageType() == PIPELINE_STAGE_VARIANT_CALL){
            if(findStageFromStages(allStages, PIPELINE_STAGE_MAPPING).getStatus()!=PIPELINE_STAGE_STATUS_FINISHED){
                throw new MissingUpstreamException();
            }
            return;
        }




        List<Integer> stagesWithassemblyAsUpstream = List.of(PIPELINE_STAGE_MAPPING, PIPELINE_STAGE_TAXONOMY);
        
        
        if(stagesWithassemblyAsUpstream.contains((int)runStage.getStageType())){
            if(findStageFromStages(allStages, PIPELINE_STAGE_ASSEMBLY).getStatus()!=PIPELINE_STAGE_STATUS_FINISHED){
                throw new MissingUpstreamException();
            }
            return;
        }

        List<Integer> stagesWithTaxonomyAsUpstream = List.of(PIPELINE_STAGE_MLST, PIPELINE_STAGE_AMR, PIPELINE_STAGE_SEROTYPE);

        if(stagesWithTaxonomyAsUpstream.contains((int)runStage.getStageType())){
            if(findStageFromStages(allStages, PIPELINE_STAGE_TAXONOMY).getStatus()!=PIPELINE_STAGE_STATUS_FINISHED){
                throw new MissingUpstreamException();
            }
            return;
        }

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

        Map<String, String> qcOutputMap = JsonUtil.toMap(qcStage.getOutputUrl(), String.class);
        Map<String, Object> params = JsonUtil.toMap(assebmlyStage.getParameters());
        Map<String, String> inputMap = new HashMap<>();
        inputMap.put(PIPELINE_STAGE_ASSEMBLY_INPUT_R1, qcOutputMap.get(PIPELINE_STAGE_QC_OUTPUT_R1));
        inputMap.put(PIPELINE_STAGE_ASSEMBLY_INPUT_R2, qcOutputMap.get(PIPELINE_STAGE_QC_OUTPUT_R2));
        params.put(PIPELINE_STAGE_PARAMETERS_LONG_READ_KEY, getReadLenFromReadLenStage(readDetectLengthStage));

        serializedParams = JsonUtil.toJson(params);
        String serializedInputMap = JsonUtil.toJson(inputMap);

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

        Map<String, String> qcOutputMap = JsonUtil.toMap(qcStage.getOutputUrl(), String.class);

        paramsMap.put(PIPELINE_STAGE_READ_LENGTH_DETECT_NAME, isLongRead);
        if (assemblyStage != null) {
            Map<String, String> assemblyOutputMap = JsonUtil.toMap(assemblyStage.getOutputUrl(), String.class);
            Map<String, Object> params = JsonUtil.toMap(mappingStage.getParameters());
            paramsMap.put(PIPELINE_STAGE_PARAMETER_REFSEQ_CONFIG,
                    new RefSeqConfig(assemblyOutputMap.get(PIPELINE_STAGE_ASSEMBLY_OUTPUT_CONTIGS_KEY)));
        }

        inputMap.put(PIPELINE_STAGE_MAPPING_INPUT_R1, qcOutputMap.get(PIPELINE_STAGE_QC_OUTPUT_R1));
        inputMap.put(PIPELINE_STAGE_MAPPING_INPUT_R2, qcOutputMap.get(PIPELINE_STAGE_QC_OUTPUT_R2));

        this.applyUpdatesToUpdateStage(patch, mappingStage, inputMap, paramsMap, PIPELINE_STAGE_STATUS_QUEUING,
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
        Map<String,String> inputMap = new HashMap<>();
        Map<String,Object> paramsMap = new HashMap<>();


        Map<String,String> mappingOutputMap = JsonUtil.toMap(mappingStage.getOutputUrl(),String.class);
        Map<String,Object> params = JsonUtil.toMap(mappingStage.getParameters());

        paramsMap.put(PIPELINE_STAGE_PARAMETER_REFSEQ_CONFIG, params.get(PIPELINE_STAGE_PARAMETER_REFSEQ_CONFIG));

        inputMap.put(PIPELINE_STAGE_VARIENT_CALL_INPUT_BAM_KEY, mappingOutputMap.get(PIPELINE_STAGE_MAPPING_OUTPUT_BAM_KEY));
        inputMap.put(PIPELINE_STAGE_VARIENT_CALL_INPUT_BAM_INDEX_KEY, mappingOutputMap.get(PIPELINE_STAGE_MAPPING_OUTPUT_BAM_INDEX_KEY));
        
        this.applyUpdatesToUpdateStage(patch, varientCallStage, inputMap, paramsMap, PIPELINE_STAGE_STATUS_QUEUING, varientCallStage.getVersion());

        plan.updateStageCommands.add(new UpdateStageCommand(patch,varientCallStage.getStageId(), varientCallStage.getVersion()-1));
        plan.runStages.add(varientCallStage);
        return plan;
        
    }

    private OrchestratePlan planForConsensus(BioPipelineStage consensusStage, List<BioPipelineStage> upstreamStages) throws JsonMappingException, JsonProcessingException {
        // the final one

        OrchestratePlan plan = new OrchestratePlan();
        BioPipelineStage patch = new BioPipelineStage();

        BioPipelineStage varientStage = upstreamStages.stream().filter(s->s.getStageType() == PIPELINE_STAGE_VARIANT_CALL).findFirst().orElse(null);

        Map<String,String> varientOutputMap = JsonUtil.toMap(varientStage.getOutputUrl(), String.class);
        Map<String,Object> varientParams = JsonUtil.toMap(varientStage.getParameters());
        
        
        Map<String,String> inputMap = new HashMap<>();
        Map<String,Object> paramsMap = new HashMap<>();

        paramsMap.put(PIPELINE_STAGE_PARAMETER_REFSEQ_CONFIG, varientParams.get(PIPELINE_STAGE_PARAMETER_REFSEQ_CONFIG));
        
        inputMap.put(PIPELINE_STAGE_CONSENSUS_INPUT_VCFGZ, varientOutputMap.get(PIPELINE_STAGE_VARIENT_OUTPUT_VCF_GZ));
        inputMap.put(PIPELINE_STAGE_CONSENSUS_INPUT_VCFGZ_TBI, varientOutputMap.get(PIPELINE_STAGE_VARIENT_OUTPUT_VCF_TBI));


        this.applyUpdatesToUpdateStage(patch, consensusStage, inputMap, paramsMap, PIPELINE_STAGE_STATUS_QUEUING, consensusStage.getVersion());

        plan.updateStageCommands.add(new UpdateStageCommand(patch, consensusStage.getStageId(), consensusStage.getVersion()-1));
        plan.runStages.add(consensusStage);
    
        return plan;


    }

    private OrchestratePlan planForQc(BioPipelineStage qcStage, List<BioPipelineStage> upstreamStages)
            throws JsonMappingException, JsonProcessingException {

        OrchestratePlan plan = new OrchestratePlan();
        BioPipelineStage patch = new BioPipelineStage();
        String serializedParams = null;
        if (!upstreamStages.isEmpty()) {
            BioPipelineStage readDetectLengthStage = upstreamStages.get(0);
            Map<String, Object> params = JsonUtil.toMap(qcStage.getParameters());
            String isLongRead = readDetectLengthStage.getOutputUrl();
            if (!StringUtils.isBlank(isLongRead)) {
                boolean longRead = Boolean.parseBoolean(isLongRead);
                params.put(PIPELINE_STAGE_PARAMETERS_LONG_READ_KEY, longRead);
                serializedParams = JsonUtil.toJson(params);
            }
        }

        this.applyUpdatesToUpdateStage(patch, qcStage, null, serializedParams, PIPELINE_STAGE_STATUS_QUEUING,
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
                return stages.stream().filter(s->s.getStageType() == PIPELINE_STAGE_ASSEMBLY).toList();
            case PIPELINE_STAGE_AMR:
                return stages.stream().filter(s->s.getStageType() == PIPELINE_STAGE_TAXONOMY || s.getStageType() == PIPELINE_STAGE_ASSEMBLY).toList();
            case PIPELINE_STAGE_MLST:
                return stages.stream().filter(s->s.getStageType() == PIPELINE_STAGE_TAXONOMY || s.getStageType() == PIPELINE_STAGE_ASSEMBLY).toList();
            case PIPELINE_STAGE_SEROTYPE:
                return stages.stream().filter(s->s.getStageType() == PIPELINE_STAGE_TAXONOMY || s.getStageType() == PIPELINE_STAGE_ASSEMBLY).toList();
            default:
                break;
        }

        return null;
    }


    private OrchestratePlan planForTaxonomy(List<BioPipelineStage> upstreamStages, BioPipelineStage taxStage) throws JsonMappingException, JsonProcessingException{


        OrchestratePlan plan = new OrchestratePlan();
        BioPipelineStage patch = new BioPipelineStage();


        BioPipelineStage assembly = upstreamStages.stream().filter(s->s.getStageType() == PIPELINE_STAGE_ASSEMBLY).findFirst().orElse(null);

        Map<String,String> assemblyOutputMap = JsonUtil.toMap(assembly.getOutputUrl(), String.class);
        String contigPath = assemblyOutputMap.get(PIPELINE_STAGE_ASSEMBLY_OUTPUT_CONTIGS_KEY);

        Map<String,String> inputMap = new HashMap<>();
        inputMap.put(PIPELINE_STAGE_TAXONOMY_INPUT, contigPath);

        this.applyUpdatesToUpdateStage(patch, taxStage, inputMap, null, PIPELINE_STAGE_STATUS_QUEUING, taxStage.getVersion());
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


    private OrchestratePlan makePlanDownstreamTaxonomy(List<BioPipelineStage> allStages, BioPipelineStage taxonomyStage) throws JsonMappingException, JsonProcessingException{
        BioPipelineStage amrStage = findStageFromStages(allStages, PIPELINE_STAGE_AMR);
        BioPipelineStage mlstStage = findStageFromStages(allStages, PIPELINE_STAGE_MLST);
        BioPipelineStage virulenceFactorStage = findStageFromStages(allStages, PIPELINE_STAGE_VIRULENCE);
        

        List<BioPipelineStage> upstreamStages = findUpstreamStages(allStages, amrStage);
        OrchestratePlan amrPlan = planForAMR(upstreamStages, amrStage);
        OrchestratePlan mlstPlan = planForMLST(upstreamStages, mlstStage);
        OrchestratePlan virulenceFactorStagePlan = planForVirulenFactorStage(upstreamStages, virulenceFactorStage);
        
        OrchestratePlan plan = new OrchestratePlan();
        plan.runStages.addAll(amrPlan.runStages);
        plan.updateStageCommands.addAll(amrPlan.updateStageCommands);

        plan.runStages.addAll(mlstPlan.runStages);
        plan.updateStageCommands.addAll(mlstPlan.updateStageCommands);

        plan.runStages.addAll(virulenceFactorStagePlan.runStages);
        plan.updateStageCommands.addAll(virulenceFactorStagePlan.updateStageCommands);

        return plan;

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

    



    




    public OrchestratePlan makePlan(List<BioPipelineStage> stages, long runStageId)
            throws JsonMappingException, JsonProcessingException, MissingUpstreamException {



        this.validateUpstreamStage(stages, runStageId);
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
        }
        return null;

    }

    public OrchestratePlan makeDownstreamPlan(BioPipelineStage currentStage, List<BioPipelineStage> allStages)
            throws InvocationTargetException, IllegalAccessException, NoSuchMethodException, JsonProcessingException, MissingUpstreamException {


        if (currentStage.getStageType() == PipelineService.PIPELINE_STAGE_QC) {
            return planFollowingQc(allStages, currentStage);
        } else if (currentStage.getStageType() == PipelineService.PIPELINE_STAGE_ASSEMBLY) {
            return planDownstreamAssembly(allStages, currentStage);
        } else if (currentStage.getStageType() == PipelineService.PIPELINE_STAGE_MAPPING) {
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
