package com.xjtlu.bio.analysisPipeline;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.RefSeqConfig;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.entity.BioSample;
import com.xjtlu.bio.requestParameters.CreateSampleRequest.PipelineStageParameters;
import com.xjtlu.bio.service.PipelineService;

public class AnalysisPipelineStagesBuilder {

    private static ObjectMapper objectMapper = new ObjectMapper();

    private static Map<String, Object> substractStageParams(String stageName, Map<String, Object> pipelineStageParams) {
        Object obj = pipelineStageParams.get(stageName);
        if (obj == null) {
            return new HashMap<>();
        }
        return (Map) obj;
    }

    public static List<BioPipelineStage> buildBacteriaStages() {
        // todo
        return null;
    }

    private static BioPipelineStage buildReadLengthDetectStage(long pid, String readUrl, int stageIndex) {
        BioPipelineStage bioPipelineStage = new BioPipelineStage();
        bioPipelineStage.setInputUrl(readUrl);
        bioPipelineStage.setStatus(PipelineService.PIPELINE_STAGE_STATUS_PENDING);
        bioPipelineStage.setPipelineId(pid);
        bioPipelineStage.setStageName(PipelineService.PIPELINE_STAGE_READ_LENGTH_DETECT_NAME);
        bioPipelineStage.setStageIndex(stageIndex);
        bioPipelineStage.setStageType(PipelineService.PIPELINE_STAGE_READ_LENGTH_DETECT);
        return bioPipelineStage;
    }

    public static List<BioPipelineStage> buildVirusStages(long pid, int pipelineType, BioSample bioSample,
            PipelineStageParameters pipelineStageParams) throws JsonProcessingException {

        ArrayList<BioPipelineStage> stages = new ArrayList<>(16);
        String qcInputRead1 = bioSample.getRead1Url();
        String qcInputRead2 = bioSample.getRead2Url();

        Object refseqObj = pipelineStageParams==null?null:pipelineStageParams.getRefseq();
        long refseqId = -1;

        String longReadParamKey = PipelineService.PIPELINE_STAGE_PARAMETERS_LONG_READ_KEY;

        Map<String,Object> pipelineStagesConfig = pipelineStageParams==null?new HashMap<>():pipelineStageParams.getExtraParams();

        if (refseqObj != null) {
            refseqId = refseqObj instanceof Integer ? (Integer) refseqObj
                    : refseqObj instanceof Long ? (Long) refseqObj : -1;
        }

        
        RefSeqConfig refseqConfig = new RefSeqConfig();

        if (refseqId >= 0) {
            refseqConfig.setInnerRefSeq(true);
            refseqConfig.setRefseqId(refseqId);
        }else {
            refseqConfig.setInnerRefSeq(false);
            refseqConfig.setRefseqId(-1);
        }

        int index = 0;

        boolean isPair = bioSample.getIsPair();
        if (!isPair) {
            BioPipelineStage readLengthDetectStage = buildReadLengthDetectStage(pid, bioSample.getRead1Url(), index);
            stages.add(readLengthDetectStage);
            index++;
        }

        BioPipelineStage qc = new BioPipelineStage();

        Map<String, String> qcInputMap = new HashMap<>();
        Map<String,Object> qcParams = substractStageParams("qc", pipelineStagesConfig);
        
        qcParams.put(longReadParamKey, false);
        

        qcInputMap.put(PipelineService.PIPELINE_STAGE_QC_INPUT_R1, qcInputRead1);
        qcInputMap.put(PipelineService.PIPELINE_STAGE_QC_INPUT_R2, qcInputRead2);
        String qcInputMapStr = objectMapper.writeValueAsString(qcInputMap);
        qc.setStageIndex(index);
        qc.setStageName(PipelineService.PIPELINE_STAGE_NAME_QC);
        qc.setStageType(PipelineService.PIPELINE_STAGE_QC);
        qc.setStatus(PipelineService.PIPELINE_STAGE_STATUS_PENDING);
        qc.setPipelineId(pid);
        qc.setInputUrl(qcInputMapStr);
        qc.setParameters(objectMapper.writeValueAsString(qcParams));
        stages.add(qc);

        index++;

        if (refseqId == -1) {

            BioPipelineStage assembly = new BioPipelineStage();

            //TODO
            Map<String,Object> assemblyParams = substractStageParams("assembly", pipelineStagesConfig);
            
            assemblyParams.put(longReadParamKey, false);
            
            String assemlyParamsStr = objectMapper.writeValueAsString(assemblyParams);
            assembly.setStatus(PipelineService.PIPELINE_STAGE_STATUS_PENDING);
            assembly.setPipelineId(pid);
            assembly.setStageIndex(index);
            assembly.setParameters(assemlyParamsStr);
            assembly.setStageType(PipelineService.PIPELINE_STAGE_ASSEMBLY);
            assembly.setStageName(PipelineService.PIPELINE_STAGE_NAME_ASSEMBLY);
            stages.add(assembly);
            index++;
        }

        BioPipelineStage mappingStage = new BioPipelineStage();
        mappingStage.setStatus(PipelineService.PIPELINE_STAGE_STATUS_PENDING);
        mappingStage.setPipelineId(pid);
        mappingStage.setStageIndex(index);
        mappingStage.setStageType(PipelineService.PIPELINE_STAGE_MAPPING);
        mappingStage.setStageName(PipelineService.PIPELINE_STAGE_NAME_MAPPING);



        
        Map<String, Object> mappingParams = substractStageParams("mapping", pipelineStagesConfig);
        mappingParams.put(longReadParamKey, false);
        mappingParams.put(PipelineService.PIPELINE_STAGE_PARAMETER_REFSEQ_CONFIG, refseqConfig);
        mappingStage.setParameters(objectMapper.writeValueAsString(mappingParams));

        stages.add(mappingStage);
        index++;

        Map<String, Object> varientStageParams = substractStageParams("varient", pipelineStagesConfig);
        varientStageParams.put(PipelineService.PIPELINE_STAGE_PARAMETER_REFSEQ_CONFIG, refseqConfig);
        varientStageParams.put(longReadParamKey, false);
        BioPipelineStage varient = new BioPipelineStage();
        varient.setStageName(PipelineService.PIPELINE_STAGE_NAME_VARIANT);
        varient.setPipelineId(pid);
        varient.setStageIndex(index);

        varient.setParameters(objectMapper.writeValueAsString(varientStageParams));
        varient.setStageType(PipelineService.PIPELINE_STAGE_VARIANT_CALL);
        varient.setStatus(PipelineService.PIPELINE_STAGE_STATUS_PENDING);

        stages.add(varient);

        index++;

        BioPipelineStage consensus = new BioPipelineStage();
        Map<String, Object> consesusParams = substractStageParams("consensus", pipelineStagesConfig);
        consesusParams.put(PipelineService.PIPELINE_STAGE_PARAMETER_REFSEQ_CONFIG, refseqConfig);
        consesusParams.put(longReadParamKey, false);
        consensus.setStageName("生成一致性序列");
        consensus.setPipelineId(pid);
        consensus.setStageIndex(index);
        consensus.setParameters(objectMapper.writeValueAsString(consesusParams));
        consensus.setStageType(PipelineService.PIPELINE_STAGE_CONSENSUS);
        consensus.setStatus(PipelineService.PIPELINE_STAGE_STATUS_PENDING);
        stages.add(consensus);

        if (pipelineType != PipelineService.PIPELINE_VIRUS_COVID) {
            return stages;
        }

        Map<String, Object> snpParams = substractStageParams("snp", pipelineStagesConfig);
        snpParams.put(PipelineService.PIPELINE_STAGE_PARAMETER_REFSEQ_CONFIG, refseqConfig);
        snpParams.put(longReadParamKey, false);
        BioPipelineStage snp = new BioPipelineStage();
        snp.setStageName("SNP注释");
        snp.setPipelineId(pid);
        snp.setStageIndex(index);
        snp.setParameters(objectMapper.writeValueAsString(snp));
        snp.setStageType(PipelineService.PIPELINE_STAGE_SNP_CORE);
        snp.setStatus(PipelineService.PIPELINE_STAGE_STATUS_PENDING);
        stages.add(snp);

        index++;

        BioPipelineStage depthConverage = new BioPipelineStage();
        Map<String, Object> depthParams = substractStageParams("depth", pipelineStagesConfig);
        depthParams.put(PipelineService.PIPELINE_STAGE_PARAMETER_REFSEQ_CONFIG, refseqConfig);
        depthParams.put(longReadParamKey, false);
        depthConverage.setStageName("深度分布图");
        depthConverage.setPipelineId(pid);
        depthConverage.setStageIndex(index);
        depthConverage.setParameters(objectMapper.writeValueAsString(depthParams));
        depthConverage.setStageType(PipelineService.PIPELINE_STAGE_DEPTH_COVERAGE);
        depthConverage.setStatus(PipelineService.PIPELINE_STAGE_STATUS_PENDING);
        stages.add(snp);
        index++;


        return stages;
    }
}
