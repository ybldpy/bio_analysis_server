package com.xjtlu.bio.utils;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.xjtlu.bio.service.PipelineService;

import org.apache.commons.lang3.StringUtils;
import org.jspecify.annotations.NonNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.*;
import com.xjtlu.bio.entity.BioPipelineStage;

@Component
public class BioStageUtil {


    @Value("${analysis-pipeline.stage.baseWorkDir}")
    private String stageWorkDirPath;
    @Value("${analysis-pipeline.stage.baseInputDir}")
    private String stageInputDirPath;


    private static ObjectMapper objectMapper = new JsonMapper();
    public Path stageExecutorWorkDir(BioPipelineStage bioPipelineStage){
        return Paths.get(stageWorkDirPath, String.valueOf(bioPipelineStage.getStageId()));
    }

    public Path stageExecutorInputDir(BioPipelineStage bioPipelineStage){
        return Paths.get(stageInputDirPath, String.valueOf(bioPipelineStage.getStageId()));
    }

    public String createStoreObjectName(BioPipelineStage pipelineStage, String name){
        return String.format(
            "stageOutput/%d/%s",
            pipelineStage.getStageId(),
            name
        );
    }

    public QCStageOutput qcStageOutput(Path dir, boolean hasR2){

        return new QCStageOutput(
            dir.resolve(QCStageOutput.R1).toString(),
            hasR2?dir.resolve(QCStageOutput.R2).toString():null,
            dir.resolve(QCStageOutput.JSON).toString(),
            dir.resolve(QCStageOutput.HTML).toString()
        );
    }

    public AssemblyStageOutput assemblyOutput(BioPipelineStage bioPipelineStage, Path dir){
        Path config = dir.resolve(AssemblyStageOutput.CONTIG);
        Path scaffold = dir.resolve(AssemblyStageOutput.SCAFFOLD);
        return new AssemblyStageOutput(config.toString(), scaffold.toString());
    }

    public MappingStageOutput mappingOutput(BioPipelineStage bioPipelineStage, Path dir){
        Path bamPath = dir.resolve(MappingStageOutput.BAM);
        Path bamIndex = dir.resolve(MappingStageOutput.BAM_INDEX);
        return new MappingStageOutput(bamPath.toString(), bamIndex.toString());
    }

    public VariantStageOutput varientOutput(BioPipelineStage bioPipelineStage, @NonNull Path dir){
        Path vcfGz = dir.resolve(VariantStageOutput.VCF_GZ);
        Path vcfTbi = dir.resolve(VariantStageOutput.VCF_TBI);
        return new VariantStageOutput(vcfGz.toString(), vcfTbi.toString());
    }

    public ConsensusStageOutput consensusOutput(BioPipelineStage bioPipelineStage, @NonNull Path dir){
        Path consesnusFa = dir.resolve(ConsensusStageOutput.CONSENSUS);
        return new ConsensusStageOutput(consesnusFa.toString());
    }




    private static Map<String,String> createInputMapForAssembly(BioPipelineStage curStage) throws JsonProcessingException {
        Map<String,String> outputMap = JsonUtil.toMap(curStage.getOutputUrl(), String.class);
        HashMap<String,String> inputMap = new HashMap<>();
        inputMap.put(PipelineService.PIPELINE_STAGE_ASSEMBLY_INPUT_R1, outputMap.get(PipelineService.PIPELINE_STAGE_QC_OUTPUT_R1));
        String outputR2 = outputMap.get(PipelineService.PIPELINE_STAGE_QC_OUTPUT_R2);
        if(StringUtils.isNotBlank(outputR2)){
            inputMap.put(PipelineService.PIPELINE_STAGE_ASSEMBLY_INPUT_R2, outputR2);
        }
        return inputMap;
    }

    private static Map<String,String> createInputMapForMapping(BioPipelineStage curStage, BioPipelineStage mappingStage) throws JsonProcessingException {

        HashMap<String,String> inputMap = new HashMap<>();
        if(curStage.getStageType() == PipelineService.PIPELINE_STAGE_QC){
            Map<String,String> outputMap = objectMapper.readValue(curStage.getOutputUrl(), Map.class);
            inputMap.put(PipelineService.PIPELINE_STAGE_MAPPING_INPUT_R1, outputMap.get(PipelineService.PIPELINE_STAGE_QC_OUTPUT_R1));
            inputMap.put(PipelineService.PIPELINE_STAGE_MAPPING_INPUT_R2, outputMap.get(PipelineService.PIPELINE_STAGE_QC_OUTPUT_R2));
        } else {
            //这里如果上个stage不是qc那么就是assemly。对于assemly的话，直接使用qc的trimmed后的read就可以了。
            Map<String,String> assemblyStageInputMap = objectMapper.readValue(curStage.getInputUrl(),Map.class);
            inputMap.put(PipelineService.PIPELINE_STAGE_MAPPING_INPUT_R1, assemblyStageInputMap.get(PipelineService.PIPELINE_STAGE_ASSEMBLY_INPUT_R1));
            inputMap.put(PipelineService.PIPELINE_STAGE_MAPPING_INPUT_R2, assemblyStageInputMap.get(PipelineService.PIPELINE_STAGE_ASSEMBLY_INPUT_R2));
        }
        return inputMap;
    }

    private static Map<String,String> createInputMapForVarient(BioPipelineStage curStage, BioPipelineStage varientStage) throws JsonProcessingException {
        Map<String,String> outputMap = JsonUtil.toMap(curStage.getOutputUrl(), String.class);
        String bamUrl = outputMap.get(PipelineService.PIPELINE_STAGE_MAPPING_OUTPUT_BAM_KEY);
        String bamIndexUrl = outputMap.get(PipelineService.PIPELINE_STAGE_MAPPING_OUTPUT_BAM_INDEX_KEY);
        return Map.of(PipelineService.PIPELINE_STAGE_VARIENT_CALL_INPUT_BAM_KEY, bamUrl, PipelineService.PIPELINE_STAGE_VARIENT_CALL_INPUT_BAM_INDEX_KEY, bamIndexUrl);
    }

    private static Map<String,String> createInputMapForConsensus(BioPipelineStage curStage, BioPipelineStage consensusStage) throws JsonProcessingException {
        Map<String,String> outputMap = JsonUtil.toMap(curStage.getOutputUrl(), String.class);
        return Map.of(PipelineService.PIPELINE_STAGE_CONSENSUS_INPUT_VCFGZ
                , outputMap.get(PipelineService.PIPELINE_STAGE_VARIENT_OUTPUT_VCF_GZ)
                , PipelineService.PIPELINE_STAGE_CONSENSUS_INPUT_VCFGZ_TBI, outputMap.get(PipelineService.PIPELINE_STAGE_VARIENT_OUTPUT_VCF_TBI));
    }



    public Map<String,String> createInputMapForNextStage(BioPipelineStage curStage, BioPipelineStage nextStage) throws JsonProcessingException {


        if(curStage.getStageType() == PipelineService.PIPELINE_STAGE_READ_LENGTH_DETECT){
            return objectMapper.readValue(nextStage.getInputUrl(),Map.class);
        }
        if(nextStage.getStageType() == PipelineService.PIPELINE_STAGE_ASSEMBLY){
            return createInputMapForAssembly(curStage);
        }
        if(nextStage.getStageType() == PipelineService.PIPELINE_STAGE_MAPPING){
            return createInputMapForMapping(curStage,nextStage);
        }
        if(nextStage.getStageType() == PipelineService.PIPELINE_STAGE_VARIANT_CALL){
            return createInputMapForVarient(curStage, nextStage);
        }
        if(nextStage.getStageType() == PipelineService.PIPELINE_STAGE_CONSENSUS){
            return createInputMapForConsensus(curStage, nextStage);
        }
        return null;
    }


    public Map<String,Object> createParamsForNextStage(BioPipelineStage curStage, BioPipelineStage nextStage){
        return null;
    }



}
