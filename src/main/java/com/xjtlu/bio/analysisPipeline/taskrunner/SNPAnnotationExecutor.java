package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_SNP_ANNOTATION;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.analysisPipeline.context.StageContext;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.SNPAnnotationInputs;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.RefSeqConfig;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.SNPAnnotationStageParameters;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.SNPAnnotationStageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.utils.JsonUtil;

public class SNPAnnotationExecutor extends AbstractPipelineStageExector<SNPAnnotationStageOutput, SNPAnnotationInputs, SNPAnnotationStageParameters> implements PipelineStageExecutor<SNPAnnotationStageOutput>{




    @Override
    protected Class<SNPAnnotationInputs> stageInputType() {
        return SNPAnnotationInputs.class;
    }


    @Override
    protected Class<SNPAnnotationStageParameters> stageParameterType() {
        return SNPAnnotationStageParameters.class;
    }
    @Override
    protected StageRunResult<SNPAnnotationStageOutput> _execute(StageExecutionInput stageExecutionInput)
            throws JsonMappingException, JsonProcessingException, LoadFailException {
        // TODO Auto-generated method stub
        StageContext bioPipelineStage = stageExecutionInput.stageContext;

        SNPAnnotationInputs snpAnnotationInputs = stageExecutionInput.input;
        SNPAnnotationStageParameters snpAnnotationStageParameters = stageExecutionInput.stageParameters;

        RefSeqConfig refSeqConfig = snpAnnotationStageParameters.getRefSeqConfig();
        List<String> accessions = refSeqConfig.getAccessions();

        HashMap<String,Path> loadMap = new HashMap<>();
        for(String accession:accessions){
            String accessionAnnotationFileName = accession + ".gff3";
            loadMap.put(accessionAnnotationFileName, stageExecutionInput.inputDir.resolve(accessionAnnotationFileName));
        }

        loadMap.put(snpAnnotationInputs.getVcfUrl(), stageExecutionInput.inputDir.resolve(snpAnnotationInputs.getVcfUrl().substring(snpAnnotationInputs.getVcfUrl().lastIndexOf("/")+1)));
        loadInput(loadMap);

        File refseq = refSeqService.getRefseq(refSeqConfig.getRefseqId());
        if (refseq == null){
            return this.runFail(bioPipelineStage, ERROR_LOAD_REFSEQ);
        }

        


    }

    @Override
    public int id() {
        // TODO Auto-generated method stub
        return PIPELINE_STAGE_SNP_ANNOTATION;
    }




}
