package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_SNP_ANNOTATION;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
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


    private String getGFFObjectName(String gff){
        return "GFF"+gff;
    }


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
            throws JsonMappingException, JsonProcessingException, LoadFailException, NotGetRefSeqException {
        // TODO Auto-generated method stub
        StageContext bioPipelineStage = stageExecutionInput.stageContext;

        SNPAnnotationInputs snpAnnotationInputs = stageExecutionInput.input;
        SNPAnnotationStageParameters snpAnnotationStageParameters = stageExecutionInput.stageParameters;

        RefSeqConfig refSeqConfig = snpAnnotationStageParameters.getRefSeqConfig();
        List<String> accessions = refSeqConfig.getAccessions();

        long refseqId = refSeqConfig.getRefseqId();
        File refseq = refSeqService.getRefSeqIndex(refseqId);
        if (refseq == null) {
            throw new NotGetRefSeqException(String.valueOf(refseqId), "Not found RefSeq = "+refseqId);  
        }

        HashMap<String,Path> loadMap = new HashMap<>();
        List<Path> accessionGFFPaths = new ArrayList<>();
        for(String accession:accessions){
            String accessionAnnotationFileName = accession + ".gff3";
            Path gffLocalPath = stageExecutionInput.inputDir.resolve(accessionAnnotationFileName);
            accessionGFFPaths.add(gffLocalPath);
            loadMap.put(getGFFObjectName(accessionAnnotationFileName), gffLocalPath);
        }

        Path vcfInputPath =  stageExecutionInput.inputDir.resolve(snpAnnotationInputs.getVcfUrl().substring(snpAnnotationInputs.getVcfUrl().lastIndexOf("/")+1));

        loadMap.put(snpAnnotationInputs.getVcfUrl(), vcfInputPath);
        loadInput(loadMap);

        Path gffPath = accessionGFFPaths.get(0);

        Path outputPath = stageExecutionInput.workDir.resolve("annotated.vcf");
        List<String> cmd = this.analysisPipelineToolsConfig.getVep();
        cmd.add("-i");
        cmd.add(vcfInputPath.toString());
        cmd.add("-o");
        cmd.add(outputPath.toString());
        cmd.add("--vcf");
        cmd.add("--gff");
        cmd.add(gffPath.toString());
        cmd.add("--fasta");
        cmd.add(refseq.toPath().toString());
        cmd.add("--offline");
        cmd.add("--force_overwrite");

        boolean res = _execute(cmd, null, stageExecutionInput, outputPath);
        if(!res){
            return this.runFail(bioPipelineStage, "run failed");
        }
        return StageRunResult.OK(new SNPAnnotationStageOutput(outputPath), bioPipelineStage);

    }

    @Override
    public int id() {
        // TODO Auto-generated method stub
        return PIPELINE_STAGE_SNP_ANNOTATION;
    }




}
