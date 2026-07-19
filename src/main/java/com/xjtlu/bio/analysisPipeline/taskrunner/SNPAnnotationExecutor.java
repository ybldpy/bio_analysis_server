package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_SNP_ANNOTATION;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.SNPAnnotationInputs;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.SNPAnnotationStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.RefSeqConfig;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.SNPAnnotationStageOutput;

@Component
public class SNPAnnotationExecutor extends
        AbstractPipelineStageExector<SNPAnnotationStageOutput, SNPAnnotationInputs, SNPAnnotationStageParameters>
        implements PipelineStageExecutor<SNPAnnotationStageOutput> {

    private String getGFFObjectName(String gff) {
        return "GFF" + gff;
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

        HashMap<String, Path> loadMap = new HashMap<>();

        Path vcfInputPath = stageExecutionInput.inputDir.resolve(
                snpAnnotationInputs.getVcfUrl().substring(snpAnnotationInputs.getVcfUrl().lastIndexOf("/") + 1));
        Path refseqPath = stageExecutionInput.inputDir.resolve(
                refSeqConfig.getRefseqObjectName().substring(refSeqConfig.getRefseqObjectName().lastIndexOf("/") + 1));
        Path gffPath = stageExecutionInput.inputDir
                .resolve(refSeqConfig.getGff3Url().substring(refSeqConfig.getGff3Url().lastIndexOf("/") + 1));

        loadMap.put(snpAnnotationInputs.getVcfUrl(), vcfInputPath);
        loadMap.put(refSeqConfig.getRefseqObjectName(), refseqPath);
        loadMap.put(refSeqConfig.getGff3Url(), gffPath);

        loadInput(loadMap);

        Path outputPath = stageExecutionInput.workDir.resolve("annotated.vcf");
        List<String> cmd = new ArrayList<>();
        if (false) {
            cmd.addAll(this.analysisPipelineToolsConfig.getVep());
            cmd.add("-i");
            cmd.add(vcfInputPath.toString());
            cmd.add("-o");
            cmd.add(outputPath.toString());
            cmd.add("--vcf");
            cmd.add("--gff");
            cmd.add(gffPath.toString());
            cmd.add("--fasta");
            cmd.add(refseqPath.toString());
            // cmd.add("--offline");
            cmd.add("--force_overwrite");
        } else {
            cmd.addAll(this.analysisPipelineToolsConfig.getBcftools());

            cmd.add("csq");

            cmd.add("-f");
            cmd.add(refseqPath.toString());

            cmd.add("-g");
            cmd.add(gffPath.toString());

            // 输出普通 VCF，和你现在 outputPath = annotated.vcf 对应
            cmd.add("-Ov");

            cmd.add("-o");
            cmd.add(outputPath.toString());

            cmd.add(vcfInputPath.toString());
        }

        boolean res = _execute(cmd, null, stageExecutionInput, outputPath);
        if (!res) {
            return this.runFail(bioPipelineStage, "run failed", stageExecutionInput.workDir);
        }
        return OK(new SNPAnnotationStageOutput(outputPath), stageExecutionInput);

    }

    @Override
    public int id() {
        // TODO Auto-generated method stub
        return PIPELINE_STAGE_SNP_ANNOTATION;
    }

}
