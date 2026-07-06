package com.xjtlu.bio.bio_analysis;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.TaxonomyStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.SequenceMeta;
import com.xjtlu.bio.analysisPipeline.taskrunner.PipelineStageExecutor;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.TaxonomyStageExecutor;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.TaxonomyStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.TaxonomyStageOutput.TaxonomyClassificationOutput;
import com.xjtlu.bio.entity.BioPipelineStage;

import jakarta.annotation.Resource;

@SpringBootTest(properties = {
        "localstorageService.baseDir=/home/jcy/bioTest",
        "analysis-pipeline.stage.baseWorkDir=/home/jcy/bioTest/workDir",
        "analysis-pipeline.stage.baseInputDir=/home/jcy/bioTest/inputDir"
})
@ActiveProfiles("dev")
class TaxonomyStageExecutorTest {

    @Resource
    private TaxonomyStageExecutor taxonomyStageExecutor;

    @Resource
    private ObjectMapper objectMapper;

    private static String toString(TaxonomyStageOutput taxonomyStageOutput) {
        if (taxonomyStageOutput == null) {
            return "TaxonomyStageOutput{null}";
        }

        StringBuilder sb = new StringBuilder();

        sb.append("TaxonomyStageOutput {\n");
        sb.append("  status = ").append(taxonomyStageOutput.getStatus()).append("\n");
        sb.append("  evidenceResource = ")
                .append(taxonomyStageOutput.getEvidenceResource())
                .append("\n");

        sb.append("  comfirmedTaxonomy = ");
        if (taxonomyStageOutput.getComfirmedTaxonomy() == null) {
            sb.append("null\n");
        } else {
            sb.append(toString(taxonomyStageOutput.getComfirmedTaxonomy())).append("\n");
        }

        sb.append("  candicates = ");

        List<TaxonomyStageOutput.TaxonomyClassificationOutput> candicates = taxonomyStageOutput.getCandicates();

        if (candicates == null) {
            sb.append("null\n");
        } else if (candicates.isEmpty()) {
            sb.append("[]\n");
        } else {
            sb.append("[\n");
            for (int i = 0; i < candicates.size(); i++) {
                sb.append("    ")
                        .append(i)
                        .append(": ")
                        .append(toString(candicates.get(i)))
                        .append("\n");
            }
            sb.append("  ]\n");
        }

        sb.append("}");

        return sb.toString();
    }

    private static String toString(
            TaxonomyStageOutput.TaxonomyClassificationOutput output) {
        if (output == null) {
            return "null";
        }

        return "TaxonomyClassificationOutput{"
                + "taxId=" + output.getTaxId()
                + ", name='" + output.getName() + '\''
                + ", speciesTaxId=" + output.getSpeciesTaxId()
                + ", speciesName='" + output.getSpeciesName() + '\''
                + ", score=" + output.getScore()
                + '}';
    }

    @Test
    public void testTaxonomyExecutor() throws Exception {
        BioPipelineStage stage = new BioPipelineStage();

        stage.setStageId(10001L);
        stage.setVersion(1);
        stage.setStageType(Constants.StageType.PIPELINE_STAGE_TAXONOMY);
        stage.setPipelineId(1l);

        TaxonomyStageInputUrls inputUrls = new TaxonomyStageInputUrls();

        // 这里不要随便写本地路径，除非你的 StorageService 支持 file://
        // 正常情况下这里应该是 MinIO / S3 / 本项目 storage 能识别的 object key 或 url
        inputUrls.setR1("sinput/taxonomy/coli.fna");

        stage.setInputUrl(objectMapper.writeValueAsString(inputUrls));

        BaseStageParams params = new BaseStageParams();
        SequenceMeta sequenceMeta = new SequenceMeta();
        sequenceMeta.setReadLenType(Constants.SequenceInput.READ_LEN_TYPE_SHORT);
        sequenceMeta.setSequenceLevel(Constants.SequenceInput.SEQUENCE_LEVEL_ASSEMBLY);
        sequenceMeta.setQualityEncoding(Constants.SequenceInput.QUALITY_ENCODING_33);
        params.setReadMeta(sequenceMeta);
        stage.setParameters(objectMapper.writeValueAsString(params));

        StageRunResult<TaxonomyStageOutput> result = taxonomyStageExecutor.execute(stage);

        assertNotNull(result);

        // 下面这些 getter 名字按你项目里的 StageRunResult 实际方法改
        assertTrue(result.isSuccess(), result.getErrorLog());

        TaxonomyStageOutput output = result.getStageOutput();
        assertNotNull(output);

        assertNotNull(output.getCandicates());
        assertFalse(output.getCandicates().isEmpty());

        assertNotNull(output.getStatus());
        assertNotNull(output.getEvidenceResource());

        // System.out.println("taxonomy status = " + output.getStatus());
        // System.out.println("evidence = " + output.getEvidenceResource());
        // System.out.println("candidates = " + output.getCandicates());

        System.out.println(toString(output));

        // if (output.getComfirmedTaxonomy() != null) {
        //     System.out.println("confirmed = " + output.getComfirmedTaxonomy());
        // }
    }
}
