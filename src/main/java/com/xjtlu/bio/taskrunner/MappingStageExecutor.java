package com.xjtlu.bio.taskrunner;

import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_PARAMETER_REFSEQ_CONFIG;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.xjtlu.bio.configuration.AnalysisPipelineToolsConfig;
import jakarta.annotation.Resource;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.taskrunner.stageOutput.MappingStageOutput;
import com.xjtlu.bio.utils.JsonUtil;
import com.xjtlu.bio.service.StorageService.GetObjectResult;
import com.xjtlu.bio.service.stage.RefSeqConfig;

@Component
public class MappingStageExecutor extends AbstractPipelineStageExector<MappingStageOutput>
        implements PipelineStageExecutor<MappingStageOutput> {

    @Override
    public int id() {
        // TODO Auto-generated method stub
        return PipelineService.PIPELINE_STAGE_MAPPING;
    }


    @Override
    public StageRunResult<MappingStageOutput> _execute(StageExecutionInput stageExecutionInput) {
        // TODO Auto-generated method stub

        BioPipelineStage bioPipelineStage = stageExecutionInput.bioPipelineStage;
        String inputUrls = bioPipelineStage.getInputUrl();
        Map<String, String> inputUrlJson = null;
        Map<String, Object> params = null;
        try {
            inputUrlJson = JsonUtil.toMap(inputUrls, String.class);
            params = JsonUtil.toMap(bioPipelineStage.getParameters());
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return StageRunResult.fail(PARSE_JSON_ERROR, bioPipelineStage, null);
        }

        RefSeqConfig refSeqConfig = JsonUtil.mapToPojo(
                (Map<String, Object>) params.get(PIPELINE_STAGE_PARAMETER_REFSEQ_CONFIG), RefSeqConfig.class);

        if (refSeqConfig == null) {
            return StageRunResult.fail("未能加载参考基因", bioPipelineStage, null);
        }

        File refSeq = refSeqConfig.getRefseqId() >= 0 ? this.refSeqService.getRefseq(refSeqConfig.getRefseqId())
                : this.refSeqService.getRefseq(refSeqConfig.getRefseqObjectName());

        if (refSeq == null) {
            return StageRunResult.fail("参考基因组加载失败", bioPipelineStage, null);
        }

        String inputR1Url = inputUrlJson.get(PipelineService.PIPELINE_STAGE_MAPPING_INPUT_R1);
        String inputR2Url = inputUrlJson.get(PipelineService.PIPELINE_STAGE_MAPPING_INPUT_R2);

        Path inputTmpPath = stageExecutionInput.inputDir;
        Path workDir = stageExecutionInput.workDir;

        Path r1TmpPath = inputTmpPath.resolve(inputR1Url.substring(inputR1Url.lastIndexOf("/") + 1));
        Path r2TmpPath = inputR2Url == null ? null
                : inputTmpPath.resolve(inputR2Url.substring(inputR2Url.lastIndexOf("/") + 1));

        Map<String, Path> loadMap = new HashMap<>();
        loadMap.put(inputR1Url, r1TmpPath);
        if (r2TmpPath != null) {
            loadMap.put(inputR2Url, r2TmpPath);
        }
        boolean loadResult = this.loadInput(loadMap);

        if (!loadResult) {
            return this.runFail(bioPipelineStage, "load input failed");
        }

        // Path samTmp = workDir.resolve("aln.sam");
        // Path bamTmp = workDir.resolve("aln.bam"); // view 输出的 BAM（未排序）

        MappingStageOutput mappingStageOutput = bioStageUtil.mappingOutput(bioPipelineStage, workDir);
        Path bamSortedTmp = Path.of(mappingStageOutput.getBamPath());
        Path bamIndexTmp = Path.of(mappingStageOutput.getBamIndexPath()); // index 结果

        // String pipelineCmd = buildMappingPipelineCmd(refSeq, r1TmpPath, r2TmpPath,
        // bamSortedTmp);
        // logger.info("{} run cmd {}", bioPipelineStage, pipelineCmd);
        // List<String> cmd = new ArrayList<>();
        // cmd.add("sh");
        // cmd.add("-c");
        // cmd.add(pipelineCmd);

        List<String> cmd = new ArrayList<>();
        cmd.addAll(this.analysisPipelineToolsConfig.getMinimap2());
        cmd.add("-ax");
        cmd.add("sr");
        cmd.add(refSeq.getAbsolutePath());
        cmd.add(r1TmpPath.toString());
        if (r2TmpPath != null) {
            cmd.add(r2TmpPath.toString());
        }

        Path samPath = workDir.resolve("aln.sam");
        ExecuteResult executeResult = _execute(cmd, workDir, samPath, null);
        if (!executeResult.success()) {
            logger.error("stage id = {}, exit code = {}, exception = ", bioPipelineStage.getStageId(),
                    executeResult.runCode, executeResult.ex);
            return this.runFail(bioPipelineStage, "运行mapping tool失败", executeResult.ex, inputTmpPath, workDir);
        }

        List<StageOutputValidationResult> errors = validateOutputFiles(samPath);
        if (!errors.isEmpty()) {
            StageOutputValidationResult error = errors.get(0);
            logger.error("stage id = {}. 未生成sam文件", bioPipelineStage.getStageId(), error.ioException);
            return this.runFail(bioPipelineStage, "未生成文件", error.ioException, inputTmpPath, workDir);
        }

        Path bamPath = workDir.resolve("aln.bam");

        cmd.clear();

        cmd.addAll(this.analysisPipelineToolsConfig.getSamtools()); // 例如:
                                                                    // ["/usr/conda/condabin/conda","run","-n","bio-map","samtools"]

        cmd.add("view");
        cmd.add("-b"); // 输出 BAM
        cmd.add("-h"); // 保留 header（建议加，虽然有时不是必须）
        cmd.add("-o");
        cmd.add(bamPath.toString()); // 输出文件
        cmd.add(samPath.toString()); // 输入 SAM 文件

        executeResult = _execute(cmd, workDir);
        if (!executeResult.success()) {
            logger.error("stage id = {}, 生成bam失败. exit code = {}, exception = ", bioPipelineStage.getStageId(), executeResult.runCode, executeResult.runCode);
            return this.runFail(bioPipelineStage, "运行mapping tools失败", executeResult.ex, inputTmpPath, workDir);
        }

        List<StageOutputValidationResult> errorOutput = validateOutputFiles(bamPath);
        if (!errorOutput.isEmpty()) {

            StageOutputValidationResult error = errorOutput.get(0);
            logger.error("stage id = {}. 未生成bam文件", bioPipelineStage.getStageId(), error.ioException);
            return this.runFail(bioPipelineStage, createStageOutputValidationErrorMessge(errorOutput), null,
                    inputTmpPath, workDir);
        }

        cmd.clear();

        cmd.addAll(analysisPipelineToolsConfig.getSamtools());
        cmd.add("sort");
        cmd.add("-o");
        cmd.add(bamSortedTmp.toString());
        cmd.add(bamPath.toString());

        executeResult = _execute(cmd, workDir);
        if (!executeResult.success()) {
            logger.error("stage id = {}, 生成bam sorted失败. exit code = {}, exception = ", bioPipelineStage.getStageId(),executeResult.runCode, executeResult.ex);
            return this.runFail(bioPipelineStage, "生成bam索引失败", executeResult.ex, inputTmpPath, workDir);
        }

        errorOutput = validateOutputFiles(bamSortedTmp);
        if (!errorOutput.isEmpty()) {
            StageOutputValidationResult error = errorOutput.get(0);
            logger.error("stage id = {}, 未生成sorted文件. ", bioPipelineStage.getStageId(), error.ioException);
            return this.runFail(bioPipelineStage, createStageOutputValidationErrorMessge(errorOutput), null,
                    inputTmpPath, workDir);
        }


        cmd.clear();

        cmd.addAll(this.analysisPipelineToolsConfig.getSamtools());
        cmd.add("index");
        cmd.add("-o");
        cmd.add(bamIndexTmp.toString());
        cmd.add(bamSortedTmp.toString());

        executeResult = _execute(cmd, workDir);
        if(!executeResult.success()){
            logger.error("stage id = {}, 生成bam index失败. exit code = {}, exception = ", bioPipelineStage.getStageId(),executeResult.runCode, executeResult.ex);
            return this.runFail(bioPipelineStage, "生成bam索引失败", executeResult.ex, inputTmpPath, workDir);
        }

        errorOutput = validateOutputFiles(bamIndexTmp);
        if (!errorOutput.isEmpty()) {
            StageOutputValidationResult error = errorOutput.get(0);
            logger.error("stage id = {}, 未生成index文件. ", bioPipelineStage.getStageId(), error.ioException);
            return this.runFail(bioPipelineStage, createStageOutputValidationErrorMessge(errorOutput), null,
                    inputTmpPath, workDir);
        }


        StageRunResult<MappingStageOutput> stageRunResult = StageRunResult
                .OK(new MappingStageOutput(bamSortedTmp.toString(), bamIndexTmp.toString()), bioPipelineStage);

        return stageRunResult;
    }

    private String buildMappingPipelineCmd(File refSeq, Path r1, Path r2, Path bamSortedOut) {
        StringBuilder sb = new StringBuilder(256);
        // mapping tool（输出到 stdout）
        sb.append(String.join(" ", this.analysisPipelineToolsConfig.getMinimap2())).append(" -ax sr ")
                .append(quote(refSeq.getAbsolutePath())).append(' ')
                .append(quote(r1.toString())).append(' ');
        if (r2 != null) {
            sb.append(quote(r2.toString())).append(' ');
        }
        // view: 从 stdin 读 SAM，输出 BAM 到 stdout
        sb.append("| ").append((String.join(" ", this.analysisPipelineToolsConfig.getSamtools())))
                .append(" view -bS - ");
        // sort: 从 stdin 读 BAM，输出最终排序 bam
        sb.append("| ").append((String.join(" ", this.analysisPipelineToolsConfig.getSamtools()))).append(" sort -o ")
                .append(quote(bamSortedOut.toString())).append(" -");
        return sb.toString();
    }

    private static String quote(String s) {
        if (s == null)
            return "''";
        return "'" + s.replace("'", "'\"'\"'") + "'";
    }

}
