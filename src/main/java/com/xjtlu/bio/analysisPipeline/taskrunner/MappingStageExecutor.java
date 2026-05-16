package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_MAPPING;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.MappingInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.MappingParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.ReadMeta;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.RefSeqConfig;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.MappingStageOutput;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

@Component
public class MappingStageExecutor
        extends AbstractPipelineStageExector<MappingStageOutput, MappingInputUrls, MappingParameters>
        implements PipelineStageExecutor<MappingStageOutput> {

    @Override
    public int id() {
        // TODO Auto-generated method stub
        return PIPELINE_STAGE_MAPPING;
    }

    @Override
    protected Class<MappingInputUrls> stageInputType() {
        return MappingInputUrls.class;
    }

    @Override
    protected Class<MappingParameters> stageParameterType() {
        return MappingParameters.class;
    }

    @Override
    public StageRunResult<MappingStageOutput> _execute(StageExecutionInput stageExecutionInput)
            throws JsonMappingException, JsonProcessingException, LoadFailException {
        // TODO Auto-generated method stub

        StageContext bioPipelineStage = stageExecutionInput.stageContext;
        
        MappingInputUrls mappingInputUrls = stageExecutionInput.input;
        MappingParameters parameters = stageExecutionInput.stageParameters;

        RefSeqConfig refSeqConfig = parameters.getRefSeqConfig();

        if (refSeqConfig == null) {
            return StageRunResult.fail("未能加载参考基因", stageExecutionInput.workDir, bioPipelineStage, null);
        }

        

        String refseqUrl = refSeqConfig.getRefseqObjectName();
        String inputR1Url = mappingInputUrls.getR1Url();
        String inputR2Url = mappingInputUrls.getR2Url();

        Path inputTmpPath = stageExecutionInput.inputDir;
        Path workDir = stageExecutionInput.workDir;

        Path r1TmpPath = inputTmpPath.resolve(inputR1Url.substring(inputR1Url.lastIndexOf("/") + 1));
        Path r2TmpPath = inputR2Url == null ? null
                : inputTmpPath.resolve(inputR2Url.substring(inputR2Url.lastIndexOf("/") + 1));

        Path refseqLocalPath = inputTmpPath.resolve(refseqUrl.substring(refseqUrl.lastIndexOf("/")+1));


        Map<String, Path> loadMap = new HashMap<>();
        loadMap.put(inputR1Url, r1TmpPath);
        loadMap.put(refseqUrl, refseqLocalPath);
        if (r2TmpPath != null) {
            loadMap.put(inputR2Url, r2TmpPath);
        }


        this.loadInput(loadMap);

        Path bamSortedTmp = stageExecutionInput.workDir.resolve("aln_sorted.bam");
        Path bamIndexTmp = stageExecutionInput.workDir.resolve("aln_sorted.bam.bai");

        
        List<String> cmd = new ArrayList<>();
        cmd.addAll(this.analysisPipelineToolsConfig.getMinimap2());
        cmd.add("-ax");
        cmd.add(parameters.getReadMeta().getReadLenType() == ReadMeta.READ_LEN_TYPE_SHORT? "sr":"map-ont");
        cmd.add(refseqLocalPath.toString());
        cmd.add(r1TmpPath.toString());
        
        if (r2TmpPath != null) {
            cmd.add(r2TmpPath.toString());
        }

        Path samPath = workDir.resolve("aln.sam");
        ExecuteResult executeResult = _execute(cmd, workDir, samPath, null);
        if (!executeResult.success()) {
            logger.error("stage id = {}, exit code = {}, exception = ", bioPipelineStage,
                    executeResult.runCode, executeResult.ex);
            return this.runFail(bioPipelineStage, "运行mapping tool失败", executeResult.ex, workDir);
        }

        List<StageOutputValidationResult> errors = validateOutputFiles(samPath);
        if (!errors.isEmpty()) {
            StageOutputValidationResult error = errors.get(0);
            logger.error("stage id = {}. 未生成sam文件", bioPipelineStage, error.ioException);
            return this.runFail(bioPipelineStage, "未生成文件", error.ioException,workDir);
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
            logger.error("stage id = {}, 生成bam失败. exit code = {}, exception = ", bioPipelineStage,
                    executeResult.runCode, executeResult.runCode);
            return this.runFail(bioPipelineStage, "运行mapping tools失败", executeResult.ex, workDir);
        }

        List<StageOutputValidationResult> errorOutput = validateOutputFiles(bamPath);
        if (!errorOutput.isEmpty()) {

            StageOutputValidationResult error = errorOutput.get(0);
            logger.error("stage id = {}. 未生成bam文件", bioPipelineStage, error.ioException);
            return this.runFail(bioPipelineStage, createStageOutputValidationErrorMessge(errorOutput), null, workDir);
        }

        cmd.clear();

        cmd.addAll(analysisPipelineToolsConfig.getSamtools());
        cmd.add("sort");
        cmd.add("-o");
        cmd.add(bamSortedTmp.toString());
        cmd.add(bamPath.toString());

        executeResult = _execute(cmd, workDir);
        if (!executeResult.success()) {
            logger.error("stage id = {}, 生成bam sorted失败. exit code = {}, exception = ", bioPipelineStage,
                    executeResult.runCode, executeResult.ex);
            return this.runFail(bioPipelineStage, "生成bam索引失败", executeResult.ex, workDir);
        }

        errorOutput = validateOutputFiles(bamSortedTmp);
        if (!errorOutput.isEmpty()) {
            StageOutputValidationResult error = errorOutput.get(0);
            logger.error("stage id = {}, 未生成sorted文件. ", bioPipelineStage, error.ioException);
            return this.runFail(bioPipelineStage, createStageOutputValidationErrorMessge(errorOutput), null, workDir);
        }

        cmd.clear();

        cmd.addAll(this.analysisPipelineToolsConfig.getSamtools());
        cmd.add("index");
        cmd.add("-o");
        cmd.add(bamIndexTmp.toString());
        cmd.add(bamSortedTmp.toString());

        executeResult = _execute(cmd, workDir);
        if (!executeResult.success()) {
            logger.error("stage id = {}, 生成bam index失败. exit code = {}, exception = ", bioPipelineStage,
                    executeResult.runCode, executeResult.ex);
            return this.runFail(bioPipelineStage, "生成bam索引失败", executeResult.ex, workDir);
        }

        errorOutput = validateOutputFiles(bamIndexTmp);
        if (!errorOutput.isEmpty()) {
            StageOutputValidationResult error = errorOutput.get(0);
            logger.error("stage id = {}, 未生成index文件. ", bioPipelineStage, error.ioException);
            return this.runFail(bioPipelineStage, createStageOutputValidationErrorMessge(errorOutput), null, workDir);
        }

        StageRunResult<MappingStageOutput> stageRunResult = OK(new MappingStageOutput(bamSortedTmp.toString(), bamIndexTmp.toString()), stageExecutionInput);

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
