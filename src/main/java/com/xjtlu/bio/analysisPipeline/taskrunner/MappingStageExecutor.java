package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_MAPPING;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.stageMetrics.MappingResultMetrics;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceGenome;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.MappingInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.MappingParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.SequenceMeta;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.MappingStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.MappingMetricsCalculator;
import com.xjtlu.bio.utils.JsonUtil;
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

        Path inputTmpPath = stageExecutionInput.inputDir;
        Path workDir = stageExecutionInput.workDir;

        SequenceMeta readMeta = parameters == null ? null : parameters.getSequenceMeta();
        if (readMeta == null
                || (readMeta.getReadLenType() != Constants.SequenceInput.READ_LEN_TYPE_SHORT
                    && readMeta.getReadLenType() != Constants.SequenceInput.READ_LEN_TYPE_LONG)) {
            return runFail(bioPipelineStage,
                    "Mapping requires a valid readLenType from ReadInspect", workDir);
        }
        if (mappingInputUrls == null || mappingInputUrls.getR1Url() == null
                || mappingInputUrls.getR1Url().isBlank()) {
            return runFail(bioPipelineStage, "Mapping requires an R1 input", workDir);
        }

        boolean shortReads = readMeta.getReadLenType() == Constants.SequenceInput.READ_LEN_TYPE_SHORT;
        ReferenceGenome referenceGenome = parameters.getReferenceGenome();
        String inputR1Url = mappingInputUrls.getR1Url();
        String inputR2Url = mappingInputUrls.getR2Url();
        if (inputR2Url != null && inputR2Url.isBlank()) {
            inputR2Url = null;
        }

        Path r1TmpPath = inputTmpPath.resolve(inputR1Url.substring(inputR1Url.lastIndexOf("/") + 1));
        Path r2TmpPath = inputR2Url == null ? null
                : inputTmpPath.resolve(inputR2Url.substring(inputR2Url.lastIndexOf("/") + 1));

        Path refseqLocalPath = materializeReferenceGenome(referenceGenome, inputTmpPath);

        Map<String, Path> loadMap = new HashMap<>();
        loadMap.put(inputR1Url, r1TmpPath);
        if (r2TmpPath != null) {
            loadMap.put(inputR2Url, r2TmpPath);
        }


        this.loadInput(loadMap);

        Path bamSortedTmp = stageExecutionInput.workDir.resolve("aln_sorted.bam");
        Path bamIndexTmp = stageExecutionInput.workDir.resolve("aln_sorted.bam.bai");

        Path bowtie2IndexPrefix = workDir.resolve("bowtie2_reference");
        if (shortReads) {
            List<String> indexCommand = new ArrayList<>(analysisPipelineToolsConfig.getBowtie2Build());
            indexCommand.addAll(List.of(
                    "--seed", "1",
                    refseqLocalPath.toString(),
                    bowtie2IndexPrefix.toString()));
            ExecuteResult indexResult = _execute(indexCommand, workDir);
            if (!indexResult.success()) {
                return runFail(bioPipelineStage, "构建Bowtie2索引失败", indexResult.ex, workDir);
            }
            List<StageOutputValidationResult> indexErrors = validateBowtie2Index(bowtie2IndexPrefix);
            if (!indexErrors.isEmpty()) {
                return runFail(bioPipelineStage,
                        "Bowtie2索引不完整: " + createStageOutputValidationErrorMessge(indexErrors),
                        null, workDir);
            }
        }

        Path samPath = workDir.resolve("aln.sam");
        List<String> cmd = buildMappingCommand(
                shortReads, refseqLocalPath, bowtie2IndexPrefix, r1TmpPath, r2TmpPath, samPath);
        logger.info("stage = {}, readLenType = {}, mapping tool = {}", bioPipelineStage,
                readMeta.getReadLenType(), shortReads ? "Bowtie2" : "minimap2");
        ExecuteResult executeResult = shortReads
                ? _execute(cmd, workDir)
                : _execute(cmd, workDir, samPath, null);
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

        Path idxstatsPath = workDir.resolve("mapping_idxstats.tsv");
        cmd.clear();
        cmd.addAll(analysisPipelineToolsConfig.getSamtools());
        cmd.add("idxstats");
        cmd.add(bamSortedTmp.toAbsolutePath().toString());

        executeResult = _execute(cmd, workDir, idxstatsPath, null);
        if (!executeResult.success()) {
            logger.error("stage id = {}, 生成idxstats失败. exit code = {}, exception = ", bioPipelineStage,
                    executeResult.runCode, executeResult.ex);
            return this.runFail(bioPipelineStage, "生成idxstats失败", executeResult.ex, workDir);
        }

        errorOutput = validateOutputFiles(idxstatsPath);
        if (!errorOutput.isEmpty()) {
            StageOutputValidationResult error = errorOutput.get(0);
            logger.error("stage id = {}, 未生成idxstats文件. ", bioPipelineStage, error.ioException);
            return this.runFail(bioPipelineStage, createStageOutputValidationErrorMessge(errorOutput), null, workDir);
        }



        Path coverageDepthPath = stageExecutionInput.workDir.resolve("coverage_depth.tsv");
        cmd = buildCoverageDepthCommand(
                analysisPipelineToolsConfig.getSamtools(),
                bamSortedTmp,
                parameters);

        executeResult = _execute(cmd, workDir, coverageDepthPath, null);
        if (!executeResult.success()) {
            logger.error("stage id = {}, 生成coverage depth失败. exit code = {}, exception = ", bioPipelineStage,
                    executeResult.runCode, executeResult.ex);
            return this.runFail(bioPipelineStage, "生成coverage depth失败", executeResult.ex, workDir);
        }

        errorOutput = validateOutputFiles(coverageDepthPath);
        if (!errorOutput.isEmpty()) {
            StageOutputValidationResult error = errorOutput.get(0);
            logger.error("stage id = {}, 未生成coverage depth文件. ", bioPipelineStage, error.ioException);
            return this.runFail(bioPipelineStage, createStageOutputValidationErrorMessge(errorOutput), null, workDir);
        }

        Path metricsJsonPath = workDir.resolve(MappingStageOutput.METRICS_JSON);
        MappingResultMetrics metrics;
        try {
            metrics = MappingMetricsCalculator.calculate(coverageDepthPath, idxstatsPath);
            Files.writeString(
                    metricsJsonPath,
                    JsonUtil.toJson(metrics),
                    StandardCharsets.UTF_8);
        } catch (IOException e) {
            logger.error(
                    "stage id = {}, 生成mapping metrics失败. depth={}, idxstats={}",
                    bioPipelineStage,
                    coverageDepthPath,
                    idxstatsPath,
                    e);
            return this.runFail(bioPipelineStage, "生成mapping metrics失败: " + e.getMessage(), e, workDir);
        }

        errorOutput = validateOutputFiles(metricsJsonPath);
        if (!errorOutput.isEmpty()) {
            StageOutputValidationResult error = errorOutput.get(0);
            logger.error("stage id = {}, 未生成mapping metrics文件. ", bioPipelineStage, error.ioException);
            return this.runFail(bioPipelineStage, createStageOutputValidationErrorMessge(errorOutput), null, workDir);
        }

        return OK(
            new MappingStageOutput(
                    bamSortedTmp,
                    bamIndexTmp,
                    coverageDepthPath,
                    metricsJsonPath,
                    metrics),
            stageExecutionInput
        );

    }

    static List<String> buildCoverageDepthCommand(
            List<String> samtoolsCommand,
            Path sortedBam,
            MappingParameters parameters) {
        if (parameters.getMinimumMappingQuality() < 0) {
            throw new IllegalArgumentException(
                    "minimumMappingQuality must be at least 0");
        }
        if (parameters.getMinimumBaseQuality() < 0) {
            throw new IllegalArgumentException(
                    "minimumBaseQuality must be at least 0");
        }

        List<String> command = new ArrayList<>(samtoolsCommand);
        command.addAll(List.of(
                "depth",
                "-aa",
                // samtools depth uses -q for base quality and -Q for mapping
                // quality; bcftools mpileup uses the opposite letter mapping.
                "-q",
                String.valueOf(parameters.getMinimumBaseQuality()),
                "-Q",
                String.valueOf(parameters.getMinimumMappingQuality()),
                sortedBam.toAbsolutePath().toString()));
        return command;
    }

    private List<String> buildMappingCommand(
            boolean shortReads,
            Path reference,
            Path bowtie2IndexPrefix,
            Path r1,
            Path r2,
            Path samPath) {
        List<String> command = new ArrayList<>();
        if (shortReads) {
            command.addAll(analysisPipelineToolsConfig.getBowtie2());
            command.addAll(List.of(
                    "--local", "--very-sensitive-local", "--seed", "1",
                    "-x", bowtie2IndexPrefix.toString()));
            if (r2 == null) {
                command.addAll(List.of("-U", r1.toString()));
            } else {
                command.addAll(List.of("-1", r1.toString(), "-2", r2.toString()));
            }
            // Write SAM directly so wrappers such as conda run do not buffer
            // the entire alignment output or alter its stdout formatting.
            command.addAll(List.of("-S", samPath.toString()));
        } else {
            command.addAll(analysisPipelineToolsConfig.getMinimap2());
            command.addAll(List.of("-ax", "map-ont", reference.toString(), r1.toString()));
            if (r2 != null) {
                command.add(r2.toString());
            }
        }
        return command;
    }

    private List<StageOutputValidationResult> validateBowtie2Index(Path prefix) {
        List<String> parts = List.of("1", "2", "3", "4", "rev.1", "rev.2");
        Path[] smallIndex = parts.stream()
                .map(part -> Path.of(prefix + "." + part + ".bt2"))
                .toArray(Path[]::new);
        List<StageOutputValidationResult> errors = validateOutputFiles(smallIndex);
        if (errors.isEmpty()) {
            return errors;
        }
        Path[] largeIndex = parts.stream()
                .map(part -> Path.of(prefix + "." + part + ".bt2l"))
                .toArray(Path[]::new);
        List<StageOutputValidationResult> largeErrors = validateOutputFiles(largeIndex);
        return largeErrors.isEmpty() ? largeErrors : errors;
    }

}
