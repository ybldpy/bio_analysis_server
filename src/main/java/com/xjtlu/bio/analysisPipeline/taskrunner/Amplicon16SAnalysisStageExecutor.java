package com.xjtlu.bio.analysisPipeline.taskrunner;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.MetagenomicsAnalysisStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.Amplicon16SAnalysisStageOutput;

public class Amplicon16SAnalysisStageExecutor extends
        AbstractPipelineStageExector<Amplicon16SAnalysisStageOutput, MetagenomicsAnalysisStageInputUrls, BaseStageParams>
        implements PipelineStageExecutor<Amplicon16SAnalysisStageOutput> {

    @Value("${analysis-pipeline.stage.amlicon16s.silvaTrainset}")
    private String silvaTrainSet;

    @Override
    protected Class<MetagenomicsAnalysisStageInputUrls> stageInputType() {
        return MetagenomicsAnalysisStageInputUrls.class;
    }

    @Override
    protected Class<BaseStageParams> stageParameterType() {
        return BaseStageParams.class;
    }

    private static class RunOutput {
        Path r1FilteredPath;
        Path r2FilteredPath;
        Path asvTablePath;
        Path asvSequencePath;
        Path representativeSequencePath;
        Path dada2TrackTsvPath;
    }

    private ExecuteResult runDada2Taxonomy(Path avsFasta, Path trainset, Path output, Path workDir) {

        List<String> cmd = new ArrayList<>();
        cmd.addAll(this.analysisPipelineToolsConfig.getDada2Taxonomy());
        cmd.addAll(List.of(
                "--asv-fasta",
                avsFasta.toAbsolutePath().toString(),
                "--silva-train-set",
                trainset.toAbsolutePath().toString(),
                "--output-file",
                output.toAbsolutePath().toString()));
        return _execute(cmd, workDir);
    }

    /*
     * outputDir/
     * ├── filtered/
     * │ ├── sample01_R1.filtered.fastq.gz
     * │ └── sample01_R2.filtered.fastq.gz
     * ├── asv_table.tsv
     * ├── asv_sequences.tsv
     * ├── representative_sequences.fasta
     * ├── dada2_track.tsv
     * └── sequence_table.rds
     */
    private ExecuteResult runDada2(Path r1, Path r2, Path workDir, Path outputDir) {

        String sampleName = r1.getFileName().toString()
                .replaceFirst("(?i)(_R?1)?\\.(fastq|fq)(\\.gz)?$", "");

        List<String> cmd = new ArrayList<>();
        cmd.addAll(analysisPipelineToolsConfig.getDada2());
        cmd.add("--r1");
        cmd.add(r1.toAbsolutePath().toString());
        if (r2 != null) {
            cmd.add("--r2");
            cmd.add(r2.toAbsolutePath().toString());
        }

        cmd.addAll(List.of(
                "--sample-name", sampleName,
                "--output-dir", outputDir.toString()));

        return _execute(cmd, workDir);
    }

    /*
     * summary/
     * ├── asv_abundance.tsv
     * ├── kingdom_abundance.tsv
     * ├── phylum_abundance.tsv
     * ├── class_abundance.tsv
     * ├── order_abundance.tsv
     * ├── family_abundance.tsv
     * ├── genus_abundance.tsv
     * ├── alpha_diversity.tsv
     * └── classification_summary.tsv
     */
    private ExecuteResult doSummary(Path workDir, Path asvTable, Path taxonomyFile, Path outputDir) {
        List<String> cmd = new ArrayList<>();
        cmd.addAll(this.analysisPipelineToolsConfig.getAmplicon16sSummary());
        cmd.addAll(List.of(
                "--asv-table", asvTable.toString(),
                "--taxonomy-file", taxonomyFile.toString(),
                "--output-dir", outputDir.toString()));
        return _execute(cmd, workDir);
    }

    @Override
    protected StageRunResult<Amplicon16SAnalysisStageOutput> _execute(
            StageExecutionInput stageExecutionInput)
            throws JsonMappingException, JsonProcessingException, LoadFailException, NotGetRefSeqException {

        MetagenomicsAnalysisStageInputUrls inputUrls = stageExecutionInput.input;
        Map<String, Path> loadMap = new HashMap<>();

        Path r1LocalPath = stageExecutionInput.inputDir
                .resolve(inputUrls.getR1Url().substring(inputUrls.getR1Url().lastIndexOf("/") + 1));
        Path r2LocalPath = null;
        loadMap.put(inputUrls.getR1Url(), r1LocalPath);

        if (!StringUtils.isBlank(inputUrls.getR2Url())) {
            r2LocalPath = stageExecutionInput.inputDir
                    .resolve(inputUrls.getR2Url().substring(inputUrls.getR2Url().lastIndexOf("/") + 1));
            loadMap.put(inputUrls.getR2Url(), r2LocalPath);
        }

        loadInput(loadMap);
        Path dada2OutputDir = stageExecutionInput.workDir.resolve("dada2Output");
        ExecuteResult executeResult = runDada2(r1LocalPath, r2LocalPath, stageExecutionInput.workDir, dada2OutputDir);

        if (!executeResult.success()) {
            Exception ex = executeResult.ex;

            String errorMsg;

            if (ex != null) {
                String exceptionMessage = ex.getMessage();

                if (StringUtils.isBlank(exceptionMessage)) {
                    exceptionMessage = ex.getClass().getSimpleName();
                }

                errorMsg = "DADA2 analysis failed: " + exceptionMessage;
                logger.error(errorMsg, ex);
            } else {
                errorMsg = "DADA2 analysis failed, exit code: "
                        + executeResult.runCode;
                logger.error(errorMsg);
            }

            return runFail(
                    stageExecutionInput.stageContext,
                    errorMsg,
                    stageExecutionInput.workDir);
        }

        Path dada2TaxonomyTsvPath = stageExecutionInput.workDir.resolve("taxonomy.tsv");
        Path asvFastaPath = dada2OutputDir.resolve("representative_sequences.fasta");
        Path trainsetPath = Path.of(this.silvaTrainSet);

        executeResult = this.runDada2Taxonomy(asvFastaPath, trainsetPath, dada2TaxonomyTsvPath,
                stageExecutionInput.workDir);

        if (!executeResult.success()) {
            Exception ex = executeResult.ex;

            String errorMsg;

            if (ex != null) {
                String exceptionMessage = ex.getMessage();

                if (StringUtils.isBlank(exceptionMessage)) {
                    exceptionMessage = ex.getClass().getSimpleName();
                }

                errorMsg = "DADA2 taxonomy assignment failed: "
                        + exceptionMessage;
                logger.error(errorMsg, ex);
            } else {
                errorMsg = "DADA2 taxonomy assignment failed, exit code: "
                        + executeResult.runCode;
                logger.error(errorMsg);
            }

            return runFail(
                    stageExecutionInput.stageContext,
                    errorMsg,
                    stageExecutionInput.workDir);
        }

        Path asvTablePath = dada2OutputDir.resolve("asv_table.tsv");

        Path summaryDir = stageExecutionInput.workDir.resolve("summaryDir");
        executeResult = doSummary(stageExecutionInput.workDir, asvTablePath, dada2TaxonomyTsvPath, summaryDir);

        if (!executeResult.success()) {
            Exception ex = executeResult.ex;

            String errorMsg;

            if (ex != null) {
                String exceptionMessage = ex.getMessage();

                if (StringUtils.isBlank(exceptionMessage)) {
                    exceptionMessage = ex.getClass().getSimpleName();
                }

                errorMsg = "Amplicon 16S summary analysis failed: "
                        + exceptionMessage;
                logger.error(errorMsg, ex);
            } else {
                errorMsg = "Amplicon 16S summary analysis failed, exit code: "
                        + executeResult.runCode;
                logger.error(errorMsg);
            }

            return runFail(
                    stageExecutionInput.stageContext,
                    errorMsg,
                    stageExecutionInput.workDir);
        }

        Path asvAbundanceTsvPath = summaryDir.resolve("asv_abundance.tsv");
        Path genusAbundanceTsvPath = summaryDir.resolve("genus_abundance.tsv");
        Path alphaDiversityTsvPath = summaryDir.resolve("alpha_diversity.tsv");

        Amplicon16SAnalysisStageOutput amplicon16sAnalysisStageOutput = new Amplicon16SAnalysisStageOutput(asvTablePath,
                asvFastaPath,
                dada2TaxonomyTsvPath,
                asvAbundanceTsvPath,
                genusAbundanceTsvPath,
                alphaDiversityTsvPath,
                null,
                null);

        return OK(amplicon16sAnalysisStageOutput, stageExecutionInput);
    }

    @Override
    public int id() {
        return Constants.StageType.PIPELINE_STAGE_METAGENOMICS_AMPLICON16S;
    }

}
