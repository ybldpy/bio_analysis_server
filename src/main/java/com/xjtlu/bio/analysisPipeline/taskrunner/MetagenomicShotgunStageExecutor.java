package com.xjtlu.bio.analysisPipeline.taskrunner;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.MetagenomicsAnalysisStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.MetagenomicsShotgunAnalysisStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.SequenceFileUtil;

public class MetagenomicShotgunStageExecutor extends
        AbstractPipelineStageExector<MetagenomicsShotgunAnalysisStageOutput, MetagenomicsAnalysisStageInputUrls, BaseStageParams>
        implements PipelineStageExecutor<MetagenomicsShotgunAnalysisStageOutput> {

    @Value("${humanBowtie2IndexDir}")
    private String humanBowtie2IndexDir;

    @Value("${kraken2Standard8Dir}")
    private String kraken2DBDir;

    @Override
    protected Class<MetagenomicsAnalysisStageInputUrls> stageInputType() {

        return MetagenomicsAnalysisStageInputUrls.class;
    }

    @Override
    protected Class<BaseStageParams> stageParameterType() {

        return BaseStageParams.class;
    }

    private boolean doRemoveHost(
            Path workDir,
            Path r1,
            Path r2,
            Path r1Out,
            Path r2Out) {

        List<String> cmd = new ArrayList<>();
        cmd.addAll(this.analysisPipelineToolsConfig.getBowtie2());

        Path bowtie2LogPath = workDir.resolve("bowtie2.log");
        cmd.addAll(List.of(
                "--very-sensitive-local",
                "-x", this.humanBowtie2IndexDir));

        boolean isPaired = r2 != null;

        /*
         * 双端数据需要根据 SAM flag 严格保留：
         * 当前 read 未比对，并且 mate 也未比对。
         */
        Path hostSamPath = workDir.resolve("host_alignment.sam");
        Path nonHostBamPath = workDir.resolve("nonhost.bam");
        Path nameSortedBamPath = workDir.resolve("nonhost.name_sorted.bam");

        if (isPaired) {

            cmd.addAll(List.of(
                    "-1", r1.toString(),
                    "-2", r2.toString(),
                    "-S", hostSamPath.toString()));
        } else {
            cmd.addAll(List.of(
                    "-U",
                    r1.toString(),
                    "--un-gz",
                    r1Out.toString(),
                    "-S", "/dev/null"));
        }

        ExecuteResult executeResult = _execute(cmd, workDir);

        if (!executeResult.success()) {

            logger.error(
                    "Bowtie2 host alignment failed. r1={}, r2={}",
                    r1,
                    r2,
                    executeResult.ex);

            return false;
        }

        cmd.clear();

        // 2. Samtools view：只保留 R1、R2 两端都未比对的记录
        cmd.addAll(this.analysisPipelineToolsConfig.getSamtools());

        cmd.addAll(List.of(
                "view",
                "-@", String.valueOf(2),
                "-b",
                "-f", "12",
                "-F", "2304",
                "-o", nonHostBamPath.toString(),
                hostSamPath.toString()));

        executeResult = _execute(cmd, workDir);

        if (!executeResult.success()) {
            logger.error(
                    "Failed to filter non-host read pairs. sam={}",
                    hostSamPath,
                    executeResult.ex);

            return false;
        }

        cmd.clear();

        // 3. 按 read name 排序，保证同一个 pair 聚在一起
        cmd.addAll(this.analysisPipelineToolsConfig.getSamtools());

        cmd.addAll(List.of(
                "sort",
                "-n",
                "-@", String.valueOf(2),
                "-o", nameSortedBamPath.toString(),
                nonHostBamPath.toString()));

        ExecuteResult sortResult = _execute(cmd, workDir);

        if (!sortResult.success()) {
            logger.error(
                    "Failed to name-sort non-host BAM. bam={}",
                    nonHostBamPath,
                    sortResult.ex);
            return false;
        }

        cmd.clear();
        // 4. 转回双端 FASTQ
        cmd.addAll(this.analysisPipelineToolsConfig.getSamtools());

        cmd.addAll(List.of(
                "fastq",
                "-@", String.valueOf(2),
                "-n",
                "-1", r1Out.toString(),
                "-2", r2Out.toString(),
                "-0", "/dev/null",
                "-s", "/dev/null",
                nameSortedBamPath.toString()));

        ExecuteResult fastqResult = _execute(cmd, workDir);

        if (!fastqResult.success()) {
            logger.error(
                    "Failed to export non-host paired FASTQ. r1Out={}, r2Out={}",
                    r1Out,
                    r2Out,
                    fastqResult.ex);
            return false;
        }

        return true;

    }

    private ExecuteResult doTaxonomyByKraken2(Path workDir, Path r1, Path r2, Path outputPath, Path reportPath) {

        List<String> cmd = new ArrayList<>();
        cmd.addAll(analysisPipelineToolsConfig.getKraken2());

        cmd.addAll(List.of(
                "--memory-mapping",
                "--db", this.kraken2DBDir,
                "--threads", "1",
                "--gzip-compressed",
                "--use-names",
                "--report", reportPath.toString(),
                "--output", outputPath.toString()));

        if (r2 != null) {
            cmd.add("--paired");
            cmd.add(r1.toString());
            cmd.add(r2.toString());
        } else {
            cmd.add(r1.toString());
        }

        return _execute(cmd, workDir);
    }

    private int getMeanReadLength(Path fastqPath) throws IOException {

        int maxReads = 25000;

        long totalLength = 0;
        int readCount = 0;

        try (BufferedReader reader = SequenceFileUtil.getReader(fastqPath)) {
            while (readCount < maxReads) {

                String header = reader.readLine();
                String sequence = reader.readLine();
                String separator = reader.readLine();
                String quality = reader.readLine();

                if (header == null
                        || sequence == null
                        || separator == null
                        || quality == null) {
                    break;
                }

                totalLength += sequence.length();
                readCount++;
            }
        }

        return Math.round((float) totalLength / readCount);
    }

    private int getNearestBrackenReadLength(int readLength) {

        int[] availableLengths = {
                50, 75, 100, 150, 200, 250, 300
        };

        int nearest = availableLengths[0];

        for (int length : availableLengths) {
            if (Math.abs(length - readLength) < Math.abs(nearest - readLength)) {
                nearest = length;
            }
        }

        return nearest;
    }

    private ExecuteResult doBracken(Path workDir,
            Path readPath,
            Path krakenReportPath,
            Path brackenOutputPath,
            Path brackenReportPath,
            int readLen,
            String taxonomyLevel) {

        List<String> cmd = new ArrayList<>();
        cmd.addAll(this.analysisPipelineToolsConfig.getBracken());

        cmd.addAll(List.of(
                "-d", this.kraken2DBDir,
                "-i", krakenReportPath.toString(),
                "-o", brackenOutputPath.toString(),
                "-w", brackenReportPath.toString(),
                "-r", String.valueOf(getNearestBrackenReadLength(readLen)),
                "-l", taxonomyLevel,
                "-t", "4"));

        return _execute(cmd, workDir);

    }

    private void generateAlphaDiversity(
            Path brackenSpeciesPath,
            Path alphaDiversityPath) throws IOException {

        List<Double> abundances = new ArrayList<>();

        try (BufferedReader reader = Files.newBufferedReader(brackenSpeciesPath)) {

            String line = reader.readLine(); // 跳过表头

            while ((line = reader.readLine()) != null) {

                String[] columns = line.split("\t");

                // Bracken 第 6 列：new_est_reads
                double estimatedReads = Double.parseDouble(columns[5]);

                if (estimatedReads > 0) {
                    abundances.add(estimatedReads);
                }
            }
        }

        double totalReads = abundances.stream()
                .mapToDouble(Double::doubleValue)
                .sum();

        int observedSpecies = abundances.size();

        double shannon = 0;
        double sumSquaredProportions = 0;

        for (double abundance : abundances) {

            double proportion = abundance / totalReads;

            shannon -= proportion * Math.log(proportion);
            sumSquaredProportions += proportion * proportion;
        }

        double simpson = 1 - sumSquaredProportions;

        double inverseSimpson = sumSquaredProportions == 0
                ? 0
                : 1 / sumSquaredProportions;

        double pielouEvenness = observedSpecies > 1
                ? shannon / Math.log(observedSpecies)
                : 0;

        try (BufferedWriter writer = Files.newBufferedWriter(alphaDiversityPath)) {

            writer.write(
                    "total_estimated_reads\t"
                            + "observed_species\t"
                            + "shannon\t"
                            + "simpson\t"
                            + "inverse_simpson\t"
                            + "pielou_evenness");

            writer.newLine();

            writer.write(
                    totalReads + "\t"
                            + observedSpecies + "\t"
                            + shannon + "\t"
                            + simpson + "\t"
                            + inverseSimpson + "\t"
                            + pielouEvenness);

            writer.newLine();
        }
    }

    @Override
    protected StageRunResult<MetagenomicsShotgunAnalysisStageOutput> _execute(
            StageExecutionInput stageExecutionInput)
            throws JsonMappingException, JsonProcessingException, LoadFailException, NotGetRefSeqException {

        MetagenomicsAnalysisStageInputUrls inputUrls = stageExecutionInput.input;

        Map<String, Path> loadMap = new HashMap<>();
        Path r1LocalPath = stageExecutionInput.inputDir
                .resolve(inputUrls.getR1Url().substring(inputUrls.getR1Url().lastIndexOf("/") + 1));
        loadMap.put(inputUrls.getR1Url(), r1LocalPath);
        Path r2LocalPath = null;
        if (!StringUtils.isBlank(inputUrls.getR2Url())) {
            r2LocalPath = stageExecutionInput.inputDir
                    .resolve(inputUrls.getR2Url().substring(inputUrls.getR2Url().lastIndexOf("/") + 1));
            loadMap.put(inputUrls.getR2Url(), r2LocalPath);
        }

        loadInput(loadMap);

        Path hostRemovalDir = stageExecutionInput.workDir.resolve("hostRemoval");
        try {
            Files.createDirectory(hostRemovalDir, null);
        } catch (IOException e) {
            String errorMsg = "Failed to create host removal directory: "
                    + hostRemovalDir;
            logger.error(errorMsg, e);

            return runFail(
                    stageExecutionInput.stageContext,
                    errorMsg,
                    stageExecutionInput.workDir);
        }

        Path removedHostR1Path = hostRemovalDir.resolve("nohost_" + r1LocalPath.getFileName());
        Path removedHostR2Path = r2LocalPath != null ? hostRemovalDir.resolve("nohost_" + r2LocalPath.getFileName())
                : null;

        boolean removeSuccess = doRemoveHost(hostRemovalDir, r1LocalPath, r2LocalPath, removedHostR1Path,
                removedHostR2Path);

        if (!removeSuccess) {
            String sequenceType = r2LocalPath == null
                    ? "single-end"
                    : "paired-end";

            String errorMsg = "Human host removal failed for "
                    + sequenceType
                    + " metagenomics reads";

            logger.error(
                    "{}. r1={}, r2={}",
                    errorMsg,
                    r1LocalPath,
                    r2LocalPath);

            return runFail(
                    stageExecutionInput.stageContext,
                    errorMsg,
                    stageExecutionInput.workDir);
        }

        Path krakenDir = stageExecutionInput.workDir.resolve("kraken2");
        try {
            Files.createDirectories(krakenDir);
        } catch (IOException e) {
            String errorMsg = "Failed to create taxonomy directory: "
                    + hostRemovalDir;
            logger.error(errorMsg, e);

            return runFail(
                    stageExecutionInput.stageContext,
                    errorMsg,
                    stageExecutionInput.workDir);
        }

        Path krakenOutputPath = krakenDir.resolve("kraken2_output.tsv");
        Path krakenReportPath = krakenDir.resolve("kraken2_report.tsv");

        ExecuteResult krakenResult = doTaxonomyByKraken2(
                krakenDir,
                removedHostR1Path,
                removedHostR2Path,
                krakenOutputPath,
                krakenReportPath);

        if (!krakenResult.success()) {
            String errorMsg = "Kraken2 taxonomy classification failed";
            logger.error(errorMsg, krakenResult.ex);

            return runFail(
                    stageExecutionInput.stageContext,
                    errorMsg,
                    stageExecutionInput.workDir);
        }

        Path brackenSpeciesPath = stageExecutionInput.workDir.resolve("bracken_species.tsv");

        Path brackenSpeciesReportPath = stageExecutionInput.workDir.resolve("bracken_species_report.tsv");

        int readLen = -1;

        try {
            readLen = getMeanReadLength(removedHostR1Path);
        } catch (IOException e) {

            String errorMsg = "Failed to calculate read length for Bracken: "
                    + e.getMessage();

            logger.error(errorMsg, e);

            return runFail(
                    stageExecutionInput.stageContext,
                    errorMsg,
                    stageExecutionInput.workDir);
        }

        ExecuteResult brackenSpeciesResult = doBracken(
                stageExecutionInput.workDir,
                removedHostR1Path,
                krakenReportPath,
                brackenSpeciesPath,
                brackenSpeciesReportPath,
                readLen,
                "S");
        if (!brackenSpeciesResult.success()) {
            String errorMsg = "Bracken species abundance analysis failed";
            logger.error(errorMsg, brackenSpeciesResult.ex);

            return runFail(
                    stageExecutionInput.stageContext,
                    errorMsg,
                    stageExecutionInput.workDir);
        }

        Path alphaDiversityPath = stageExecutionInput.workDir.resolve("alpha_diversity.tsv");

        try {
            generateAlphaDiversity(
                    brackenSpeciesPath,
                    alphaDiversityPath);
        } catch (IOException e) {
            String errorMsg = "Failed to generate alpha diversity result";

            logger.error(errorMsg, e);

            return runFail(
                    stageExecutionInput.stageContext,
                    errorMsg,
                    stageExecutionInput.workDir);
        }


        

        return null;

    }

    @Override
    public int id() {

        return Constants.StageType.PIPELINE_STAGE_METAGENOMICS_SHORTGUN;
    }

}
