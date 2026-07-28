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
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.MetagenomicsAnalysisStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.MetagenomicsShotgunAnalysisStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.SequenceFileUtil;



@Component
public class MetagenomicShotgunStageExecutor extends
        AbstractPipelineStageExector<MetagenomicsShotgunAnalysisStageOutput, MetagenomicsAnalysisStageInputUrls, BaseStageParams>
        implements PipelineStageExecutor<MetagenomicsShotgunAnalysisStageOutput> {

    @Value("${humanBowtie2IndexDir}")
    private String humanBowtie2IndexDir;

    @Value("${kraken2Standard8Dir}")
    private String kraken2DBDir;

    @Value("${eggnogDBDir}")
    private String eggnogDBDir;

    @Value("${checkM2DB}")
    private String checkM2DBPath;

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
                "-x", Path.of(this.humanBowtie2IndexDir, "grch38").toString()));

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

    private ExecuteResult doMegahit(
            Path workDir,
            Path r1Path,
            Path r2Path,
            Path outputDir,
            int threads) {

        List<String> cmd = new ArrayList<>();
        cmd.addAll(this.analysisPipelineToolsConfig.getMegahit());

        if (r2Path != null) {
            cmd.addAll(List.of(
                    "-1", r1Path.toString(),
                    "-2", r2Path.toString()));
        } else {
            cmd.addAll(List.of(
                    "-r", r1Path.toString()));
        }

        cmd.addAll(List.of(
                "-o", outputDir.toString(),
                "-t", String.valueOf(threads)));

        return _execute(cmd, workDir);
    }

    private ExecuteResult doAssemblySummary(
            Path workDir,
            Path contigsPath,
            Path summaryPath) {

        List<String> cmd = new ArrayList<>();
        cmd.addAll(this.analysisPipelineToolsConfig.getSeqkit());

        cmd.addAll(List.of(
                "stats",
                "--all",
                "--tabular",
                "-o", summaryPath.toString(),
                contigsPath.toString()));

        return _execute(cmd, workDir);
    }

    private ExecuteResult doProdigal(
            Path workDir,
            Path contigsPath,
            Path predictedGenesPath,
            Path predictedProteinsPath,
            Path predictedGenesGffPath) {

        List<String> cmd = new ArrayList<>();
        cmd.addAll(this.analysisPipelineToolsConfig.getProdigal());

        cmd.addAll(List.of(
                "-i", contigsPath.toString(),
                "-p", "meta",
                "-d", predictedGenesPath.toString(),
                "-a", predictedProteinsPath.toString(),
                "-o", predictedGenesGffPath.toString(),
                "-f", "gff"));

        return _execute(cmd, workDir);
    }

    private ExecuteResult doEggnogMapper(
            Path workDir,
            Path predictedProteinsPath,
            Path outputDir,
            String outputPrefix,
            int threads) {

        List<String> cmd = new ArrayList<>();
        cmd.addAll(this.analysisPipelineToolsConfig.getEggnogMapper());

        cmd.addAll(List.of(
                "-m", "diamond",
                "-i", predictedProteinsPath.toString(),
                "--itype", "proteins",
                "--data_dir", this.eggnogDBDir,
                "-o", outputPrefix,
                "--output_dir", outputDir.toString(),
                "--cpu", String.valueOf(threads)));

        return _execute(cmd, workDir);
    }

    private ExecuteResult doBinning(
            Path workDir,
            Path contigsPath,
            Path r1Path,
            Path r2Path,
            Path binsDir,
            int threads) {

        Path contigsIndexPrefix = workDir.resolve("contigs_index");
        Path alignmentSamPath = workDir.resolve("reads_to_contigs.sam");
        Path sortedBamPath = workDir.resolve("reads_to_contigs.sorted.bam");
        Path contigDepthPath = workDir.resolve("contig_depth.txt");
        Path binPrefix = binsDir.resolve("bin");

        /*
         * 1. 为 MEGAHIT contigs 构建 Bowtie2 索引
         */
        List<String> buildIndexCmd = new ArrayList<>();
        buildIndexCmd.addAll(
                this.analysisPipelineToolsConfig.getBowtie2Build());

        buildIndexCmd.addAll(List.of(
                contigsPath.toString(),
                contigsIndexPrefix.toString()));

        ExecuteResult buildIndexResult = _execute(
                buildIndexCmd,
                workDir);

        if (!buildIndexResult.success()) {
            return buildIndexResult;
        }

        /*
         * 2. 将去宿主后的 reads 回贴到 contigs
         */
        List<String> mappingCmd = new ArrayList<>();
        mappingCmd.addAll(
                this.analysisPipelineToolsConfig.getBowtie2());

        mappingCmd.addAll(List.of(
                "--very-sensitive",
                "-x", contigsIndexPrefix.toString(),
                "-p", String.valueOf(threads)));

        if (r2Path != null) {
            mappingCmd.addAll(List.of(
                    "-1", r1Path.toString(),
                    "-2", r2Path.toString()));
        } else {
            mappingCmd.addAll(List.of(
                    "-U", r1Path.toString()));
        }

        mappingCmd.addAll(List.of(
                "-S", alignmentSamPath.toString()));

        ExecuteResult mappingResult = _execute(
                mappingCmd,
                workDir);

        if (!mappingResult.success()) {
            return mappingResult;
        }

        /*
         * 3. 将 SAM 转成按照坐标排序的 BAM
         */
        List<String> sortCmd = new ArrayList<>();
        sortCmd.addAll(
                this.analysisPipelineToolsConfig.getSamtools());

        sortCmd.addAll(List.of(
                "sort",
                "-@", String.valueOf(threads),
                "-o", sortedBamPath.toString(),
                alignmentSamPath.toString()));

        ExecuteResult sortResult = _execute(
                sortCmd,
                workDir);

        if (!sortResult.success()) {
            return sortResult;
        }

        /*
         * 4. 根据 BAM 计算每条 contig 的覆盖深度
         */
        List<String> depthCmd = new ArrayList<>();
        depthCmd.addAll(
                this.analysisPipelineToolsConfig
                        .getJgiSummarizeBamContigDepths());

        depthCmd.addAll(List.of(
                "--outputDepth", contigDepthPath.toString(),
                sortedBamPath.toString()));

        ExecuteResult depthResult = _execute(
                depthCmd,
                workDir);

        if (!depthResult.success()) {
            return depthResult;
        }

        /*
         * 5. MetaBAT2 分箱
         */
        List<String> metabatCmd = new ArrayList<>();
        metabatCmd.addAll(
                this.analysisPipelineToolsConfig.getMetabat2());

        metabatCmd.addAll(List.of(
                "-i", contigsPath.toString(),
                "-a", contigDepthPath.toString(),
                "-o", binPrefix.toString(),
                "-t", String.valueOf(threads)));

        return _execute(
                metabatCmd,
                workDir);
    }

    private ExecuteResult doCheckM2(
            Path workDir,
            Path binsDir,
            Path outputDir,
            int threads) {

        List<String> cmd = new ArrayList<>();
        cmd.addAll(this.analysisPipelineToolsConfig.getCheckm2());

        cmd.addAll(List.of(
                "predict",
                "--input", binsDir.toString(),
                "--output-directory", outputDir.toString(),
                "--threads", String.valueOf(threads),
                "--database_path", this.checkM2DBPath,
                "-x", "fa"));

        // Map<String, String> runningEnv = Map.of("CUDA_VISIBLE_DEVICES", "-1");


        return _execute(cmd, workDir);
        // ProcessBuilder pb = new ProcessBuilder(cmd);
        // pb.directory(workDir.toFile());

        // // 不需要日志：直接丢弃 stdout/stderr
        // pb.redirectErrorStream(true);
        // pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);

        // try {
        //     Process process = pb.start();
        //     int code = process.waitFor();
        //     return new ExecuteResult(code, null);
        // } catch (InterruptedException e) {
        //     Thread.currentThread().interrupt();
        //     return new ExecuteResult(-1, e);
        // } catch (IOException e) {
        //     return new ExecuteResult(-1, e);
        // }



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
            Files.createDirectory(hostRemovalDir);
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

        Path megahitOutputDir = stageExecutionInput.workDir.resolve("megahit");

        ExecuteResult megahitResult = doMegahit(
                stageExecutionInput.workDir,
                removedHostR1Path,
                removedHostR2Path,
                megahitOutputDir,
                2);

        if (!megahitResult.success()) {
            String errorMsg = "MEGAHIT metagenome assembly failed";

            logger.error(errorMsg, megahitResult.ex);

            return runFail(
                    stageExecutionInput.stageContext,
                    errorMsg,
                    stageExecutionInput.workDir);
        }

        Path contigsPath = megahitOutputDir.resolve("final.contigs.fa");

        Path assemblySummaryPath = stageExecutionInput.workDir.resolve("assembly_summary.tsv");

        ExecuteResult assemblySummaryResult = doAssemblySummary(
                stageExecutionInput.workDir,
                contigsPath,
                assemblySummaryPath);

        if (!assemblySummaryResult.success()) {
            String errorMsg = "Metagenome assembly summary failed";
            logger.error(errorMsg, assemblySummaryResult.ex);

            return runFail(
                    stageExecutionInput.stageContext,
                    errorMsg,
                    stageExecutionInput.workDir);
        }

        Path prodigalDir = stageExecutionInput.workDir.resolve("prodigal");
        try {
            Files.createDirectories(prodigalDir);
        } catch (IOException e) {
        }

        Path predictedGenesPath = prodigalDir.resolve("predicted_genes.fna");

        Path predictedProteinsPath = prodigalDir.resolve("predicted_proteins.faa");

        Path predictedGenesGffPath = prodigalDir.resolve("predicted_genes.gff");

        ExecuteResult prodigalResult = doProdigal(
                stageExecutionInput.workDir,
                contigsPath,
                predictedGenesPath,
                predictedProteinsPath,
                predictedGenesGffPath);

        if (!prodigalResult.success()) {
            String errorMsg = "Prodigal metagenome gene prediction failed";

            logger.error(errorMsg, prodigalResult.ex);

            return runFail(
                    stageExecutionInput.stageContext,
                    errorMsg,
                    stageExecutionInput.workDir);
        }

        Path eggnogOutputDir = stageExecutionInput.workDir.resolve("../eggnog");

        try {
            Files.createDirectories(eggnogOutputDir);
        } catch (IOException e) {
            String errorMsg = "Failed to create Prodigal output directory: "
                    + prodigalDir;

            logErr(errorMsg, e);

            return runFail(
                    stageExecutionInput.stageContext,
                    errorMsg,
                    stageExecutionInput.workDir);
        }

        String eggnogOutputPrefix = "functional_annotation";

        if(false){ExecuteResult eggnogResult = doEggnogMapper(
                stageExecutionInput.workDir,
                predictedProteinsPath,
                eggnogOutputDir,
                eggnogOutputPrefix,
                10);

        if (!eggnogResult.success()) {
            String errorMsg = "eggNOG functional annotation failed";

            logger.error(errorMsg, eggnogResult.ex);

            return runFail(
                    stageExecutionInput.stageContext,
                    errorMsg,
                    stageExecutionInput.workDir);
        }}

        Path functionalAnnotationPath = eggnogOutputDir.resolve(
                eggnogOutputPrefix + ".emapper.annotations");

        Path binningDir = stageExecutionInput.workDir.resolve("binning");

        Path binsDir = binningDir.resolve("bins");

        try {
            Files.createDirectories(binningDir);
            Files.createDirectories(binsDir);
        } catch (IOException e) {

            String errorMsg = "Failed to create functional annotation output directories";

            logger.error(errorMsg, e);

            return runFail(
                    stageExecutionInput.stageContext,
                    errorMsg,
                    stageExecutionInput.workDir);

        }

        ExecuteResult binningResult = doBinning(
                binningDir,
                contigsPath,
                removedHostR1Path,
                removedHostR2Path,
                binsDir,
                2);

        if (!binningResult.success()) {
            String errorMsg = "Metagenome binning failed";

            logger.error(errorMsg, binningResult.ex);

            return runFail(
                    stageExecutionInput.stageContext,
                    errorMsg,
                    stageExecutionInput.workDir);
        }

        Path checkM2OutputDir = stageExecutionInput.workDir.resolve("checkm2");

        try {
            Files.createDirectories(checkM2OutputDir);
        } catch (IOException e) {
            String errorMsg = "Failed to create CheckM2 output directory: "
                    + checkM2OutputDir;

            logger.error(errorMsg, e);

            return runFail(
                    stageExecutionInput.stageContext,
                    errorMsg,
                    stageExecutionInput.workDir);
        }

        ExecuteResult checkM2Result = doCheckM2(
                stageExecutionInput.workDir,
                binsDir,
                checkM2OutputDir,
                2);

        if (!checkM2Result.success()) {
            Exception ex = checkM2Result.ex;

            String errorMsg;
            if (ex != null) {
                String message = ex.getMessage();

                if (StringUtils.isBlank(message)) {
                    message = ex.getClass().getSimpleName();
                }

                errorMsg = "CheckM2 bin quality assessment failed: " + message;
                logger.error(errorMsg, ex);
            } else {
                errorMsg = "CheckM2 bin quality assessment failed, exit code: "
                        + checkM2Result.runCode;

                logger.error(errorMsg);
            }

            return runFail(
                    stageExecutionInput.stageContext,
                    errorMsg,
                    stageExecutionInput.workDir);
        }

        MetagenomicsShotgunAnalysisStageOutput metagenomicsShotgunAnalysisStageOutput = 
                new MetagenomicsShotgunAnalysisStageOutput(
                    krakenReportPath,
                    brackenSpeciesPath, 
                    alphaDiversityPath, 
                    contigsPath,
                    assemblySummaryPath,
                    predictedGenesPath,
                    predictedProteinsPath,
                    functionalAnnotationPath,
                    null,
                    binsDir,
                    checkM2OutputDir.resolve("quality_report.tsv")
                );

        return OK(metagenomicsShotgunAnalysisStageOutput, stageExecutionInput);

    }

    @Override
    public int id() {

        return Constants.StageType.PIPELINE_STAGE_METAGENOMICS_SHORTGUN;
    }

}
