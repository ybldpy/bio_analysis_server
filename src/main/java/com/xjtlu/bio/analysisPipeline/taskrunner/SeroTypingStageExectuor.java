package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.TaxonomyClassification.STATUS_CONFIDENT;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_SEROTYPE;
import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.READ_LEN_TYPE_SHORT;
import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.SEQUENCE_LEVEL_READ;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.context.domain.TaxonomyContext;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.SeroTypeStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.SeroTypingStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.SequenceMeta;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.SeroTypingStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.SequenceFileUtil;

@Component
public class SeroTypingStageExectuor
        extends AbstractPipelineStageExector<SeroTypingStageOutput, SeroTypeStageInputUrls, SeroTypingStageParameters>
        implements PipelineStageExecutor<SeroTypingStageOutput> {

    private static final int TAX_ID_SALMONELLA = 28901;
    private static final int TAX_ID_ESCHERICHIA_COLI = 562;
    private static final int TAX_ID_KLEBSIELLA_PNEUMONIAE = 573;
    private static final int TAX_ID_STREPTOCOCCUS_PNEUMONIAE = 1313;

    public static final int INPUT_TYPE_READS = 1 << 0;
    public static final int INPUT_TYPE_CONTIGS = 1 << 1;

    public enum InputMode {
        READS, CONTIGS, UNSUPPORTED
    }

    @Value("${seroBADatabase}")
    private String seroBADatabase;

    @Value("${pFasterDir}")
    private String pFasterDir;

    public static final Set<Integer> SUPPORTED_SEROTYPE_TAXID = Set.of(
            TAX_ID_ESCHERICHIA_COLI,
            TAX_ID_KLEBSIELLA_PNEUMONIAE,
            TAX_ID_SALMONELLA,
            TAX_ID_STREPTOCOCCUS_PNEUMONIAE);

    public static boolean canDoSeroType(TaxonomyContext ctx) {
        return ctx != null && ctx.getStatus() == STATUS_CONFIDENT
                && ctx.getTaxid() != null && SUPPORTED_SEROTYPE_TAXID.contains(ctx.getTaxid());
    }

    public static int inputType(TaxonomyContext ctx) {
        if (!canDoSeroType(ctx)) {
            return 0;
        }
        return switch (ctx.getTaxid()) {
            case TAX_ID_SALMONELLA, TAX_ID_STREPTOCOCCUS_PNEUMONIAE -> INPUT_TYPE_READS | INPUT_TYPE_CONTIGS;
            // These are the assembly branches currently integrated in this executor.
            case TAX_ID_ESCHERICHIA_COLI, TAX_ID_KLEBSIELLA_PNEUMONIAE -> INPUT_TYPE_CONTIGS;
            default -> 0;
        };
    }

    /** Shared by scheduling and execution so both use the same input capability rules. */
    public static InputMode resolveInputMode(TaxonomyContext ctx, SeroTypeStageInputUrls input,
            SequenceMeta meta) {
        if (!canDoSeroType(ctx) || input == null) {
            return InputMode.UNSUPPORTED;
        }
        boolean shortReads = meta != null && meta.getSequenceLevel() == SEQUENCE_LEVEL_READ
                && meta.getReadLenType() == READ_LEN_TYPE_SHORT
                && StringUtils.isNotBlank(input.getR1Url())
                && !StringUtils.equals(input.getR1Url(), input.getContigsUrl());
        boolean paired = shortReads && StringUtils.isNotBlank(input.getR2Url())
                && !StringUtils.equals(input.getR2Url(), input.getContigsUrl())
                && !StringUtils.equals(input.getR1Url(), input.getR2Url());
        if (ctx.getTaxid() == TAX_ID_SALMONELLA && shortReads
                && (StringUtils.isBlank(input.getR2Url()) || paired)) {
            return InputMode.READS;
        }
        if (ctx.getTaxid() == TAX_ID_STREPTOCOCCUS_PNEUMONIAE && paired) {
            return InputMode.READS;
        }
        return StringUtils.isNotBlank(input.getContigsUrl()) ? InputMode.CONTIGS : InputMode.UNSUPPORTED;
    }

    @Override
    protected Class<SeroTypeStageInputUrls> stageInputType() {
        return SeroTypeStageInputUrls.class;
    }

    @Override
    protected Class<SeroTypingStageParameters> stageParameterType() {
        return SeroTypingStageParameters.class;
    }

    private StageRunResult<SeroTypingStageOutput> executeSalmonellaType(StageExecutionInput input,
            Path firstInput, Path r2Path, InputMode inputMode) {
        List<String> cmd = new ArrayList<>(analysisPipelineToolsConfig.getSeqsero2());
        cmd.add("-i");
        cmd.add(firstInput.toAbsolutePath().toString());
        if (inputMode == InputMode.READS && r2Path != null) {
            cmd.add(r2Path.toAbsolutePath().toString());
        }
        String mode = inputMode == InputMode.CONTIGS ? "4" : r2Path == null ? "3" : "2";
        if (inputMode == InputMode.CONTIGS) {
            cmd.add("-m");
            cmd.add("k");
        }
        cmd.add("-t");
        cmd.add(mode);
        cmd.add("-d");
        cmd.add(input.workDir.toAbsolutePath().toString());
        Path result = input.workDir.resolve("SeqSero_result.txt");
        return _execute(cmd, null, input, result)
                ? OK(new SeroTypingStageOutput(result), input)
                : runFail(input.stageContext, "SeqSero2 execution or output validation failed", input.workDir);
    }

    private StageRunResult<SeroTypingStageOutput> executeECoilType(StageExecutionInput input, Path contigs) {
        List<String> cmd = new ArrayList<>(analysisPipelineToolsConfig.getEctyper());
        cmd.add("-i");
        cmd.add(contigs.toAbsolutePath().toString());
        cmd.add("-o");
        cmd.add(input.workDir.toAbsolutePath().toString());
        Path result = input.workDir.resolve("output.tsv");
        return _execute(cmd, null, input, result)
                ? OK(new SeroTypingStageOutput(result), input)
                : runFail(input.stageContext, "ECTyper execution or output validation failed", input.workDir);
    }

    private StageRunResult<SeroTypingStageOutput> executeKlebsiellaType(StageExecutionInput input, Path contigs) {
        List<String> configuredCommand = analysisPipelineToolsConfig.getKaptive();
        if (configuredCommand == null || configuredCommand.isEmpty()) {
            return runFail(input.stageContext, "Kaptive executable command is not configured",
                    input.workDir);
        }
        if (configuredCommand.contains("-k") || configuredCommand.contains("assembly")
                || configuredCommand.contains("type")) {
            return runFail(input.stageContext,
                    "Kaptive command must contain only the executable prefix; configure kaptiveKDatabase "
                            + "and kaptiveODatabase separately instead of embedding a database in the command",
                    input.workDir);
        }

        try {
            // Validate both inputs before starting either process; never infer the other locus's database.
            Path kDatabase = kaptiveDatabase(analysisPipelineToolsConfig.getKaptiveKDatabase(), "K");
            Path oDatabase = kaptiveDatabase(analysisPipelineToolsConfig.getKaptiveODatabase(), "O");
            if (Files.isSameFile(kDatabase, oDatabase)) {
                return runFail(input.stageContext, "Kaptive K and O databases must be distinct files", input.workDir);
            }

            Path kWorkDir = Files.createDirectories(input.workDir.resolve("kaptive/K"));
            Path oWorkDir = Files.createDirectories(input.workDir.resolve("kaptive/O"));
            Path kResult = kWorkDir.resolve("kaptive_results.tsv");
            Path oResult = oWorkDir.resolve("kaptive_results.tsv");
            if (!executeKaptive(input, configuredCommand, kDatabase, contigs, kWorkDir, kResult)) {
                return runFail(input.stageContext, "Kaptive K execution or output validation failed", input.workDir);
            }
            if (!executeKaptive(input, configuredCommand, oDatabase, contigs, oWorkDir, oResult)) {
                return runFail(input.stageContext, "Kaptive O execution or output validation failed", input.workDir);
            }

            Path result = input.workDir.resolve("kaptive_results.tsv");
            combineKaptiveReports(kResult, oResult, result);
            return OK(new SeroTypingStageOutput(result, kResult, oResult), input);
        } catch (IOException | IllegalArgumentException e) {
            return runFail(input.stageContext, input.workDir, "Kaptive K/O analysis failed: " + e.getMessage(), e);
        }
    }

    private Path kaptiveDatabase(String configuredPath, String locus) {
        if (StringUtils.isBlank(configuredPath)) {
            throw new IllegalArgumentException("Kaptive " + locus + " database file is not configured");
        }
        Path database = Path.of(configuredPath).toAbsolutePath().normalize();
        if (!Files.isRegularFile(database) || !Files.isReadable(database)) {
            throw new IllegalArgumentException("Kaptive " + locus + " database must be a readable file, not a directory: "
                    + database);
        }
        return database;
    }

    private boolean executeKaptive(StageExecutionInput input, List<String> executable, Path database,
            Path contigs, Path workDir, Path result) {
        List<String> cmd = new ArrayList<>(executable);
        cmd.add("assembly");
        cmd.add(database.toString());
        cmd.add(contigs.toAbsolutePath().toString());
        cmd.add("-o");
        cmd.add(result.toAbsolutePath().toString());
        return _execute(cmd, null, workDir, input, result);
    }

    private void combineKaptiveReports(Path kResult, Path oResult, Path combined) throws IOException {
        List<String> kLines = Files.readAllLines(kResult);
        List<String> oLines = Files.readAllLines(oResult);
        if (kLines.size() < 2 || oLines.size() < 2 || kLines.get(0).isBlank()
                || !kLines.get(0).equals(oLines.get(0))
                || kLines.stream().skip(1).allMatch(String::isBlank)
                || oLines.stream().skip(1).allMatch(String::isBlank)) {
            throw new IOException("Kaptive K/O reports must contain result rows and matching TSV headers");
        }
        List<String> lines = new ArrayList<>();
        lines.add("locus_type\t" + kLines.get(0));
        kLines.stream().skip(1).filter(line -> !line.isBlank()).forEach(line -> lines.add("K\t" + line));
        oLines.stream().skip(1).filter(line -> !line.isBlank()).forEach(line -> lines.add("O\t" + line));
        Files.write(combined, lines);
    }

    private StageRunResult<SeroTypingStageOutput> executeStreptococcusType(StageExecutionInput input,
            Path r1, Path r2, Path contigs, InputMode inputMode) {
        List<String> cmd = new ArrayList<>();
        Path result;
        Path cwd = input.workDir;
        if (inputMode == InputMode.READS) {
            // SeroBA creates the output directory itself; the stage work directory already exists.
            Path output = input.workDir.resolve("seroba");
            result = output.resolve("pred.tsv");
            cmd.addAll(analysisPipelineToolsConfig.getSeroBA());
            cmd.add("runSerotyping");
            cmd.add(r1.toAbsolutePath().toString());
            cmd.add(r2.toAbsolutePath().toString());
            cmd.add(output.toAbsolutePath().toString());
            cmd.add("--databases");
            cmd.add(Path.of(seroBADatabase).toAbsolutePath().toString());
        } else {
            result = input.workDir.resolve("prediction.txt");
            cmd.addAll(analysisPipelineToolsConfig.getpFaster());
            cmd.add("-f");
            cmd.add(contigs.toAbsolutePath().toString());
            cmd.add("-o");
            cmd.add(input.workDir.toAbsolutePath().toString());
            cwd = Path.of(pFasterDir);
        }
        return _execute(cmd, null, cwd.toAbsolutePath(), input, result)
                ? OK(new SeroTypingStageOutput(result), input)
                : runFail(input.stageContext, "Pneumococcal serotyping execution or output validation failed", input.workDir);
    }

    private Path downloadInput(String objectName, Path target) throws IOException {
        Files.createDirectories(target.getParent());
        try {
            loadInput(Map.of(objectName, target));
        } catch (LoadFailException e) {
            throw new IOException("Unable to load serotyping input: " + objectName, e);
        }
        if (!Files.isRegularFile(target)) {
            throw new IOException("Serotyping input is not a regular file: " + objectName);
        }
        return target;
    }

    private Path downloadRead(String objectName, Path inputDir, String mate) throws IOException {
        // SeroBA requires both names to share the prefix before their final underscore.
        // Fixed mate names also prevent different storage keys with identical basenames colliding.
        String suffix = ".fastq" + compressionSuffix(objectName);
        Path downloaded = downloadInput(objectName,
                inputDir.resolve("reads").resolve("sample_" + mate + suffix));
        return SequenceFileUtil.unpackZipIfNeeded(downloaded, ".fastq");
    }

    private Path downloadContigs(String objectName, Path inputDir) throws IOException {
        // Some tools forward filenames through a shell internally. Do not expose uploaded basenames.
        Path target = inputDir.resolve("contigs").resolve("assembly.fasta" + compressionSuffix(objectName));
        downloadInput(objectName, target);
        return uncompressIfCompressedFormat(target);
    }

    private static String compressionSuffix(String objectName) {
        if (Constants.CompressedFormat.isGzip(objectName)) {
            return ".gz";
        }
        return objectName.toLowerCase(Locale.ROOT).endsWith(Constants.CompressedFormat.ZIP) ? ".zip" : "";
    }

    @Override
    protected StageRunResult<SeroTypingStageOutput> _execute(StageExecutionInput execution)
            throws JsonProcessingException, LoadFailException {
        SeroTypingStageParameters parameters = execution.stageParameters;
        SeroTypeStageInputUrls inputs = execution.input;
        TaxonomyContext taxonomy = parameters == null ? null : parameters.getTaxonomyContext();
        InputMode mode = resolveInputMode(taxonomy, inputs, parameters == null ? null : parameters.getReadMeta());
        if (mode == InputMode.UNSUPPORTED) {
            return runFail(execution.stageContext,
                    "No compatible serotyping input: provide contigs or supported short reads "
                            + "(paired reads are required for SeroBA), with a supported, confirmed taxonomy",
                    execution.workDir);
        }

        Path contigs = null;
        Path r1 = null;
        Path r2 = null;
        try {
            if (mode == InputMode.CONTIGS) {
                contigs = downloadContigs(inputs.getContigsUrl(), execution.inputDir);
            } else {
                r1 = downloadRead(inputs.getR1Url(), execution.inputDir, "R1");
                if (StringUtils.isNotBlank(inputs.getR2Url())) {
                    r2 = downloadRead(inputs.getR2Url(), execution.inputDir, "R2");
                }
            }
        } catch (IOException e) {
            return runFail(execution.stageContext, execution.workDir,
                    "Unable to prepare serotyping input: " + e.getMessage(), e);
        }

        return switch (taxonomy.getTaxid()) {
            case TAX_ID_SALMONELLA -> executeSalmonellaType(execution,
                    mode == InputMode.CONTIGS ? contigs : r1, r2, mode);
            case TAX_ID_ESCHERICHIA_COLI -> executeECoilType(execution, contigs);
            case TAX_ID_KLEBSIELLA_PNEUMONIAE -> executeKlebsiellaType(execution, contigs);
            case TAX_ID_STREPTOCOCCUS_PNEUMONIAE -> executeStreptococcusType(execution, r1, r2, contigs, mode);
            default -> runFail(execution.stageContext, "No supported serotyping tool", execution.workDir);
        };
    }

    @Override
    public int id() {
        return PIPELINE_STAGE_SEROTYPE;
    }
}
