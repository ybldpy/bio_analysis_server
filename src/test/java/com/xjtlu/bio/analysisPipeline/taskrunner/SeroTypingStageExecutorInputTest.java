package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.READ_LEN_TYPE_LONG;
import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.READ_LEN_TYPE_SHORT;
import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.SEQUENCE_LEVEL_ASSEMBLY;
import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.SEQUENCE_LEVEL_READ;
import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.SEQUENCE_LEVEL_REFERENCE;
import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.SEQUENCE_LEVEL_UNKNOWN;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_QUEUING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_SEROTYPE;
import static com.xjtlu.bio.analysisPipeline.Constants.TaxonomyClassification.STATUS_AMBIGUOUS;
import static com.xjtlu.bio.analysisPipeline.Constants.TaxonomyClassification.STATUS_CONFIDENT;
import static com.xjtlu.bio.analysisPipeline.taskrunner.SeroTypingStageExectuor.InputMode.CONTIGS;
import static com.xjtlu.bio.analysisPipeline.taskrunner.SeroTypingStageExectuor.InputMode.READS;
import static com.xjtlu.bio.analysisPipeline.taskrunner.SeroTypingStageExectuor.InputMode.UNSUPPORTED;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.test.util.ReflectionTestUtils;

import com.xjtlu.bio.analysisPipeline.BioStageUtil;
import com.xjtlu.bio.analysisPipeline.context.domain.TaxonomyContext;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.SeroTypeStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.SeroTypingStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.SequenceMeta;
import com.xjtlu.bio.analysisPipeline.stageResult.TaxonomyResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.SeroTypingStageExectuor.InputMode;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.SeroTypingStageOutput;
import com.xjtlu.bio.configuration.AnalysisPipelineToolsConfig;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.LocalStorageService;
import com.xjtlu.bio.utils.JsonUtil;

/** Real input staging and executor lifecycle; every external process is replaced by a fixture. */
class SeroTypingStageExecutorInputTest {

    private static final int SALMONELLA = 28901;
    private static final int ECOLI = 562;
    private static final int KLEBSIELLA = 573;
    private static final int PNEUMOCOCCUS = 1313;
    private static final String CONTIG_KEY = "assemblies/contigs.fasta";
    private static final String READ1_KEY = "reads/first.fastq";
    private static final String READ2_KEY = "reads/second.fastq";
    private static final String ASSEMBLY_TEXT = ">fixture_contig\nACGTACGT\n";
    private static final String FIRST_READ = "@fixture/1\nACGT\n+\nIIII\n";
    private static final String SECOND_READ = "@fixture/2\nTGCA\n+\nIIII\n";
    private static final String KAPTIVE_HEADER = "Assembly\tBest match locus\tBest match type\tMatch confidence\tProblems\n";
    private static final String KAPTIVE_K_ROW = "fixture\tK_fixture_locus\tK_fixture_type\tGood\t\n";
    private static final String KAPTIVE_O_ROW = "fixture\tO_fixture_locus\tO_fixture_type\tGood\tmarker with spaces\n";

    @TempDir
    Path tempDir;

    private Path storageRoot;
    private Path inputDir;
    private Path workDir;
    private Path pfasterDir;
    private Path kaptiveKDatabase;
    private Path kaptiveODatabase;
    private RecordingStorage storage;
    private AnalysisPipelineToolsConfig tools;
    private StubExecutor executor;

    @BeforeEach
    void setUp() throws IOException {
        storageRoot = Files.createDirectories(tempDir.resolve("storage"));
        workDir = tempDir.resolve("work with spaces/42");
        inputDir = tempDir.resolve("input with spaces/42");
        pfasterDir = Files.createDirectories(tempDir.resolve("pfaster checkout"));
        kaptiveKDatabase = Files.writeString(tempDir.resolve("explicit k database.gbk"), "isolated K fixture database\n");
        kaptiveODatabase = Files.writeString(tempDir.resolve("explicit o database.gbk"), "isolated O fixture database\n");
        writeObject(CONTIG_KEY, ASSEMBLY_TEXT);
        writeObject(READ1_KEY, FIRST_READ);
        writeObject(READ2_KEY, SECOND_READ);

        storage = new RecordingStorage();
        ReflectionTestUtils.setField(storage, "base", storageRoot.toString());
        BioStageUtil directories = new BioStageUtil();
        ReflectionTestUtils.setField(directories, "stageWorkDirPath", workDir.getParent().toString());
        ReflectionTestUtils.setField(directories, "stageInputDirPath", inputDir.getParent().toString());
        tools = new AnalysisPipelineToolsConfig();
        tools.setSeqsero2(List.of("mock-runtime", "mock-seqsero2"));
        tools.setEctyper(List.of("mock-runtime", "mock-ectyper"));
        tools.setKaptive(List.of("mock-runtime", "mock-kaptive"));
        tools.setKaptiveKDatabase(kaptiveKDatabase.toString());
        tools.setKaptiveODatabase(kaptiveODatabase.toString());
        tools.setSeroBA(List.of("mock-runtime", "mock-seroba"));
        tools.setpFaster(List.of("mock-runtime", "mock-pfaster"));

        executor = new StubExecutor();
        executor.inputRoot = inputDir;
        ReflectionTestUtils.setField(executor, "storageService", storage);
        ReflectionTestUtils.setField(executor, "bioStageUtil", directories);
        ReflectionTestUtils.setField(executor, "analysisPipelineToolsConfig", tools);
        ReflectionTestUtils.setField(executor, "seroBADatabase", tempDir.resolve("seroba database").toString());
        ReflectionTestUtils.setField(executor, "pFasterDir", pfasterDir.toString());
    }

    @Test
    void salmonellaPairedReadsAreBothDownloadedEvenWhenContigsAreAlsoAvailable() throws Exception {
        StageRunResult<SeroTypingStageOutput> result = run(SALMONELLA,
                input(CONTIG_KEY, READ1_KEY, READ2_KEY), shortReads());

        assertSuccessful(result, workDir.resolve("SeqSero_result.txt"));
        List<String> command = onlyCommand("mock-seqsero2");
        assertEquals("2", option(command, "-t"));
        assertEquals(readPath(1, false).toString(), option(command, "-i"));
        assertTrue(command.contains(readPath(2, false).toString()));
        assertEquals(workDir.toString(), option(command, "-d"));
        assertLoadedReads(false);
        assertEquals(2, storage.downloaded.size());
        assertTrue(storage.downloaded.containsAll(List.of(READ1_KEY, READ2_KEY)));
        assertFalse(storage.downloaded.contains(CONTIG_KEY));
        assertEquals(FIRST_READ, Files.readString(storageRoot.resolve(READ1_KEY)));
        assertFalse(Files.exists(inputDir));
    }

    @Test
    void explicitReadModeDoesNotGuessAssemblyModeFromTheSourceFilename() throws Exception {
        String misleadingName = writeObject("reads/first.fasta", FIRST_READ);

        StageRunResult<SeroTypingStageOutput> result = run(SALMONELLA,
                input(CONTIG_KEY, misleadingName, null), shortReads());

        assertSuccessful(result, workDir.resolve("SeqSero_result.txt"));
        List<String> command = onlyCommand("mock-seqsero2");
        assertEquals("3", option(command, "-t"));
        assertEquals(readPath(1, false).toString(), option(command, "-i"));
        assertFalse(command.contains("4"));
        assertEquals(List.of(misleadingName), storage.downloaded);
        assertArrayEquals(FIRST_READ.getBytes(StandardCharsets.UTF_8), executor.stagedFiles.get(readPath(1, false)));
    }

    @ParameterizedTest
    @ValueSource(strings = { ".gz", ".gzip" })
    void compressedReadCopiesKeepARecognizedSuffixAndACommonPairPrefix(String compressedSuffix) throws Exception {
        String first = writeObject("left/same.fastq" + compressedSuffix, FIRST_READ);
        String second = writeObject("right/same.fastq" + compressedSuffix, SECOND_READ);

        StageRunResult<SeroTypingStageOutput> result = run(SALMONELLA,
                input(CONTIG_KEY, first, second), shortReads());

        assertSuccessful(result, workDir.resolve("SeqSero_result.txt"));
        List<String> command = onlyCommand("mock-seqsero2");
        assertEquals(readPath(1, true).toString(), option(command, "-i"));
        assertTrue(command.contains(readPath(2, true).toString()));
        assertArrayEquals(Files.readAllBytes(storageRoot.resolve(first)), executor.stagedFiles.get(readPath(1, true)));
        assertArrayEquals(Files.readAllBytes(storageRoot.resolve(second)), executor.stagedFiles.get(readPath(2, true)));
    }

    @ParameterizedTest
    @ValueSource(ints = { SALMONELLA, PNEUMOCOCCUS })
    void separateSingleFileReadArchivesBecomePlainFixedMateFiles(int species) throws Exception {
        String first = writeObject("left/same.fastq.zip", FIRST_READ);
        String second = writeObject("right/same.fastq.zip", SECOND_READ);
        byte[] originalFirst = Files.readAllBytes(storageRoot.resolve(first));
        byte[] originalSecond = Files.readAllBytes(storageRoot.resolve(second));

        StageRunResult<SeroTypingStageOutput> result = run(species,
                input(CONTIG_KEY, first, second), shortReads());

        assertTrue(result.isSuccess(), result.getErrorLog());
        List<String> command = onlyCommand(species == SALMONELLA ? "mock-seqsero2" : "mock-seroba");
        assertTrue(command.contains(readPath(1, false).toString()));
        assertTrue(command.contains(readPath(2, false).toString()));
        assertFalse(command.stream().anyMatch(argument -> argument.endsWith(".zip")));
        assertLoadedReads(false);
        assertArrayEquals(originalFirst, executor.stagedFiles.get(inputDir.resolve("reads/sample_R1.fastq.zip")));
        assertArrayEquals(originalSecond, executor.stagedFiles.get(inputDir.resolve("reads/sample_R2.fastq.zip")));
        assertArrayEquals(originalFirst, Files.readAllBytes(storageRoot.resolve(first)));
        assertArrayEquals(originalSecond, Files.readAllBytes(storageRoot.resolve(second)));
        assertEquals(List.of(first, second), storage.downloaded);
        assertFalse(Files.exists(inputDir));
    }

    @Test
    void singleReadArchiveWithUppercaseZipSuffixUsesSingleReadMode() throws Exception {
        String source = writeObject("reads/archive with spaces.ZIP", FIRST_READ);

        StageRunResult<SeroTypingStageOutput> result = run(SALMONELLA,
                input(null, source, null), shortReads());

        assertSuccessful(result, workDir.resolve("SeqSero_result.txt"));
        List<String> command = onlyCommand("mock-seqsero2");
        assertEquals("3", option(command, "-t"));
        assertEquals(readPath(1, false).toString(), option(command, "-i"));
        assertArrayEquals(FIRST_READ.getBytes(StandardCharsets.UTF_8), executor.stagedFiles.get(readPath(1, false)));
        assertEquals(List.of(source), storage.downloaded);
    }

    @ParameterizedTest
    @ValueSource(strings = { "long", "assembly", "unknown", "read-aliases-contigs" })
    void salmonellaUsesExplicitAssemblyModeWhenReadsAreNotCompatible(String reason) throws Exception {
        SequenceMeta meta = metadata(reason);
        String first = reason.equals("read-aliases-contigs") ? CONTIG_KEY : READ1_KEY;

        StageRunResult<SeroTypingStageOutput> result = run(SALMONELLA,
                input(CONTIG_KEY, first, READ2_KEY), meta);

        assertSuccessful(result, workDir.resolve("SeqSero_result.txt"));
        List<String> command = onlyCommand("mock-seqsero2");
        assertEquals("4", option(command, "-t"));
        assertEquals("k", option(command, "-m"));
        assertEquals(plainContigPath().toString(), option(command, "-i"));
        assertContigsOnlyDownloaded();
    }

    @ParameterizedTest
    @MethodSource("nonReadSequenceLevels")
    void unknownAndReferenceSequenceLevelsNeverCertifyReadsAsShortReadInput(int sequenceLevel,
            boolean contigsAvailable) throws Exception {
        SequenceMeta meta = shortReads();
        meta.setSequenceLevel(sequenceLevel);
        SeroTypeStageInputUrls inputs = input(contigsAvailable ? CONTIG_KEY : null, READ1_KEY, READ2_KEY);
        assertEquals(contigsAvailable ? CONTIGS : UNSUPPORTED,
                SeroTypingStageExectuor.resolveInputMode(taxonomy(SALMONELLA, STATUS_CONFIDENT), inputs, meta));

        StageRunResult<SeroTypingStageOutput> result = run(SALMONELLA, inputs, meta);

        if (contigsAvailable) {
            assertSuccessful(result, workDir.resolve("SeqSero_result.txt"));
            List<String> command = onlyCommand("mock-seqsero2");
            assertEquals("4", option(command, "-t"));
            assertContigsOnlyDownloaded();
        } else {
            assertRejectedBeforeTool(result);
            assertTrue(storage.downloaded.isEmpty());
        }
    }

    @Test
    void pneumococcalPairedShortReadsUseSerobaWithAnAbsentChildOutputPrefix() throws Exception {
        StageRunResult<SeroTypingStageOutput> result = run(PNEUMOCOCCUS,
                input(CONTIG_KEY, READ1_KEY, READ2_KEY), shortReads());

        assertSuccessful(result, workDir.resolve("seroba/pred.tsv"));
        List<String> command = onlyCommand("mock-seroba");
        int operation = command.indexOf("runSerotyping");
        assertTrue(operation >= 0);
        assertEquals(readPath(1, false).toString(), command.get(operation + 1));
        assertEquals(readPath(2, false).toString(), command.get(operation + 2));
        assertEquals(workDir.resolve("seroba").toString(), command.get(operation + 3));
        assertEquals(tempDir.resolve("seroba database").toString(), option(command, "--databases"));
        assertFalse(executor.serobaPrefixAlreadyExisted,
                "SeroBA creates its output prefix and rejects a precreated directory");
        assertLoadedReads(false);
        assertFalse(storage.downloaded.contains(CONTIG_KEY));
    }

    @ParameterizedTest
    @ValueSource(strings = { "assembly", "single", "long", "unknown" })
    void pneumococcalNonPairedShortReadLayoutsUseTheAvailableAssembly(String reason) throws Exception {
        SeroTypeStageInputUrls inputs = reason.equals("assembly")
                ? input(CONTIG_KEY, null, null)
                : input(CONTIG_KEY, READ1_KEY, reason.equals("single") ? null : READ2_KEY);

        StageRunResult<SeroTypingStageOutput> result = run(PNEUMOCOCCUS, inputs, metadata(reason));

        assertSuccessful(result, workDir.resolve("prediction.txt"));
        List<String> command = onlyCommand("mock-pfaster");
        assertEquals(plainContigPath().toString(), option(command, "-f"));
        assertEquals(workDir.toString(), option(command, "-o"));
        assertEquals(pfasterDir, executor.processWorkDir);
        assertContigsOnlyDownloaded();
    }

    @ParameterizedTest
    @ValueSource(ints = { ECOLI, KLEBSIELLA })
    void assemblyOnlyToolsDoNotDownloadUnneededReadInputs(int species) throws Exception {
        StageRunResult<SeroTypingStageOutput> result = run(species,
                input(CONTIG_KEY, READ1_KEY, READ2_KEY), shortReads());

        assertSuccessful(result, workDir.resolve(species == ECOLI ? "output.tsv" : "kaptive_results.tsv"));
        if (species == ECOLI) {
            List<String> command = onlyCommand("mock-ectyper");
            assertEquals(plainContigPath().toString(), option(command, "-i"));
            assertEquals(workDir.toString(), option(command, "-o"));
        } else {
            assertEquals(2, executor.commands.size());
            assertKaptiveInvocation(0, "K", kaptiveKDatabase);
            assertKaptiveInvocation(1, "O", kaptiveODatabase);
        }
        assertContigsOnlyDownloaded();
    }

    @Test
    void kaptiveRunsBothExplicitDatabasesInIsolatedDirectoriesAndPreservesLabeledRawCells() throws Exception {
        StageRunResult<SeroTypingStageOutput> result = run(KLEBSIELLA,
                input(CONTIG_KEY, READ1_KEY, READ2_KEY), shortReads());

        assertSuccessful(result, workDir.resolve("kaptive_results.tsv"));
        assertEquals(2, executor.commands.size());
        assertKaptiveInvocation(0, "K", kaptiveKDatabase);
        assertKaptiveInvocation(1, "O", kaptiveODatabase);
        Path kReport = workDir.resolve("kaptive/K/kaptive_results.tsv");
        Path oReport = workDir.resolve("kaptive/O/kaptive_results.tsv");
        assertEquals(kReport, result.getStageOutput().getKaptiveKResultPath());
        assertEquals(oReport, result.getStageOutput().getKaptiveOResultPath());
        assertEquals(KAPTIVE_HEADER + KAPTIVE_K_ROW, Files.readString(kReport));
        assertEquals(KAPTIVE_HEADER + KAPTIVE_O_ROW, Files.readString(oReport));
        assertEquals("locus_type\t" + KAPTIVE_HEADER + "K\t" + KAPTIVE_K_ROW + "O\t" + KAPTIVE_O_ROW,
                Files.readString(result.getStageOutput().getResultPath()));
        assertContigsOnlyDownloaded();
    }

    @ParameterizedTest
    @ValueSource(ints = { SALMONELLA, PNEUMOCOCCUS })
    void compressedAssemblyIsMaterializedAsCanonicalPlainFastaWithoutChangingTheSource(int species) throws Exception {
        String compressed = writeObject("assemblies/original.fasta.gz", ASSEMBLY_TEXT);
        byte[] original = Files.readAllBytes(storageRoot.resolve(compressed));

        StageRunResult<SeroTypingStageOutput> result = run(species,
                input(compressed, compressed, null), metadata("assembly"));

        assertTrue(result.isSuccess(), result.getErrorLog());
        List<String> command = onlyCommand(species == SALMONELLA ? "mock-seqsero2" : "mock-pfaster");
        Path canonical = inputDir.resolve("contigs/assembly.fasta");
        assertEquals(canonical.toString(), option(command, species == SALMONELLA ? "-i" : "-f"));
        assertArrayEquals(ASSEMBLY_TEXT.getBytes(StandardCharsets.UTF_8), executor.stagedFiles.get(canonical));
        assertArrayEquals(original, Files.readAllBytes(storageRoot.resolve(compressed)));
        assertEquals(List.of(compressed), storage.downloaded);
    }

    @ParameterizedTest
    @MethodSource("assemblyCompressionCases")
    void allAssemblyBranchesReceivePlainFastaFromSingleFileZipOrGzip(int species, String suffix) throws Exception {
        String source = writeObject("assemblies/source with spaces.fasta" + suffix, ASSEMBLY_TEXT);
        byte[] original = Files.readAllBytes(storageRoot.resolve(source));

        StageRunResult<SeroTypingStageOutput> result = run(species,
                input(source, source, null), metadata("assembly"));

        assertTrue(result.isSuccess(), result.getErrorLog());
        assertEquals(species == KLEBSIELLA ? 2 : 1, executor.commands.size());
        for (List<String> command : executor.commands) {
            assertTrue(command.contains(plainContigPath().toString()));
            assertFalse(command.stream().anyMatch(argument -> argument.endsWith(".zip")
                    || argument.endsWith(".gz") || argument.endsWith(".gzip")));
        }
        assertArrayEquals(ASSEMBLY_TEXT.getBytes(StandardCharsets.UTF_8), executor.stagedFiles.get(plainContigPath()));
        assertArrayEquals(original, Files.readAllBytes(storageRoot.resolve(source)));
        assertEquals(List.of(source), storage.downloaded);
        assertFalse(Files.exists(inputDir));
    }

    @Test
    void plainAssemblyWithSpacesInItsStorageBasenameGetsTheFixedSafeLocalFilename() throws Exception {
        String source = writeObject("assemblies/assembly input with spaces.fasta", ASSEMBLY_TEXT);

        StageRunResult<SeroTypingStageOutput> result = run(SALMONELLA,
                input(source, null, null), metadata("assembly"));

        assertSuccessful(result, workDir.resolve("SeqSero_result.txt"));
        assertEquals(inputDir.resolve("contigs/assembly.fasta").toString(),
                option(onlyCommand("mock-seqsero2"), "-i"));
        assertEquals(List.of(source), storage.downloaded);
        assertEquals(1, executor.stagedFiles.size());
        assertArrayEquals(ASSEMBLY_TEXT.getBytes(StandardCharsets.UTF_8), executor.stagedFiles.get(plainContigPath()));
    }

    @Test
    void identicalReadBasenamesCannotOverwriteEachOtherDuringStaging() throws Exception {
        String first = writeObject("source-one/same.fastq", FIRST_READ);
        String second = writeObject("source-two/same.fastq", SECOND_READ);
        String contigs = writeObject("source-three/same.fastq", ASSEMBLY_TEXT);

        StageRunResult<SeroTypingStageOutput> result = run(SALMONELLA,
                input(contigs, first, second), shortReads());

        assertTrue(result.isSuccess(), result.getErrorLog());
        assertLoadedReads(false);
        assertEquals(2, executor.stagedFiles.size());
        assertFalse(storage.downloaded.contains(contigs));
    }

    @ParameterizedTest
    @ValueSource(strings = { "r1", "r2", "contigs" })
    void missingRequiredStoredObjectFailsBeforeAnyProcessStarts(String missing) throws Exception {
        String first = missing.equals("r1") ? "missing/first.fastq" : READ1_KEY;
        String second = missing.equals("r2") ? "missing/second.fastq" : READ2_KEY;
        String contigs = missing.equals("contigs") ? "missing/contigs.fasta" : CONTIG_KEY;

        StageRunResult<SeroTypingStageOutput> result = run(missing.equals("contigs") ? ECOLI : SALMONELLA,
                input(contigs, first, second), shortReads());

        assertRejectedBeforeTool(result);
        assertFalse(result.getFailReason().contains("\nnull"), "The failure must retain a useful input cause");
    }

    @ParameterizedTest
    @ValueSource(strings = { "ecoli-reads-only", "unknown-reads-only", "long-reads-only", "no-input" })
    void incompatibleOrIncompleteInputsAreRejectedWithoutGuessingAReadMode(String layout) throws Exception {
        SeroTypeStageInputUrls inputs = layout.equals("no-input")
                ? input(null, null, null) : input(null, READ1_KEY, READ2_KEY);
        SequenceMeta meta = layout.equals("unknown-reads-only") ? null
                : layout.equals("long-reads-only") ? metadata("long") : shortReads();

        StageRunResult<SeroTypingStageOutput> result = run(layout.equals("ecoli-reads-only") ? ECOLI : SALMONELLA,
                inputs, meta);

        assertRejectedBeforeTool(result);
        assertTrue(storage.downloaded.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(strings = { "K-unspecified", "O-unspecified", "K-directory", "O-directory",
            "K-missing", "O-missing", "same-file", "same-file-alias" })
    void kaptivePreflightsBothDistinctDatabaseFilesBeforeStartingEitherProcess(String configuration) throws Exception {
        if (configuration.equals("same-file")) {
            tools.setKaptiveODatabase(kaptiveKDatabase.toString());
        } else if (configuration.equals("same-file-alias")) {
            Path alias = Files.createSymbolicLink(tempDir.resolve("database alias.gbk"), kaptiveKDatabase);
            tools.setKaptiveODatabase(alias.toString());
        } else {
            String configuredPath = configuration.endsWith("unspecified") ? null
                    : configuration.endsWith("directory")
                            ? Files.createDirectory(tempDir.resolve("ambiguous database directory")).toString()
                            : tempDir.resolve("missing database.gbk").toString();
            if (configuration.startsWith("K-")) {
                tools.setKaptiveKDatabase(configuredPath);
            } else {
                tools.setKaptiveODatabase(configuredPath);
            }
        }

        StageRunResult<SeroTypingStageOutput> result = run(KLEBSIELLA,
                input(CONTIG_KEY, null, null), metadata("assembly"));

        assertRejectedBeforeTool(result);
        String reason = result.getFailReason().toLowerCase(Locale.ROOT);
        assertTrue(reason.contains("database") || reason.contains("数据库"), result.getFailReason());
    }

    @Test
    void bothRelativeKaptiveDatabasesAreMadeAbsoluteBeforeChangingTheProcessWorkingDirectories() throws Exception {
        Path currentDirectory = Path.of("").toAbsolutePath();
        Path relativeK = currentDirectory.relativize(kaptiveKDatabase.toAbsolutePath());
        Path relativeO = currentDirectory.relativize(kaptiveODatabase.toAbsolutePath());
        assertFalse(relativeK.isAbsolute());
        assertFalse(relativeO.isAbsolute());
        tools.setKaptiveKDatabase(relativeK.toString());
        tools.setKaptiveODatabase(relativeO.toString());

        StageRunResult<SeroTypingStageOutput> result = run(KLEBSIELLA,
                input(CONTIG_KEY, null, null), metadata("assembly"));

        assertSuccessful(result, workDir.resolve("kaptive_results.tsv"));
        assertEquals(2, executor.commands.size());
        assertKaptiveInvocation(0, "K", kaptiveKDatabase);
        assertKaptiveInvocation(1, "O", kaptiveODatabase);
    }

    @ParameterizedTest
    @ValueSource(strings = { "K-exit", "O-exit", "K-missing", "O-missing" })
    void eitherKaptiveProcessFailureOrMissingReportFailsTheWholeStage(String problem) throws Exception {
        String locus = problem.substring(0, 1);
        if (problem.endsWith("exit")) {
            executor.failedKaptiveLocus = locus;
        } else {
            executor.missingKaptiveReport = locus;
        }

        StageRunResult<SeroTypingStageOutput> result = run(KLEBSIELLA,
                input(CONTIG_KEY, null, null), metadata("assembly"));

        assertFalse(result.isSuccess());
        assertNull(result.getStageOutput());
        assertNotNull(result.getFailReason());
        assertFalse(result.getFailReason().isBlank());
        assertEquals(locus.equals("K") ? 1 : 2, executor.commands.size());
        assertFalse(Files.exists(workDir.resolve("kaptive_results.tsv")),
                "A failed K/O pair must not publish a combined success report");
    }

    @Test
    void missingRequiredToolReportCannotBeReturnedAsSuccessfulOutput() throws Exception {
        executor.writeReports = false;

        StageRunResult<SeroTypingStageOutput> result = run(ECOLI,
                input(CONTIG_KEY, null, null), metadata("assembly"));

        assertFalse(result.isSuccess());
        assertEquals(1, executor.commands.size());
        assertNull(result.getStageOutput());
    }

    @ParameterizedTest
    @MethodSource("routingCases")
    void publicInputResolverDescribesCompatibilityWithoutRelyingOnFilenameSuffixes(int species, String metadata,
            boolean contigsAvailable, boolean firstAvailable, boolean secondAvailable, InputMode expected) {
        SeroTypeStageInputUrls inputs = input(contigsAvailable ? CONTIG_KEY : null,
                firstAvailable ? READ1_KEY : null, secondAvailable ? READ2_KEY : null);

        assertEquals(expected, SeroTypingStageExectuor.resolveInputMode(
                taxonomy(species, STATUS_CONFIDENT), inputs, metadata(metadata)));
    }

    @Test
    void inputResolverRejectsUnconfirmedTaxonomyAndMissingContext() {
        SeroTypeStageInputUrls inputs = input(CONTIG_KEY, READ1_KEY, READ2_KEY);

        assertEquals(UNSUPPORTED, SeroTypingStageExectuor.resolveInputMode(null, inputs, shortReads()));
        assertEquals(UNSUPPORTED, SeroTypingStageExectuor.resolveInputMode(
                taxonomy(SALMONELLA, STATUS_AMBIGUOUS), inputs, shortReads()));
        assertEquals(UNSUPPORTED, SeroTypingStageExectuor.resolveInputMode(
                taxonomy(999, STATUS_CONFIDENT), inputs, shortReads()));
    }

    private static Stream<Arguments> routingCases() {
        return Stream.of(
                Arguments.of(SALMONELLA, "short", true, true, false, READS),
                Arguments.of(SALMONELLA, "short", true, true, true, READS),
                Arguments.of(SALMONELLA, "long", true, true, false, CONTIGS),
                Arguments.of(SALMONELLA, "unknown", true, true, true, CONTIGS),
                Arguments.of(SALMONELLA, "unknown", false, true, true, UNSUPPORTED),
                Arguments.of(SALMONELLA, "assembly", true, true, false, CONTIGS),
                Arguments.of(PNEUMOCOCCUS, "short", true, true, true, READS),
                Arguments.of(PNEUMOCOCCUS, "short", true, true, false, CONTIGS),
                Arguments.of(PNEUMOCOCCUS, "short", false, true, false, UNSUPPORTED),
                Arguments.of(ECOLI, "short", true, true, true, CONTIGS),
                Arguments.of(KLEBSIELLA, "short", true, true, true, CONTIGS),
                Arguments.of(ECOLI, "short", false, true, true, UNSUPPORTED));
    }

    private static Stream<Arguments> nonReadSequenceLevels() {
        return Stream.of(Arguments.of(SEQUENCE_LEVEL_UNKNOWN, true),
                Arguments.of(SEQUENCE_LEVEL_UNKNOWN, false),
                Arguments.of(SEQUENCE_LEVEL_REFERENCE, true),
                Arguments.of(SEQUENCE_LEVEL_REFERENCE, false));
    }

    private static Stream<Arguments> assemblyCompressionCases() {
        return Stream.of(SALMONELLA, ECOLI, KLEBSIELLA, PNEUMOCOCCUS)
                .flatMap(species -> Stream.of(".zip", ".ZIP", ".gz", ".gzip")
                        .map(suffix -> Arguments.of(species, suffix)));
    }

    private StageRunResult<SeroTypingStageOutput> run(int species, SeroTypeStageInputUrls inputs, SequenceMeta meta)
            throws Exception {
        SeroTypingStageParameters parameters = new SeroTypingStageParameters();
        parameters.setTaxonomyContext(taxonomy(species, STATUS_CONFIDENT));
        parameters.setReadMeta(meta);
        BioPipelineStage stage = new BioPipelineStage();
        stage.setStageId(42L);
        stage.setPipelineId(1L);
        stage.setStageType(PIPELINE_STAGE_SEROTYPE);
        stage.setStageIndex(-1);
        stage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
        stage.setVersion(3);
        stage.setInputUrl(JsonUtil.toJson(inputs));
        stage.setParameters(JsonUtil.toJson(parameters));
        return executor.execute(stage);
    }

    private static SeroTypeStageInputUrls input(String contigs, String first, String second) {
        SeroTypeStageInputUrls inputs = new SeroTypeStageInputUrls();
        inputs.setContigsUrl(contigs);
        inputs.setR1Url(first);
        inputs.setR2Url(second);
        return inputs;
    }

    private static SequenceMeta metadata(String name) {
        if (name.equals("unknown")) {
            return null;
        }
        SequenceMeta meta = new SequenceMeta();
        meta.setSequenceLevel(name.equals("assembly") ? SEQUENCE_LEVEL_ASSEMBLY : SEQUENCE_LEVEL_READ);
        meta.setReadLenType(name.equals("long") ? READ_LEN_TYPE_LONG : READ_LEN_TYPE_SHORT);
        return meta;
    }

    private static SequenceMeta shortReads() {
        return metadata("short");
    }

    private static TaxonomyContext taxonomy(int species, int status) {
        TaxonomyResult result = new TaxonomyResult();
        result.setStatus(status);
        TaxonomyResult.Taxon best = new TaxonomyResult.Taxon();
        best.setTaxid(species);
        best.setName("Fixture species");
        best.setRank("S");
        result.setBest(best);
        return TaxonomyContext.of(result);
    }

    private String writeObject(String key, String contents) throws IOException {
        Path destination = storageRoot.resolve(key);
        Files.createDirectories(destination.getParent());
        if (key.toLowerCase(Locale.ROOT).endsWith(".zip")) {
            try (ZipOutputStream archive = new ZipOutputStream(Files.newOutputStream(destination))) {
                archive.putNextEntry(new ZipEntry("payload/"));
                archive.closeEntry();
                archive.putNextEntry(new ZipEntry("payload/sequence.txt"));
                archive.write(contents.getBytes(StandardCharsets.UTF_8));
                archive.closeEntry();
            }
        } else if (key.endsWith(".gz") || key.endsWith(".gzip")) {
            try (OutputStream compressed = new GZIPOutputStream(Files.newOutputStream(destination))) {
                compressed.write(contents.getBytes(StandardCharsets.UTF_8));
            }
        } else {
            Files.writeString(destination, contents);
        }
        return key;
    }

    private Path readPath(int mate, boolean compressed) {
        return inputDir.resolve("reads/sample_R" + mate + ".fastq" + (compressed ? ".gz" : ""));
    }

    private Path plainContigPath() {
        return inputDir.resolve("contigs/assembly.fasta");
    }

    private void assertLoadedReads(boolean compressed) {
        assertArrayEquals(FIRST_READ.getBytes(StandardCharsets.UTF_8), executor.stagedFiles.get(readPath(1, compressed)));
        assertArrayEquals(SECOND_READ.getBytes(StandardCharsets.UTF_8), executor.stagedFiles.get(readPath(2, compressed)));
    }

    private void assertContigsOnlyDownloaded() {
        assertEquals(List.of(CONTIG_KEY), storage.downloaded);
        assertArrayEquals(ASSEMBLY_TEXT.getBytes(StandardCharsets.UTF_8), executor.stagedFiles.get(plainContigPath()));
        assertFalse(executor.stagedFiles.containsKey(readPath(1, false)));
        assertFalse(executor.stagedFiles.containsKey(readPath(2, false)));
    }

    private void assertSuccessful(StageRunResult<SeroTypingStageOutput> result, Path expectedReport) {
        assertTrue(result.isSuccess(), result.getErrorLog());
        assertNotNull(result.getStageOutput());
        assertEquals(expectedReport, result.getStageOutput().getResultPath());
    }

    private void assertRejectedBeforeTool(StageRunResult<SeroTypingStageOutput> result) {
        assertFalse(result.isSuccess());
        assertNull(result.getStageOutput());
        assertTrue(executor.commands.isEmpty());
        assertNotNull(result.getFailReason());
        assertFalse(result.getFailReason().isBlank());
    }

    private List<String> onlyCommand(String tool) {
        assertEquals(1, executor.commands.size());
        List<String> command = executor.commands.get(0);
        assertTrue(command.contains(tool), command.toString());
        return command;
    }

    private void assertKaptiveInvocation(int invocation, String locus, Path database) {
        List<String> command = executor.commands.get(invocation);
        assertEquals(List.of("mock-runtime", "mock-kaptive"), command.subList(0, 2));
        assertEquals(1, command.stream().filter("assembly"::equals).count());
        int operation = command.indexOf("assembly");
        assertEquals(database.toAbsolutePath().normalize().toString(), command.get(operation + 1));
        assertEquals(plainContigPath().toString(), command.get(operation + 2));
        Path processDirectory = workDir.resolve("kaptive").resolve(locus);
        assertEquals(processDirectory, executor.processDirectories.get(invocation));
        assertEquals(processDirectory.resolve("kaptive_results.tsv").toString(), option(command, "-o"));
        assertFalse(command.contains("-k"));
    }

    private static String option(List<String> command, String flag) {
        int index = command.indexOf(flag);
        assertTrue(index >= 0 && index + 1 < command.size(), "Missing option " + flag + ": " + command);
        return command.get(index + 1);
    }

    private static final class RecordingStorage extends LocalStorageService {

        private final List<String> downloaded = new ArrayList<>();

        @Override
        public GetObjectResult getObject(String key, String destination) {
            downloaded.add(key);
            return super.getObject(key, destination);
        }
    }

    private static final class StubExecutor extends SeroTypingStageExectuor {

        private final List<List<String>> commands = new ArrayList<>();
        private final List<Path> processDirectories = new ArrayList<>();
        private final Map<Path, byte[]> stagedFiles = new LinkedHashMap<>();
        private Path inputRoot;
        private Path processWorkDir;
        private boolean serobaPrefixAlreadyExisted;
        private boolean writeReports = true;
        private String failedKaptiveLocus;
        private String missingKaptiveReport;

        @Override
        protected ExecuteResult _execute(List<String> command, Path directory) {
            return simulate(command, directory);
        }

        @Override
        protected ExecuteResult _execute(List<String> command, Path directory, Path stdout, Path stderr) {
            return simulate(command, directory);
        }

        private ExecuteResult simulate(List<String> command, Path directory) {
            commands.add(List.copyOf(command));
            processDirectories.add(directory);
            processWorkDir = directory;
            try {
                try (Stream<Path> copied = Files.walk(inputRoot)) {
                    for (Path file : copied.filter(Files::isRegularFile).toList()) {
                        stagedFiles.put(file, Files.readAllBytes(file));
                    }
                }
                if (!writeReports) {
                    return new ExecuteResult(0, null);
                }
                Path report;
                String contents = "isolated software contract fixture\n";
                if (command.contains("mock-seqsero2")) {
                    report = Path.of(option(command, "-d")).resolve("SeqSero_result.txt");
                } else if (command.contains("mock-ectyper")) {
                    report = Path.of(option(command, "-o")).resolve("output.tsv");
                } else if (command.contains("mock-kaptive")) {
                    String locus = directory.getFileName().toString();
                    if (locus.equals(failedKaptiveLocus)) {
                        return new ExecuteResult(7, null);
                    }
                    if (locus.equals(missingKaptiveReport)) {
                        return new ExecuteResult(0, null);
                    }
                    report = Path.of(option(command, "-o"));
                    contents = KAPTIVE_HEADER + (locus.equals("K") ? KAPTIVE_K_ROW : KAPTIVE_O_ROW);
                } else if (command.contains("mock-pfaster")) {
                    report = Path.of(option(command, "-o")).resolve("prediction.txt");
                } else if (command.contains("mock-seroba")) {
                    int operation = command.indexOf("runSerotyping");
                    Path prefix = Path.of(command.get(operation + 3));
                    serobaPrefixAlreadyExisted = Files.exists(prefix);
                    report = prefix.resolve("pred.tsv");
                } else {
                    throw new AssertionError("Unexpected external process: " + command);
                }
                Files.createDirectories(report.getParent());
                Files.writeString(report, contents);
                return new ExecuteResult(0, null);
            } catch (IOException e) {
                throw new AssertionError("Cannot prepare isolated process fixture", e);
            }
        }
    }
}
