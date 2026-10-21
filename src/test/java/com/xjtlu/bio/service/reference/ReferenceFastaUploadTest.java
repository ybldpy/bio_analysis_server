package com.xjtlu.bio.service.reference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.xjtlu.bio.service.reference.ReferenceFastaUpload.Prepared;

class ReferenceFastaUploadTest {

    private static final String FASTA = ">record-one synthetic description\r\nacgtuRYSWKMBDHVN\r\n"
            + ">record-two 保留描述\nACGT\nnn\n";
    private static final long LIMIT = 1024 * 1024;

    @TempDir
    Path tempDir;

    @ParameterizedTest
    @ValueSource(strings = {"sample.fa", "sample.FA", "sample.fasta", "sample.FNA"})
    void validatesMultirecordFastaAndPreservesTheExactBytes(String filename) throws Exception {
        byte[] bytes = bytes(FASTA);
        TrackingInputStream input = new TrackingInputStream(bytes);

        Prepared result = ReferenceFastaUpload.prepare(input, filename, tempDir, LIMIT);

        assertEquals(tempDir.resolve("reference.fna"), result.path());
        assertEquals(22, result.totalBases());
        assertEquals(2, result.recordCount());
        assertEquals(hash(bytes), result.sha256());
        assertArrayEquals(bytes, Files.readAllBytes(result.path()));
        assertFalse(input.closed);
    }

    @ParameterizedTest
    @ValueSource(strings = {"sample.fa.gz", "sample.FNA.GZ", "sample.gzip", "sample.GZIP"})
    void gzipPreparationClosesInternalStreamsButNotTheCallerStream(String filename) throws Exception {
        TrackingInputStream input = new TrackingInputStream(gzip(bytes(FASTA)));

        Prepared result = ReferenceFastaUpload.prepare(input, filename, tempDir, LIMIT);

        assertArrayEquals(bytes(FASTA), Files.readAllBytes(result.path()));
        assertEquals(hash(bytes(FASTA)), result.sha256());
        assertEquals(2, result.recordCount());
        assertFalse(input.closed);
        assertFalse(Files.exists(tempDir.resolve("upload.compressed")));
    }

    @Test
    void zipUsesOnlyTheFirstNonDirectoryEntryAndIgnoresLaterInvalidFiles() throws Exception {
        byte[] zipped = zip(List.of(entry("folder/", ""), entry("folder/first.anything", FASTA),
                entry("later.fasta", "not FASTA")));
        TrackingInputStream input = new TrackingInputStream(zipped);

        Prepared result = ReferenceFastaUpload.prepare(input, "upload.ZIP", tempDir, LIMIT);

        assertArrayEquals(bytes(FASTA), Files.readAllBytes(result.path()));
        assertEquals(hash(bytes(FASTA)), result.sha256());
        assertFalse(Files.exists(tempDir.resolve("folder")));
        assertFalse(Files.exists(tempDir.resolve("later.fasta")));
        assertFalse(input.closed);
    }

    @Test
    void invalidFirstZipFileIsNotReplacedByALaterValidFile() throws Exception {
        byte[] zipped = zip(List.of(entry("first.fa", "not FASTA"), entry("later.fa", ">id\nACGT\n")));

        IOException exception = assertThrows(IOException.class,
                () -> ReferenceFastaUpload.prepare(new ByteArrayInputStream(zipped), "sample.zip", tempDir, LIMIT));

        assertTrue(exception.getMessage().contains("header"));
        assertArrayEquals(bytes("not FASTA"), Files.readAllBytes(tempDir.resolve("reference.fna")));
    }

    @Test
    void maliciousOriginalAndZipEntryPathsNeverBecomeDestinationPaths() throws Exception {
        Path work = Files.createDirectory(tempDir.resolve("isolated"));
        byte[] zipped = zip(List.of(entry("../../escaped.fna", ">id\nACGT\n")));

        Prepared result = ReferenceFastaUpload.prepare(new ByteArrayInputStream(zipped),
                "../../outside.ZIP", work, LIMIT);

        assertEquals(work.resolve("reference.fna"), result.path());
        assertFalse(Files.exists(tempDir.resolve("escaped.fna")));
        assertFalse(Files.exists(tempDir.resolve("outside.ZIP")));
        try (Stream<Path> files = Files.list(work)) {
            assertEquals(List.of(work.resolve("reference.fna")), files.toList());
        }
    }

    @Test
    void ordinaryOriginalFilenameIsOnlyAFormatHint() throws Exception {
        Path work = Files.createDirectory(tempDir.resolve("isolated"));
        Prepared result = ReferenceFastaUpload.prepare(new ByteArrayInputStream(bytes(">id\nUuNn\n")),
                "/absolute/untrusted/original.FASTA", work, LIMIT);
        assertEquals(work.resolve("reference.fna"), result.path());
        assertEquals(4, result.totalBases());
    }

    @Test
    void acceptsTabDelimitedHeaderDescriptionsWithoutRewritingThem() throws Exception {
        byte[] content = bytes(">record-one\tdescription\tmore\nACGT\n");
        Prepared result = ReferenceFastaUpload.prepare(new ByteArrayInputStream(content), "sample.fa", tempDir, LIMIT);
        assertEquals(1, result.recordCount());
        assertEquals(4, result.totalBases());
        assertArrayEquals(content, Files.readAllBytes(result.path()));
    }

    @Test
    void duplicateHeaderIdsAreStillRejectedWhenDelimitedWithTabs() {
        byte[] content = bytes(">same\tdescription\nACGT\n>same another description\nNN\n");
        IOException exception = assertThrows(IOException.class, () -> ReferenceFastaUpload.prepare(
                new ByteArrayInputStream(content), "sample.fa", tempDir, LIMIT));
        assertTrue(exception.getMessage().contains("Duplicate"));
    }

    @ParameterizedTest
    @MethodSource("badFasta")
    void rejectsMalformedFastaWithoutClosingTheInput(String text, String expected) throws Exception {
        TrackingInputStream input = new TrackingInputStream(bytes(text));

        IOException exception = assertThrows(IOException.class,
                () -> ReferenceFastaUpload.prepare(input, "bad.fa", tempDir, LIMIT));

        assertTrue(exception.getMessage().contains(expected), exception.getMessage());
        assertFalse(input.closed);
    }

    private static Stream<Arguments> badFasta() {
        return Stream.of(
                Arguments.of("", "no records"),
                Arguments.of("\r\n\n", "no records"),
                Arguments.of("ACGT\n", "header"),
                Arguments.of(">\nACGT\n", "nonempty record ID"),
                Arguments.of(">    \nACGT\n", "nonempty record ID"),
                Arguments.of(">id\n", "empty sequence"),
                Arguments.of(">first\n>second\nACGT\n", "empty sequence"),
                Arguments.of(">first\nACGT\n>second", "empty sequence"),
                Arguments.of(">same first description\nACGT\n>same second description\nNN\n", "Duplicate"),
                Arguments.of(">id\nAC GT\n", "non-IUPAC"),
                Arguments.of(">id\nAC-GT\n", "non-IUPAC"),
                Arguments.of(">id\nAC.GT\n", "non-IUPAC"),
                Arguments.of(">id\nAC1GT\n", "non-IUPAC"),
                Arguments.of(">id\nACXGT\n", "non-IUPAC"),
                Arguments.of(">id\nAC*GT\n", "non-IUPAC"),
                Arguments.of(">id\nAC\tGT\n", "control"),
                Arguments.of(">id\u0000description\nACGT\n", "control"),
                Arguments.of(">id\nA\u0085C\n", "control"),
                Arguments.of(">id\nAſC\n", "non-IUPAC"));
    }

    @Test
    void duplicateIdsAreCaseSensitiveAndDescriptionsAreNotPartOfTheId() throws Exception {
        Prepared result = ReferenceFastaUpload.prepare(new ByteArrayInputStream(bytes(">ID description\nA\n>id description\nU\n")),
                "valid.fa", tempDir, LIMIT);
        assertEquals(2, result.recordCount());
        assertEquals(2, result.totalBases());
    }

    @Test
    void acceptsExactlyTheByteLimitAndRejectsOneByteMore() throws Exception {
        byte[] content = bytes(">id\nACGT\n");
        Path exact = Files.createDirectory(tempDir.resolve("exact"));
        assertEquals(4, ReferenceFastaUpload.prepare(new ByteArrayInputStream(content),
                "exact.fa", exact, content.length).totalBases());
        Path over = Files.createDirectory(tempDir.resolve("over"));
        IOException exception = assertThrows(IOException.class,
                () -> ReferenceFastaUpload.prepare(new ByteArrayInputStream(content), "over.fa", over, content.length - 1));
        assertTrue(exception.getMessage().contains("uploaded bytes exceed"));
        assertTrue(Files.size(over.resolve("reference.fna")) <= content.length - 1);
    }

    @Test
    void boundsTheEntireCompressedUploadIncludingUnusedZipEntries() throws Exception {
        byte[] archive = zip(List.of(entry("first.fa", ">id\nACGT\n"), entry("unused.txt", "trailing bytes")));
        IOException exception = assertThrows(IOException.class,
                () -> ReferenceFastaUpload.prepare(new ByteArrayInputStream(archive), "sample.zip", tempDir, archive.length - 1));
        assertTrue(exception.getMessage().contains("uploaded compressed bytes exceed"));
        assertFalse(Files.exists(tempDir.resolve("reference.fna")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"gzip", "zip"})
    void decompressionCannotExceedTheConfiguredLimit(String format) throws Exception {
        String content = ">id\n" + "A".repeat(10000) + "\n";
        byte[] archive = "gzip".equals(format) ? gzip(bytes(content)) : zip(List.of(entry("first.fa", content)));
        assertTrue(archive.length < 1024);
        IOException exception = assertThrows(IOException.class,
                () -> ReferenceFastaUpload.prepare(new ByteArrayInputStream(archive), "sample." + format, tempDir, 1024));
        assertTrue(exception.getMessage().contains("decompressed"), exception.getMessage());
        assertTrue(Files.size(tempDir.resolve("reference.fna")) <= 1024);
    }

    @Test
    void directoryEntryPayloadCannotBypassTheZipDecompressionLimit() throws Exception {
        byte[] archive = zip(List.of(entry("malformed-directory/", "x".repeat(10000)), entry("first.fa", ">id\nA\n")));
        assertTrue(archive.length < 1024);
        IOException exception = assertThrows(IOException.class,
                () -> ReferenceFastaUpload.prepare(new ByteArrayInputStream(archive), "sample.zip", tempDir, 1024));
        assertTrue(exception.getMessage().contains("decompressed ZIP"));
        assertFalse(Files.exists(tempDir.resolve("reference.fna")));
    }

    @Test
    void directoryPayloadAndSelectedFileShareOneDecompressionBudget() throws Exception {
        byte[] archive = zip(List.of(entry("dir/", "x".repeat(700)), entry("first.fa", ">id\n" + "A".repeat(700))));
        assertTrue(archive.length < 1024);
        IOException exception = assertThrows(IOException.class,
                () -> ReferenceFastaUpload.prepare(new ByteArrayInputStream(archive), "sample.zip", tempDir, 1024));
        assertTrue(exception.getMessage().contains("decompressed ZIP"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"gzip", "zip"})
    void rejectsCorruptCompressedUploadsAndKeepsCallerStreamOpen(String suffix) {
        TrackingInputStream input = new TrackingInputStream(bytes("not an archive"));
        assertThrows(IOException.class, () -> ReferenceFastaUpload.prepare(input, "broken." + suffix, tempDir, LIMIT));
        assertFalse(input.closed);
    }

    @Test
    void rejectsZipWithNoFileEntry() throws Exception {
        byte[] archive = zip(List.of(entry("directory/", "")));
        IOException exception = assertThrows(IOException.class,
                () -> ReferenceFastaUpload.prepare(new ByteArrayInputStream(archive), "empty.zip", tempDir, LIMIT));
        assertTrue(exception.getMessage().contains("does not contain a file entry"));
    }

    @Test
    void rejectsMalformedUtf8WithoutSilentlyReplacingHeaderText() throws Exception {
        byte[] malformed = { '>', 'i', 'd', ' ', (byte) 0xC3, (byte) 0x28, '\n', 'A', '\n' };
        assertThrows(IOException.class, () -> ReferenceFastaUpload.prepare(new ByteArrayInputStream(malformed),
                "invalid.fa", tempDir, LIMIT));
    }

    @Test
    void validatesLongUnwrappedLinesAndHeadersWithoutLineBuffering() throws Exception {
        String header = ">id " + "description".repeat(20000) + "\n";
        String sequence = "acgtun".repeat(30000);
        byte[] content = bytes(header + sequence);

        Prepared result = ReferenceFastaUpload.prepare(new ByteArrayInputStream(content), "long.fa", tempDir, LIMIT);

        assertEquals(sequence.length(), result.totalBases());
        assertEquals(1, result.recordCount());
        assertEquals(hash(content), result.sha256());
    }

    @Test
    void recognizesDuplicateVeryLongIdsAcrossReaderBufferBoundaries() throws Exception {
        String id = "record".repeat(10000);
        String text = ">" + id + " description one\nACGT\n>" + id + " description two\nNN\n";

        IOException exception = assertThrows(IOException.class,
                () -> ReferenceFastaUpload.prepare(new ByteArrayInputStream(bytes(text)), "large-id.fa", tempDir, LIMIT));

        assertTrue(exception.getMessage().contains("Duplicate FASTA record ID"));
    }

    @Test
    void doesNotOverwriteExistingOutputOrFollowAnOutputSymlink() throws Exception {
        Path original = Files.writeString(tempDir.resolve("reference.fna"), "existing file\n");
        assertThrows(IOException.class, () -> ReferenceFastaUpload.prepare(new ByteArrayInputStream(bytes(">id\nA\n")),
                "sample.fa", tempDir, LIMIT));
        assertEquals("existing file\n", Files.readString(original));

        Path otherWork = Files.createDirectory(tempDir.resolve("symlink-work"));
        Files.createSymbolicLink(otherWork.resolve("reference.fna"), original);
        assertThrows(IOException.class, () -> ReferenceFastaUpload.prepare(new ByteArrayInputStream(bytes(">id\nA\n")),
                "sample.fa", otherWork, LIMIT));
        assertEquals("existing file\n", Files.readString(original));
    }

    @Test
    void rejectsInvalidArgumentsWithoutClosingTheInput() throws Exception {
        TrackingInputStream input = new TrackingInputStream(bytes(">id\nA\n"));
        assertThrows(IOException.class, () -> ReferenceFastaUpload.prepare(input, "sample.txt", tempDir, LIMIT));
        assertThrows(IOException.class, () -> ReferenceFastaUpload.prepare(input, null, tempDir, LIMIT));
        assertThrows(IOException.class, () -> ReferenceFastaUpload.prepare(input, "sample.fa", tempDir, 0));
        assertThrows(IOException.class, () -> ReferenceFastaUpload.prepare(input, "sample.fa", tempDir.resolve("missing"), LIMIT));
        assertThrows(IOException.class, () -> ReferenceFastaUpload.prepare(null, "sample.fa", tempDir, LIMIT));
        assertFalse(input.closed);
    }

    @Test
    void toleratesCallerStreamsThatReturnZeroBeforeDeliveringTheirContent() throws Exception {
        byte[] content = bytes(">id\nACGT\n");
        InputStream source = new ByteArrayInputStream(content) {
            private boolean first = true;

            @Override
            public synchronized int read(byte[] buffer, int offset, int length) {
                if (first) { first = false; return 0; }
                return super.read(buffer, offset, length);
            }
        };
        Prepared prepared = ReferenceFastaUpload.prepare(source, "sample.fa", tempDir, content.length);
        assertEquals(4, prepared.totalBases());
        assertNotNull(prepared.sha256());
    }

    private static byte[] bytes(String text) { return text.getBytes(StandardCharsets.UTF_8); }

    private static String hash(byte[] content) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content));
    }

    private static byte[] gzip(byte[] content) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (GZIPOutputStream output = new GZIPOutputStream(bytes)) { output.write(content); }
        return bytes.toByteArray();
    }

    private record Entry(String name, byte[] content) {
    }

    private static Entry entry(String name, String content) { return new Entry(name, bytes(content)); }

    private static byte[] zip(List<Entry> entries) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ZipOutputStream output = new ZipOutputStream(bytes)) {
            for (Entry entry : entries) {
                output.putNextEntry(new ZipEntry(entry.name()));
                output.write(entry.content());
                output.closeEntry();
            }
        }
        return bytes.toByteArray();
    }

    private static final class TrackingInputStream extends ByteArrayInputStream {
        private boolean closed;

        private TrackingInputStream(byte[] content) { super(content); }

        @Override
        public void close() throws IOException {
            closed = true;
            super.close();
        }
    }
}
