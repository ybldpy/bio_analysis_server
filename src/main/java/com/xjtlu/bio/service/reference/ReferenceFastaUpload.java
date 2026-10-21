package com.xjtlu.bio.service.reference;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/** Bounded, format-only file preparation; no taxonomy or biological interpretation is performed. */
public final class ReferenceFastaUpload {

    private static final String OUTPUT_NAME = "reference.fna";
    private static final String COMPRESSED_NAME = "upload.compressed";

    private ReferenceFastaUpload() {
    }

    public record Prepared(Path path, int totalBases, int recordCount, String sha256) {
    }

    /**
     * Uses a caller-owned, isolated work directory and never closes {@code input}.
     * Both the entire uploaded byte stream and the decompressed bytes are bounded
     * by {@code maxBytes}. The caller is responsible for directory cleanup on failure.
     */
    public static Prepared prepare(InputStream input, String originalFilename, Path workDir, long maxBytes)
            throws IOException {
        if (input == null) {
            throw new IOException("Reference FASTA input stream is required");
        }
        if (maxBytes <= 0L) {
            throw new IOException("Reference FASTA maxBytes must be positive");
        }
        if (workDir == null || !Files.isDirectory(workDir, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Reference FASTA workDir must be an existing isolated directory");
        }
        Format format = format(originalFilename);
        Path output = workDir.resolve(OUTPUT_NAME);
        Path compressed = workDir.resolve(COMPRESSED_NAME);
        MessageDigest digest = sha256Digest();

        if (format == Format.FASTA) {
            try (OutputStream destination = createNew(output)) {
                copyBounded(input, destination, maxBytes, digest, "uploaded");
            }
        } else {
            // Copy the complete upload first: trailing ZIP entries cannot evade
            // the compressed-byte limit even though only the first file is used.
            try (OutputStream destination = createNew(compressed)) {
                copyBounded(input, destination, maxBytes, null, "uploaded compressed");
            }
            if (format == Format.GZIP) {
                try (InputStream source = Files.newInputStream(compressed);
                        GZIPInputStream decompressed = new GZIPInputStream(source);
                        OutputStream destination = createNew(output)) {
                    copyBounded(decompressed, destination, maxBytes, digest, "decompressed");
                }
            } else {
                extractFirstZipFile(compressed, output, maxBytes, digest);
            }
        }

        Counts counts = validate(output);
        if (format != Format.FASTA) {
            Files.delete(compressed);
        }
        return new Prepared(output, counts.totalBases(), counts.recordCount(),
                HexFormat.of().formatHex(digest.digest()));
    }

    private static Format format(String filename) throws IOException {
        if (filename == null || filename.isBlank()) {
            throw new IOException("Reference FASTA filename is required");
        }
        String lower = filename.toLowerCase(Locale.ROOT);
        if (lower.endsWith(".gz") || lower.endsWith(".gzip")) {
            return Format.GZIP;
        }
        if (lower.endsWith(".zip")) {
            return Format.ZIP;
        }
        if (lower.endsWith(".fasta") || lower.endsWith(".fa") || lower.endsWith(".fna")) {
            return Format.FASTA;
        }
        throw new IOException("Reference upload must be FASTA (.fasta/.fa/.fna), gzip or ZIP");
    }

    private static OutputStream createNew(Path path) throws IOException {
        // CREATE_NEW also rejects an existing symlink; no uploaded filename is resolved here.
        return Files.newOutputStream(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    }

    private static long copyBounded(InputStream source, OutputStream destination, long limit,
            MessageDigest digest, String label) throws IOException {
        byte[] buffer = new byte[8192];
        long count = 0L;
        while (true) {
            // Read at most one byte beyond the remaining budget, including at EOF.
            int request = (int) Math.min(buffer.length, limit - count < buffer.length
                    ? limit - count + 1L : buffer.length);
            int read = source.read(buffer, 0, request);
            if (read == -1) {
                return count;
            }
            if (read == 0) {
                // A defensive fallback for nonconforming caller-owned InputStreams.
                int value = source.read();
                if (value == -1) {
                    return count;
                }
                buffer[0] = (byte) value;
                read = 1;
            }
            if (read > limit - count) {
                throw new IOException("Reference FASTA " + label + " bytes exceed maxBytes=" + limit);
            }
            destination.write(buffer, 0, read);
            if (digest != null) {
                digest.update(buffer, 0, read);
            }
            count += read;
        }
    }

    private static void extractFirstZipFile(Path compressed, Path output, long maxBytes, MessageDigest digest)
            throws IOException {
        try (InputStream source = Files.newInputStream(compressed);
                ZipInputStream archive = new ZipInputStream(source)) {
            long skippedBytes = 0L;
            ZipEntry entry;
            while ((entry = archive.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    // Directory entries should be empty, but malformed archives can
                    // contain data. Never drain them without a decompression limit.
                    skippedBytes += copyBounded(archive, OutputStream.nullOutputStream(),
                            maxBytes - skippedBytes, null, "decompressed ZIP");
                    archive.closeEntry();
                    continue;
                }
                try (OutputStream destination = createNew(output)) {
                    copyBounded(archive, destination, maxBytes - skippedBytes, digest, "decompressed ZIP");
                }
                // Do not request or inspect further file entries. Their paths and
                // contents cannot change the one-slot upload contract.
                return;
            }
        }
        throw new IOException("Reference ZIP does not contain a file entry");
    }

    private static Counts validate(Path path) throws IOException {
        FastaValidator validator = new FastaValidator();
        try (Reader reader = new InputStreamReader(Files.newInputStream(path),
                StandardCharsets.UTF_8.newDecoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT))) {
            char[] buffer = new char[8192];
            for (int read; (read = reader.read(buffer)) != -1;) {
                for (int i = 0; i < read; i++) {
                    validator.accept(buffer[i]);
                }
            }
        }
        return validator.finish();
    }

    private static MessageDigest sha256Digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private enum Format { FASTA, GZIP, ZIP }

    private record Counts(int totalBases, int recordCount) {
    }

    /** Never buffers a complete header or sequence line, even for unwrapped FASTA. */
    private static final class FastaValidator {
        private final Set<String> identifiers = new HashSet<>();
        private MessageDigest identifierDigest;
        private boolean lineStart = true;
        private boolean header;
        private boolean identifierStarted;
        private boolean identifierEnded;
        private int totalBases;
        private int recordCount;
        private int recordBases;

        private void accept(char value) throws IOException {
            if (value == '\r' || value == '\n') {
                if (header) {
                    finishHeader();
                }
                lineStart = true;
                return;
            }
            if (Character.isISOControl(value) && !(header && value == '\t')) {
                throw new IOException("FASTA contains a control character");
            }
            if (lineStart) {
                lineStart = false;
                if (value == '>') {
                    if (recordCount > 0 && recordBases == 0) {
                        throw new IOException("FASTA record " + recordCount + " has an empty sequence");
                    }
                    if (recordCount == Integer.MAX_VALUE) {
                        throw new IOException("FASTA record count exceeds the integer limit");
                    }
                    recordCount++;
                    recordBases = 0;
                    header = true;
                    identifierStarted = false;
                    identifierEnded = false;
                    identifierDigest = sha256Digest();
                    return;
                }
            }
            if (recordCount == 0) {
                throw new IOException("FASTA sequence data must follow a '>' header");
            }
            if (header) {
                if (Character.isWhitespace(value)) {
                    identifierEnded |= identifierStarted;
                } else if (!identifierEnded) {
                    identifierStarted = true;
                    // Hash the exact UTF-16 token incrementally. This bounds memory
                    // for very long IDs/descriptions without rewriting any bytes.
                    identifierDigest.update((byte) (value >>> 8));
                    identifierDigest.update((byte) value);
                }
                return;
            }
            if (!isNucleotide(value)) {
                throw new IOException("FASTA sequence contains a non-IUPAC nucleotide character (U+"
                        + String.format(Locale.ROOT, "%04X", (int) value) + ")");
            }
            if (totalBases == Integer.MAX_VALUE) {
                throw new IOException("FASTA total sequence length exceeds the integer limit");
            }
            totalBases++;
            recordBases++;
        }

        private void finishHeader() throws IOException {
            if (!identifierStarted) {
                throw new IOException("FASTA header must contain a nonempty record ID");
            }
            if (!identifiers.add(HexFormat.of().formatHex(identifierDigest.digest()))) {
                throw new IOException("Duplicate FASTA record ID at record " + recordCount);
            }
            identifierDigest = null;
            header = false;
        }

        private Counts finish() throws IOException {
            if (header) {
                finishHeader();
            }
            if (recordCount == 0) {
                throw new IOException("FASTA contains no records");
            }
            if (recordBases == 0) {
                throw new IOException("FASTA record " + recordCount + " has an empty sequence");
            }
            return new Counts(totalBases, recordCount);
        }

        private static boolean isNucleotide(char value) {
            return switch (value) {
                case 'A', 'a', 'C', 'c', 'G', 'g', 'T', 't', 'U', 'u',
                        'R', 'r', 'Y', 'y', 'S', 's', 'W', 'w', 'K', 'k',
                        'M', 'm', 'B', 'b', 'D', 'd', 'H', 'h', 'V', 'v', 'N', 'n' -> true;
                default -> false;
            };
        }
    }
}
