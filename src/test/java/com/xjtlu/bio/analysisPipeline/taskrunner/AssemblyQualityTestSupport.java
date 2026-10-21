package com.xjtlu.bio.analysisPipeline.taskrunner;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import com.fasterxml.jackson.databind.ObjectMapper;

/** Test-only assembly artifact readers; no assembler or external tool is invoked. */
public final class AssemblyQualityTestSupport {

    private static final String DNA = "ACGTRYSWKMBDHVN";
    private static final String COMPLEMENTS = "TGCAYRSWMKVHDBN";
    private static final ObjectMapper JSON = new ObjectMapper();

    private AssemblyQualityTestSupport() {
    }

    /** Uses the first whitespace-delimited header token as the record ID. */
    public static Map<String, String> readFasta(Path path) throws IOException {
        Map<String, String> records = new LinkedHashMap<>();
        String id = null;
        StringBuilder sequence = new StringBuilder();
        long lineNumber = 0;
        try (BufferedReader reader = reader(path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }
                if (line.charAt(0) == '>') {
                    if (id != null) {
                        addRecord(records, id, sequence, path, lineNumber);
                    }
                    String header = line.substring(1).trim();
                    if (header.isEmpty()) {
                        throw invalidFasta(path, lineNumber, "missing record ID");
                    }
                    id = header.split("\\s+", 2)[0];
                    if (records.containsKey(id)) {
                        throw invalidFasta(path, lineNumber, "duplicate record ID: " + id);
                    }
                    sequence.setLength(0);
                } else {
                    if (id == null) {
                        throw invalidFasta(path, lineNumber, "sequence before a FASTA header");
                    }
                    for (int i = 0; i < line.length(); i++) {
                        char base = line.charAt(i);
                        if (base >= 'a' && base <= 'z') {
                            base = (char) (base - 'a' + 'A');
                        }
                        if (DNA.indexOf(base) < 0) {
                            throw invalidFasta(path, lineNumber, "non-IUPAC DNA character");
                        }
                        sequence.append(base);
                    }
                }
            }
        }
        if (id == null) {
            throw invalidFasta(path, lineNumber, "no FASTA records");
        }
        addRecord(records, id, sequence, path, lineNumber);
        return records;
    }

    /** Hashes the file bytes, not decompressed or normalized content. */
    public static String sha256(Path path) throws IOException {
        MessageDigest digest = digest();
        try (InputStream input = Files.newInputStream(path)) {
            byte[] buffer = new byte[8192];
            int count;
            while ((count = input.read(buffer)) != -1) {
                digest.update(buffer, 0, count);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    /** Ignores record IDs, order and strand, but retains repeated sequences. */
    public static List<String> canonicalSequences(Path path) throws IOException {
        return canonicalize(readFasta(path).values());
    }

    /** Lengths include ambiguous bases; nBases counts N only. All counts are Long values. */
    public static Map<String, Object> fastaMetrics(Path path) throws IOException {
        Collection<String> sequences = readFasta(path).values();
        List<Integer> lengths = new ArrayList<>();
        long totalBases = 0;
        long nBases = 0;
        for (String sequence : sequences) {
            lengths.add(sequence.length());
            totalBases += sequence.length();
            for (int i = 0; i < sequence.length(); i++) {
                if (sequence.charAt(i) == 'N') {
                    nBases++;
                }
            }
        }
        lengths.sort(Collections.reverseOrder());
        long threshold = totalBases / 2 + totalBases % 2;
        long cumulative = 0;
        long n50 = 0;
        for (int length : lengths) {
            cumulative += length;
            if (cumulative >= threshold) {
                n50 = length;
                break;
            }
        }

        MessageDigest canonicalDigest = digest();
        for (String sequence : canonicalize(sequences)) {
            canonicalDigest.update(sequence.getBytes(StandardCharsets.US_ASCII));
            // DNA excludes newlines, so this preserves record boundaries and multiplicity.
            canonicalDigest.update((byte) '\n');
        }
        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("contigs", (long) sequences.size());
        metrics.put("totalBases", totalBases);
        metrics.put("largestContig", lengths.get(0).longValue());
        metrics.put("n50", n50);
        metrics.put("nBases", nBases);
        metrics.put("canonicalDigest", HexFormat.of().formatHex(canonicalDigest.digest()));
        return metrics;
    }

    /** Transposes a QUAST report.tsv; absent rows stay absent, '-' and blank cells stay missing. */
    public static Map<String, Map<String, String>> readQuastReport(Path path) throws IOException {
        Map<String, Map<String, String>> assemblies = new LinkedHashMap<>();
        try (BufferedReader reader = reader(path)) {
            String header = reader.readLine();
            if (header == null) {
                throw new IOException("Empty QUAST report: " + path);
            }
            String[] columns = header.split("\t", -1);
            if (columns.length < 2 || !"Assembly".equals(columns[0].trim())) {
                throw new IOException("Invalid QUAST report header: " + path);
            }
            List<String> labels = new ArrayList<>();
            for (int i = 1; i < columns.length; i++) {
                String label = columns[i].trim();
                if (label.isEmpty() || assemblies.containsKey(label)) {
                    throw new IOException("Empty or duplicate QUAST assembly label: " + path);
                }
                labels.add(label);
                assemblies.put(label, new LinkedHashMap<>());
            }
            long lineNumber = 1;
            String line;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (line.isEmpty()) {
                    continue;
                }
                String[] cells = line.split("\t", -1);
                if (cells.length != columns.length) {
                    throw new IOException("Invalid QUAST column count at line " + lineNumber + ": " + path);
                }
                String metric = cells[0].trim();
                if (metric.isEmpty() || assemblies.get(labels.get(0)).containsKey(metric)) {
                    throw new IOException("Empty or duplicate QUAST metric at line " + lineNumber + ": " + path);
                }
                for (int i = 0; i < labels.size(); i++) {
                    String value = cells[i + 1].trim();
                    assemblies.get(labels.get(i)).put(metric, value.isEmpty() ? "-" : value);
                }
            }
        }
        return assemblies;
    }

    public static void writeJson(Path path, Object value) throws IOException {
        Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        try (Writer writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            JSON.writerWithDefaultPrettyPrinter().writeValue(writer, value);
        }
    }

    private static BufferedReader reader(Path path) throws IOException {
        BufferedInputStream input = new BufferedInputStream(Files.newInputStream(path));
        try {
            input.mark(2);
            int first = input.read();
            int second = input.read();
            input.reset();
            InputStream decoded = first == 0x1f && second == 0x8b ? new GZIPInputStream(input) : input;
            return new BufferedReader(new InputStreamReader(decoded, StandardCharsets.UTF_8));
        } catch (IOException | RuntimeException failure) {
            try {
                input.close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private static void addRecord(Map<String, String> records, String id, StringBuilder sequence,
            Path path, long lineNumber) throws IOException {
        if (sequence.length() == 0) {
            throw invalidFasta(path, lineNumber, "empty record: " + id);
        }
        records.put(id, sequence.toString());
    }

    private static IOException invalidFasta(Path path, long lineNumber, String reason) {
        return new IOException("Invalid FASTA at line " + lineNumber + " in " + path + ": " + reason);
    }

    private static List<String> canonicalize(Collection<String> sequences) {
        List<String> canonical = new ArrayList<>();
        for (String sequence : sequences) {
            StringBuilder reverseComplement = new StringBuilder(sequence.length());
            for (int i = sequence.length() - 1; i >= 0; i--) {
                reverseComplement.append(COMPLEMENTS.charAt(DNA.indexOf(sequence.charAt(i))));
            }
            String reverse = reverseComplement.toString();
            canonical.add(sequence.compareTo(reverse) <= 0 ? sequence : reverse);
        }
        Collections.sort(canonical);
        return canonical;
    }

    private static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is required by the Java platform", impossible);
        }
    }
}
