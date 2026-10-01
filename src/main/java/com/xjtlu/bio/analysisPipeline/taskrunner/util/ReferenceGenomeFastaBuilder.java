package com.xjtlu.bio.analysisPipeline.taskrunner.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSequence;

/**
 * Materializes the selected reference sequences into one multi-record FASTA.
 */
public final class ReferenceGenomeFastaBuilder {

    private ReferenceGenomeFastaBuilder() {
    }

    public static void write(Path outputPath,
            List<ReferenceSequence> referenceSequences,
            Map<String, Path> sequencePathsByAccession) throws IOException {
        if (outputPath == null) {
            throw new IOException("Reference genome FASTA output path must not be null");
        }
        if (referenceSequences == null || referenceSequences.isEmpty()) {
            throw new IOException("No reference sequences to write");
        }
        if (sequencePathsByAccession == null) {
            throw new IOException("Reference sequence path map must not be null");
        }

        Path parentPath = outputPath.getParent();
        if (parentPath != null) {
            Files.createDirectories(parentPath);
        }

        Set<String> recordNames = new HashSet<>();
        try (BufferedWriter writer = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8)) {
            for (ReferenceSequence referenceSequence : referenceSequences) {
                if (referenceSequence == null) {
                    throw new IOException("Reference sequence must not be null");
                }

                String accession = referenceSequence.getAccession();
                if (accession == null || accession.isBlank()) {
                    throw new IOException("Reference sequence accession must not be blank");
                }

                Path sourcePath = sequencePathsByAccession.get(accession);
                if (sourcePath == null || !Files.isRegularFile(sourcePath)) {
                    throw new IOException("Reference FASTA does not exist for accession: " + accession);
                }

                boolean hasRecord = false;
                try (BufferedReader reader = Files.newBufferedReader(sourcePath, StandardCharsets.UTF_8)) {
                    String line;
                    int lineNumber = 0;
                    while ((line = reader.readLine()) != null) {
                        lineNumber++;
                        if (line.startsWith(">")) {
                            String recordName = fastaRecordName(line, sourcePath, lineNumber);
                            if (!recordNames.add(recordName)) {
                                throw new IOException(
                                        "Reference genome FASTA contains duplicate record name: "
                                                + recordName);
                            }
                            hasRecord = true;
                        } else if (!line.isBlank() && !hasRecord) {
                            throw new IOException(
                                    "FASTA sequence occurs before the first header at "
                                            + sourcePath + ":" + lineNumber);
                        }
                        writer.write(line);
                        writer.newLine();
                    }
                }
                if (!hasRecord) {
                    throw new IOException("Reference FASTA contains no records: " + sourcePath);
                }
            }
        }
    }

    private static String fastaRecordName(String header, Path fastaPath, int lineNumber)
            throws IOException {
        String value = header.substring(1).trim();
        if (value.isEmpty()) {
            throw new IOException("Blank FASTA header at " + fastaPath + ":" + lineNumber);
        }

        int firstWhitespace = -1;
        for (int i = 0; i < value.length(); i++) {
            if (Character.isWhitespace(value.charAt(i))) {
                firstWhitespace = i;
                break;
            }
        }
        return firstWhitespace < 0 ? value : value.substring(0, firstWhitespace);
    }
}
