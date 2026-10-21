package com.xjtlu.bio.analysisPipeline.taskrunner.util;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import com.xjtlu.bio.analysisPipeline.stageMetrics.VariantCallResultMetrics;
import com.xjtlu.bio.analysisPipeline.stageMetrics.VariantCallResultMetrics.ReferenceMetrics;

/**
 * Reads plain or gzip-compressed VCF files and calculates record-level QC.
 *
 * <p>Each variant record contributes to exactly one type. A multi-allelic
 * record is a SNP only when all real alternate alleles are single-base
 * substitutions, and an indel only when all real alternate alleles are
 * non-symbolic sequence alleles whose lengths differ from REF. Mixed
 * SNP/indel records, MNPs and symbolic alleles are counted as other. The
 * {@code .} and {@code *} ALT placeholders are ignored when deciding whether
 * a record contains a real alternate allele.</p>
 */
public final class VariantVcfMetricsParser {

    private static final String CONTIG_PREFIX = "##contig=<";
    private static final String FILE_FORMAT_PREFIX = "##fileformat=VCFv";
    private static final String COLUMN_HEADER_PREFIX = "#CHROM\t";

    private VariantVcfMetricsParser() {
    }

    public static VariantCallResultMetrics parse(Path vcfPath) throws IOException {
        if (!Files.isRegularFile(vcfPath)) {
            throw new IOException("VCF file does not exist: " + vcfPath);
        }

        MutableCounts overall = new MutableCounts();
        LinkedHashMap<String, MutableReference> references = new LinkedHashMap<>();
        boolean foundFileFormat = false;
        boolean foundColumnHeader = false;

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                openPossiblyCompressed(vcfPath), StandardCharsets.UTF_8))) {
            String line;
            long lineNumber = 0L;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (line.isBlank()) {
                    continue;
                }
                if (line.startsWith(FILE_FORMAT_PREFIX)) {
                    if (foundFileFormat) {
                        throw invalidVcf(vcfPath, lineNumber,
                                "duplicate fileformat header");
                    }
                    foundFileFormat = true;
                    continue;
                }
                if (line.startsWith(CONTIG_PREFIX)) {
                    ContigDefinition contig = parseContigDefinition(
                            line, vcfPath, lineNumber);
                    mergeContigDefinition(references, contig, vcfPath, lineNumber);
                    continue;
                }
                if (line.startsWith(COLUMN_HEADER_PREFIX)) {
                    if (foundColumnHeader) {
                        throw invalidVcf(vcfPath, lineNumber,
                                "duplicate #CHROM header");
                    }
                    validateColumnHeader(line, vcfPath, lineNumber);
                    foundColumnHeader = true;
                    continue;
                }
                if (line.charAt(0) == '#') {
                    continue;
                }
                if (!foundFileFormat || !foundColumnHeader) {
                    throw invalidVcf(
                            vcfPath,
                            lineNumber,
                            "data record encountered before required VCF headers");
                }

                String[] fields = line.split("\\t", -1);
                if (fields.length < 8) {
                    throw invalidVcf(
                            vcfPath, lineNumber, "expected at least eight tab-separated columns");
                }
                if (fields[0].isBlank()) {
                    throw invalidVcf(vcfPath, lineNumber, "CHROM must not be blank");
                }
                parsePosition(fields[1], vcfPath, lineNumber);
                if (fields[3].isBlank() || fields[4].isBlank()) {
                    throw invalidVcf(vcfPath, lineNumber, "REF and ALT must not be blank");
                }
                if (fields[6].isBlank()) {
                    throw invalidVcf(vcfPath, lineNumber, "FILTER must not be blank");
                }

                MutableReference reference = references.computeIfAbsent(
                        fields[0], name -> new MutableReference(name, null));
                RecordType recordType = classify(fields[3], fields[4]);
                overall.addRecord(recordType, fields[6]);
                reference.counts.addRecord(recordType, fields[6]);
            }
        }

        if (!foundFileFormat) {
            throw new IOException("Invalid VCF at " + vcfPath
                    + ": missing ##fileformat=VCFv header");
        }
        if (!foundColumnHeader) {
            throw new IOException("Invalid VCF at " + vcfPath
                    + ": missing #CHROM column header");
        }

        List<ReferenceMetrics> perReference = new ArrayList<>(references.size());
        for (MutableReference reference : references.values()) {
            perReference.add(reference.toMetrics());
        }
        return overall.toOverallMetrics(perReference);
    }

    private static void validateColumnHeader(
            String line, Path path, long lineNumber) throws IOException {
        String[] fields = line.split("\\t", -1);
        String[] required = {
                "#CHROM", "POS", "ID", "REF", "ALT", "QUAL", "FILTER", "INFO"
        };
        if (fields.length < required.length) {
            throw invalidVcf(path, lineNumber,
                    "#CHROM header must contain at least eight columns");
        }
        for (int i = 0; i < required.length; i++) {
            if (!required[i].equals(fields[i])) {
                throw invalidVcf(
                        path,
                        lineNumber,
                        "invalid VCF column " + (i + 1) + ": expected "
                                + required[i] + " but found " + fields[i]);
            }
        }
    }

    private static InputStream openPossiblyCompressed(Path path) throws IOException {
        BufferedInputStream input = new BufferedInputStream(Files.newInputStream(path));
        input.mark(2);
        int first = input.read();
        int second = input.read();
        input.reset();
        if (first == 0x1f && second == 0x8b) {
            return new GZIPInputStream(input);
        }
        return input;
    }

    private static void parsePosition(String value, Path path, long lineNumber)
            throws IOException {
        try {
            if (Long.parseLong(value) < 1L) {
                throw invalidVcf(path, lineNumber, "POS must be positive");
            }
        } catch (NumberFormatException e) {
            throw invalidVcf(path, lineNumber, "POS is not an integer: " + value, e);
        }
    }

    private static RecordType classify(String referenceAllele, String alternateField) {
        String[] alternateAlleles = alternateField.split(",", -1);
        List<String> realAlternateAlleles = new ArrayList<>(alternateAlleles.length);
        for (String allele : alternateAlleles) {
            if (!allele.equals(".") && !allele.equals("*")) {
                realAlternateAlleles.add(allele);
            }
        }
        if (realAlternateAlleles.isEmpty()) {
            return null;
        }

        boolean allSnps = true;
        boolean allIndels = true;
        boolean simpleReference = isSequenceAllele(referenceAllele);
        for (String alternateAllele : realAlternateAlleles) {
            boolean simpleAlternate = isSequenceAllele(alternateAllele);
            boolean snp = simpleReference
                    && simpleAlternate
                    && referenceAllele.length() == 1
                    && alternateAllele.length() == 1
                    && !referenceAllele.equalsIgnoreCase(alternateAllele);
            boolean indel = simpleReference
                    && simpleAlternate
                    && referenceAllele.length() != alternateAllele.length();
            allSnps &= snp;
            allIndels &= indel;
        }

        if (allSnps) {
            return RecordType.SNP;
        }
        if (allIndels) {
            return RecordType.INDEL;
        }
        return RecordType.OTHER;
    }

    private static boolean isSequenceAllele(String allele) {
        if (allele.isEmpty()) {
            return false;
        }
        for (int i = 0; i < allele.length(); i++) {
            char value = allele.charAt(i);
            if ((value < 'A' || value > 'Z') && (value < 'a' || value > 'z')) {
                return false;
            }
        }
        return true;
    }

    private static ContigDefinition parseContigDefinition(
            String line, Path path, long lineNumber) throws IOException {
        if (!line.endsWith(">")) {
            throw invalidVcf(path, lineNumber, "invalid contig header");
        }
        String body = line.substring(CONTIG_PREFIX.length(), line.length() - 1);
        Map<String, String> attributes = parseStructuredAttributes(body);
        String id = attributes.get("ID");
        if (id == null || id.isBlank()) {
            throw invalidVcf(path, lineNumber, "contig header has no ID");
        }

        Long length = null;
        String lengthValue = attributes.get("length");
        if (lengthValue != null && !lengthValue.isBlank()) {
            try {
                length = Long.valueOf(lengthValue);
            } catch (NumberFormatException e) {
                throw invalidVcf(
                        path, lineNumber, "invalid contig length: " + lengthValue, e);
            }
            if (length < 1L) {
                throw invalidVcf(path, lineNumber, "contig length must be positive");
            }
        }
        return new ContigDefinition(id, length);
    }

    private static Map<String, String> parseStructuredAttributes(String value) {
        LinkedHashMap<String, String> attributes = new LinkedHashMap<>();
        StringBuilder field = new StringBuilder();
        boolean quoted = false;
        boolean escaped = false;
        for (int i = 0; i <= value.length(); i++) {
            char current = i == value.length() ? ',' : value.charAt(i);
            if (current == ',' && !quoted) {
                addAttribute(attributes, field.toString());
                field.setLength(0);
                escaped = false;
                continue;
            }
            field.append(current);
            if (current == '"' && !escaped) {
                quoted = !quoted;
            }
            if (current == '\\' && !escaped) {
                escaped = true;
            } else {
                escaped = false;
            }
        }
        return attributes;
    }

    private static void addAttribute(Map<String, String> attributes, String rawField) {
        int equalsIndex = rawField.indexOf('=');
        if (equalsIndex < 1) {
            return;
        }
        String key = rawField.substring(0, equalsIndex).trim();
        String value = rawField.substring(equalsIndex + 1).trim();
        if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
            value = value.substring(1, value.length() - 1)
                    .replace("\\\"", "\"")
                    .replace("\\\\", "\\");
        }
        attributes.put(key, value);
    }

    private static void mergeContigDefinition(
            LinkedHashMap<String, MutableReference> references,
            ContigDefinition contig,
            Path path,
            long lineNumber) throws IOException {
        MutableReference existing = references.get(contig.id());
        if (existing == null) {
            references.put(contig.id(), new MutableReference(contig.id(), contig.length()));
            return;
        }
        if (existing.referenceLength == null) {
            existing.referenceLength = contig.length();
        } else if (contig.length() != null
                && !existing.referenceLength.equals(contig.length())) {
            throw invalidVcf(
                    path, lineNumber, "conflicting lengths for contig " + contig.id());
        }
    }

    private static IOException invalidVcf(Path path, long lineNumber, String reason) {
        return new IOException("Invalid VCF at " + path + ":" + lineNumber + ": " + reason);
    }

    private static IOException invalidVcf(
            Path path, long lineNumber, String reason, Exception cause) {
        return new IOException(
                "Invalid VCF at " + path + ":" + lineNumber + ": " + reason, cause);
    }

    private enum RecordType {
        SNP,
        INDEL,
        OTHER
    }

    private static final class MutableCounts {

        private long totalVcfRecords;
        private long variantRecords;
        private long snpCount;
        private long indelCount;
        private long otherCount;
        private long explicitPassCount;
        private long filteredCount;
        private long unfilteredCount;

        private void addRecord(RecordType recordType, String filter) {
            totalVcfRecords++;
            if (recordType == null) {
                return;
            }

            variantRecords++;
            switch (recordType) {
                case SNP -> snpCount++;
                case INDEL -> indelCount++;
                case OTHER -> otherCount++;
            }

            if (filter.equals("PASS")) {
                explicitPassCount++;
            } else if (filter.equals(".")) {
                unfilteredCount++;
            } else {
                filteredCount++;
            }
        }

        private VariantCallResultMetrics toOverallMetrics(List<ReferenceMetrics> references) {
            return new VariantCallResultMetrics(
                    totalVcfRecords,
                    variantRecords,
                    snpCount,
                    indelCount,
                    otherCount,
                    explicitPassCount,
                    filteredCount,
                    unfilteredCount,
                    references);
        }

        private ReferenceMetrics toReferenceMetrics(String name, Long length) {
            return new ReferenceMetrics(
                    name,
                    length,
                    totalVcfRecords,
                    variantRecords,
                    snpCount,
                    indelCount,
                    otherCount,
                    explicitPassCount,
                    filteredCount,
                    unfilteredCount);
        }
    }

    private static final class MutableReference {

        private final String referenceName;
        private Long referenceLength;
        private final MutableCounts counts = new MutableCounts();

        private MutableReference(String referenceName, Long referenceLength) {
            this.referenceName = referenceName;
            this.referenceLength = referenceLength;
        }

        private ReferenceMetrics toMetrics() {
            return counts.toReferenceMetrics(referenceName, referenceLength);
        }
    }

    private record ContigDefinition(String id, Long length) {
    }
}
