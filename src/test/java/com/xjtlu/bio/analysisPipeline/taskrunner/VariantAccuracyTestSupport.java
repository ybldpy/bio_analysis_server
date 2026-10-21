package com.xjtlu.bio.analysisPipeline.taskrunner;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.zip.GZIPInputStream;

/**
 * Test-only exact allele comparison for already-normalized, single-sample VCF files.
 * This does not normalize variants, compare equivalent haplotypes, match structural variant
 * spans, evaluate genotype dosage, or measure a clinical false-positive rate. Different
 * representations of a complex variant remain different keys even if biologically equivalent.
 */
public final class VariantAccuracyTestSupport {

    private static final String[] HEADER = {
            "#CHROM", "POS", "ID", "REF", "ALT", "QUAL", "FILTER", "INFO", "FORMAT"
    };
    private static final Set<String> NON_VARIANT_ALTS = Set.of("*", "<NON_REF>", "<*>");

    private VariantAccuracyTestSupport() {
    }

    /** Coordinates are VCF-style: pos is one-based. REF and ALT are compared literally. */
    public record Allele(String chrom, long pos, String ref, String alt) {
        public Allele {
            requireToken(chrom, "chrom");
            requireToken(ref, "REF");
            requireToken(alt, "ALT");
            if (pos < 1 || ".".equals(ref) || ".".equals(alt)
                    || NON_VARIANT_ALTS.contains(alt) || ref.equals(alt)) {
                throw new IllegalArgumentException("Expected a non-reference allele with a positive VCF position");
            }
        }
    }

    /** BED coordinates: zero-based start inclusive, end exclusive; zero-length regions are rejected. */
    public record Interval(String chrom, long startInclusive, long endExclusive) {
        public Interval {
            requireToken(chrom, "chrom");
            if (startInclusive < 0 || endExclusive <= startInclusive) {
                throw new IllegalArgumentException("BED interval must have 0 <= start < end");
            }
        }

        private boolean contains(Allele allele) {
            long position = allele.pos() - 1;
            return chrom.equals(allele.chrom()) && startInclusive <= position && position < endExclusive;
        }
    }

    /** Undefined ratios have a null value, including precision for no predictions. */
    public record Accuracy(long truePositives, long falsePositives, long falseNegatives,
            Double precision, Double recall, Double falseDiscoveryRate, Double falseNegativeRate) {
    }

    public enum FilterPolicy {
        PASS_ONLY,
        PASS_OR_UNFILTERED,
        ALL
    }

    /**
     * Reads the actual ALT alleles addressed by GT, not every ALT listed in a record.
     * A missing GT field or malformed/out-of-range GT is an error, even in an excluded record.
     * Explicit no-calls ('.', './.') contribute no calls; a partially missing GT ('1/.')
     * contributes its known allele. Spanning deletion '*' and gVCF non-variant placeholders
     * are not independent variant alleles. Duplicate records/dosage count only once.
     */
    public static Set<Allele> readCalledAlleles(Path vcf, FilterPolicy filterPolicy) throws IOException {
        Objects.requireNonNull(filterPolicy, "filterPolicy");
        Set<Allele> alleles = new LinkedHashSet<>();
        boolean fileFormatSeen = false;
        boolean columnsSeen = false;
        long lineNumber = 0;
        try (BufferedReader reader = reader(vcf)) {
            String line;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (line.isBlank()) {
                    continue;
                }
                if (line.startsWith("##")) {
                    if (columnsSeen) {
                        throw invalid(vcf, lineNumber, "metadata after column header");
                    }
                    if (line.startsWith("##fileformat=")) {
                        if (fileFormatSeen || !line.matches("##fileformat=VCFv4\\.[0-9]+")) {
                            throw invalid(vcf, lineNumber, "invalid or duplicate fileformat declaration");
                        }
                        fileFormatSeen = true;
                    }
                    continue;
                }
                if (line.startsWith("#")) {
                    if (columnsSeen || !fileFormatSeen) {
                        throw invalid(vcf, lineNumber, "duplicate column header or missing fileformat");
                    }
                    String[] columns = line.split("\t", -1);
                    if (columns.length != 10 || columns[9].isBlank()) {
                        throw invalid(vcf, lineNumber, "exactly one named sample is required");
                    }
                    for (int index = 0; index < HEADER.length; index++) {
                        if (!HEADER[index].equals(columns[index])) {
                            throw invalid(vcf, lineNumber, "invalid VCF column header");
                        }
                    }
                    columnsSeen = true;
                    continue;
                }
                if (!columnsSeen) {
                    throw invalid(vcf, lineNumber, "data before required VCF headers");
                }
                String[] columns = line.split("\t", -1);
                if (columns.length != 10) {
                    throw invalid(vcf, lineNumber, "record must contain exactly one sample");
                }
                try {
                    long position = Long.parseLong(columns[1]);
                    requireToken(columns[0], "chrom");
                    requireToken(columns[3], "REF");
                    if (position < 1 || ".".equals(columns[3])) {
                        throw new IllegalArgumentException("invalid position or REF");
                    }
                    String[] alternatives = alternatives(columns[4]);
                    String gt = genotype(columns[8], columns[9]);
                    boolean include = includeFilter(columns[6], filterPolicy);
                    for (String value : gt.split("[/|]", -1)) {
                        if (".".equals(value)) {
                            continue;
                        }
                        int alleleIndex = Integer.parseInt(value);
                        if (alleleIndex > alternatives.length) {
                            throw new IllegalArgumentException("GT allele index exceeds ALT count");
                        }
                        if (alleleIndex != 0) {
                            String alternative = alternatives[alleleIndex - 1];
                            if (!NON_VARIANT_ALTS.contains(alternative)) {
                                Allele allele = new Allele(columns[0], position, columns[3], alternative);
                                if (include) {
                                    alleles.add(allele);
                                }
                            }
                        }
                    }
                } catch (IllegalArgumentException failure) {
                    throw invalid(vcf, lineNumber, failure.getMessage(), failure);
                }
            }
        }
        if (!columnsSeen) {
            throw invalid(vcf, lineNumber, "missing #CHROM column header with sample");
        }
        return Collections.unmodifiableSet(alleles);
    }

    /** Reads the first three BED columns; optional additional columns are ignored. */
    public static List<Interval> readBed(Path bed) throws IOException {
        List<Interval> intervals = new ArrayList<>();
        long lineNumber = 0;
        try (BufferedReader reader = reader(bed)) {
            String line;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#") || line.startsWith("track ")
                        || line.startsWith("browser ")) {
                    continue;
                }
                String[] columns = line.split("\\s+");
                if (columns.length < 3) {
                    throw invalid(bed, lineNumber, "BED requires chrom, start and end");
                }
                try {
                    intervals.add(new Interval(columns[0], Long.parseLong(columns[1]),
                            Long.parseLong(columns[2])));
                } catch (IllegalArgumentException failure) {
                    throw invalid(bed, lineNumber, failure.getMessage(), failure);
                }
            }
        }
        // Empty explicit regions must not silently broaden evaluation to the entire genome.
        if (intervals.isEmpty()) {
            throw new IOException("BED contains no evaluation intervals: " + bed);
        }
        return List.copyOf(intervals);
    }

    public static Accuracy compare(Collection<Allele> truth, Collection<Allele> called) {
        return compare(truth, called, List.of());
    }

    /**
     * Empty intervals explicitly mean unrestricted evaluation. Otherwise the identical,
     * caller-supplied BED region set is applied to both truth and predictions by their POS
     * anchor (not full REF/SV span). No coverage-derived regions or missing calls are removed.
     * Overlapping intervals and repeated alleles never inflate counts.
     */
    public static Accuracy compare(Collection<Allele> truth, Collection<Allele> called,
            List<Interval> evaluationIntervals) {
        Objects.requireNonNull(evaluationIntervals, "evaluationIntervals");
        evaluationIntervals.forEach(interval -> Objects.requireNonNull(interval, "interval"));
        Set<Allele> expected = inScope(truth, evaluationIntervals);
        Set<Allele> observed = inScope(called, evaluationIntervals);
        long truePositives = observed.stream().filter(expected::contains).count();
        long falsePositives = observed.size() - truePositives;
        long falseNegatives = expected.size() - truePositives;
        return new Accuracy(truePositives, falsePositives, falseNegatives,
                ratio(truePositives, observed.size()), ratio(truePositives, expected.size()),
                ratio(falsePositives, observed.size()), ratio(falseNegatives, expected.size()));
    }

    private static Set<Allele> inScope(Collection<Allele> alleles, List<Interval> intervals) {
        Objects.requireNonNull(alleles, "alleles");
        Set<Allele> result = new HashSet<>();
        for (Allele allele : alleles) {
            Objects.requireNonNull(allele, "allele");
            if (intervals.isEmpty() || intervals.stream().anyMatch(interval -> interval.contains(allele))) {
                result.add(allele);
            }
        }
        return result;
    }

    private static Double ratio(long numerator, long denominator) {
        return denominator == 0 ? null : (double) numerator / denominator;
    }

    private static String[] alternatives(String value) {
        if (".".equals(value)) {
            return new String[0];
        }
        String[] alternatives = value.split(",", -1);
        Set<String> distinct = new HashSet<>();
        for (String alternative : alternatives) {
            requireToken(alternative, "ALT");
            if (".".equals(alternative) || !distinct.add(alternative)) {
                throw new IllegalArgumentException("invalid or duplicate ALT");
            }
        }
        return alternatives;
    }

    private static String genotype(String format, String sample) {
        String[] formats = format.split(":", -1);
        int gtIndex = -1;
        for (int index = 0; index < formats.length; index++) {
            if ("GT".equals(formats[index])) {
                if (gtIndex >= 0) {
                    throw new IllegalArgumentException("duplicate GT field");
                }
                gtIndex = index;
            }
        }
        String[] values = sample.split(":", -1);
        if (gtIndex < 0 || gtIndex >= values.length) {
            throw new IllegalArgumentException("missing GT field or GT sample value");
        }
        String gt = values[gtIndex];
        if (!gt.matches("(?:\\.|[0-9]+)(?:[/|](?:\\.|[0-9]+))*")) {
            throw new IllegalArgumentException("invalid GT: " + gt);
        }
        return gt;
    }

    private static boolean includeFilter(String filter, FilterPolicy policy) {
        requireToken(filter, "FILTER");
        String[] flags = filter.split(";", -1);
        for (String flag : flags) {
            if (flag.isEmpty() || (flags.length > 1 && ("PASS".equals(flag) || ".".equals(flag)))) {
                throw new IllegalArgumentException("invalid FILTER");
            }
        }
        return policy == FilterPolicy.ALL || "PASS".equals(filter)
                || (policy == FilterPolicy.PASS_OR_UNFILTERED && ".".equals(filter));
    }

    private static void requireToken(String value, String name) {
        if (value == null || value.isBlank() || value.chars().anyMatch(Character::isWhitespace)) {
            throw new IllegalArgumentException(name + " must be a nonblank token");
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

    private static IOException invalid(Path path, long line, String reason) {
        return new IOException("Invalid variant evaluation input at " + path + ":" + line + ": " + reason);
    }

    private static IOException invalid(Path path, long line, String reason, Throwable cause) {
        return new IOException("Invalid variant evaluation input at " + path + ":" + line + ": " + reason,
                cause);
    }
}
