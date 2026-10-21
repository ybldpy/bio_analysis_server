package com.xjtlu.bio.analysisPipeline.taskrunner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

import com.xjtlu.bio.analysisPipeline.taskrunner.VariantAccuracyTestSupport.Allele;

/**
 * Test-only, non-functional random-letter fixtures with truth fixed before any caller runs.
 * SAM alignments are constructed directly: this exercises calling, not mapping accuracy.
 */
public final class VariantAccuracyFixture {

    public static final long SEED = 20261016L;
    public static final String REFERENCE_NAME = "TEXT_REFERENCE";
    public static final int REFERENCE_LENGTH = 2400;
    public static final List<String> CASES = List.of(
            "high_support", "reference_only", "reference_with_noise", "low_depth",
            "low_mapping_quality", "low_base_quality", "uncovered_truth", "mixed_evidence");

    private static final int REFERENCE_SPAN = 150;
    private static final int HIGH_SUPPORT = 60;
    private static final int HIGH_MAPPING_QUALITY = 60;
    private static final int HIGH_BASE_QUALITY = 40;

    public record Fixture(Path reference, Path sam, Path truthVcf, List<Allele> truth,
            int expectedTp, int expectedFp, int expectedFn, Map<String, Object> manifest) {
    }

    private record Event(String id, String type, Allele allele) {
    }

    private record ReadBlock(String id, int position, Allele alternate, int reads,
            int alternateReads, int mappingQuality, int baseQuality) {
    }

    private record AlignedRead(String cigar, String sequence, int editDistance) {
    }

    private VariantAccuracyFixture() {
    }

    /** Writes reference.fasta, reads.sam and truth.vcf directly into a fresh supplied directory. */
    public static Fixture create(Path root, String caseName) throws IOException {
        Objects.requireNonNull(root, "root");
        if (!CASES.contains(caseName)) {
            throw new IllegalArgumentException("Unknown variant accuracy fixture case: " + caseName);
        }
        Path directory = root.toAbsolutePath().normalize();
        Files.createDirectories(directory);
        String referenceText = randomReference();
        List<Event> ledger = eventLedger(referenceText);
        List<Event> truthEvents;
        List<ReadBlock> blocks = new ArrayList<>();
        int expectedTp = 0;

        switch (caseName) {
            case "high_support" -> {
                truthEvents = ledger;
                for (Event event : ledger) {
                    blocks.add(variantBlock(event, HIGH_SUPPORT, HIGH_MAPPING_QUALITY, HIGH_BASE_QUALITY));
                }
                expectedTp = ledger.size();
            }
            case "reference_only", "reference_with_noise" -> {
                truthEvents = List.of();
                int[] positions = {300, 900, 1500};
                for (int i = 0; i < positions.length; i++) {
                    boolean noise = "reference_with_noise".equals(caseName) && i == 0;
                    blocks.add(new ReadBlock("reference_region_" + (i + 1), positions[i],
                            noise ? ledger.get(0).allele() : null, HIGH_SUPPORT,
                            noise ? HIGH_SUPPORT / 10 : 0, HIGH_MAPPING_QUALITY, HIGH_BASE_QUALITY));
                }
            }
            case "low_depth" -> {
                truthEvents = List.of(ledger.get(0));
                blocks.add(variantBlock(ledger.get(0), 6, HIGH_MAPPING_QUALITY, HIGH_BASE_QUALITY));
            }
            case "low_mapping_quality" -> {
                truthEvents = List.of(ledger.get(0));
                blocks.add(variantBlock(ledger.get(0), HIGH_SUPPORT, 10, HIGH_BASE_QUALITY));
            }
            case "low_base_quality" -> {
                truthEvents = List.of(ledger.get(0));
                blocks.add(variantBlock(ledger.get(0), HIGH_SUPPORT, HIGH_MAPPING_QUALITY, 10));
            }
            case "uncovered_truth" -> {
                truthEvents = List.of(ledger.get(3));
                blocks.add(new ReadBlock("covered_reference_only", 300, null, HIGH_SUPPORT,
                        0, HIGH_MAPPING_QUALITY, HIGH_BASE_QUALITY));
            }
            case "mixed_evidence" -> {
                truthEvents = ledger;
                for (int i = 0; i < 3; i++) {
                    blocks.add(variantBlock(ledger.get(i), HIGH_SUPPORT, HIGH_MAPPING_QUALITY, HIGH_BASE_QUALITY));
                }
                blocks.add(variantBlock(ledger.get(3), 6, HIGH_MAPPING_QUALITY, HIGH_BASE_QUALITY));
                blocks.add(variantBlock(ledger.get(4), HIGH_SUPPORT, 10, HIGH_BASE_QUALITY));
                // The final deletion remains true despite deliberately having no covering reads.
                expectedTp = 3;
            }
            default -> throw new IllegalArgumentException("Unknown fixture case: " + caseName);
        }

        List<Allele> truth = truthEvents.stream().map(Event::allele).toList();
        int expectedFn = truth.size() - expectedTp;
        Path reference = directory.resolve("reference.fasta");
        Path sam = directory.resolve("reads.sam");
        Path truthVcf = directory.resolve("truth.vcf");
        writeNew(reference, fasta(referenceText));
        writeNew(sam, sam(referenceText, blocks));
        writeNew(truthVcf, truthVcf(truthEvents));

        Map<String, Object> manifest = new LinkedHashMap<>();
        manifest.put("case", caseName);
        manifest.put("seed", SEED);
        manifest.put("referenceName", REFERENCE_NAME);
        manifest.put("referenceLength", REFERENCE_LENGTH);
        manifest.put("referenceSource", "Seeded random ACGT letters; not a biological reference");
        manifest.put("truthSource", "Independent event ledger fixed before read generation and calling");
        manifest.put("truthDerivedFromCaller", false);
        manifest.put("lowSupportAndUncoveredTruthRetained", true);
        manifest.put("truthCount", truth.size());
        manifest.put("truthEvents", truthEvents.stream().map(event -> describeEvent(event, blocks)).toList());
        manifest.put("readBlocks", blocks.stream().map(VariantAccuracyFixture::describeBlock).toList());
        manifest.put("samReadCount", blocks.stream().mapToInt(ReadBlock::reads).sum());
        manifest.put("referenceSpanPerRead", REFERENCE_SPAN);
        manifest.put("readStartRule", "eventPosition - 110 + (readIndex % 15) * 3; one-based SAM POS");
        manifest.put("strandRule", "Alternating SAM FLAG 0/16; SEQ is already in aligned orientation");
        manifest.put("sample", "fixture");
        manifest.put("expectedCounts", Map.of("tp", expectedTp, "fp", 0, "fn", expectedFn));
        manifest.put("expectedCountsScope", "Final PASS alternate-genotype calls under unchanged default thresholds");
        manifest.put("files", Map.of("reference", reference.toString(), "sam", sam.toString(),
                "truthVcf", truthVcf.toString()));
        manifest.put("limitations", List.of(
                "Synthetic calling/acceptance-policy regression only; not biological or clinical validation.",
                "Direct SAM construction does not validate a mapper or real-read error distribution.",
                "Truth is not removed for low depth, poor quality, or absent coverage.",
                "Expected counts are test expectations, not measured caller results."));
        return new Fixture(reference, sam, truthVcf, List.copyOf(truth), expectedTp, 0, expectedFn, manifest);
    }

    private static String randomReference() {
        Random random = new Random(SEED);
        StringBuilder reference = new StringBuilder(REFERENCE_LENGTH);
        for (int i = 0; i < REFERENCE_LENGTH; i++) {
            reference.append("ACGT".charAt(random.nextInt(4)));
        }
        return reference.toString();
    }

    private static List<Event> eventLedger(String reference) {
        int deletion2 = unambiguousDeletionPosition(reference, 900, 2);
        int deletion1 = unambiguousDeletionPosition(reference, 1800, 1);
        return List.of(
                snp(reference, "truth_snp_1", 300),
                insertion(reference, "truth_insertion_3", 600, "GTA"),
                deletion(reference, "truth_deletion_2", deletion2, 2),
                snp(reference, "truth_snp_2", 1200),
                insertion(reference, "truth_insertion_1", 1500, "C"),
                deletion(reference, "truth_deletion_1", deletion1, 1));
    }

    private static Event snp(String reference, String id, int position) {
        String ref = reference.substring(position - 1, position);
        return new Event(id, "SNP", new Allele(REFERENCE_NAME, position, ref,
                String.valueOf(differentBase(ref.charAt(0)))));
    }

    private static Event insertion(String reference, String id, int position, String inserted) {
        String anchor = reference.substring(position - 1, position);
        StringBuilder bases = new StringBuilder(inserted);
        int last = bases.length() - 1;
        if (bases.charAt(last) == anchor.charAt(0)) {
            bases.setCharAt(last, differentBase(anchor.charAt(0)));
        }
        return new Event(id, "INSERTION", new Allele(REFERENCE_NAME, position, anchor, anchor + bases));
    }

    private static Event deletion(String reference, String id, int position, int length) {
        String ref = reference.substring(position - 1, position + length);
        return new Event(id, "DELETION", new Allele(REFERENCE_NAME, position, ref, ref.substring(0, 1)));
    }

    private static int unambiguousDeletionPosition(String reference, int near, int length) {
        for (int position = near; position < near + 30; position++) {
            // VCF anchor and the final deleted base must differ to prevent an equivalent left shift.
            if (reference.charAt(position - 1) != reference.charAt(position + length - 1)) {
                return position;
            }
        }
        throw new IllegalStateException("No unambiguous deletion anchor near " + near);
    }

    private static char differentBase(char reference) {
        return reference == 'A' ? 'C' : 'A';
    }

    private static ReadBlock variantBlock(Event event, int count, int mappingQuality, int baseQuality) {
        return new ReadBlock(event.id(), Math.toIntExact(event.allele().pos()), event.allele(),
                count, count, mappingQuality, baseQuality);
    }

    private static String fasta(String reference) {
        StringBuilder fasta = new StringBuilder(">" + REFERENCE_NAME + "\n");
        for (int offset = 0; offset < reference.length(); offset += 60) {
            fasta.append(reference, offset, Math.min(reference.length(), offset + 60)).append('\n');
        }
        return fasta.toString();
    }

    private static String sam(String reference, List<ReadBlock> blocks) {
        StringBuilder sam = new StringBuilder("@HD\tVN:1.6\tSO:unsorted\n");
        sam.append("@SQ\tSN:").append(REFERENCE_NAME).append("\tLN:").append(REFERENCE_LENGTH).append('\n');
        sam.append("@RG\tID:fixture\tSM:fixture\tPL:ILLUMINA\n");
        for (ReadBlock block : blocks) {
            for (int i = 0; i < block.reads(); i++) {
                int start = block.position() - 110 + (i % 15) * 3;
                Allele alternate = i < block.alternateReads() ? block.alternate() : null;
                AlignedRead read = alignedRead(reference, start, alternate);
                sam.append(block.id()).append('_').append(i + 1)
                        .append('\t').append(i % 2 == 0 ? 0 : 16)
                        .append('\t').append(REFERENCE_NAME).append('\t').append(start)
                        .append('\t').append(block.mappingQuality()).append('\t').append(read.cigar())
                        .append("\t*\t0\t0\t").append(read.sequence()).append('\t')
                        .append(String.valueOf((char) (33 + block.baseQuality())).repeat(read.sequence().length()))
                        .append("\tRG:Z:fixture\tNM:i:").append(read.editDistance()).append('\n');
            }
        }
        return sam.toString();
    }

    private static AlignedRead alignedRead(String reference, int oneBasedStart, Allele alternate) {
        int start = oneBasedStart - 1;
        int end = start + REFERENCE_SPAN;
        if (alternate == null) {
            return new AlignedRead(REFERENCE_SPAN + "M", reference.substring(start, end), 0);
        }
        int position = Math.toIntExact(alternate.pos()) - 1;
        if (!reference.regionMatches(position, alternate.ref(), 0, alternate.ref().length())) {
            throw new IllegalStateException("Event ledger REF does not match the reference");
        }
        if (alternate.ref().length() == 1 && alternate.alt().length() == 1) {
            return new AlignedRead(REFERENCE_SPAN + "M",
                    reference.substring(start, position) + alternate.alt() + reference.substring(position + 1, end), 1);
        }
        int leading = position - start + 1; // Include the unchanged VCF anchor base in the first M.
        if (alternate.ref().length() == 1) {
            String inserted = alternate.alt().substring(1);
            return new AlignedRead(leading + "M" + inserted.length() + "I" + (REFERENCE_SPAN - leading) + "M",
                    reference.substring(start, position + 1) + inserted + reference.substring(position + 1, end),
                    inserted.length());
        }
        int deleted = alternate.ref().length() - 1;
        return new AlignedRead(leading + "M" + deleted + "D" + (REFERENCE_SPAN - leading - deleted) + "M",
                reference.substring(start, position + 1) + reference.substring(position + deleted + 1, end), deleted);
    }

    private static String truthVcf(List<Event> truthEvents) {
        StringBuilder vcf = new StringBuilder("##fileformat=VCFv4.2\n");
        vcf.append("##source=VariantAccuracyFixture-independent-event-ledger\n");
        vcf.append("##contig=<ID=").append(REFERENCE_NAME).append(",length=").append(REFERENCE_LENGTH).append(">\n");
        vcf.append("##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">\n");
        vcf.append("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tfixture\n");
        for (Event event : truthEvents) {
            Allele allele = event.allele();
            vcf.append(allele.chrom()).append('\t').append(allele.pos()).append('\t').append(event.id())
                    .append('\t').append(allele.ref()).append('\t').append(allele.alt())
                    .append("\t.\tPASS\t.\tGT\t1\n");
        }
        return vcf.toString();
    }

    private static Map<String, Object> describeEvent(Event event, List<ReadBlock> blocks) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("id", event.id());
        result.put("type", event.type());
        result.put("chrom", event.allele().chrom());
        result.put("pos", event.allele().pos());
        result.put("ref", event.allele().ref());
        result.put("alt", event.allele().alt());
        result.put("nominalAlternateReads", blocks.stream()
                .filter(block -> event.allele().equals(block.alternate())).mapToInt(ReadBlock::alternateReads).sum());
        result.put("inTruthRegardlessOfEvidence", true);
        return result;
    }

    private static Map<String, Object> describeBlock(ReadBlock block) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("id", block.id());
        result.put("eventOrCoveredPosition", block.position());
        result.put("reads", block.reads());
        result.put("alternateReads", block.alternateReads());
        result.put("mappingQuality", block.mappingQuality());
        result.put("baseQuality", block.baseQuality());
        result.put("referenceReads", block.reads() - block.alternateReads());
        return result;
    }

    private static void writeNew(Path path, String content) throws IOException {
        Files.writeString(path, content, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
    }
}
