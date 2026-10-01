package com.xjtlu.bio.analysisPipeline.referenceGenome;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Analysis-time aggregate of the reference sequences selected for a sample.
 *
 * <p>The aggregate contains one sequence for an unsegmented genome and may
 * contain independently selected sequences for a segmented genome. Therefore
 * it is not necessarily evidence that all sequences came from one isolate.</p>
 */
public class ReferenceGenome {
    private Integer taxId;
    private String organismName;
    private List<ReferenceSequence> sequences = new ArrayList<>();
    private boolean complete;
    private ReferenceCompositionStatus compositionStatus = ReferenceCompositionStatus.UNKNOWN;

    public ReferenceGenome() {
    }

    public ReferenceGenome(Integer taxId, String organismName,
            List<ReferenceSequence> sequences, boolean complete,
            ReferenceCompositionStatus compositionStatus) {
        this.taxId = taxId;
        this.organismName = organismName;
        setSequences(sequences);
        this.complete = complete;
        this.compositionStatus = compositionStatus == null
                ? ReferenceCompositionStatus.UNKNOWN
                : compositionStatus;
    }

    public static ReferenceGenome fromSingleSequence(ReferenceSequence sequence) {
        if (sequence == null) {
            throw new IllegalArgumentException("Reference sequence must not be null");
        }
        return fromSelectedSequences(List.of(sequence));
    }

    /**
     * Builds an analysis reference from the independently selected sequence for
     * each segment. Runtime code obtains the sequence files from
     * {@link #getSequences()}.
     */
    public static ReferenceGenome fromSelectedSequences(List<ReferenceSequence> selectedSequences) {
        if (selectedSequences == null || selectedSequences.isEmpty()) {
            throw new IllegalArgumentException("At least one selected reference sequence is required");
        }

        List<ReferenceSequence> sequences = new ArrayList<>(selectedSequences);
        ReferenceSequence firstSequence = sequences.get(0);
        if (firstSequence == null) {
            throw new IllegalArgumentException("Selected reference sequence must not be null");
        }

        boolean isSegmentedAggregate = sequences.size() > 1;
        Set<String> segmentKeys = new HashSet<>();
        for (ReferenceSequence sequence : sequences) {
            if (sequence == null) {
                throw new IllegalArgumentException("Selected reference sequence must not be null");
            }
            if (!Objects.equals(firstSequence.getTaxId(), sequence.getTaxId())) {
                throw new IllegalArgumentException("Selected reference sequences must have the same tax ID");
            }
            if (isSegmentedAggregate) {
                String segmentKey = sequence.getSegmentKey();
                if (segmentKey == null || segmentKey.isBlank()) {
                    throw new IllegalArgumentException(
                            "Every sequence in a segmented reference must have a segment key");
                }
                if (!segmentKeys.add(segmentKey)) {
                    throw new IllegalArgumentException(
                            "Segmented reference contains duplicate segment key: " + segmentKey);
                }
            }
        }
        if (isSegmentedAggregate) {
            sequences.sort(Comparator.comparingInt(ReferenceSequence::getSegmentOrdinal));
        }

        boolean isCompleteGenome = !isSegmentedAggregate
                && (firstSequence.getSegmentKey() == null || firstSequence.getSegmentKey().isBlank())
                && "COMPLETE".equalsIgnoreCase(firstSequence.getCompleteness());
        ReferenceCompositionStatus compositionStatus = isSegmentedAggregate
                ? ReferenceCompositionStatus.UNKNOWN
                : ReferenceCompositionStatus.SINGLE_SEQUENCE;

        return new ReferenceGenome(
                firstSequence.getTaxId(),
                firstSequence.getOrganismName(),
                sequences,
                isCompleteGenome,
                compositionStatus);
    }

    public Integer getTaxId() {
        return taxId;
    }

    public void setTaxId(Integer taxId) {
        this.taxId = taxId;
    }

    public String getOrganismName() {
        return organismName;
    }

    public void setOrganismName(String organismName) {
        this.organismName = organismName;
    }

    public List<ReferenceSequence> getSequences() {
        return sequences;
    }

    public void setSequences(List<ReferenceSequence> sequences) {
        this.sequences = sequences == null ? new ArrayList<>() : new ArrayList<>(sequences);
    }

    public boolean isComplete() {
        return complete;
    }

    public void setComplete(boolean complete) {
        this.complete = complete;
    }

    public ReferenceCompositionStatus getCompositionStatus() {
        return compositionStatus;
    }

    public void setCompositionStatus(ReferenceCompositionStatus compositionStatus) {
        this.compositionStatus = compositionStatus == null
                ? ReferenceCompositionStatus.UNKNOWN
                : compositionStatus;
    }
}
