package com.xjtlu.bio.analysisPipeline.referenceGenome;

import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Snapshot of one reference sequence used by an analysis pipeline.
 *
 * <p>A sequence may represent a complete unsegmented genome or one segment of
 * a segmented genome. This object deliberately does not depend on the
 * persistence-layer entity.</p>
 */
public class ReferenceSequence {
    private static final String SEGMENT_KEY_PREFIX = "SEGMENT_";
    private static final Pattern SEGMENT_KEY_PATTERN = Pattern.compile("^SEGMENT_([1-9]\\d*)$");

    private Long referenceId;
    private String accession;
    private String sourceDb;
    private Integer taxId;
    private String organismName;
    private Integer genomeLength;
    private String completeness;
    private Boolean isAnnotated;
    private Integer geneCount;
    private Integer proteinCount;
    private String segment;
    private Integer segmentOrdinal;
    private String bioproject;
    private Date releaseDate;
    private Date updateDate;
    private Integer orgType;
    private String path;
    private String annotationFile;
    private String rawMetadata;

    public ReferenceSequence() {
    }

    public ReferenceSequence(Long referenceId, String accession, String sourceDb, Integer taxId,
            String organismName, Integer genomeLength, String completeness, Boolean isAnnotated,
            Integer geneCount, Integer proteinCount, String segment, String bioproject,
            Date releaseDate, Date updateDate, Integer orgType, String path,
            String annotationFile, String rawMetadata) {
        this.referenceId = referenceId;
        this.accession = accession;
        this.sourceDb = sourceDb;
        this.taxId = taxId;
        this.organismName = organismName;
        this.genomeLength = genomeLength;
        this.completeness = completeness;
        this.isAnnotated = isAnnotated;
        this.geneCount = geneCount;
        this.proteinCount = proteinCount;
        this.segment = segment;
        this.bioproject = bioproject;
        this.releaseDate = releaseDate;
        this.updateDate = updateDate;
        this.orgType = orgType;
        this.path = path;
        this.annotationFile = annotationFile;
        this.rawMetadata = rawMetadata;
    }

    /** @deprecated Segment identity is now derived from an explicit ordinal. */
    @Deprecated
    public static String normalizeSegmentKey(String rawSegment) {
        return segmentKeyForOrdinal(SegmentOrdinalResolver.resolveExplicitOrdinal(rawSegment));
    }

    public static String segmentKeyForOrdinal(Integer segmentOrdinal) {
        if (segmentOrdinal == null) {
            return null;
        }
        if (segmentOrdinal <= 0) {
            throw new IllegalArgumentException("Segment ordinal must be greater than zero");
        }
        return SEGMENT_KEY_PREFIX + segmentOrdinal;
    }

    public static Integer segmentOrdinalFromKey(String segmentKey) {
        if (segmentKey == null || segmentKey.isBlank()) {
            return null;
        }
        Matcher matcher = SEGMENT_KEY_PATTERN.matcher(segmentKey);
        if (!matcher.matches()) {
            return null;
        }
        try {
            return Integer.valueOf(matcher.group(1));
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    public Long getReferenceId() {
        return referenceId;
    }

    public void setReferenceId(Long referenceId) {
        this.referenceId = referenceId;
    }

    public String getAccession() {
        return accession;
    }

    public void setAccession(String accession) {
        this.accession = accession;
    }

    public String getSourceDb() {
        return sourceDb;
    }

    public void setSourceDb(String sourceDb) {
        this.sourceDb = sourceDb;
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

    public Integer getGenomeLength() {
        return genomeLength;
    }

    public void setGenomeLength(Integer genomeLength) {
        this.genomeLength = genomeLength;
    }

    public String getCompleteness() {
        return completeness;
    }

    public void setCompleteness(String completeness) {
        this.completeness = completeness;
    }

    public Boolean getIsAnnotated() {
        return isAnnotated;
    }

    public void setIsAnnotated(Boolean isAnnotated) {
        this.isAnnotated = isAnnotated;
    }

    public Integer getGeneCount() {
        return geneCount;
    }

    public void setGeneCount(Integer geneCount) {
        this.geneCount = geneCount;
    }

    public Integer getProteinCount() {
        return proteinCount;
    }

    public void setProteinCount(Integer proteinCount) {
        this.proteinCount = proteinCount;
    }

    public String getSegment() {
        return segment;
    }

    public void setSegment(String segment) {
        this.segment = segment;
    }

    public Integer getSegmentOrdinal() {
        return segmentOrdinal;
    }

    public void setSegmentOrdinal(Integer segmentOrdinal) {
        if (segmentOrdinal != null && segmentOrdinal <= 0) {
            throw new IllegalArgumentException("Segment ordinal must be greater than zero");
        }
        this.segmentOrdinal = segmentOrdinal;
    }

    public String getSegmentKey() {
        return segmentKeyForOrdinal(segmentOrdinal);
    }

    /** Accepts legacy serialized numeric keys; segmentOrdinal remains the source of truth. */
    public void setSegmentKey(String segmentKey) {
        if (segmentOrdinal == null) {
            segmentOrdinal = segmentOrdinalFromKey(segmentKey);
        }
    }

    public String getBioproject() {
        return bioproject;
    }

    public void setBioproject(String bioproject) {
        this.bioproject = bioproject;
    }

    public Date getReleaseDate() {
        return releaseDate;
    }

    public void setReleaseDate(Date releaseDate) {
        this.releaseDate = releaseDate;
    }

    public Date getUpdateDate() {
        return updateDate;
    }

    public void setUpdateDate(Date updateDate) {
        this.updateDate = updateDate;
    }

    public Integer getOrgType() {
        return orgType;
    }

    public void setOrgType(Integer orgType) {
        this.orgType = orgType;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getAnnotationFile() {
        return annotationFile;
    }

    public void setAnnotationFile(String annotationFile) {
        this.annotationFile = annotationFile;
    }

    public String getRawMetadata() {
        return rawMetadata;
    }

    public void setRawMetadata(String rawMetadata) {
        this.rawMetadata = rawMetadata;
    }
}
