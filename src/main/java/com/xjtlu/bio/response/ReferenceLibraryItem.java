package com.xjtlu.bio.response;

import java.nio.file.InvalidPathException;
import java.nio.file.Path;

import com.xjtlu.bio.entity.BioReferenceSequence;

/** Management projection, not an entity or a historical pipeline snapshot. */
public record ReferenceLibraryItem(Long referenceId, String accession, String sourceDb,
        Integer taxId, Integer orgType, String organismName, Integer genomeLength,
        String segment, String completeness, Boolean isAnnotated, Boolean enabled,
        String fastaObjectName) {
    public static ReferenceLibraryItem from(BioReferenceSequence row) {
        return new ReferenceLibraryItem(row.getReferenceId(), row.getAccession(), row.getSourceDb(),
                row.getTaxId(), row.getOrgType(), row.getOrganismName(), row.getGenomeLength(),
                row.getSegment(), row.getCompleteness(), row.getIsAnnotated(), row.getEnabled(), objectName(row.getPath()));
    }

    private static String objectName(String value) {
        // Legacy rows may contain server paths. A management response exposes
        // only a canonical relative object key, never an absolute filesystem path.
        if (value == null || value.isBlank() || value.contains("\\")
                || value.matches("^[A-Za-z][A-Za-z0-9+.-]*:.*")
                || value.codePoints().anyMatch(Character::isISOControl)) return null;
        try {
            Path path = Path.of(value);
            if (path.isAbsolute() || !value.equals(path.normalize().toString())) return null;
            for (Path part : path) {
                if (part.toString().equals("..")) return null;
            }
            return value;
        } catch (InvalidPathException e) {
            return null;
        }
    }
}
