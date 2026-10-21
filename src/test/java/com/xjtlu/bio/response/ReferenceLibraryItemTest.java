package com.xjtlu.bio.response;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xjtlu.bio.entity.BioReferenceSequence;

class ReferenceLibraryItemTest {
    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "/server/private.fna", "../private.fna", "folder/../private.fna",
            "./file.fna", "folder//file.fna", "folder/", "C:\\private\\file.fna", "C:/private/file.fna",
            "file:///private/file.fna", "https://example.test/file.fna", "file\n.fna", ".", ".."})
    void hidesMissingOrNonObjectPaths(String path) {
        BioReferenceSequence row = new BioReferenceSequence();
        row.setPath(path);
        assertNull(ReferenceLibraryItem.from(row).fastaObjectName());
        assertEquals(path, row.getPath(), "Projection must not mutate a historical row");
    }

    @ParameterizedTest
    @ValueSource(strings = {"refseq/custom/test/reference.fna", "refseq/virus/test.fna", "文件.fna"})
    void retainsCanonicalRelativeObjectKeys(String path) {
        BioReferenceSequence row = new BioReferenceSequence();
        row.setPath(path);
        assertEquals(path, ReferenceLibraryItem.from(row).fastaObjectName());
    }

    @Test
    void responseKeepsDisabledStatusButNotEntityBlobOrInternalAnnotationPath() throws Exception {
        BioReferenceSequence row = new BioReferenceSequence();
        row.setReferenceId(42L);
        row.setAccession("CUSTOM_test");
        row.setEnabled(false);
        row.setRawMetadata("private metadata");
        row.setAnnotationFile("/server/private.gff");
        row.setPath("refseq/custom/test/reference.fna");
        JsonNode response = new ObjectMapper().valueToTree(ReferenceLibraryItem.from(row));
        assertEquals(42L, response.path("referenceId").asLong());
        assertFalse(response.path("enabled").asBoolean());
        assertEquals("refseq/custom/test/reference.fna", response.path("fastaObjectName").asText());
        assertFalse(response.has("rawMetadata"));
        assertFalse(response.has("annotationFile"));
        assertFalse(response.has("path"));
    }
}
