package com.xjtlu.bio.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.xjtlu.bio.common.Result;

/** Bounded, artificial multipart fixtures for the real HTTP upload integration test. */
final class ReferenceLibraryHttpUploadCases {

    private ReferenceLibraryHttpUploadCases() {
    }

    record UploadCase(String name, String filename, byte[] content, Map<String, String> fields,
            int expectedHttp, int expectedStatus, byte[] expectedStoredBytes) {
        UploadCase {
            fields = Map.copyOf(fields);
        }
    }

    static List<UploadCase> cases(String runMarker, int taxId, byte[] fixture) {
        List<UploadCase> cases = new ArrayList<>();

        Map<String, String> plainFields = fields(runMarker, taxId);
        plainFields.remove("accession");
        cases.add(new UploadCase("plain_auto_accession", "fixture.fa", fixture, plainFields,
                200, Result.SUCCESS, fixture));

        Map<String, String> gzipFields = fields(runMarker, taxId);
        byte[] compressed = gzip(fixture);
        cases.add(new UploadCase("gzip_explicit_accession", "fixture.fna.gz", compressed, gzipFields,
                200, Result.SUCCESS, fixture));
        cases.add(new UploadCase("zip_explicit_accession", "fixture.zip", zip(fixture), fields(runMarker, taxId),
                200, Result.SUCCESS, fixture));
        cases.add(new UploadCase("duplicate_disabled_accession", "fixture.fna.gz", compressed, gzipFields,
                200, Result.DUPLICATE_OPERATION, null));

        cases.add(invalid("missing_file", "fixture.fa", null, fields(runMarker, taxId), 400));
        cases.add(invalid("empty_file", "fixture.fa", new byte[0], fields(runMarker, taxId), 200));
        cases.add(invalid("missing_fasta_header", "fixture.fa", bytes("ACGT\n"),
                fields(runMarker, taxId), 200));
        cases.add(invalid("invalid_sequence_character", "fixture.fa", bytes(">fixture\nAC$T\n"),
                fields(runMarker, taxId), 200));
        cases.add(invalid("missing_sequence", "fixture.fa", bytes(">fixture\n"),
                fields(runMarker, taxId), 200));
        cases.add(invalid("duplicate_record_ids", "fixture.fa", bytes(">same first\nAC\n>same second\nGT\n"),
                fields(runMarker, taxId), 200));
        cases.add(invalid("unsupported_extension", "fixture.txt", fixture, fields(runMarker, taxId), 200));
        cases.add(invalid("corrupt_gzip", "fixture.fa.gz", bytes("not a gzip archive"),
                fields(runMarker, taxId), 200));

        cases.add(invalid("missing_tax_id", "fixture.fa", fixture,
                changed(fields(runMarker, taxId), "taxId", null), 200));
        cases.add(invalid("nonpositive_tax_id", "fixture.fa", fixture,
                changed(fields(runMarker, taxId), "taxId", "0"), 200));
        cases.add(invalid("malformed_tax_id", "fixture.fa", fixture,
                changed(fields(runMarker, taxId), "taxId", "not-an-integer"), 400));
        cases.add(invalid("missing_org_type", "fixture.fa", fixture,
                changed(fields(runMarker, taxId), "orgType", null), 200));
        cases.add(invalid("obsolete_org_type_two", "fixture.fa", fixture,
                changed(fields(runMarker, taxId), "orgType", "2"), 200));
        cases.add(invalid("malformed_org_type", "fixture.fa", fixture,
                changed(fields(runMarker, taxId), "orgType", "not-an-integer"), 400));
        cases.add(invalid("malformed_enabled", "fixture.fa", fixture,
                changed(fields(runMarker, taxId), "enabled", "not-a-boolean"), 400));
        cases.add(invalid("invalid_accession", "fixture.fa", fixture,
                changed(fields(runMarker, taxId), "accession", accession() + " invalid"), 200));

        cases.add(invalid("partial_target_context", "fixture.fa", fixture,
                changed(fields(runMarker, taxId), "targetTaxId", Integer.toString(taxId)), 200));
        Map<String, String> unsupportedPipelineFields = fields(runMarker, taxId);
        unsupportedPipelineFields.put("targetTaxId", Integer.toString(taxId));
        unsupportedPipelineFields.put("pipelineType", "400");
        cases.add(invalid("unsupported_pipeline_type", "fixture.fa", fixture, unsupportedPipelineFields, 200));
        cases.add(invalid("confirmation_without_target", "fixture.fa", fixture,
                changed(fields(runMarker, taxId), "confirmAddTaxonomyMember", "true"), 200));

        return List.copyOf(cases);
    }

    private static UploadCase invalid(String name, String filename, byte[] content,
            Map<String, String> fields, int expectedHttp) {
        return new UploadCase(name, filename, content, fields, expectedHttp, Result.PARAMETER_NOT_VALID, null);
    }

    private static Map<String, String> fields(String runMarker, int taxId) {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("accession", accession());
        fields.put("taxId", Integer.toString(taxId));
        fields.put("orgType", "0");
        fields.put("enabled", "false");
        fields.put("organismName", runMarker);
        return fields;
    }

    private static Map<String, String> changed(Map<String, String> fields, String name, String value) {
        if (value == null) {
            fields.remove(name);
        } else {
            fields.put(name, value);
        }
        return fields;
    }

    private static String accession() {
        return "CUSTOM_HTTP_" + UUID.randomUUID().toString().replace("-", "");
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] gzip(byte[] content) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (GZIPOutputStream output = new GZIPOutputStream(bytes)) {
                output.write(content);
            }
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot prepare in-memory gzip fixture", e);
        }
    }

    private static byte[] zip(byte[] content) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (ZipOutputStream output = new ZipOutputStream(bytes)) {
                output.putNextEntry(new ZipEntry("fixture.fa"));
                output.write(content);
                output.closeEntry();
            }
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot prepare in-memory ZIP fixture", e);
        }
    }
}
