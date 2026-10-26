package com.xjtlu.bio.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.MultipartHttpServletRequest;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.CoercionAction;
import com.fasterxml.jackson.databind.cfg.CoercionInputShape;
import com.fasterxml.jackson.databind.type.LogicalType;
import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.requestParameters.BatchPipelineInputUploadRequest;
import com.xjtlu.bio.response.BatchPipelineInputUploadResponse;
import com.xjtlu.bio.service.PipelineInputBatchService;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.Part;

/** Strict multipart contract for the initial atomic, complete sample-input upload. */
@RestController
@RequestMapping("/pipelineInput")
public class PipelineInputBatchController {
    private static final String MANIFEST = "manifest";
    private static final int MAX_MANIFEST_BYTES = 1024 * 1024;

    private final PipelineInputBatchService service;
    private final ObjectMapper manifestMapper;

    public PipelineInputBatchController(PipelineInputBatchService service, ObjectMapper objectMapper) {
        this.service = service;
        this.manifestMapper = objectMapper.copy()
                .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
                .disable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
                .disable(MapperFeature.ALLOW_COERCION_OF_SCALARS);
        this.manifestMapper.coercionConfigFor(LogicalType.Textual)
                .setCoercion(CoercionInputShape.Integer, CoercionAction.Fail)
                .setCoercion(CoercionInputShape.Float, CoercionAction.Fail)
                .setCoercion(CoercionInputShape.Boolean, CoercionAction.Fail);
    }

    @PostMapping(value = "/uploadBatch", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<Result<BatchPipelineInputUploadResponse>> uploadBatch(
            MultipartHttpServletRequest request) {
        BatchPipelineInputUploadRequest manifest;
        try {
            manifest = readManifest(request);
        } catch (IllegalArgumentException | IOException | ServletException ex) {
            return ResponseEntity.badRequest().body(new Result<>(Result.PARAMETER_NOT_VALID, null,
                    "上传清单格式不正确：仅接受一个 application/json manifest，字段及类型须符合批量上传协议"));
        }

        MultiValueMap<String, MultipartFile> files = new LinkedMultiValueMap<>();
        request.getMultiFileMap().forEach((name, values) -> {
            if (!MANIFEST.equals(name)) {
                // Keep duplicate file parts visible to the service's one-to-one validation.
                files.put(name, new ArrayList<>(values));
            }
        });
        Result<BatchPipelineInputUploadResponse> result = service.uploadBatch(manifest, files);
        return result.getStatus() == Result.INTERNAL_FAIL
                ? ResponseEntity.internalServerError().body(result)
                : ResponseEntity.ok(result);
    }

    private BatchPipelineInputUploadRequest readManifest(MultipartHttpServletRequest request)
            throws IOException, ServletException {
        Collection<Part> parts = request.getParts();
        List<Part> manifestParts = new ArrayList<>();
        int plainManifestCount = 0;
        int fileManifestCount = 0;
        for (Part part : parts) {
            if (MANIFEST.equals(part.getName())) {
                manifestParts.add(part);
                if (part.getSubmittedFileName() == null) {
                    plainManifestCount++;
                } else {
                    fileManifestCount++;
                }
            } else if (part.getSubmittedFileName() == null) {
                throw new IllegalArgumentException("Extra text part");
            }
        }

        List<MultipartFile> manifestFiles = request.getFiles(MANIFEST);
        // Servlet requests expose a file through both APIs; MockMvc file() may expose only the map.
        if (plainManifestCount + Math.max(fileManifestCount, manifestFiles.size()) != 1) {
            throw new IllegalArgumentException("Exactly one manifest part is required");
        }
        for (String parameter : request.getParameterMap().keySet()) {
            if (!MANIFEST.equals(parameter) || plainManifestCount != 1
                    || request.getParameterValues(parameter).length != 1) {
                throw new IllegalArgumentException("Extra or duplicate text parameter");
            }
        }

        if (!manifestParts.isEmpty()) {
            Part part = manifestParts.get(0);
            validateManifestMediaType(part.getContentType());
            try (InputStream input = part.getInputStream()) {
                return parseManifest(input);
            }
        }
        MultipartFile file = manifestFiles.get(0);
        validateManifestMediaType(file.getContentType());
        try (InputStream input = file.getInputStream()) {
            return parseManifest(input);
        }
    }

    private BatchPipelineInputUploadRequest parseManifest(InputStream input) throws IOException {
        byte[] bytes = input.readNBytes(MAX_MANIFEST_BYTES + 1);
        if (bytes.length > MAX_MANIFEST_BYTES) {
            throw new IllegalArgumentException("Manifest exceeds the metadata limit");
        }
        return manifestMapper.readValue(bytes, BatchPipelineInputUploadRequest.class);
    }

    private static void validateManifestMediaType(String contentType) {
        if (contentType == null) {
            throw new IllegalArgumentException("Manifest content type is required");
        }
        MediaType type = MediaType.parseMediaType(contentType);
        if (!"application".equalsIgnoreCase(type.getType()) || !"json".equalsIgnoreCase(type.getSubtype())) {
            throw new IllegalArgumentException("Manifest must be application/json");
        }
    }
}
