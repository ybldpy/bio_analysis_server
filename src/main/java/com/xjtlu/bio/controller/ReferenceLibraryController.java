package com.xjtlu.bio.controller;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.BindException;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.support.MissingServletRequestPartException;

import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.requestParameters.ReferenceEnabledRequest;
import com.xjtlu.bio.requestParameters.ReferenceLibraryQuery;
import com.xjtlu.bio.requestParameters.UploadReferenceRequest;
import com.xjtlu.bio.service.ReferenceLibraryService;

/** New management endpoints; the old BioRefseq query contract is unchanged. */
@RestController
@RequestMapping("/referenceGenome/library")
public class ReferenceLibraryController {
    private final ReferenceLibraryService service;

    public ReferenceLibraryController(ReferenceLibraryService service) { this.service = service; }

    @InitBinder("uploadReferenceRequest")
    public void bindUploadMetadata(WebDataBinder binder) {
        binder.setAllowedFields("accession", "taxId", "orgType", "organismName", "segment", "completeness", "enabled");
    }

    @PostMapping("/query")
    public ResponseEntity<?> query(@RequestBody(required = false) ReferenceLibraryQuery request) {
        return respond(service.query(request));
    }

    @GetMapping("/{referenceId}")
    public ResponseEntity<?> detail(@PathVariable long referenceId) {
        return respond(service.detail(referenceId));
    }

    @PostMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<?> upload(@RequestParam("file") MultipartFile file,
            @ModelAttribute("uploadReferenceRequest") UploadReferenceRequest metadata, BindingResult binding) {
        if (binding.hasErrors()) {
            return ResponseEntity.badRequest().body(new Result<>(Result.PARAMETER_NOT_VALID, null, "上传元数据字段类型不正确"));
        }
        return respond(service.upload(metadata, file));
    }

    @PostMapping("/{referenceId}/enabled")
    public ResponseEntity<?> setEnabled(@PathVariable long referenceId,
            @RequestBody ReferenceEnabledRequest request) {
        return respond(service.setEnabled(referenceId, request == null ? null : request.getEnabled()));
    }

    @ExceptionHandler({HttpMessageNotReadableException.class, BindException.class,
            MissingServletRequestParameterException.class, MissingServletRequestPartException.class,
            MethodArgumentTypeMismatchException.class})
    public ResponseEntity<?> invalidRequest(Exception error) {
        return ResponseEntity.badRequest().body(new Result<>(Result.PARAMETER_NOT_VALID, null, "请求格式或字段类型不正确"));
    }

    private ResponseEntity<?> respond(Result<?> result) {
        return ResponseEntity.status(result.getStatus() == Result.INTERNAL_FAIL ? 500 : 200).body(result);
    }
}
