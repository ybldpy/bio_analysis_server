package com.xjtlu.bio.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

import com.xjtlu.bio.requestParameters.ReferenceGenomeQuery;
import com.xjtlu.bio.service.RefSeqService;

import jakarta.annotation.Resource;

@Controller
@RequestMapping("/referenceGenome")
public class BioReferenceGenomeController {



    @Resource
    private RefSeqService refSeqService;

    @PostMapping("/queryReferenceGenomes")
    public ResponseEntity queryReferenceGenomes(@RequestBody ReferenceGenomeQuery referenceGenomeQuery){
        return ResponseEntity.ok(refSeqService.queryReferenceGenomes(referenceGenomeQuery));

    }

}
