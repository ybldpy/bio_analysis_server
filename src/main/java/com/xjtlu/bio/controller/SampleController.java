package com.xjtlu.bio.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;


class BuildPipelineParams{

    private String sampleName;
    

}

@Controller
@RequestMapping("/sample")
public class SampleController {





    @PostMapping("buildPipeline")
    public String buildPipeline() {
        //TODO: process POST request
    }
    

}
