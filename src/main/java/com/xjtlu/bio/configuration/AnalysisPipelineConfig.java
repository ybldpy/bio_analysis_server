package com.xjtlu.bio.configuration;


import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;
import java.util.Map;

@ConfigurationProperties(value = "analysisPipeline")
public class AnalysisPipelineConfig {

    private Map<String, List<String>> tools;

    public Map<String, List<String>> getTools() {
        return tools;
    }

    public void setTools(Map<String, List<String>> tools) {
        this.tools = tools;
    }
}
