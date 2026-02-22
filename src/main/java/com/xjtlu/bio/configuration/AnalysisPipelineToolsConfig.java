package com.xjtlu.bio.configuration;


import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;


@Component
@ConfigurationProperties(value = "analysis-pipeline.tools")
public class AnalysisPipelineToolsConfig {

    private List<String> fastp;
    private List<String> spades;

    private List<String> amrfinder;
    private List<String> virulenceFactor;
    private List<String> mlst;
    private List<String> kraken2;

    public List<String> getMlst() {
        return mlst;
    }

    public List<String> getKraken2() {
        return kraken2;
    }

    public void setKraken2(List<String> kraken2) {
        this.kraken2 = kraken2;
    }

    public void setMlst(List<String> mlst) {
        this.mlst = mlst;
    }

    public List<String> getVirulenceFactor() {
        return virulenceFactor;
    }

    public void setVirulenceFactor(List<String> virulenceFactor) {
        this.virulenceFactor = virulenceFactor;
    }

    public List<String> getAmrfinder() {
        return amrfinder;
    }

    public void setAmrfinder(List<String> amrfinder) {
        this.amrfinder = amrfinder;
    }

    public List<String> getFastp() {
        return fastp;
    }

    public void setFastp(List<String> fastp) {
        this.fastp = fastp;
    }

    public List<String> getSpades() {
        return spades;
    }

    public void setSpades(List<String> spades) {
        this.spades = spades;
    }

    public List<String> getMinimap2() {
        return minimap2;
    }

    public void setMinimap2(List<String> minimap2) {
        this.minimap2 = minimap2;
    }

    public List<String> getBcftools() {
        return bcftools;
    }

    public void setBcftools(List<String> bcftools) {
        this.bcftools = bcftools;
    }

    public List<String> getSamtools() {
        return samtools;
    }

    public void setSamtools(List<String> samtools) {
        this.samtools = samtools;
    }

    private List<String> samtools;
    private List<String> minimap2;
    private List<String> bcftools;

}
