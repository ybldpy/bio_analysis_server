package com.xjtlu.bio.configuration;


import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;


@Component
@ConfigurationProperties(value = "analysis-pipeline.tools")
public class AnalysisPipelineToolsConfig {

    private List<String> fastp;
    private List<String> spades;

    private List<String> amrfinder;
    private List<String> virulenceFactor;
    public List<String> getVep() {
        return new ArrayList(vep);
    }

    public void setVep(List<String> vep) {
        this.vep = vep;
    }

    private List<String> mlst;
    private List<String> kraken2;

    private List<String> seqsero2;
    private List<String> vep;

    public List<String> getEctyper() {
        return ectyper;
    }

    public void setEctyper(List<String> ectyper) {
        this.ectyper = ectyper;
    }

    public List<String> getKaptive() {
        return new ArrayList(kaptive);
    }

    public void setKaptive(List<String> kaptive) {
        this.kaptive = kaptive;
    }

    public List<String> getSeroBA() {
        return new ArrayList(seroBA);
    }

    public void setSeroBA(List<String> seroBA) {
        this.seroBA = seroBA;
    }

    private List<String> ectyper;

    private List<String> kaptive;

    private List<String> seroBA;
    
     

    public List<String> getSeqsero2() {
        return new ArrayList(seqsero2);
    }

    public void setSeqsero2(List<String> seqsero2) {
        this.seqsero2 = seqsero2;
    }

    public List<String> getMlst() {
        return new ArrayList(mlst);
    }

    public List<String> getKraken2() {
        return new ArrayList(kraken2);
    }

    public void setKraken2(List<String> kraken2) {
        this.kraken2 = kraken2;
    }

    public void setMlst(List<String> mlst) {
        this.mlst = mlst;
    }

    public List<String> getVirulenceFactor() {
        return new ArrayList(virulenceFactor);
    }

    public void setVirulenceFactor(List<String> virulenceFactor) {
        this.virulenceFactor = virulenceFactor;
    }

    public List<String> getAmrfinder() {
        return new ArrayList(amrfinder);
    }

    public void setAmrfinder(List<String> amrfinder) {
        this.amrfinder = amrfinder;
    }

    public List<String> getFastp() {
        return new ArrayList(fastp);
    }

    public void setFastp(List<String> fastp) {
        this.fastp = fastp;
    }

    public List<String> getSpades() {
        return new ArrayList(spades);
    }

    public void setSpades(List<String> spades) {
        this.spades = spades;
    }

    public List<String> getMinimap2() {
        return new ArrayList(minimap2);
    }

    public void setMinimap2(List<String> minimap2) {
        this.minimap2 = minimap2;
    }

    public List<String> getBcftools() {
        return new ArrayList(bcftools);
    }

    public void setBcftools(List<String> bcftools) {
        this.bcftools = bcftools;
    }

    public List<String> getSamtools() {
        return new ArrayList(samtools);
    }

    public void setSamtools(List<String> samtools) {
        this.samtools = samtools;
    }

    private List<String> samtools;
    private List<String> minimap2;
    private List<String> bcftools;

}
