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


    private List getCopy(List tool){
        return new ArrayList<>(tool);
    }


    public List<String> getFastANI() {
        return getCopy(fastANI);
    }

    public void setFastANI(List<String> fastANI) {
        this.fastANI = fastANI;
    }

    private List<String> fastANI;


    
    public List<String> getVep() {
        return getCopy(vep);
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
        return getCopy(kaptive);
    }

    public void setKaptive(List<String> kaptive) {
        this.kaptive = kaptive;
    }

    public List<String> getSeroBA() {
        return getCopy(seroBA);
    }

    public void setSeroBA(List<String> seroBA) {
        this.seroBA = seroBA;
    }

    private List<String> ectyper;

    private List<String> kaptive;

    private List<String> seroBA;
    
     

    public List<String> getSeqsero2() {
        return getCopy(seqsero2);
    }

    public void setSeqsero2(List<String> seqsero2) {
        this.seqsero2 = seqsero2;
    }

    public List<String> getMlst() {
        return getCopy(mlst);
    }

    public List<String> getKraken2() {
        return getCopy(kraken2);
    }

    public void setKraken2(List<String> kraken2) {
        this.kraken2 = kraken2;
    }

    public void setMlst(List<String> mlst) {
        this.mlst = mlst;
    }

    public List<String> getVirulenceFactor() {
        return getCopy(virulenceFactor);
    }

    public void setVirulenceFactor(List<String> virulenceFactor) {
        this.virulenceFactor = virulenceFactor;
    }

    public List<String> getAmrfinder() {
        return getCopy(amrfinder);
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
        return getCopy(bcftools);
    }

    public void setBcftools(List<String> bcftools) {
        this.bcftools = bcftools;
    }

    public List<String> getSamtools() {
        return getCopy(samtools);
    }

    public void setSamtools(List<String> samtools) {
        this.samtools = samtools;
    }

    public List<String> getFastplong() {
        return fastplong;
    }

    public void setFastplong(List<String> fastplong) {
        this.fastplong = fastplong;
    }

    


    private List<String> dada2;

    private List<String> fastplong;

    private List<String> samtools;
    public List<String> getDada2Taxonomy() {
        return dada2Taxonomy;
    }


    public void setDada2Taxonomy(List<String> dada2Taxonomy) {
        this.dada2Taxonomy = dada2Taxonomy;
    }

    private List<String> minimap2;
    private List<String> bcftools;

    private List<String> dada2Taxonomy;

    private List<String> amplicon16sSummary;

    private List<String> bowtie2;

    private List<String> bracken;
    
    
    
    


    public List<String> getDada2() {
        return dada2;
    }


    public void setDada2(List<String> dada2) {
        this.dada2 = dada2;
    }


    public List<String> getAmplicon16sSummary() {
        return amplicon16sSummary;
    }


    public void setAmplicon16sSummary(List<String> amplicon16sSummary) {
        this.amplicon16sSummary = amplicon16sSummary;
    }


    public List<String> getBowtie2() {
        return bowtie2;
    }


    public void setBowtie2(List<String> bowtie2) {
        this.bowtie2 = bowtie2;
    }


    public List<String> getBracken() {
        return bracken;
    }


    public void setBracken(List<String> bracken) {
        this.bracken = bracken;
    }

}
