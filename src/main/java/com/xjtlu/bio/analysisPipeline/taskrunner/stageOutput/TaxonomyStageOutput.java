package com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput;

import java.nio.file.Path;
import java.util.List;

public class TaxonomyStageOutput implements StageOutput{
    
    


    public static class TaxonomyClassificationOutput{
        private int taxId;
        private String name;
        private int speciesTaxId;
        private String speciesName;
        private double score;
        public TaxonomyClassificationOutput(int taxId, String name, int speciesTaxId, String speciesName,
                double score) {
            this.taxId = taxId;
            this.name = name;
            this.speciesTaxId = speciesTaxId;
            this.speciesName = speciesName;
            this.score = score;
        }
        public int getTaxId() {
            return taxId;
        }
        public void setTaxId(int taxId) {
            this.taxId = taxId;
        }
        public String getName() {
            return name;
        }
        public void setName(String name) {
            this.name = name;
        }
        public int getSpeciesTaxId() {
            return speciesTaxId;
        }
        public void setSpeciesTaxId(int speciesTaxId) {
            this.speciesTaxId = speciesTaxId;
        }
        public String getSpeciesName() {
            return speciesName;
        }
        public void setSpeciesName(String speciesName) {
            this.speciesName = speciesName;
        }
        public double getScore() {
            return score;
        }
        public void setScore(double score) {
            this.score = score;
        }

    }


    private int status;
    private TaxonomyClassificationOutput comfirmedTaxonomy;
    private List<TaxonomyClassificationOutput> candicates;


    private String evidenceResource;






    public int getStatus() {
        return status;
    }


    public void setStatus(int status) {
        this.status = status;
    }


    public TaxonomyClassificationOutput getComfirmedTaxonomy() {
        return comfirmedTaxonomy;
    }


    public void setComfirmedTaxonomy(TaxonomyClassificationOutput comfirmedTaxonomy) {
        this.comfirmedTaxonomy = comfirmedTaxonomy;
    }


    public List<TaxonomyClassificationOutput> getCandicates() {
        return candicates;
    }


    public void setCandicates(List<TaxonomyClassificationOutput> candicates) {
        this.candicates = candicates;
    }


    public String getEvidenceResource() {
        return evidenceResource;
    }


    public void setEvidenceResource(String evidenceResource) {
        this.evidenceResource = evidenceResource;
    }


    @Override
    public Path getParentPath() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getParentPath'");
    }


    public TaxonomyStageOutput(){

    }

    public TaxonomyStageOutput(List<TaxonomyClassificationOutput> candicates, TaxonomyClassificationOutput comfirmed, int status, String evidenceResource){
        this.candicates = candicates;
        this.comfirmedTaxonomy = comfirmed;
        this.status = status;
        this.evidenceResource = evidenceResource;
    }


    


    // @Override
    // public Path getParentPath() {
    //     // TODO Auto-generated method stub
    //     return output.getParent();
    // }


}
