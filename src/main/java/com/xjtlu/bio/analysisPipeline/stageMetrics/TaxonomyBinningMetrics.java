package com.xjtlu.bio.analysisPipeline.stageMetrics;

import java.util.ArrayList;
import java.util.List;

/**
 * Assembly-level taxonomy metrics calculated by grouping Kraken2 contig
 * assignments at a common taxonomic rank.
 *
 * <p>Fractions are base-weighted. They must not be interpreted as the
 * fragment percentages from a Kraken2 report, because one fragment in an
 * assembly report is one contig regardless of its length.</p>
 */
public class TaxonomyBinningMetrics {

    private long totalContigs;
    private long totalAssemblyBases;
    private long speciesAssignedContigs;
    private long speciesAssignedBases;
    private long unresolvedContigs;
    private long unresolvedBases;
    private long unclassifiedContigs;
    private long unclassifiedBases;
    private Integer dominantSpeciesTaxId;
    private String dominantSpeciesName;
    private double dominantSpeciesAssemblyFraction;
    private List<TaxonomicBinMetrics> bins = new ArrayList<>();

    public TaxonomyBinningMetrics() {
    }

    public long getTotalContigs() {
        return totalContigs;
    }

    public void setTotalContigs(long totalContigs) {
        this.totalContigs = totalContigs;
    }

    public long getTotalAssemblyBases() {
        return totalAssemblyBases;
    }

    public void setTotalAssemblyBases(long totalAssemblyBases) {
        this.totalAssemblyBases = totalAssemblyBases;
    }

    public long getSpeciesAssignedContigs() {
        return speciesAssignedContigs;
    }

    public void setSpeciesAssignedContigs(long speciesAssignedContigs) {
        this.speciesAssignedContigs = speciesAssignedContigs;
    }

    public long getSpeciesAssignedBases() {
        return speciesAssignedBases;
    }

    public void setSpeciesAssignedBases(long speciesAssignedBases) {
        this.speciesAssignedBases = speciesAssignedBases;
    }

    public long getUnclassifiedContigs() {
        return unclassifiedContigs;
    }

    public long getUnresolvedContigs() {
        return unresolvedContigs;
    }

    public void setUnresolvedContigs(long unresolvedContigs) {
        this.unresolvedContigs = unresolvedContigs;
    }

    public long getUnresolvedBases() {
        return unresolvedBases;
    }

    public void setUnresolvedBases(long unresolvedBases) {
        this.unresolvedBases = unresolvedBases;
    }

    public void setUnclassifiedContigs(long unclassifiedContigs) {
        this.unclassifiedContigs = unclassifiedContigs;
    }

    public long getUnclassifiedBases() {
        return unclassifiedBases;
    }

    public void setUnclassifiedBases(long unclassifiedBases) {
        this.unclassifiedBases = unclassifiedBases;
    }

    public Integer getDominantSpeciesTaxId() {
        return dominantSpeciesTaxId;
    }

    public void setDominantSpeciesTaxId(Integer dominantSpeciesTaxId) {
        this.dominantSpeciesTaxId = dominantSpeciesTaxId;
    }

    public String getDominantSpeciesName() {
        return dominantSpeciesName;
    }

    public void setDominantSpeciesName(String dominantSpeciesName) {
        this.dominantSpeciesName = dominantSpeciesName;
    }

    public double getDominantSpeciesAssemblyFraction() {
        return dominantSpeciesAssemblyFraction;
    }

    public void setDominantSpeciesAssemblyFraction(double dominantSpeciesAssemblyFraction) {
        this.dominantSpeciesAssemblyFraction = dominantSpeciesAssemblyFraction;
    }

    public List<TaxonomicBinMetrics> getBins() {
        return bins;
    }

    public void setBins(List<TaxonomicBinMetrics> bins) {
        this.bins = bins == null ? new ArrayList<>() : new ArrayList<>(bins);
    }

    public static class TaxonomicBinMetrics {

        private int taxId;
        private String name;
        private String rankCode;
        private long contigCount;
        private long totalBases;
        private double assemblyFraction;

        public TaxonomicBinMetrics() {
        }

        public TaxonomicBinMetrics(
                int taxId,
                String name,
                String rankCode,
                long contigCount,
                long totalBases,
                double assemblyFraction) {
            this.taxId = taxId;
            this.name = name;
            this.rankCode = rankCode;
            this.contigCount = contigCount;
            this.totalBases = totalBases;
            this.assemblyFraction = assemblyFraction;
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

        public String getRankCode() {
            return rankCode;
        }

        public void setRankCode(String rankCode) {
            this.rankCode = rankCode;
        }

        public long getContigCount() {
            return contigCount;
        }

        public void setContigCount(long contigCount) {
            this.contigCount = contigCount;
        }

        public long getTotalBases() {
            return totalBases;
        }

        public void setTotalBases(long totalBases) {
            this.totalBases = totalBases;
        }

        public double getAssemblyFraction() {
            return assemblyFraction;
        }

        public void setAssemblyFraction(double assemblyFraction) {
            this.assemblyFraction = assemblyFraction;
        }
    }
}
