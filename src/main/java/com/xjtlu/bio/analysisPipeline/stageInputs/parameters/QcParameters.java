package com.xjtlu.bio.analysisPipeline.stageInputs.parameters;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;

/**
 * Internal QC filter snapshots. Request-side overrides are intentionally disabled.
 * Defaults match the installed fastp 1.0.1 / fastplong 0.4.1; making them explicit
 * does not relax filtering. Adapter trimming and other tool options are unchanged.
 */
public class QcParameters extends BaseStageParams {

    private ShortReadFilters shortReadFilters = new ShortReadFilters();
    private LongReadFilters longReadFilters = new LongReadFilters();

    public ShortReadFilters getShortReadFilters() {
        return shortReadFilters;
    }

    @JsonSetter(nulls = Nulls.SKIP)
    public void setShortReadFilters(ShortReadFilters shortReadFilters) {
        this.shortReadFilters = shortReadFilters == null ? new ShortReadFilters() : shortReadFilters;
    }

    public LongReadFilters getLongReadFilters() {
        return longReadFilters;
    }

    @JsonSetter(nulls = Nulls.SKIP)
    public void setLongReadFilters(LongReadFilters longReadFilters) {
        this.longReadFilters = longReadFilters == null ? new LongReadFilters() : longReadFilters;
    }

    /** Shared option names, but each read type owns an independent instance. */
    public static class ReadFilters {
        private int qualifiedQualityPhred = 15;
        private int unqualifiedPercentLimit = 40;
        private int meanQuality = 0;
        private int minimumLength;
        private int maximumLength = 0;
        private int nBaseLimit;

        protected ReadFilters(int minimumLength, int nBaseLimit) {
            this.minimumLength = minimumLength;
            this.nBaseLimit = nBaseLimit;
        }

        public int getQualifiedQualityPhred() {
            return qualifiedQualityPhred;
        }

        @JsonSetter(nulls = Nulls.SKIP)
        public void setQualifiedQualityPhred(int qualifiedQualityPhred) {
            this.qualifiedQualityPhred = qualifiedQualityPhred;
        }

        public int getUnqualifiedPercentLimit() {
            return unqualifiedPercentLimit;
        }

        @JsonSetter(nulls = Nulls.SKIP)
        public void setUnqualifiedPercentLimit(int unqualifiedPercentLimit) {
            this.unqualifiedPercentLimit = unqualifiedPercentLimit;
        }

        public int getMeanQuality() {
            return meanQuality;
        }

        @JsonSetter(nulls = Nulls.SKIP)
        public void setMeanQuality(int meanQuality) {
            this.meanQuality = meanQuality;
        }

        public int getMinimumLength() {
            return minimumLength;
        }

        @JsonSetter(nulls = Nulls.SKIP)
        public void setMinimumLength(int minimumLength) {
            this.minimumLength = minimumLength;
        }

        public int getMaximumLength() {
            return maximumLength;
        }

        @JsonSetter(nulls = Nulls.SKIP)
        public void setMaximumLength(int maximumLength) {
            this.maximumLength = maximumLength;
        }

        public int getNBaseLimit() {
            return nBaseLimit;
        }

        @JsonSetter(nulls = Nulls.SKIP)
        public void setNBaseLimit(int nBaseLimit) {
            this.nBaseLimit = nBaseLimit;
        }
    }

    public static final class ShortReadFilters extends ReadFilters {
        public ShortReadFilters() {
            super(15, 5);
        }
    }

    public static final class LongReadFilters extends ReadFilters {
        private int nPercentLimit = 10;

        public LongReadFilters() {
            super(20, 1_000_000);
        }

        public int getNPercentLimit() {
            return nPercentLimit;
        }

        @JsonSetter(nulls = Nulls.SKIP)
        public void setNPercentLimit(int nPercentLimit) {
            this.nPercentLimit = nPercentLimit;
        }
    }
}
