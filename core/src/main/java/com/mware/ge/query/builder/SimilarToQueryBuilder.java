package com.mware.ge.query.builder;

import com.mware.ge.Authorizations;
import com.mware.ge.GeObject;

public class SimilarToQueryBuilder extends GeQueryBuilder {
    private final String[] propertyNames;
    private final String text;
    private Integer minTermFrequency;
    private Integer maxQueryTerms;
    private Integer minDocFrequency;
    private Integer maxDocFrequency;
    private Float boost;

    protected SimilarToQueryBuilder(String[] propertyNames, String text) {
        this.propertyNames = propertyNames;
        this.text = text;
    }

    public String[] getPropertyNames() {
        return propertyNames;
    }

    public String getText() {
        return text;
    }

    /**
     * The minimum number of times a term must appear in the source data to be considered for a match.
     */
    public SimilarToQueryBuilder minTermFrequency(int minTermFrequency) {
        this.minTermFrequency = minTermFrequency;
        return this;
    }

    public Integer getMinTermFrequency() {
        return minTermFrequency;
    }

    /**
     * The maximum number of terms to be searched for.
     */
    public SimilarToQueryBuilder maxQueryTerms(int maxQueryTerms) {
        this.maxQueryTerms = maxQueryTerms;
        return this;
    }

    public Integer getMaxQueryTerms() {
        return maxQueryTerms;
    }

    /**
     * The minimum number of documents a term must be in to be considered for a similarity match.
     */
    public SimilarToQueryBuilder minDocFrequency(int minDocFrequency) {
        this.minDocFrequency = minDocFrequency;
        return this;
    }

    public Integer getMinDocFrequency() {
        return minDocFrequency;
    }

    /**
     * The maximum number of documents a term must be in to be considered for a similarity match.
     */
    public SimilarToQueryBuilder maxDocFrequency(int maxDocFrequency) {
        this.maxDocFrequency = maxDocFrequency;
        return this;
    }

    public Integer getMaxDocFrequency() {
        return maxDocFrequency;
    }

    /**
     * The amount of boost to apply to the similarity query.
     */
    public SimilarToQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    public Float getBoost() {
        return boost;
    }

    @Override
    public boolean matches(GeObject geObject, Authorizations authorizations) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public GeQueryBuilder clone() {
        SimilarToQueryBuilder qb = new SimilarToQueryBuilder(propertyNames, text);
        return qb.minTermFrequency(minTermFrequency)
                .maxQueryTerms(maxQueryTerms)
                .minDocFrequency(minDocFrequency)
                .maxDocFrequency(maxDocFrequency)
                .boost(boost);
    }
}
