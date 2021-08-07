package com.mware.ge.query.builder;

import com.mware.ge.Authorizations;
import com.mware.ge.GeObject;

public class SimilarToQueryBuilder extends GeQueryBuilder {
    private final String[] similarToFields;
    private final String similarToText;
    private Integer minTermFrequency;
    private Integer maxQueryTerms;
    private Integer minDocFrequency;
    private Integer maxDocFrequency;
    private Float boost;

    public SimilarToQueryBuilder(String[] similarToFields, String similarToText) {
        this.similarToFields = similarToFields;
        this.similarToText = similarToText;
    }

    /**
     * The minimum number of times a term must appear in the source data to be considered for a match.
     */
    public SimilarToQueryBuilder minTermFrequency(int minTermFrequency) {
        this.minTermFrequency = minTermFrequency;
        return this;
    }

    /**
     * The maximum number of terms to be searched for.
     */
    public SimilarToQueryBuilder maxQueryTerms(int maxQueryTerms) {
        this.maxQueryTerms = maxQueryTerms;
        return this;
    }

    /**
     * The minimum number of documents a term must be in to be considered for a similarity match.
     */
    public SimilarToQueryBuilder minDocFrequency(int minDocFrequency) {
        this.minDocFrequency = minDocFrequency;
        return this;
    }

    /**
     * The maximum number of documents a term must be in to be considered for a similarity match.
     */
    public SimilarToQueryBuilder maxDocFrequency(int maxDocFrequency) {
        this.maxDocFrequency = maxDocFrequency;
        return this;
    }

    /**
     * The amount of boost to apply to the similarity query.
     */
    public SimilarToQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    @Override
    public boolean matches(GeObject geObject, Authorizations authorizations) {
        throw new UnsupportedOperationException("Not supported");
    }


}
