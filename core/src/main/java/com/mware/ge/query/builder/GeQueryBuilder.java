package com.mware.ge.query.builder;

import com.mware.ge.Authorizations;
import com.mware.ge.GeObject;
import com.mware.ge.query.Query;
import com.mware.ge.query.QueryBase;
import com.mware.ge.query.SortDirection;
import com.mware.ge.scoring.ScoringStrategy;
import com.mware.ge.sorting.SortingStrategy;

import java.util.ArrayList;
import java.util.List;

public abstract class GeQueryBuilder {
    public static final int DEFAULT_SKIP = 0;

    private long skip = DEFAULT_SKIP;
    private Long limit = null;
    private ScoringStrategy scoringStrategy;
    private Double minScore = null;
    private final List<QueryBase.SortContainer> sortContainers = new ArrayList<>();

    /**
     * Sort the results by the given property name.
     *
     * @param propertyName The property to sort by.
     * @param direction    The direction to sort.
     * @return The query object, allowing you to chain methods.
     */
    public GeQueryBuilder sort(String propertyName, SortDirection direction) {
        sortContainers.add(new QueryBase.PropertySortContainer(propertyName, direction));
        return this;
    }

    /**
     * Sort the results by the given {@link SortingStrategy}.
     *
     * @param sortingStrategy The {@link SortingStrategy} to sort by.
     * @param direction       The direction to sort.
     * @return The query object, allowing you to chain methods.
     */
    public GeQueryBuilder sort(SortingStrategy sortingStrategy, SortDirection direction) {
        sortContainers.add(new QueryBase.SortingStrategySortContainer(sortingStrategy, direction));
        return this;
    }

    public List<Query.SortContainer> getSortContainers() {
        return sortContainers;
    }

    /**
     * Skips the given number of items.
     */
    public <T extends GeQueryBuilder> T skip(int count) {
        this.skip = count;
        return (T) this;
    }

    public long getSkip() {
        return skip;
    }

    /**
     * Limits the number of items returned. null will return all elements.
     */
    public <T extends GeQueryBuilder> T limit(Integer limit) {
        if (limit == null) {
            this.limit = null;
        } else {
            this.limit = (long) limit;
        }
        return (T) this;
    }

    /**
     * Limits the number of items returned. null will return all elements.
     */
    public <T extends GeQueryBuilder> T limit(Long count) {
        this.limit = count;
        return (T) this;
    }

    public Long getLimit() {
        return limit;
    }

    /**
     * Minimum score to return
     */
    public <T extends GeQueryBuilder> T minScore(double minScore) {
        this.minScore = minScore;
        return (T) this;
    }

    public Double getMinScore() {
        return minScore;
    }

    public <T extends GeQueryBuilder> T scoringStrategy(ScoringStrategy scoringStrategy) {
        this.scoringStrategy = scoringStrategy;
        return (T) this;
    }

    public ScoringStrategy getScoringStrategy() {
        return scoringStrategy;
    }

    public abstract boolean matches(GeObject geObject, Authorizations authorizations);
}
