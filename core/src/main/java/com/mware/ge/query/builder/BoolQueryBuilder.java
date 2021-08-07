package com.mware.ge.query.builder;

import com.mware.ge.Authorizations;
import com.mware.ge.GeObject;
import org.apache.commons.lang3.BooleanUtils;

import java.util.ArrayList;
import java.util.List;

public class BoolQueryBuilder extends GeQueryBuilder {
    private final List<GeQueryBuilder> andClauses = new ArrayList<>();
    private final List<GeQueryBuilder> orClauses = new ArrayList<>();
    private final List<GeQueryBuilder> notClauses = new ArrayList<>();

    public BoolQueryBuilder() {
    }

    public BoolQueryBuilder and(GeQueryBuilder queryBuilder) {
        if (queryBuilder == null) {
            throw new IllegalArgumentException("inner bool query clause cannot be null");
        }
        andClauses.add(queryBuilder);
        return this;
    }

    public BoolQueryBuilder or(GeQueryBuilder queryBuilder) {
        if (queryBuilder == null) {
            throw new IllegalArgumentException("inner bool query clause cannot be null");
        }
        orClauses.add(queryBuilder);
        return this;
    }

    public BoolQueryBuilder andNot(GeQueryBuilder queryBuilder) {
        if (queryBuilder == null) {
            throw new IllegalArgumentException("inner bool query clause cannot be null");
        }
        notClauses.add(queryBuilder);
        return this;
    }

    public List<GeQueryBuilder> getAndClauses() {
        return andClauses;
    }

    public List<GeQueryBuilder> getNotClauses() {
        return notClauses;
    }

    public List<GeQueryBuilder> getOrClauses() {
        return orClauses;
    }

    @Override
    public boolean matches(GeObject geObject, Authorizations authorizations) {
        List<Boolean> andClauseResults = new ArrayList<>();
        List<Boolean> orClauseResults = new ArrayList<>();
        for (GeQueryBuilder andClause : andClauses) {
            andClauseResults.add(andClause.matches(geObject, authorizations));
        }
        for (GeQueryBuilder notClause : notClauses) {
            andClauseResults.add(!notClause.matches(geObject, authorizations));
        }
        for (GeQueryBuilder orClause : orClauses) {
            orClauseResults.add(orClause.matches(geObject, authorizations));
        }

        if (andClauseResults.size() > 0 && orClauseResults.size() == 0)
            return BooleanUtils.and(andClauseResults.toArray(Boolean[]::new));
        else if (andClauseResults.size() == 0 && orClauseResults.size() > 0)
            return BooleanUtils.or(orClauseResults.toArray(Boolean[]::new));
        else if (andClauseResults.size() == 0 && orClauseResults.size() == 0)
            return false;
        else {
            Boolean and = BooleanUtils.and(andClauseResults.toArray(Boolean[]::new));
            Boolean or = BooleanUtils.or(orClauseResults.toArray(Boolean[]::new));
            return and && or;
        }
    }
}
