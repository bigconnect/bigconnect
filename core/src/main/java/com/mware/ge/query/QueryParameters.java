/*
 * This file is part of the BigConnect project.
 *
 * Copyright (c) 2013-2020 MWARE SOLUTIONS SRL
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License version 3
 * as published by the Free Software Foundation with the addition of the
 * following permission added to Section 15 as permitted in Section 7(a):
 * FOR ANY PART OF THE COVERED WORK IN WHICH THE COPYRIGHT IS OWNED BY
 * MWARE SOLUTIONS SRL, MWARE SOLUTIONS SRL DISCLAIMS THE WARRANTY OF
 * NON INFRINGEMENT OF THIRD PARTY RIGHTS

 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program; if not, see http://www.gnu.org/licenses or write to
 * the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
 * Boston, MA, 02110-1301 USA, or download the license from the following URL:
 * https://www.gnu.org/licenses/agpl-3.0.txt
 *
 * The interactive user interfaces in modified source and object code versions
 * of this program must display Appropriate Legal Notices, as required under
 * Section 5 of the GNU Affero General Public License.
 *
 * You can be released from the requirements of the license by purchasing
 * a commercial license. Buying such a license is mandatory as soon as you
 * develop commercial activities involving the BigConnect software without
 * disclosing the source code of your own applications.
 *
 * These activities include: offering paid services to customers as an ASP,
 * embedding the product in a web application, shipping BigConnect with a
 * closed source product.
 */
package com.mware.ge.query;

import com.google.common.base.Joiner;
import com.mware.ge.Authorizations;
import com.mware.ge.query.builder.GeQueryBuilder;
import com.mware.ge.scoring.ScoringStrategy;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.util.IterableUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class QueryParameters implements Serializable {
    private static final long serialVersionUID = 2L;

    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(QueryParameters.class);

    private Double minScore = null;
    private final Authorizations authorizations;
    private Long limit = null;
    private long skip = GeQueryBuilder.DEFAULT_SKIP;
    private final List<QueryBase.HasContainer> hasContainers = new ArrayList<>();
    private final List<QueryBase.SortContainer> sortContainers = new ArrayList<>();
    private final List<String> edgeLabels = new ArrayList<>();
    private final List<String> conceptTypes = new ArrayList<>();
    private final List<String> inVertexTypes = new ArrayList<>();
    private final List<String> outVertexTypes = new ArrayList<>();
    private List<String> ids;
    private ScoringStrategy scoringStrategy;

    public QueryParameters(Authorizations authorizations) {
        this.authorizations = authorizations;
    }

    public void addHasContainer(QueryBase.HasContainer hasContainer) {
        this.hasContainers.add(hasContainer);
    }

    public Long getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        if (limit == null) {
            this.limit = null;
        } else {
            this.limit = (long) limit;
        }
    }

    public void setLimit(Long limit) {
        this.limit = limit;
    }

    public long getSkip() {
        return skip;
    }

    public void setSkip(long skip) {
        this.skip = skip;
    }

    public Double getMinScore() {
        return minScore;
    }
    public void setMinScore(Double minScore) {
        this.minScore = minScore;
    }

    public Authorizations getAuthorizations() {
        return authorizations;
    }

    public List<QueryBase.HasContainer> getHasContainers() {
        return hasContainers;
    }

    public List<QueryBase.SortContainer> getSortContainers() {
        return sortContainers;
    }

    public void addSortContainer(QueryBase.SortContainer sortContainer) {
        sortContainers.add(sortContainer);
    }

    public List<String> getEdgeLabels() {
        return edgeLabels;
    }

    public List<String> getConceptTypes() {
        return conceptTypes;
    }

    public List<String> getOutVertexTypes() {
        return outVertexTypes;
    }

    public void addOutVertexType(String outVertexType) {
        this.inVertexTypes.add(outVertexType);
    }

    public void addEdgeLabel(String edgeLabel) {
        this.edgeLabels.add(edgeLabel);
    }

    public void addConceptType(String conceptType) {
        this.conceptTypes.add(conceptType);
    }

    public List<String> getInVertexTypes() {
        return inVertexTypes;
    }

    public void addInVertexType(String inVertexType) {
        this.inVertexTypes.add(inVertexType);
    }

    public ScoringStrategy getScoringStrategy() {
        return scoringStrategy;
    }
    public void setScoringStrategy(ScoringStrategy scoringStrategy) {
        this.scoringStrategy = scoringStrategy;
    }

    /**
     * Get the ids of the elements that should be searched in this query.
     *
     * @return null if all elements should be searched. A List of element ids otherwise. Empty list indicates that all elements are filtered out.
     */
    public List<String> getIds() {
        return ids;
    }

    /**
     * When called the first time, all ids are added to the filter.
     * When called two or more times, the provided id's are and'ed with the those provided in the previous lists.
     *
     * @param ids The ids of the elements that should be searched in this query.
     */
    public void addIds(Collection<String> ids) {
        if (this.ids == null) {
            this.ids = new ArrayList<>(ids);
        } else {
            this.ids.retainAll(ids);
            if (this.ids.isEmpty()) {
                LOGGER.warn("No ids remain after addIds. All elements will be filtered out.");
            }
        }
    }

    public abstract QueryParameters clone();

    protected QueryParameters cloneTo(QueryParameters result) {
        result.setSkip(this.getSkip());
        result.setLimit(this.getLimit());
        result.setMinScore(this.getMinScore());
        result.setScoringStrategy(this.getScoringStrategy());
        result.hasContainers.addAll(this.getHasContainers());
        result.sortContainers.addAll(this.getSortContainers());
        result.edgeLabels.addAll(this.getEdgeLabels());
        result.ids = this.ids == null ? null : new ArrayList<>(this.ids);
        return result;
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "{" +
                "authorizations=" + authorizations +
                ", limit=" + limit +
                ", skip=" + skip +
                ", hasContainers=" + Joiner.on(", ").join(hasContainers) +
                ", sortContainers=" + Joiner.on(", ").join(sortContainers) +
                ", edgeLabels=" + Joiner.on(", ").join(edgeLabels) +
                ", ids=" + (ids == null  ? null : Joiner.on(", ").join(ids)) +
                ", scoring=" + (scoringStrategy == null ? "null" : scoringStrategy.getClass().getName()) +
                '}';
    }
}
