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
package com.mware.ge.inmemory;

import com.google.common.collect.ImmutableSet;
import com.mware.ge.*;
import com.mware.ge.mutation.*;
import com.mware.ge.query.ExtendedDataQueryableIterable;
import com.mware.ge.query.QueryableIterable;
import com.mware.ge.search.IndexHint;
import com.mware.ge.values.storable.Value;

public abstract class InMemoryElement<TElement extends InMemoryElement> extends ElementBase {
    private final String id;
    private final FetchHints fetchHints;
    private transient InMemoryGraph graph;
    private InMemoryTableElement<TElement> inMemoryTableElement;
    private final Long endTime;
    private final Authorizations authorizations;

    protected InMemoryElement(
            InMemoryGraph graph,
            String id,
            InMemoryTableElement<TElement> inMemoryTableElement,
            FetchHints fetchHints,
            Long endTime,
            Authorizations authorizations
    ) {
        this.graph = graph;
        this.id = id;
        this.fetchHints = fetchHints;
        this.endTime = endTime;
        this.authorizations = authorizations;
        this.inMemoryTableElement = inMemoryTableElement;
    }

    @Override
    public String getId() {
        return this.id;
    }

    @Override
    public Visibility getVisibility() {
        return inMemoryTableElement.getVisibility();
    }

    @Override
    public long getTimestamp() {
        return inMemoryTableElement.getTimestamp();
    }

    @Override
    public Long getEndTime() {
        return this.endTime;
    }

    @Override
    public void deleteProperty(String key, String name, Visibility visibility, Authorizations authorizations) {
        getGraph().deleteProperty(this, inMemoryTableElement, key, name, visibility, authorizations);
    }

    @Override
    public void softDeleteProperty(String key, String name, Visibility visibility, Authorizations authorizations) {
        softDeleteProperty(key, name, null, visibility, IndexHint.INDEX, authorizations);
    }

    protected void softDeleteProperty(String key, String name, Long timestamp, Visibility visibility, IndexHint indexHint, Authorizations authorizations) {
        Property property = getProperty(key, name, visibility);
        if (property != null) {
            getGraph().softDeleteProperty(inMemoryTableElement, property, timestamp, indexHint, authorizations);
        }
    }

    private void deleteExtendedData(String tableName, String row, String columnName, String key, Visibility visibility) {
        getGraph().deleteExtendedData(this, tableName, row, columnName, key, visibility, authorizations);
    }

    protected void extendedData(ExtendedDataMutation extendedData, Authorizations authorizations) {
        ExtendedDataRowId extendedDataRowId = new ExtendedDataRowId(
                ElementType.getTypeFromElement(this),
                getId(),extendedData.getTableName(),
                extendedData.getRow()
        );
        getGraph().extendedData(this, extendedDataRowId, extendedData, authorizations);
    }

    @Override
    public void markPropertyHidden(Property property, Long timestamp, Visibility visibility, Authorizations authorizations) {
        getGraph().markPropertyHidden(
                this,
                inMemoryTableElement,
                property,
                timestamp,
                visibility,
                authorizations
        );
    }

    @Override
    public void markPropertyVisible(Property property, Long timestamp, Visibility visibility, Authorizations authorizations) {
        getGraph().markPropertyVisible(this, inMemoryTableElement, property.getKey(), property.getName(), property.getVisibility(), timestamp, visibility, authorizations);
    }

    @Override
    public Value getPropertyValue(String name) {
        Property p = getProperty(name);
        return p == null ? null : p.getValue();
    }

    @Override
    public Value getPropertyValue(String key, String name) {
        Property p = getProperty(key, name);
        return p == null ? null : p.getValue();
    }

    public void addPropertyValue(String key, String name, Value value, Metadata metadata, Visibility visibility, Long timestamp, boolean indexAfterAdd, Authorizations authorizations) {
        getGraph().addPropertyValue(this, inMemoryTableElement, key, name, value, metadata, visibility, timestamp, authorizations);
        if (indexAfterAdd) {
            getGraph().getSearchIndex().addElement(getGraph(), this, authorizations);
        }
    }

    @Override
    public void markPropertyVisible(String key, String name, Visibility propertyVisibility, Long timestamp, Visibility visibility, Authorizations authorizations) {
        getGraph().markPropertyVisible(this, inMemoryTableElement, key, name, propertyVisibility, timestamp, visibility, authorizations);
    }

    @Override
    public boolean isHidden(Authorizations authorizations) {
        return inMemoryTableElement.isHidden(authorizations);
    }

    @Override
    public Iterable<Property> getProperties() {
        if (!getFetchHints().isIncludeProperties()) {
            throw new GeMissingFetchHintException(getFetchHints(), "includeProperties");
        }
        return inMemoryTableElement.getProperties(fetchHints, endTime, authorizations);
    }

    @Override
    public Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(String key, String name, Visibility visibility, Long startTime, Long endTime, Authorizations authorizations) {
        return inMemoryTableElement.getHistoricalPropertyValues(key, name, visibility, startTime, endTime, authorizations);
    }

    @Override
    public abstract <T extends Element> ExistingElementMutation<T> prepareMutation();

    @Override
    public Authorizations getAuthorizations() {
        return this.authorizations;
    }

    @Override
    public InMemoryGraph getGraph() {
        return this.graph;
    }

    void updatePropertiesInternal(VertexBuilder vertexBuilder) {
        updatePropertiesInternal(
                vertexBuilder.getProperties(),
                vertexBuilder.getPropertyDeletes(),
                vertexBuilder.getPropertySoftDeletes(),
                vertexBuilder.getExtendedData(),
                vertexBuilder.getExtendedDataDeletes(),
                vertexBuilder.getIndexHint()
        );
    }

    void updatePropertiesInternal(EdgeBuilderBase edgeBuilder) {
        updatePropertiesInternal(
                edgeBuilder.getProperties(),
                edgeBuilder.getPropertyDeletes(),
                edgeBuilder.getPropertySoftDeletes(),
                edgeBuilder.getExtendedData(),
                edgeBuilder.getExtendedDataDeletes(),
                edgeBuilder.getIndexHint()
        );
    }

    protected void updatePropertiesInternal(
            Iterable<Property> properties,
            Iterable<PropertyDeleteMutation> propertyDeleteMutations,
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations,
            Iterable<ExtendedDataMutation> extendedDatas,
            Iterable<ExtendedDataDeleteMutation> extendedDataDeletes,
            IndexHint indexHint
    ) {
        for (Property property : properties) {
            addPropertyValue(
                    property.getKey(),
                    property.getName(),
                    property.getValue(),
                    property.getMetadata(),
                    property.getVisibility(),
                    property.getTimestamp(),
                    false,
                    authorizations
            );
        }
        for (PropertyDeleteMutation propertyDeleteMutation : propertyDeleteMutations) {
            deleteProperty(propertyDeleteMutation.getKey(), propertyDeleteMutation.getName(), propertyDeleteMutation.getVisibility(), authorizations);
        }
        for (PropertySoftDeleteMutation propertySoftDeleteMutation : propertySoftDeleteMutations) {
            softDeleteProperty(propertySoftDeleteMutation.getKey(), propertySoftDeleteMutation.getName(), propertySoftDeleteMutation.getTimestamp(), propertySoftDeleteMutation.getVisibility(), indexHint, authorizations);
        }
        for (ExtendedDataMutation extendedData : extendedDatas) {
            getGraph().ensurePropertyDefined(extendedData.getColumnName(), extendedData.getValue());
            extendedData(extendedData, authorizations);
        }
        for (ExtendedDataDeleteMutation extendedDataDelete : extendedDataDeletes) {
            deleteExtendedData(
                    extendedDataDelete.getTableName(),
                    extendedDataDelete.getRow(),
                    extendedDataDelete.getColumnName(),
                    extendedDataDelete.getKey(),
                    extendedDataDelete.getVisibility()
            );
        }
    }

    protected <T extends Element> void saveExistingElementMutation(ExistingElementMutationImpl<T> mutation, IndexHint indexHint, Authorizations authorizations) {
        if (mutation.getElement() != this) {
            throw new GeException("cannot save mutation from another element");
        }

        // Order matters a lot here

        // Metadata must be altered first because the lookup of a property can include visibility which will be
        // altered by alterElementPropertyVisibilities
        graph.alterElementPropertyMetadata(inMemoryTableElement, mutation.getSetPropertyMetadatas(), authorizations);

        // Altering properties comes next because alterElementVisibility may alter the vertex and we won't find it
        graph.alterElementPropertyVisibilities(
                inMemoryTableElement, mutation.getAlterPropertyVisibilities(), authorizations);

        Iterable<Property> properties = mutation.getProperties();
        Iterable<PropertyDeleteMutation> propertyDeleteMutations = mutation.getPropertyDeletes();
        Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations = mutation.getPropertySoftDeletes();

        updatePropertiesInternal(
                properties,
                propertyDeleteMutations,
                propertySoftDeleteMutations,
                mutation.getExtendedData(),
                mutation.getExtendedDataDeletes(),
                indexHint
        );

        InMemoryGraph graph = getGraph();

        if (mutation.getNewElementVisibility() != null) {
            graph.alterElementVisibility(inMemoryTableElement, mutation.getNewElementVisibility());
        }

        if (mutation instanceof EdgeMutation) {
            EdgeMutation edgeMutation = (EdgeMutation) mutation;
            if (edgeMutation.getNewEdgeLabel() != null) {
                graph.alterEdgeLabel((InMemoryTableEdge) inMemoryTableElement, edgeMutation.getAlterEdgeLabelTimestamp(), edgeMutation.getNewEdgeLabel());
            }
        }

        if (mutation instanceof VertexMutation) {
            VertexMutation vertexMutation = (VertexMutation) mutation;
            if (vertexMutation.getNewConceptType() != null) {
                graph.alterConceptType((InMemoryTableVertex) inMemoryTableElement, vertexMutation.getAlterConceptTypeTimestamp(), vertexMutation.getNewConceptType());
            }
        }
    }

    public boolean canRead(Authorizations authorizations) {
        return inMemoryTableElement.canRead(getFetchHints(), authorizations);
    }

    @Override
    public Iterable<Visibility> getHiddenVisibilities() {
        return inMemoryTableElement.getHiddenVisibilities();
    }

    public FetchHints getFetchHints() {
        return fetchHints;
    }

    protected InMemoryTableElement<TElement> getInMemoryTableElement() {
        return inMemoryTableElement;
    }

    protected void setInMemoryTableElement(InMemoryTableElement<TElement> inMemoryTableElement) {
        this.inMemoryTableElement = inMemoryTableElement;
    }

    @Override
    public ImmutableSet<String> getExtendedDataTableNames() {
        if (!getFetchHints().isIncludeExtendedDataTableNames()) {
            throw new GeMissingFetchHintException(getFetchHints(), "includeExtendedDataTableNames");
        }

        return graph.getExtendedDataTableNames(ElementType.getTypeFromElement(this), id, getFetchHints(), authorizations);
    }

    @Override
    public QueryableIterable<ExtendedDataRow> getExtendedData(String tableName, FetchHints fetchHints) {
        return new ExtendedDataQueryableIterable(
                getGraph(),
                this,
                tableName,
                graph.getExtendedDataTable(ElementType.getTypeFromElement(this), id, tableName, fetchHints, authorizations)
        );
    }
}
