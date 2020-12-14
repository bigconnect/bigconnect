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
package com.mware.ge.store;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.mware.ge.*;
import com.mware.ge.mutation.*;
import com.mware.ge.property.MutableProperty;
import com.mware.ge.query.ExtendedDataQueryableIterable;
import com.mware.ge.query.QueryableIterable;
import com.mware.ge.search.IndexHint;
import com.mware.ge.store.util.HiddenProperty;
import com.mware.ge.store.util.SoftDeletedProperty;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.util.IterableUtils;
import com.mware.ge.util.PropertyCollection;
import com.mware.ge.values.storable.Value;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

public abstract class StorableElement extends ElementBase implements Serializable, HasTimestamp {
    public static final String CF_PROPERTY = "PROP";
    public static final String CF_PROPERTY_METADATA = "PROPMETA";
    public static final String CF_PROPERTY_SOFT_DELETE = "PROPD";
    public static final String CF_EXTENDED_DATA = "EXTDATA";
    public static final String CF_PROPERTY_HIDDEN = "PROPH";
    public static final String CF_PROPERTY_VISIBLE = "PROPV";
    public static final String CF_HIDDEN = "H";
    public static final String CF_SOFT_DELETE = "D";
    public static final String METADATA_COLUMN_FAMILY = "";
    public static final String METADATA_COLUMN_QUALIFIER = "";
    public static final String DELETE_ROW_COLUMN_FAMILY = "";
    public static final String DELETE_ROW_COLUMN_QUALIFIER = "";
    public static final String CQ_SOFT_DELETE = "D";
    public static final String CQ_HIDDEN = "H";
    public static final byte[] SOFT_DELETE_VALUE = "".getBytes();
    public static final byte[] HIDDEN_VALUE = "".getBytes();
    public static final byte[] HIDDEN_VALUE_DELETED = "X".getBytes();

    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(StorableElement.class);

    protected StorableGraph graph;
    protected String id;
    protected Visibility visibility;
    protected long timestamp = 0L;
    protected long softDeleteTimestamp = 0;
    protected FetchHints fetchHints;
    protected Set<Visibility> hiddenVisibilities;

    protected PropertyCollection properties = new PropertyCollection();
    protected ImmutableSet<String> extendedDataTableNames;
    protected ConcurrentSkipListSet<PropertyDeleteMutation> propertyDeleteMutations;
    protected ConcurrentSkipListSet<PropertySoftDeleteMutation> propertySoftDeleteMutations;
    protected Authorizations authorizations;
    protected List<SoftDeletedProperty> softDeletedProperties = new ArrayList<>();
    protected List<HiddenProperty> hiddenPropertyMarkers = new ArrayList<>();
    protected List<HiddenProperty> visiblePropertyMarkers = new ArrayList<>();
    protected Set<StorablePropertyEntry> storableProperties = new HashSet<>();
    protected Set<StorableMetadataEntry> storablePropertyMetadatas = new HashSet<>();

    protected StorableElement(
            StorableGraph graph,
            String id,
            Visibility visibility,
            Iterable<Property> properties,
            Iterable<PropertyDeleteMutation> propertyDeleteMutations,
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations,
            Iterable<Visibility> hiddenVisibilities,
            ImmutableSet<String> extendedDataTableNames,
            long timestamp,
            FetchHints fetchHints,
            Authorizations authorizations
    ) {
        this.graph = graph;
        this.id = id;
        this.visibility = visibility;
        this.timestamp = timestamp;
        this.fetchHints = fetchHints;
        this.properties = new PropertyCollection();
        this.extendedDataTableNames = extendedDataTableNames;
        this.authorizations = authorizations;
        this.hiddenVisibilities = hiddenVisibilities == null ? new HashSet<>() : IterableUtils.toSet(hiddenVisibilities);
        updatePropertiesInternal(properties, propertyDeleteMutations, propertySoftDeleteMutations);
    }

    @Override
    public void deleteProperty(String key, String name, Visibility visibility, Authorizations authorizations) {
        Property property = getProperty(key, name, visibility);
        if (property != null) {
            this.properties.removeProperty(property);
            getGraph().deleteProperty(this, property, authorizations);
        }
    }

    @Override
    public void softDeleteProperty(String key, String name, Visibility visibility, Authorizations authorizations) {
        Property property = getProperty(key, name, visibility);
        if (property != null) {
            this.properties.removeProperty(property);
            getGraph().softDeleteProperty(this, property, authorizations);
        }
    }

    public void softDeleteProperties(Authorizations authorizations) {
        List<Property> propertyList = IterableUtils.toList(getProperties());
        propertyList.forEach(p -> properties.removeProperty(p));
        getGraph().softDeleteProperties(propertyList, this, authorizations);
    }

    @Override
    public void markPropertyHidden(Property property, Long timestamp, Visibility visibility, Authorizations authorizations) {
        getGraph().markPropertyHidden(this, property, timestamp, visibility, authorizations);
    }

    @Override
    public void markPropertyVisible(Property property, Long timestamp, Visibility visibility, Authorizations authorizations) {
        getGraph().markPropertyVisible(this, property, timestamp, visibility, authorizations);
    }

    protected <TElement extends Element> void saveExistingElementMutation(ExistingElementMutation<TElement> mutation, Authorizations authorizations) {
        // Order matters a lot in this method
        StorableElement element = (StorableElement) mutation.getElement();

        // metadata must be altered first because the lookup of a property can include visibility which will be altered by alterElementPropertyVisibilities
        getGraph().alterPropertyMetadatas(element, mutation.getSetPropertyMetadatas());

        // altering properties comes next because alterElementVisibility may alter the vertex and we won't find it
        getGraph().alterElementPropertyVisibilities(element, mutation.getAlterPropertyVisibilities());

        Iterable<PropertyDeleteMutation> propertyDeletes = mutation.getPropertyDeletes();
        Iterable<PropertySoftDeleteMutation> propertySoftDeletes = mutation.getPropertySoftDeletes();
        Iterable<Property> properties = mutation.getProperties();

        updatePropertiesInternal(properties, propertyDeletes, propertySoftDeletes);
        getGraph().saveProperties(element, properties, propertyDeletes, propertySoftDeletes);

        if (mutation.getNewElementVisibility() != null) {
            getGraph().alterElementVisibility(element, mutation.getNewElementVisibility(), authorizations);
        }

        if (mutation instanceof EdgeMutation) {
            EdgeMutation edgeMutation = (EdgeMutation) mutation;

            String newEdgeLabel = edgeMutation.getNewEdgeLabel();
            if (newEdgeLabel != null) {
                getGraph().alterEdgeLabel((StorableEdge) mutation.getElement(), newEdgeLabel, authorizations);
            }
        }

        if (mutation instanceof VertexMutation) {
            VertexMutation vertexMutation = (VertexMutation) mutation;
            String newConceptType = vertexMutation.getNewConceptType();
            if (newConceptType != null) {
                getGraph().alterConceptType((StorableVertex) mutation.getElement(), newConceptType);
            }
        }

        if (mutation.getIndexHint() != IndexHint.DO_NOT_INDEX) {
            getGraph().getSearchIndex().updateElement(graph, mutation, authorizations);
        }

        ElementType elementType = ElementType.getTypeFromElement(mutation.getElement());
        getGraph().saveExtendedDataMutations(
                mutation.getElement(),
                elementType,
                mutation.getIndexHint(),
                mutation.getExtendedData(),
                mutation.getExtendedDataDeletes(),
                authorizations
        );
        getGraph().invalidateElementFromCache(elementType, element.id);
    }

    @Override
    public Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(String key, String name, Visibility visibility, Long startTime, Long endTime, Authorizations authorizations) {
        return getGraph().getHistoricalPropertyValues(this, key, name, visibility, startTime, endTime, authorizations);
    }

    @Override
    public abstract <T extends Element> ExistingElementMutation<T> prepareMutation();

    @Override
    public QueryableIterable<ExtendedDataRow> getExtendedData(String tableName, FetchHints fetchHints) {
        return new ExtendedDataQueryableIterable(
                getGraph(),
                this,
                tableName,
                getGraph().getExtendedData(
                        ElementType.getTypeFromElement(this),
                        getId(),
                        tableName,
                        fetchHints,
                        getAuthorizations()
                )
        );
    }

    @Override
    public Property getProperty(String key, String name) {
        return getProperty(key, name, null);
    }

    @Override
    public Value getPropertyValue(String name, int index) {
        return getPropertyValue(null, name, index);
    }

    @Override
    public Value getPropertyValue(String key, String name, int index) {
        Property reservedProperty = getReservedProperty(name);
        if (reservedProperty != null) {
            return reservedProperty.getValue();
        }

        Property property = this.properties.getProperty(key, name, index);
        if (property == null) {
            return null;
        }
        return property.getValue();
    }

    @Override
    public String getId() {
        return this.id;
    }

    @Override
    public Visibility getVisibility() {
        return this.visibility;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    public void setVisibility(Visibility visibility) {
        this.visibility = visibility;
    }

    @Override
    public Iterable<Property> getProperties() {
        if (!getFetchHints().isIncludeProperties()) {
            throw new GeMissingFetchHintException(getFetchHints(), "includeProperties");
        }
        return this.properties.getProperties();
    }

    public Iterable<PropertyDeleteMutation> getPropertyDeleteMutations() {
        return this.propertyDeleteMutations;
    }

    public Iterable<PropertySoftDeleteMutation> getPropertySoftDeleteMutations() {
        return this.propertySoftDeleteMutations;
    }

    @Override
    public Iterable<Property> getProperties(String key, String name) {
        Property reservedProperty = getReservedProperty(name);
        if (reservedProperty != null) {
            return Lists.newArrayList(reservedProperty);
        }
        getFetchHints().assertPropertyIncluded(name);
        return this.properties.getProperties(key, name);
    }

    @Override
    public StorableGraph getGraph() {
        return graph;
    }

    // this method differs setProperties in that it only updates the in memory representation of the properties
    protected void updatePropertiesInternal(
            Iterable<Property> properties,
            Iterable<PropertyDeleteMutation> propertyDeleteMutations,
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations
    ) {
        if (propertyDeleteMutations != null) {
            this.propertyDeleteMutations = new ConcurrentSkipListSet<>();
            for (PropertyDeleteMutation propertyDeleteMutation : propertyDeleteMutations) {
                removePropertyInternal(
                        propertyDeleteMutation.getKey(),
                        propertyDeleteMutation.getName(),
                        propertyDeleteMutation.getVisibility()
                );
                this.propertyDeleteMutations.add(propertyDeleteMutation);
            }
        }
        if (propertySoftDeleteMutations != null) {
            this.propertySoftDeleteMutations = new ConcurrentSkipListSet<>();
            for (PropertySoftDeleteMutation propertySoftDeleteMutation : propertySoftDeleteMutations) {
                removePropertyInternal(
                        propertySoftDeleteMutation.getKey(),
                        propertySoftDeleteMutation.getName(),
                        propertySoftDeleteMutation.getVisibility()
                );
                this.propertySoftDeleteMutations.add(propertySoftDeleteMutation);
            }
        }

        for (Property property : properties) {
            addPropertyInternal(property);
        }
    }

    protected void removePropertyInternal(String key, String name, Visibility visibility) {
        Property property = getProperty(key, name, visibility);
        if (property != null) {
            this.properties.removeProperty(property);
        }
    }

    public void addPropertyInternal(Property property) {
        if (property.getKey() == null) {
            throw new IllegalArgumentException("key is required for property");
        }
        Property existingProperty = getProperty(property.getKey(), property.getName(), property.getVisibility());
        if (existingProperty == null) {
            this.properties.addProperty(property);
        } else {
            if (existingProperty instanceof MutableProperty) {
                ((MutableProperty) existingProperty).update(property);
            } else {
                throw new GeException("Could not update property of type: " + existingProperty.getClass().getName());
            }
        }
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }

    @Override
    public String toString() {
        if (this instanceof Edge) {
            Edge edge = (Edge) this;
            return getId() + ":[" + edge.getVertexId(Direction.OUT) + "-" + edge.getLabel() + "->" + edge.getVertexId(Direction.IN) + "]";
        }
        return getId();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Element) {
            Element objElem = (Element) obj;
            return getId().equals(objElem.getId());
        }
        return super.equals(obj);
    }

    @Override
    public Authorizations getAuthorizations() {
        return authorizations;
    }

    public Iterable<Visibility> getHiddenVisibilities() {
        return hiddenVisibilities;
    }

    @Override
    public ImmutableSet<String> getExtendedDataTableNames() {
        if (!getFetchHints().isIncludeExtendedDataTableNames()) {
            throw new GeMissingFetchHintException(getFetchHints(), "includeExtendedDataTableNames");
        }

        return extendedDataTableNames;
    }

    @Override
    public FetchHints getFetchHints() {
        return fetchHints;
    }

    @Override
    protected Iterable<Property> internalGetProperties(String key, String name) {
        getFetchHints().assertPropertyIncluded(name);
        return this.properties.getProperties(key, name);
    }
}
