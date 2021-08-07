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
package com.mware.ge;

import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.property.MutablePropertyImpl;
import com.mware.ge.util.FilterIterable;
import com.mware.ge.values.storable.EdgeVertexIds;

import java.util.ArrayList;

import static com.mware.ge.values.storable.Values.stringValue;

public abstract class ElementBase implements Element {
    private transient Property idProperty;
    private transient Property edgeLabelProperty;
    private transient Property outVertexIdProperty;
    private transient Property inVertexIdProperty;
    private transient Property inOrOutVertexIdProperty;
    private transient Property tableNameProperty;
    private transient Property rowIdProperty;
    private transient Property elementTypeProperty;
    private transient Property elementIdProperty;

    @Override
    public Property getProperty(String key, String name, Visibility visibility) {
        Property reservedProperty = getReservedProperty(name);
        if (reservedProperty != null) {
            return reservedProperty;
        }
        for (Property p : internalGetProperties(key, name)) {
            if (visibility == null) {
                return p;
            }
            if (!visibility.equals(p.getVisibility())) {
                continue;
            }
            return p;
        }
        return null;
    }

    protected Property getReservedProperty(String name) {
        if (ID_PROPERTY_NAME.equals(name)) {
            return getIdProperty();
        } else if (Edge.LABEL_PROPERTY_NAME.equals(name) && this instanceof Edge) {
            return getEdgeLabelProperty();
        } else if (Edge.OUT_VERTEX_ID_PROPERTY_NAME.equals(name) && this instanceof Edge) {
            return getOutVertexIdProperty();
        } else if (Edge.IN_VERTEX_ID_PROPERTY_NAME.equals(name) && this instanceof Edge) {
            return getInVertexIdProperty();
        } else if (Edge.IN_OR_OUT_VERTEX_ID_PROPERTY_NAME.equals(name) && this instanceof Edge) {
            return getInOrOutVertexIdProperty();
        } else if (ExtendedDataRow.TABLE_NAME.equals(name) && this instanceof ExtendedDataRow) {
            return getTableNameProperty();
        } else if (ExtendedDataRow.ROW_ID.equals(name) && this instanceof ExtendedDataRow) {
            return getRowIdProperty();
        } else if (ExtendedDataRow.ELEMENT_ID.equals(name) && this instanceof ExtendedDataRow) {
            return getElementIdProperty();
        } else if (ExtendedDataRow.ELEMENT_TYPE.equals(name) && this instanceof ExtendedDataRow) {
            return getElementTypeProperty();
        }
        return null;
    }

    @Override
    public Property getProperty(String name, Visibility visibility) {
        return getProperty(ElementMutation.DEFAULT_KEY, name, visibility);
    }

    @Override
    public Iterable<Property> getProperties(String name) {
        return getProperties(null, name);
    }

    protected Iterable<Property> internalGetProperties(String key, String name) {
        getFetchHints().assertPropertyIncluded(name);
        return new FilterIterable<>(getProperties()) {
            @Override
            protected boolean isIncluded(Property property) {
                if (key != null && !property.getKey().equals(key)) {
                    return false;
                }
                return property.getName().equals(name);
            }
        };
    }

    @Override
    public Iterable<Property> getProperties(String key, String name) {
        Property reservedProperty = getReservedProperty(name);
        if (reservedProperty != null) {
            ArrayList<Property> result = new ArrayList<>();
            result.add(reservedProperty);
            return result;
        }
        getFetchHints().assertPropertyIncluded(name);
        return internalGetProperties(key, name);
    }

    protected Property getIdProperty() {
        if (idProperty == null) {
            idProperty = new MutablePropertyImpl(
                    ElementMutation.DEFAULT_KEY,
                    ID_PROPERTY_NAME,
                    stringValue(getId()),
                    null,
                    getTimestamp(),
                    null,
                    null,
                    getFetchHints()
            );
        }
        return idProperty;
    }

    protected Property getEdgeLabelProperty() {
        if (edgeLabelProperty == null && this instanceof Edge) {
            String edgeLabel = ((Edge) this).getLabel();
            edgeLabelProperty = new MutablePropertyImpl(
                    ElementMutation.DEFAULT_KEY,
                    Edge.LABEL_PROPERTY_NAME,
                    stringValue(edgeLabel),
                    null,
                    getTimestamp(),
                    null,
                    null,
                    getFetchHints()
            );
        }
        return edgeLabelProperty;
    }

    protected Property getOutVertexIdProperty() {
        if (outVertexIdProperty == null && this instanceof Edge) {
            String outVertexId = ((Edge) this).getVertexId(Direction.OUT);
            outVertexIdProperty = new MutablePropertyImpl(
                    ElementMutation.DEFAULT_KEY,
                    Edge.OUT_VERTEX_ID_PROPERTY_NAME,
                    stringValue(outVertexId),
                    null,
                    getTimestamp(),
                    null,
                    null,
                    getFetchHints()
            );
        }
        return outVertexIdProperty;
    }

    protected Property getInVertexIdProperty() {
        if (inVertexIdProperty == null && this instanceof Edge) {
            String inVertexId = ((Edge) this).getVertexId(Direction.IN);
            inVertexIdProperty = new MutablePropertyImpl(
                    ElementMutation.DEFAULT_KEY,
                    Edge.IN_VERTEX_ID_PROPERTY_NAME,
                    stringValue(inVertexId),
                    null,
                    getTimestamp(),
                    null,
                    null,
                    getFetchHints()
            );
        }
        return inVertexIdProperty;
    }

    protected Property getInOrOutVertexIdProperty() {
        if (inOrOutVertexIdProperty == null && this instanceof Edge) {
            String inVertexId = ((Edge) this).getVertexId(Direction.IN);
            String outVertexId = ((Edge) this).getVertexId(Direction.OUT);
            inOrOutVertexIdProperty = new MutablePropertyImpl(
                    ElementMutation.DEFAULT_KEY,
                    Edge.IN_OR_OUT_VERTEX_ID_PROPERTY_NAME,
                    new EdgeVertexIds(outVertexId, inVertexId),
                    null,
                    getTimestamp(),
                    null,
                    null,
                    getFetchHints()
            );
        }
        return inOrOutVertexIdProperty;
    }

    protected Property getTableNameProperty() {
        if (tableNameProperty == null && this instanceof ExtendedDataRow) {
            String tableName = ((ExtendedDataRow) this).getId().getTableName();
            tableNameProperty = new MutablePropertyImpl(
                    ElementMutation.DEFAULT_KEY,
                    ExtendedDataRow.TABLE_NAME,
                    stringValue(tableName),
                    null,
                    getTimestamp(),
                    null,
                    null,
                    getFetchHints()
            );
        }
        return tableNameProperty;
    }

    protected Property getRowIdProperty() {
        if (rowIdProperty == null && this instanceof ExtendedDataRow) {
            String rowId = ((ExtendedDataRow) this).getId().getRowId();
            rowIdProperty = new MutablePropertyImpl(
                    ElementMutation.DEFAULT_KEY,
                    ExtendedDataRow.ROW_ID,
                    stringValue(rowId),
                    null,
                    getTimestamp(),
                    null,
                    null,
                    getFetchHints()
            );
        }
        return rowIdProperty;
    }

    protected Property getElementTypeProperty() {
        if (elementTypeProperty == null && this instanceof ExtendedDataRow) {
            String elementType = ((ExtendedDataRow) this).getId().getElementType().name();
            elementTypeProperty = new MutablePropertyImpl(
                    ElementMutation.DEFAULT_KEY,
                    ExtendedDataRow.ELEMENT_TYPE,
                    stringValue(elementType),
                    null,
                    getTimestamp(),
                    null,
                    null,
                    getFetchHints()
            );
        }
        return elementTypeProperty;
    }

    protected Property getElementIdProperty() {
        if (elementIdProperty == null && this instanceof ExtendedDataRow) {
            String elementId = ((ExtendedDataRow) this).getId().getElementId();
            elementIdProperty = new MutablePropertyImpl(
                    ElementMutation.DEFAULT_KEY,
                    ExtendedDataRow.ELEMENT_ID,
                    stringValue(elementId),
                    null,
                    getTimestamp(),
                    null,
                    null,
                    getFetchHints()
            );
        }
        return elementIdProperty;
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ElementId) {
            ElementId objElementId = (ElementId) obj;
            return getId().equals(objElementId.getId()) && getElementType().equals(objElementId.getElementType());
        }
        return super.equals(obj);
    }

    @Override
    public String toString() {
        if (this instanceof Edge) {
            Edge edge = (Edge) this;
            return getId() + ":[" + edge.getVertexId(Direction.OUT) + "-" + edge.getLabel() + "->" + edge.getVertexId(Direction.IN) + "]";
        } else if (this instanceof Vertex) {
            Vertex vertex = (Vertex) this;
            return "("+getId()+":"+vertex.getConceptType()+")";
        }
        return getId();
    }
}
