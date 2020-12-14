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

import com.google.common.collect.Sets;
import com.mware.ge.values.storable.TextValue;
import com.mware.ge.values.storable.Value;

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;

public class PropertyDefinition implements Serializable {
    private static final long serialVersionUID = 42L;

    private static final PropertyDefinition ID_PROPERTY_DEFINITION = new PropertyDefinition(
            Element.ID_PROPERTY_NAME,
            TextValue.class,
            Sets.newHashSet(TextIndexHint.EXACT_MATCH)
    );
    private static final PropertyDefinition LABEL_PROPERTY_DEFINITION = new PropertyDefinition(
            Edge.LABEL_PROPERTY_NAME,
            TextValue.class,
            Sets.newHashSet(TextIndexHint.EXACT_MATCH)
    );
    private static final PropertyDefinition OUT_VERTEX_ID_PROPERTY_DEFINITION = new PropertyDefinition(
            Edge.OUT_VERTEX_ID_PROPERTY_NAME,
            TextValue.class,
            Sets.newHashSet(TextIndexHint.EXACT_MATCH)
    );
    private static final PropertyDefinition OUT_VERTEX_TYPE_PROPERTY_DEFINITION = new PropertyDefinition(
            Edge.OUT_VERTEX_TYPE_PROPERTY_NAME,
            TextValue.class,
            Sets.newHashSet(TextIndexHint.EXACT_MATCH)
    );
    private static final PropertyDefinition IN_VERTEX_ID_PROPERTY_DEFINITION = new PropertyDefinition(
            Edge.IN_VERTEX_ID_PROPERTY_NAME,
            TextValue.class,
            Sets.newHashSet(TextIndexHint.EXACT_MATCH)
    );
    private static final PropertyDefinition IN_VERTEX_TYPE_PROPERTY_DEFINITION = new PropertyDefinition(
            Edge.IN_VERTEX_TYPE_PROPERTY_NAME,
            TextValue.class,
            Sets.newHashSet(TextIndexHint.EXACT_MATCH)
    );
    private static final PropertyDefinition IN_OR_OUT_VERTEX_ID_PROPERTY_DEFINITION = new PropertyDefinition(
            Edge.IN_OR_OUT_VERTEX_ID_PROPERTY_NAME,
            TextValue.class,
            Sets.newHashSet(TextIndexHint.EXACT_MATCH)
    );
    private static final PropertyDefinition TABLE_NAME_PROPERTY_DEFINITION = new PropertyDefinition(
            ExtendedDataRow.TABLE_NAME,
            TextValue.class,
            Sets.newHashSet(TextIndexHint.EXACT_MATCH)
    );
    private static final PropertyDefinition ELEMENT_ID_PROPERTY_DEFINITION = new PropertyDefinition(
            ExtendedDataRow.ELEMENT_ID,
            TextValue.class,
            Sets.newHashSet(TextIndexHint.EXACT_MATCH)
    );
    private static final PropertyDefinition ELEMENT_TYPE_PROPERTY_DEFINITION = new PropertyDefinition(
            ExtendedDataRow.ELEMENT_TYPE,
            TextValue.class,
            Sets.newHashSet(TextIndexHint.EXACT_MATCH)
    );
    private static final PropertyDefinition ROW_ID_PROPERTY_DEFINITION = new PropertyDefinition(
            ExtendedDataRow.ROW_ID,
            TextValue.class,
            Sets.newHashSet(TextIndexHint.EXACT_MATCH)
    );
    private final String propertyName;
    private final Class<? extends Value> dataType;
    private final Set<TextIndexHint> textIndexHints;
    private final Double boost;
    private final boolean sortable;

    public PropertyDefinition(
            String propertyName,
            Class<? extends Value> dataType,
            Set<TextIndexHint> textIndexHints) {
        this(
                propertyName,
                dataType,
                textIndexHints,
                null,
                false
        );
    }

    public PropertyDefinition(
            String propertyName,
            Class dataType,
            Set<TextIndexHint> textIndexHints,
            Double boost,
            boolean sortable
    ) {
        this.propertyName = propertyName;
        this.dataType = dataType;
        this.textIndexHints = textIndexHints;
        // to return the correct values for aggregations we need the original value. The only way to get this is to look
        // at the original text stored in the full text. The exact match index only contains lower cased values.
        if (textIndexHints != null && textIndexHints.contains(TextIndexHint.EXACT_MATCH)) {
            this.textIndexHints.add(TextIndexHint.FULL_TEXT);
        }
        this.boost = boost;
        this.sortable = sortable;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public Class<? extends Value> getDataType() {
        return dataType;
    }

    public Set<TextIndexHint> getTextIndexHints() {
        return textIndexHints;
    }

    public Double getBoost() {
        return boost;
    }

    public boolean isSortable() {
        return sortable;
    }

    public static PropertyDefinition findPropertyDefinition(Collection<PropertyDefinition> propertyDefinitions, String propertyName) {
        if (Element.ID_PROPERTY_NAME.equals(propertyName)) {
            return ID_PROPERTY_DEFINITION;
        }
        if (Edge.LABEL_PROPERTY_NAME.equals(propertyName)) {
            return LABEL_PROPERTY_DEFINITION;
        }
        if (Edge.OUT_VERTEX_ID_PROPERTY_NAME.equals(propertyName)) {
            return OUT_VERTEX_ID_PROPERTY_DEFINITION;
        }
        if (Edge.OUT_VERTEX_TYPE_PROPERTY_NAME.equals(propertyName)) {
            return OUT_VERTEX_TYPE_PROPERTY_DEFINITION;
        }
        if (Edge.IN_VERTEX_ID_PROPERTY_NAME.equals(propertyName)) {
            return IN_VERTEX_ID_PROPERTY_DEFINITION;
        }
        if (Edge.IN_VERTEX_TYPE_PROPERTY_NAME.equals(propertyName)) {
            return IN_VERTEX_TYPE_PROPERTY_DEFINITION;
        }
        if (Edge.IN_OR_OUT_VERTEX_ID_PROPERTY_NAME.equals(propertyName)) {
            return IN_OR_OUT_VERTEX_ID_PROPERTY_DEFINITION;
        }
        if (ExtendedDataRow.TABLE_NAME.equals(propertyName)) {
            return TABLE_NAME_PROPERTY_DEFINITION;
        }
        if (ExtendedDataRow.ROW_ID.equals(propertyName)) {
            return ROW_ID_PROPERTY_DEFINITION;
        }
        if (ExtendedDataRow.ELEMENT_TYPE.equals(propertyName)) {
            return ELEMENT_TYPE_PROPERTY_DEFINITION;
        }
        if (ExtendedDataRow.ELEMENT_ID.equals(propertyName)) {
            return ELEMENT_ID_PROPERTY_DEFINITION;
        }
        for (PropertyDefinition propertyDefinition : propertyDefinitions) {
            if (propertyDefinition.getPropertyName() != null && propertyDefinition.getPropertyName().equals(propertyName)) {
                return propertyDefinition;
            }
        }
        throw new GePropertyNotDefinedException("Could not find property definition for property name: " + propertyName);
    }

    @Override
    public String toString() {
        return "PropertyDefinition{" +
                "propertyName='" + propertyName + '\'' +
                ", dataType=" + dataType +
                ", textIndexHints=" + textIndexHints +
                ", boost=" + boost +
                ", sortable=" + sortable +
                '}';
    }
}
