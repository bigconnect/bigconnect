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
package com.mware.core.model.properties.types;

import com.mware.core.model.clientapi.dto.ClientApiElement;
import com.mware.core.model.clientapi.dto.ClientApiProperty;
import com.mware.ge.*;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.core.model.graph.ElementUpdateContext;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.values.storable.Value;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class SingleValueBcProperty<TRaw> extends BcPropertyBase<TRaw> {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(SingleValueBcProperty.class);

    protected SingleValueBcProperty(String propertyName) {
        super(propertyName);
    }

    public final void setProperty(ElementMutation<?> mutation, TRaw value, Visibility visibility) {
        mutation.setProperty(getPropertyName(), wrap(value), visibility);
    }

    public final void setProperty(ElementMutation<?> mutation, TRaw value, Metadata metadata, Visibility visibility) {
        setProperty(mutation, value, metadata, null, visibility);
    }

    public final void setProperty(
            ElementMutation<?> mutation,
            TRaw value,
            Metadata metadata,
            Long timestamp,
            Visibility visibility
    ) {
        // Ge's ElementMutation doesn't have a setProperty that takes a timestamp. Calling addPropertyValue
        //  is effectively the same thing
        mutation.addPropertyValue(
                ElementMutation.DEFAULT_KEY,
                getPropertyName(),
                wrap(value),
                metadata,
                timestamp,
                visibility
        );
    }

    public final void setProperty(Element element, TRaw value, Visibility visibility, Authorizations authorizations) {
        element.setProperty(getPropertyName(), wrap(value), visibility, authorizations);
    }

    public final void setProperty(
            Element element,
            TRaw value,
            Metadata metadata,
            Visibility visibility,
            Authorizations authorizations
    ) {
        element.setProperty(getPropertyName(), wrap(value), metadata, visibility, authorizations);
    }

    public void setProperty(Map<String, Value> properties, TRaw value) {
        properties.put(getPropertyName(), wrap(value));
    }

    public final TRaw getPropertyValue(Element element) {
        Value value = element != null ? element.getPropertyValue(getPropertyName()) : null;
        return value != null ? getRawConverter().apply(value) : null;
    }

    public final TRaw getPropertyValue(Element element, TRaw defaultValue) {
        TRaw value = getPropertyValue(element);
        if (value == null) {
            value = defaultValue;
        }
        return value;
    }

    public final TRaw getPropertyValueRequired(Element element) {
        checkNotNull(element, "Element cannot be null");
        Value value = element.getPropertyValue(getPropertyName());
        checkNotNull(value, "Property value of property " + getPropertyName() + " cannot be null");
        return getRawConverter().apply(value);
    }

    public final TRaw getPropertyValue(Map<String, Value> map) {
        Value value = map != null ? map.get(getPropertyName()) : null;
        return value != null ? getRawConverter().apply(value) : null;
    }

    public TRaw getPropertyValue(ClientApiElement clientApiElement) {
        return getPropertyValue(clientApiElement, null);
    }

    public TRaw getPropertyValue(ClientApiElement clientApiElement, TRaw defaultValue) {
        ClientApiProperty property = clientApiElement.getProperty(ElementMutation.DEFAULT_KEY, getPropertyName());
        if (property == null) {
            return defaultValue;
        }
        //noinspection unchecked
        return (TRaw) property.getValue();
    }

    public boolean hasProperty(Element element) {
        return element.getProperty(ElementMutation.DEFAULT_KEY, getPropertyName()) != null;
    }

    public Property getProperty(Element element) {
        return element.getProperty(getPropertyName());
    }

    public void removeProperty(Element element, Authorizations authorizations) {
        element.softDeleteProperty(ElementMutation.DEFAULT_KEY, getPropertyName(), authorizations);
    }

    public void removeProperty(ElementMutation m, Visibility visibility) {
        m.softDeleteProperty(getPropertyName(), visibility);
    }

    public void removeMetadata(Metadata metadata) {
        metadata.remove(getPropertyName());
    }

    public void removeMetadata(Metadata metadata, Visibility visibility) {
        metadata.remove(getPropertyName(), visibility);
    }

    public void alterVisibility(ExistingElementMutation<?> elementMutation, Visibility newVisibility) {
        elementMutation.alterPropertyVisibility(getPropertyName(), newVisibility);
    }

    public void removeProperty(
            List<BcPropertyUpdate> changedPropertiesOut,
            Element element,
            ElementMutation m,
            Visibility visibility
    ) {
        Object currentValue = getPropertyValue(element);
        if (currentValue != null) {
            long beforeDeletionTimestamp = System.currentTimeMillis() - 1;
            removeProperty(m, visibility);
            changedPropertiesOut.add(new BcPropertyUpdateRemove(this, beforeDeletionTimestamp, true, false));
        }
    }

    public <T extends Element> void updateProperty(
            ElementUpdateContext<T> ctx,
            TRaw newValue,
            Visibility visibility
    ) {
        updateProperty(ctx.getProperties(), ctx.getElement(), ctx.getMutation(), newValue, (Metadata) null, null, visibility);
    }

    /**
     * @param changedPropertiesOut Adds the property to this list if the property value changed
     * @deprecated Use {@link #updateProperty(List, Element, ElementMutation, Object, PropertyMetadata)}
     */
    @SuppressWarnings("deprecation")
    @Deprecated
    public void updateProperty(
            List<BcPropertyUpdate> changedPropertiesOut,
            Element element,
            ElementMutation m,
            TRaw newValue,
            PropertyMetadata metadata,
            Visibility visibility
    ) {
        updateProperty(changedPropertiesOut, element, m, newValue, metadata, null, visibility);
    }

    public void updateProperty(
            List<BcPropertyUpdate> changedPropertiesOut,
            Element element,
            ElementMutation m,
            TRaw newValue,
            PropertyMetadata metadata
    ) {
        checkNotNull(metadata, "metadata cannot be null");
        updateProperty(changedPropertiesOut, element, m, newValue, metadata.createMetadata(), null, metadata.getPropertyVisibility());
    }

    public <T extends Element> void updateProperty(
            ElementUpdateContext<T> ctx,
            TRaw newValue,
            PropertyMetadata metadata
    ) {
        updateProperty(ctx.getProperties(), ctx.getElement(), ctx.getMutation(), newValue, metadata.createMetadata(), null, metadata.getPropertyVisibility());
    }

    /**
     * @param changedPropertiesOut Adds the property to this list if the property value changed
     * @deprecated Use {@link #updateProperty(List, Element, ElementMutation, Object, PropertyMetadata, Long)}
     */
    @Deprecated
    public void updateProperty(
            List<BcPropertyUpdate> changedPropertiesOut,
            Element element,
            ElementMutation m,
            TRaw newValue,
            PropertyMetadata metadata,
            Long timestamp,
            Visibility visibility
    ) {
        updateProperty(changedPropertiesOut, element, m, newValue, metadata == null ? null : metadata.createMetadata(), timestamp, visibility);
    }

    public void updateProperty(
            List<BcPropertyUpdate> changedPropertiesOut,
            Element element,
            ElementMutation m,
            TRaw newValue,
            PropertyMetadata metadata,
            Long timestamp
    ) {
        checkNotNull(metadata, "metadata cannot be null");
        updateProperty(changedPropertiesOut, element, m, newValue, metadata.createMetadata(), timestamp, metadata.getPropertyVisibility());
    }

    public <T extends Element> void updateProperty(
            ElementUpdateContext<T> ctx,
            TRaw newValue,
            PropertyMetadata metadata,
            Long timestamp
    ) {
        updateProperty(ctx.getProperties(), ctx.getElement(), ctx.getMutation(), newValue, metadata.createMetadata(), timestamp, metadata.getPropertyVisibility());
    }

    /**
     * @param changedPropertiesOut Adds the property to this list if the property value changed
     */
    public void updateProperty(
            List<BcPropertyUpdate> changedPropertiesOut,
            Element element,
            ElementMutation m,
            TRaw newValue,
            Metadata metadata,
            Visibility visibility
    ) {
        updateProperty(changedPropertiesOut, element, m, newValue, metadata, null, visibility);
    }

    public <T extends Element> void updateProperty(
            ElementUpdateContext<T> ctx,
            TRaw newValue,
            Metadata metadata,
            Visibility visibility
    ) {
        updateProperty(ctx.getProperties(), ctx.getElement(), ctx.getMutation(), newValue, metadata, null, visibility);
    }

    /**
     * @param changedPropertiesOut Adds the property to this list if the property value changed
     */
    public void updateProperty(
            List<BcPropertyUpdate> changedPropertiesOut,
            Element element,
            ElementMutation m,
            TRaw newValue,
            Metadata metadata,
            Long timestamp,
            Visibility visibility
    ) {
        if (newValue == null) {
            LOGGER.error("passing a null value to updateProperty will not be allowed in the future: %s", this);
            return;
        }
        if (newValue instanceof String && ((String) newValue).length() == 0) {
            LOGGER.error("passing an empty string value to updateProperty will not be allowed in the future: %s", this);
            return;
        }
        TRaw currentValue = null;
        if (element != null) {
            currentValue = getPropertyValue(element);
        }
        if (currentValue == null || !isEquals(newValue, currentValue)) {
            setProperty(m, newValue, metadata, timestamp, visibility);
            changedPropertiesOut.add(new BcPropertyUpdate(this));
        }
    }

    public <T extends Element> void updateProperty(
            ElementUpdateContext<T> ctx,
            TRaw newValue,
            Metadata metadata,
            Long timestamp,
            Visibility visibility
    ) {
        updateProperty(ctx.getProperties(), ctx.getElement(), ctx.getMutation(), newValue, metadata, timestamp, visibility);
    }
}
