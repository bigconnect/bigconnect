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

import com.mware.ge.*;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.core.model.graph.ElementUpdateContext;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.values.storable.Value;

import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.*;

public abstract class BcProperty<TRaw> extends BcPropertyBase<TRaw> {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(BcProperty.class);

    protected BcProperty(String propertyName) {
        super(propertyName);
    }

    public final void addPropertyValue(ElementMutation<?> mutation, String multiKey, TRaw value, Visibility visibility) {
        mutation.addPropertyValue(multiKey, getPropertyName(), wrap(value), visibility);
    }

    public final void addPropertyValue(
            Element element,
            String multiKey,
            TRaw value,
            Visibility visibility,
            Authorizations authorizations
    ) {
        element.addPropertyValue(multiKey, getPropertyName(), wrap(value), visibility, authorizations);
    }

    public final void addPropertyValue(
            Element element,
            String multiKey,
            TRaw value,
            Metadata metadata,
            Visibility visibility,
            Authorizations authorizations
    ) {
        element.addPropertyValue(multiKey, getPropertyName(), wrap(value), metadata, visibility, authorizations);
    }

    public final void addPropertyValue(
            ElementMutation<?> mutation,
            String multiKey,
            TRaw value,
            Metadata metadata,
            Visibility visibility
    ) {
        addPropertyValue(mutation, multiKey, value, metadata, null, visibility);
    }

    public final void addPropertyValue(
            ElementMutation<?> mutation,
            String multiKey,
            TRaw value,
            Metadata metadata,
            Long timestamp,
            Visibility visibility
    ) {
        mutation.addPropertyValue(multiKey, getPropertyName(), wrap(value), metadata, timestamp, visibility);
    }

    public final TRaw getPropertyValue(Element element, String propertyKey) {
        Value value = element != null ? element.getPropertyValue(propertyKey, getPropertyName()) : null;
        return value != null ? getRawConverter().apply(value) : null;
    }

    public final TRaw getPropertyValue(Property property) {
        Value value = property.getValue();
        return value != null ? getRawConverter().apply(value) : null;
    }

    @SuppressWarnings("unchecked")
    public final Iterable<TRaw> getPropertyValues(Element element) {
        Iterable<Value> values = element != null ? element.getPropertyValues(getPropertyName()) : null;
        return values != null ? transform(values, getRawConverter()) : Collections.EMPTY_LIST;
    }

    public boolean hasProperty(Element element, String propertyKey) {
        return element.getProperty(propertyKey, getPropertyName()) != null;
    }

    public boolean hasProperty(Element element) {
        return size(element.getProperties(getPropertyName())) > 0;
    }

    public Property getProperty(Element element, String key) {
        return element.getProperty(key, getPropertyName());
    }

    public Property getOnlyProperty(Element element) {
        return getOnlyElement(element.getProperties(getPropertyName()), null);
    }

    public Property getFirstProperty(Element element) {
        return getFirst(element.getProperties(getPropertyName()), null);
    }

    public TRaw getFirstPropertyValue(Element element) {
        Property property = getFirstProperty(element);
        if (property == null) {
            return null;
        }
        return getPropertyValue(property);
    }

    public Iterable<Property> getProperties(Element element) {
        return element.getProperties(getPropertyName());
    }

    public void removeProperty(Element element, String key, Authorizations authorizations) {
        element.softDeleteProperty(key, getPropertyName(), authorizations);
    }

    public void removeProperty(ElementMutation m, String key, Visibility visibility) {
        m.softDeleteProperty(key, getPropertyName(), visibility);
    }

    public <T extends Element> void removeProperty(
            ElementUpdateContext<T> ctx,
            String propertyKey,
            Visibility visibility
    ) {
        removeProperty(ctx.getMutation(), propertyKey, visibility);
    }

    public void alterVisibility(ExistingElementMutation<?> elementMutation, String propertyKey, Visibility newVisibility) {
        elementMutation.alterPropertyVisibility(propertyKey, getPropertyName(), newVisibility);
    }

    public void removeProperty(
            List<BcPropertyUpdate> changedPropertiesOut,
            Element element,
            ElementMutation m,
            String propertyKey,
            Visibility visibility
    ) {
        Object currentValue = getPropertyValue(element, propertyKey);
        if (currentValue != null) {
            long beforeDeletionTimestamp = System.currentTimeMillis() - 1;
            removeProperty(m, propertyKey, visibility);
            changedPropertiesOut.add(new BcPropertyUpdateRemove(this, propertyKey, beforeDeletionTimestamp, true, false));
        }
    }

    public void hideProperty(
            List<BcPropertyUpdate> changedPropertiesOut,
            Element element,
            Property propertyToHide,
            String workspaceId,
            Authorizations authorizations
    ) {
        long beforeDeletionTimestamp = System.currentTimeMillis() - 1;
        element.markPropertyHidden(propertyToHide, new Visibility(workspaceId), authorizations);
        changedPropertiesOut.add(new BcPropertyUpdateRemove(this, propertyToHide.getKey(), beforeDeletionTimestamp, false, true));
    }

    /**
     * @param changedPropertiesOut Adds the property to this list if the property value changed
     * @deprecated Use {@link #updateProperty(List, Element, ElementMutation, String, Object, PropertyMetadata)}
     */
    @SuppressWarnings("deprecation")
    @Deprecated
    public void updateProperty(
            List<BcPropertyUpdate> changedPropertiesOut,
            Element element,
            ElementMutation m,
            String propertyKey,
            TRaw newValue,
            PropertyMetadata metadata,
            Visibility visibility
    ) {
        updateProperty(changedPropertiesOut, element, m, propertyKey, newValue, metadata, null, visibility);
    }

    /**
     * @param changedPropertiesOut Adds the property to this list if the property value changed
     */
    public void updateProperty(
            List<BcPropertyUpdate> changedPropertiesOut,
            Element element,
            ElementMutation m,
            String propertyKey,
            TRaw newValue,
            PropertyMetadata metadata
    ) {
        checkNotNull(metadata, "metadata is required");
        updateProperty(changedPropertiesOut, element, m, propertyKey, newValue, metadata.createMetadata(), null, metadata.getPropertyVisibility());
    }

    public <T extends Element> void updateProperty(
            ElementUpdateContext<T> ctx,
            String propertyKey,
            TRaw newValue,
            PropertyMetadata metadata
    ) {
        checkNotNull(metadata, "metadata is required");
        updateProperty(ctx.getProperties(), ctx.getElement(), ctx.getMutation(), propertyKey, newValue, metadata.createMetadata(), null, metadata.getPropertyVisibility());
    }

    /**
     * @param changedPropertiesOut Adds the property to this list if the property value changed
     * @deprecated Use {@link #updateProperty(List, Element, ElementMutation, String, Object, PropertyMetadata, Long)}
     */
    @Deprecated
    public void updateProperty(
            List<BcPropertyUpdate> changedPropertiesOut,
            Element element,
            ElementMutation m,
            String propertyKey,
            TRaw newValue,
            PropertyMetadata metadata,
            Long timestamp,
            Visibility visibility
    ) {
        checkNotNull(metadata, "metadata is required");
        updateProperty(changedPropertiesOut, element, m, propertyKey, newValue, metadata.createMetadata(), timestamp, visibility);
    }

    /**
     * @param changedPropertiesOut Adds the property to this list if the property value changed
     */
    public void updateProperty(
            List<BcPropertyUpdate> changedPropertiesOut,
            Element element,
            ElementMutation m,
            String propertyKey,
            TRaw newValue,
            PropertyMetadata metadata,
            Long timestamp
    ) {
        checkNotNull(metadata, "metadata is required");
        updateProperty(changedPropertiesOut, element, m, propertyKey, newValue, metadata.createMetadata(), timestamp, metadata.getPropertyVisibility());
    }

    public <T extends Element> void updateProperty(
            ElementUpdateContext<T> ctx,
            String propertyKey,
            TRaw newValue,
            PropertyMetadata metadata,
            Long timestamp
    ) {
        checkNotNull(metadata, "metadata is required");
        updateProperty(ctx.getProperties(), ctx.getElement(), ctx.getMutation(), propertyKey, newValue, metadata.createMetadata(), timestamp, metadata.getPropertyVisibility());
    }

    /**
     * @param changedPropertiesOut Adds the property to this list if the property value changed
     */
    public void updateProperty(
            List<BcPropertyUpdate> changedPropertiesOut,
            Element element,
            ElementMutation m,
            String propertyKey,
            TRaw newValue,
            Metadata metadata,
            Visibility visibility
    ) {
        updateProperty(changedPropertiesOut, element, m, propertyKey, newValue, metadata, null, visibility);
    }

    public <T extends Element> void updateProperty(
            ElementUpdateContext<T> ctx,
            String propertyKey,
            TRaw newValue,
            Metadata metadata,
            Visibility visibility
    ) {
        updateProperty(ctx.getProperties(), ctx.getElement(), ctx.getMutation(), propertyKey, newValue, metadata, null, visibility);
    }

    /**
     * @param changedPropertiesOut Adds the property to this list if the property value changed
     */
    public void updateProperty(
            List<BcPropertyUpdate> changedPropertiesOut,
            Element element,
            ElementMutation m,
            String propertyKey,
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
            currentValue = getPropertyValue(element, propertyKey);
        }
        if (currentValue == null || !isEquals(newValue, currentValue)) {
            addPropertyValue(m, propertyKey, newValue, metadata, timestamp, visibility);
            changedPropertiesOut.add(new BcPropertyUpdate(this, propertyKey));
        }
    }

    public <T extends Element> void updateProperty(
            ElementUpdateContext<T> ctx,
            String propertyKey,
            TRaw newValue,
            Metadata metadata,
            Long timestamp,
            Visibility visibility
    ) {
        updateProperty(ctx.getProperties(), ctx.getElement(), ctx.getMutation(), propertyKey, newValue, metadata, timestamp, visibility);
    }

    /**
     * Gets the only property value. If there are multiple property values with the same name an exception will be
     * thrown. If the property does not exist a null will be returned.
     */
    public TRaw getOnlyPropertyValue(Element element) {
        Value value = getOnlyElement(element.getPropertyValues(getPropertyName()), null);
        if (value != null) {
            return unwrap(value);
        }
        return null;
    }

    /**
     * Gets the only property value. If there are multiple property values with the same name an exception will be
     * thrown. If the property does not exist an exception will be thrown.
     */
    public TRaw getOnlyPropertyValueRequired(Element element) {
        Value value = getOnlyElement(element.getPropertyValues(getPropertyName()), null);
        checkNotNull(value, "Property value of property " + getPropertyName() + " cannot be null");
        return unwrap(value);
    }
}
