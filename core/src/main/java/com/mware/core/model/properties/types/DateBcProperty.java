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

import com.mware.core.model.graph.ElementUpdateContext;
import com.mware.ge.Element;
import com.mware.ge.values.storable.DateTimeValue;
import com.mware.ge.values.storable.NoValue;
import com.mware.ge.values.storable.Value;
import com.mware.ge.values.storable.Values;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;

/**
 * A BcProperty that converts Dates to an appropriate value for
 * storage in Ge.
 */
public class DateBcProperty extends BcProperty<ZonedDateTime> {
    public DateBcProperty(String key) {
        super(key);
    }

    @Override
    public Value wrap(ZonedDateTime value) {
        return Values.of(value);
    }

    @Override
    public ZonedDateTime unwrap(Value value) {
        if (value == null || value instanceof NoValue)
            return null;
        else
            return ((DateTimeValue) value).asObjectCopy();
    }

    public <T extends Element> void updateProperty(
            ElementUpdateContext<T> ctx,
            String propertyKey,
            OffsetDateTime newValue,
            PropertyMetadata metadata
    ) {
        updateProperty(ctx, propertyKey, newValue, metadata, null);
    }

    public <T extends Element> void updateProperty(
            ElementUpdateContext<T> ctx,
            String propertyKey,
            OffsetDateTime newValue,
            PropertyMetadata metadata,
            Long timestamp
    ) {
        ZonedDateTime date = newValue == null ? null : ZonedDateTime.from(newValue.toInstant());
        updateProperty(ctx, propertyKey, date, metadata, timestamp);
    }

    public <T extends Element> void updateProperty(
            ElementUpdateContext<T> ctx,
            String propertyKey,
            ZonedDateTime newValue,
            PropertyMetadata metadata
    ) {
        updateProperty(ctx, propertyKey, newValue, metadata, null);
    }

    public <T extends Element> void updateProperty(
            ElementUpdateContext<T> ctx,
            String propertyKey,
            ZonedDateTime newValue,
            PropertyMetadata metadata,
            Long timestamp
    ) {
        ZonedDateTime date = newValue == null ? null : ZonedDateTime.from(newValue.toInstant());
        updateProperty(ctx, propertyKey, date, metadata, timestamp);
    }

    /**
     * Updates the element with the new property value if the property value is newer than the existing property value
     * or the update does not have an existing element (for example a new element or a blind write mutation)
     */
    public <T extends Element> void updatePropertyIfValueIsNewer(
            ElementUpdateContext<T> ctx,
            String propertyKey,
            ZonedDateTime newValue,
            PropertyMetadata metadata,
            Long timestamp
    ) {
        if (isDateNewer(ctx.getElement(), propertyKey, newValue)) {
            updateProperty(ctx, propertyKey, newValue, metadata, timestamp);
        }
    }

    /**
     * Updates the element with the new property value if the property value is newer than the existing property value
     * or the update does not have an existing element (for example a new element or a blind write mutation)
     */
    public <T extends Element> void updatePropertyIfValueIsNewer(
            ElementUpdateContext<T> ctx,
            String propertyKey,
            ZonedDateTime newValue,
            PropertyMetadata metadata
    ) {
        if (isDateNewer(ctx.getElement(), propertyKey, newValue)) {
            updateProperty(ctx, propertyKey, newValue, metadata);
        }
    }

    /**
     * Updates the element with the new property value if the property value is older than the existing property value
     * or the update does not have an existing element (for example a new element or a blind write mutation)
     */
    public <T extends Element> void updatePropertyIfValueIsOlder(
            ElementUpdateContext<T> ctx,
            String propertyKey,
            ZonedDateTime newValue,
            PropertyMetadata metadata,
            Long timestamp
    ) {
        if (isDateOlder(ctx.getElement(), propertyKey, newValue)) {
            updateProperty(ctx, propertyKey, newValue, metadata, timestamp);
        }
    }

    /**
     * Updates the element with the new property value if the property value is older than the existing property value
     * or the update does not have an existing element (for example a new element or a blind write mutation)
     */
    public <T extends Element> void updatePropertyIfValueIsOlder(
            ElementUpdateContext<T> ctx,
            String propertyKey,
            ZonedDateTime newValue,
            PropertyMetadata metadata
    ) {
        if (isDateOlder(ctx.getElement(), propertyKey, newValue)) {
            updateProperty(ctx, propertyKey, newValue, metadata);
        }
    }

    private <T extends Element> boolean isDateNewer(T element, String propertyKey, ZonedDateTime newValue) {
        if (element == null) {
            return true;
        }
        ZonedDateTime existingValue = getPropertyValue(element, propertyKey);
        if (existingValue == null) {
            return true;
        }
        return existingValue.compareTo(newValue) < 0;
    }

    private <T extends Element> boolean isDateOlder(T element, String propertyKey, ZonedDateTime newValue) {
        if (element == null) {
            return true;
        }
        ZonedDateTime existingValue = getPropertyValue(element, propertyKey);
        if (existingValue == null) {
            return true;
        }
        return existingValue.compareTo(newValue) > 0;
    }

    public ZonedDateTime getPropertyValueDateTimeUtc(Element element, String propertyKey) {
        return getPropertyValueDateTime(element, propertyKey, ZoneOffset.UTC);
    }


    public ZonedDateTime getPropertyValueDateTime(Element element, String propertyKey, ZoneId zoneId) {
        ZonedDateTime value = getPropertyValue(element, propertyKey);
        if (value == null) {
            return null;
        }
        return ZonedDateTime.ofInstant(value.toInstant(), zoneId);
    }

    public ZonedDateTime getFirstPropertyValueDateTimeUtc(Element element) {
        return getFirstPropertyValueDateTime(element, ZoneOffset.UTC);
    }

    public ZonedDateTime getFirstPropertyValueDateTime(Element element, ZoneId zoneId) {
        ZonedDateTime value = getFirstPropertyValue(element);
        if (value == null) {
            return null;
        }
        return ZonedDateTime.ofInstant(value.toInstant(), zoneId);
    }
}
