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

import com.google.common.base.Function;
import com.mware.ge.Property;
import com.mware.ge.values.storable.Value;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A BcProperty provides convenience methods for converting standard
 * property values to and from their raw types to the types required to
 * store them in a Ge instance.
 *
 * @param <TRaw>   the raw value type for this property
 */
public abstract class BcPropertyBase<TRaw> {
    private final String propertyName;
    private final Function<Value, TRaw> rawConverter;

    protected BcPropertyBase(final String propertyName) {
        this.propertyName = propertyName;
        this.rawConverter = new RawConverter();
    }

    /**
     * Convert the raw value to an appropriate value for storage
     * in Ge.
     */
    public abstract Value wrap(final TRaw value);

    /**
     * Convert the Ge value to its original raw type.
     */
    public abstract TRaw unwrap(final Value value);

    public final String getPropertyName() {
        return propertyName;
    }

    public boolean isSameName(Property property) {
        return isSameName(property.getName());
    }

    public boolean isSameName(String propertyName) {
        return this.propertyName.equals(propertyName);
    }

    protected Function<Value, TRaw> getRawConverter() {
        return rawConverter;
    }

    public TRaw getPropertyValue(Property property) {
        return unwrap(property.getValue());
    }

    protected class RawConverter implements Function<Value, TRaw> {
        @Override
        @SuppressWarnings("unchecked")
        public TRaw apply(final Value input) {
            return unwrap(input);
        }
    }

    protected boolean isEquals(TRaw newValue, TRaw currentValue) {
        checkNotNull(newValue, "newValue cannot be null");
        checkNotNull(currentValue, "currentValue cannot be null");
        return newValue.equals(currentValue);
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "{propertyName='" + propertyName + "'}";
    }
}
