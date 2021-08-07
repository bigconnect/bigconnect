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

import com.mware.ge.Property;
import com.mware.ge.PropertyDefinition;
import com.mware.ge.TextIndexHint;
import com.mware.ge.GeException;
import com.mware.ge.values.storable.GeoPointValue;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.type.GeoPoint;
import com.mware.ge.util.IterableUtils;
import com.mware.ge.values.storable.TextValue;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

public enum TextPredicate implements Predicate {
    CONTAINS, DOES_NOT_CONTAIN;

    @Override
    public boolean evaluate(final Iterable<Property> properties, final Object second) {
        if (IterableUtils.count(properties) == 0 && this == DOES_NOT_CONTAIN) {
            return true;
        }

        for (Property property : properties) {
            if (evaluate(property.getValue(), second)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean evaluate(Object first, Object second) {
        if (!canEvaulate(first) || !canEvaulate(second)) {
            throw new GeException("Text predicates are only valid for string or GeoPoint fields");
        }

        String firstString = valueToString(first);
        String secondString = valueToString(second);

        switch (this) {
            case CONTAINS:
                return firstString.contains(secondString);
            case DOES_NOT_CONTAIN:
                String[] tokenizedString = firstString.split("\\W+");
                return !Arrays.asList(tokenizedString).contains(secondString);
            default:
                throw new IllegalArgumentException("Invalid text predicate: " + this);
        }
    }

    @Override
    public void validate() {
    }

    private String valueToString(Object val) {
        if (val instanceof StreamingPropertyValue) {
            return ((StreamingPropertyValue) val).readToString().toLowerCase();
        }

        return ((TextValue) val).toLower().stringValue();
    }

    private boolean canEvaulate(Object first) {
        if (first instanceof TextValue) {
            return true;
        }
        if (first instanceof GeoPointValue) {
            return true;
        }
        if (first instanceof StreamingPropertyValue && TextValue.class.isAssignableFrom(((StreamingPropertyValue) first).getValueType())) {
            return true;
        }
        return false;
    }
}
