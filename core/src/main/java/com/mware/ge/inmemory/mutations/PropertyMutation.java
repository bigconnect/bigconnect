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
package com.mware.ge.inmemory.mutations;

import com.mware.ge.Property;
import com.mware.ge.Visibility;

import java.util.Objects;

public class PropertyMutation extends Mutation {
    private final String propertyKey;
    private final String propertyName;
    private final Visibility propertyVisibility;

    protected PropertyMutation(long timestamp, String propertyKey, String propertyName, Visibility propertyVisibility, Visibility visibility) {
        super(timestamp, visibility);
        this.propertyKey = propertyKey;
        this.propertyName = propertyName;
        this.propertyVisibility = propertyVisibility;
    }

    protected PropertyMutation(long timestamp, Property property, Visibility visibility) {
        this(timestamp, property.getKey(), property.getName(), property.getVisibility(), visibility);
    }

    public String getPropertyKey() {
        return propertyKey;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public Visibility getPropertyVisibility() {
        return propertyVisibility;
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "{" +
                "propertyKey='" + propertyKey + '\'' +
                ", propertyName='" + propertyName + '\'' +
                ", propertyVisibility=" + propertyVisibility +
                ", timestamp=" + getTimestamp() +
                ", visibility=" + getVisibility() +
                '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PropertyMutation))
            return false;

        PropertyMutation other = (PropertyMutation) obj;

        return super.equals(other)
                && Objects.equals(propertyKey, other.propertyKey)
                && Objects.equals(propertyName, other.propertyName)
                && Objects.equals(propertyVisibility, other.propertyVisibility);

    }
}
