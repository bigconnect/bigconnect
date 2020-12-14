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
package com.mware.ge.property;

import com.mware.ge.Property;
import com.mware.ge.Visibility;
import com.mware.ge.mutation.PropertyDeleteMutation;
import com.mware.ge.mutation.PropertySoftDeleteMutation;

/**
 * Encapsulates the parameters necessary to perform a Property search
 */
public class PropertyDescriptor {
    private final String key;
    private final String name;
    private final Visibility visibility;

    public PropertyDescriptor(String key, String name, Visibility visibility) {
        this.key = key;
        this.name = name;
        this.visibility = visibility;
    }

    public static PropertyDescriptor from(String key, String name, Visibility visibility) {
        return new PropertyDescriptor(key, name, visibility);
    }

    public static PropertyDescriptor fromProperty(Property p) {
        return new PropertyDescriptor(p.getKey(), p.getName(), p.getVisibility());
    }

    public static PropertyDescriptor fromPropertyDeleteMutation(PropertyDeleteMutation p) {
        return new PropertyDescriptor(p.getKey(), p.getName(), p.getVisibility());
    }

    public static PropertyDescriptor fromPropertySoftDeleteMutation(PropertySoftDeleteMutation p) {
        return new PropertyDescriptor(p.getKey(), p.getName(), p.getVisibility());
    }

    public String getKey() {
        return key;
    }

    public String getName() {
        return name;
    }

    public Visibility getVisibility() {
        return visibility;
    }

    @Override
    public String toString() {
        return "PropertyDescriptor{" +
                "key='" + key + '\'' +
                ", name='" + name + '\'' +
                ", visibility=" + visibility +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PropertyDescriptor that = (PropertyDescriptor) o;

        if (key != null ? !key.equals(that.key) : that.key != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        return visibility != null ? visibility.equals(that.visibility) : that.visibility == null;

    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (visibility != null ? visibility.hashCode() : 0);
        return result;
    }
}