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
package com.mware.ge.mutation;

import com.mware.ge.Visibility;

public abstract class PropertySoftDeleteMutation implements Comparable<PropertySoftDeleteMutation> {
    public abstract String getKey();

    public abstract String getName();

    public abstract Long getTimestamp();

    public abstract Visibility getVisibility();

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof PropertySoftDeleteMutation)) {
            return false;
        }

        PropertySoftDeleteMutation that = (PropertySoftDeleteMutation) o;

        if (getKey() != null ? !getKey().equals(that.getKey()) : that.getKey() != null) {
            return false;
        }
        if (getName() != null ? !getName().equals(that.getName()) : that.getName() != null) {
            return false;
        }
        if (getVisibility() != null ? !getVisibility().equals(that.getVisibility()) : that.getVisibility() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int compareTo(PropertySoftDeleteMutation that) {
        if (this == that) {
            return 0;
        }
        if (that == null) {
            return -1;
        }

        if (getKey() != null && that.getKey() == null) {
            return -1;
        }
        if (getKey() == null && that.getKey() != null) {
            return 1;
        }
        if (getKey() != null) {
            int result = getKey().compareTo(that.getKey());
            if (result != 0) {
                return result;
            }
        }

        if (getName() != null && that.getName() == null) {
            return -1;
        }
        if (getName() == null && that.getName() != null) {
            return 1;
        }
        if (getName() != null) {
            int result = getName().compareTo(that.getName());
            if (result != 0) {
                return result;
            }
        }

        if (getVisibility() != null && that.getVisibility() == null) {
            return -1;
        }
        if (getVisibility() == null && that.getVisibility() != null) {
            return 1;
        }
        if (getVisibility() != null) {
            int result = getVisibility().compareTo(that.getVisibility());
            if (result != 0) {
                return result;
            }
        }

        return 0;
    }

    @Override
    public int hashCode() {
        int result = getKey() != null ? getKey().hashCode() : 0;
        result = 31 * result + (getName() != null ? getName().hashCode() : 0);
        result = 31 * result + (getVisibility() != null ? getVisibility().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" +
                "key='" + getKey() + '\'' +
                ", name='" + getName() + '\'' +
                ", visibility=" + getVisibility() +
                '}';
    }
}
