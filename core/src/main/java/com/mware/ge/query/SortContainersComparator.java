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

import com.mware.ge.GeException;
import com.mware.ge.Element;
import com.mware.ge.GeObject;
import com.mware.ge.values.storable.Value;
import com.mware.ge.values.storable.Values;

import java.util.Comparator;
import java.util.List;

import static com.mware.ge.util.IterableUtils.toList;

public class SortContainersComparator<T> implements Comparator<T> {
    private final Iterable<Query.SortContainer> sortContainers;

    public SortContainersComparator(Iterable<Query.SortContainer> sortContainers) {
        this.sortContainers = sortContainers;
    }

    @Override
    public int compare(T elem1, T elem2) {
        for (Query.SortContainer sortContainer : sortContainers) {
            int result = compare(sortContainer, elem1, elem2);
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    private int compare(Query.SortContainer sortContainer, T geObject1, T geObject2) {
        if (geObject1 instanceof GeObject && geObject2 instanceof GeObject) {
            GeObject elem1 = (GeObject) geObject1;
            GeObject elem2 = (GeObject) geObject2;
            if (sortContainer instanceof QueryBase.PropertySortContainer) {
                return compareProperty((QueryBase.PropertySortContainer) sortContainer, elem1, elem2);
            } else if (sortContainer instanceof QueryBase.SortingStrategySortContainer) {
                return compareSortingStrategy((QueryBase.SortingStrategySortContainer) sortContainer, elem1, elem2);
            } else {
                throw new GeException("Unexpected sort container type: " + sortContainer.getClass().getName());
            }
        } else {
            throw new GeException("unexpected searchable item combination: "+geObject1.getClass().getName()+", "+geObject2.getClass().getName());
        }
    }

    private int compareSortingStrategy(QueryBase.SortingStrategySortContainer sortContainer, GeObject elem1, GeObject elem2) {
        return sortContainer.sortingStrategy.compare(elem1, elem2, sortContainer.direction);
    }

    private int compareProperty(QueryBase.PropertySortContainer sortContainer, GeObject elem1, GeObject elem2) {
        List<Value> elem1PropertyValues = toList(elem1.getPropertyValues(sortContainer.propertyName));
        List<Value> elem2PropertyValues = toList(elem2.getPropertyValues(sortContainer.propertyName));
        if (elem1PropertyValues.size() > 0 && elem2PropertyValues.size() == 0) {
            return -1;
        } else if (elem2PropertyValues.size() > 0 && elem1PropertyValues.size() == 0) {
            return 1;
        } else {
            for (Value elem1PropertyValue : elem1PropertyValues) {
                for (Value elem2PropertyValue : elem2PropertyValues) {
                    int result = comparePropertyValues(elem1PropertyValue, elem2PropertyValue);
                    if (result != 0) {
                        return sortContainer.direction == SortDirection.ASCENDING ? result : -result;
                    }
                }
            }
        }
        return 0;
    }

    @SuppressWarnings("unchecked")
    private int comparePropertyValues(Object v1, Object v2) {
        if (v1 instanceof Value && v2 instanceof Value)
            return Values.COMPARATOR.compare((Value) v1, (Value) v2);

        if (v1.getClass() == v2.getClass() && v1 instanceof Comparable) {
            return ((Comparable) v1).compareTo(v2);
        }

        return 0;
    }
}
