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
package com.mware.ge.event;

import com.mware.ge.mutation.PropertySoftDeleteMutation;
import com.mware.ge.Element;
import com.mware.ge.Graph;
import com.mware.ge.Property;
import com.mware.ge.Visibility;

public class SoftDeletePropertyEvent extends GraphEvent {
    private final Element element;
    private final String key;
    private final String name;
    private final Visibility visibility;

    public SoftDeletePropertyEvent(Graph graph, Element element, Property property) {
        super(graph);
        this.element = element;
        this.key = property.getKey();
        this.name = property.getName();
        this.visibility = property.getVisibility();
    }

    public SoftDeletePropertyEvent(Graph graph, Element element, PropertySoftDeleteMutation propertySoftDeleteMutation) {
        super(graph);
        this.element = element;
        this.key = propertySoftDeleteMutation.getKey();
        this.name = propertySoftDeleteMutation.getName();
        this.visibility = propertySoftDeleteMutation.getVisibility();
    }

    public Element getElement() {
        return element;
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
    public int hashCode() {
        return getKey().hashCode() ^ getName().hashCode() ^ getVisibility().hashCode();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{element=" + getElement() + ", property=" + getKey() + ":" + getName() + ":" + getVisibility() + '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SoftDeletePropertyEvent)) {
            return false;
        }

        SoftDeletePropertyEvent other = (SoftDeletePropertyEvent) obj;
        return getElement().equals(other.getElement())
                && getKey().equals(other.getKey())
                && getName().equals(other.getName())
                && getVisibility().equals(other.getVisibility());
    }
}
