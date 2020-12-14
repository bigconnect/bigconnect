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
package com.mware.ge.store;

import com.google.common.collect.ImmutableSet;
import com.mware.ge.*;
import com.mware.ge.mutation.ExistingEdgeMutation;
import com.mware.ge.mutation.PropertyDeleteMutation;
import com.mware.ge.mutation.PropertySoftDeleteMutation;
import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;
import java.util.Iterator;

public class StorableEdge extends StorableElement implements Edge {
    public static final String CF_SIGNAL = "E";
    public static final String CF_OUT_VERTEX = "EOUT";
    public static final String CF_IN_VERTEX = "EIN";

    protected String outVertexId;
    protected String inVertexId;
    protected String label;
    protected String newEdgeLabel;

    public StorableEdge(
            StorableGraph graph,
            String id,
            String outVertexId,
            String inVertexId,
            String label,
            String newEdgeLabel,
            Visibility visibility,
            Iterable<Property> properties,
            Iterable<PropertyDeleteMutation> propertyDeleteMutations,
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations,
            Iterable<Visibility> hiddenVisibilities,
            ImmutableSet<String> extendedDataTableNames,
            long timestamp,
            FetchHints fetchHints,
            Authorizations authorizations
    ) {
        super(
                graph,
                id,
                visibility,
                properties,
                propertyDeleteMutations,
                propertySoftDeleteMutations,
                hiddenVisibilities,
                extendedDataTableNames,
                timestamp,
                fetchHints,
                authorizations
        );
        this.outVertexId = outVertexId;
        this.inVertexId = inVertexId;
        this.label = label;
        this.newEdgeLabel = newEdgeLabel;
    }

    public String getNewEdgeLabel() {
        return newEdgeLabel;
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public String getVertexId(Direction direction) {
        switch (direction) {
            case OUT:
                return outVertexId;
            case IN:
                return inVertexId;
            default:
                throw new IllegalArgumentException("Unexpected direction: " + direction);
        }
    }

    @Override
    public Vertex getVertex(Direction direction, Authorizations authorizations) {
        return getVertex(direction, getGraph().getDefaultFetchHints(), authorizations);
    }

    @Override
    public String getOtherVertexId(String myVertexId) {
        if (inVertexId.equals(myVertexId)) {
            return outVertexId;
        } else if (outVertexId.equals(myVertexId)) {
            return inVertexId;
        }
        throw new GeException("myVertexId(" + myVertexId + ") does not appear on edge (" + getId() + ") in either the in (" + inVertexId + ") or the out (" + outVertexId + ").");
    }

    @Override
    public Vertex getOtherVertex(String myVertexId, Authorizations authorizations) {
        return getOtherVertex(myVertexId, getGraph().getDefaultFetchHints(), authorizations);
    }

    @Override
    public Vertex getOtherVertex(String myVertexId, FetchHints fetchHints, Authorizations authorizations) {
        return getGraph().getVertex(getOtherVertexId(myVertexId), fetchHints, authorizations);
    }

    @Override
    public EdgeVertices getVertices(Authorizations authorizations) {
        return getVertices(getGraph().getDefaultFetchHints(), authorizations);
    }

    @Override
    public Vertex getVertex(Direction direction, FetchHints fetchHints, Authorizations authorizations) {
        return getGraph().getVertex(getVertexId(direction), fetchHints, authorizations);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ExistingEdgeMutation prepareMutation() {
        return new ExistingEdgeMutation(this) {
            @Override
            public Edge save(Authorizations authorizations) {
                saveExistingElementMutation(this, authorizations);
                return getElement();
            }
        };
    }

    protected void setLabel(String label) {
        this.label = label;
    }
}
