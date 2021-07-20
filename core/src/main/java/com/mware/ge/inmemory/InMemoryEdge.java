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
package com.mware.ge.inmemory;

import com.mware.ge.*;
import com.mware.ge.inmemory.mutations.AlterEdgeLabelMutation;
import com.mware.ge.inmemory.mutations.EdgeSetupMutation;
import com.mware.ge.mutation.ExistingEdgeMutation;
import com.mware.ge.search.IndexHint;

public class InMemoryEdge extends InMemoryElement<InMemoryEdge> implements Edge {
    private final EdgeSetupMutation edgeSetupMutation;

    public InMemoryEdge(
            InMemoryGraph graph,
            String id,
            InMemoryTableEdge inMemoryTableElement,
            FetchHints fetchHints,
            Long endTime,
            Authorizations authorizations
    ) {
        super(graph, id, inMemoryTableElement, fetchHints, endTime, authorizations);
        edgeSetupMutation = inMemoryTableElement.findLastMutation(EdgeSetupMutation.class);
    }

    @Override
    public String getLabel() {
        return getInMemoryTableElement().findLastMutation(AlterEdgeLabelMutation.class).getNewEdgeLabel();
    }

    @Override
    public String getVertexId(Direction direction) {
        switch (direction) {
            case IN:
                return edgeSetupMutation.getInVertexId();
            case OUT:
                return edgeSetupMutation.getOutVertexId();
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
        if (edgeSetupMutation.getInVertexId().equals(myVertexId)) {
            return edgeSetupMutation.getOutVertexId();
        } else if (edgeSetupMutation.getOutVertexId().equals(myVertexId)) {
            return edgeSetupMutation.getInVertexId();
        }
        throw new GeException("myVertexId does not appear on either the in or the out.");
    }

    @Override
    public EdgeVertices getVertices(FetchHints fetchHints, Authorizations authorizations) {
        return new EdgeVertices(
                getVertex(Direction.OUT, authorizations),
                getVertex(Direction.IN, authorizations)
        );
    }

    @Override
    @SuppressWarnings("unchecked")
    public ExistingEdgeMutation prepareMutation() {
        return new ExistingEdgeMutation(this) {
            @Override
            public Edge save(Authorizations authorizations) {
                IndexHint indexHint = getIndexHint();
                saveExistingElementMutation(this, indexHint, authorizations);
                Edge edge = getElement();
                if (indexHint != IndexHint.DO_NOT_INDEX) {
                    getGraph().updateElementAndExtendedDataInSearchIndex(edge, this, authorizations);
                }
                return edge;
            }
        };
    }
}
