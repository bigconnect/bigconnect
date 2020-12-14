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
package com.mware.ge;

import com.mware.ge.mutation.EdgeMutation;
import com.mware.ge.util.IncreasingTime;

public abstract class EdgeBuilderBase extends ElementBuilder<Edge> implements EdgeMutation {
    private String outVertexId;
    private String inVertexId;
    private String label;
    private String newEdgeLabel;
    private long alterEdgeLabelTimestamp;

    protected EdgeBuilderBase(
            String edgeId,
            String outVertexId,
            String inVertexId,
            String label,
            Visibility visibility
    ) {
        super(ElementType.EDGE, edgeId, visibility);
        this.outVertexId = outVertexId;
        this.inVertexId = inVertexId;
        this.label = label;
        this.alterEdgeLabelTimestamp = IncreasingTime.currentTimeMillis();
    }

    @Override
    public String getVertexId(Direction direction) {
        switch (direction) {
            case OUT:
                return outVertexId;
            case IN:
                return inVertexId;
            default:
                throw new GeException("unhandled direction: " + direction);
        }
    }

    @Override
    public String getEdgeLabel() {
        return label;
    }

    @Override
    public long getAlterEdgeLabelTimestamp() {
        return alterEdgeLabelTimestamp;
    }

    @Override
    public EdgeMutation alterEdgeLabel(String newEdgeLabel) {
        this.newEdgeLabel = newEdgeLabel;
        return this;
    }

    public EdgeBuilderBase overrideAlterLabelTimestamp(long alterEdgeLabelTimestamp) {
        this.alterEdgeLabelTimestamp = alterEdgeLabelTimestamp;
        return this;
    }

    @Override
    public String getNewEdgeLabel() {
        return newEdgeLabel;
    }

    /**
     * Save the edge along with any properties that were set to the graph.
     *
     * @return The newly created edge.
     */
    @Override
    public abstract Edge save(Authorizations authorizations);

    @Override
    public boolean hasChanges() {
        if (newEdgeLabel != null) {
            return true;
        }

        return super.hasChanges();
    }

    public void setInVertexId(String inVertexId) {
        this.inVertexId = inVertexId;
    }

    public void setOutVertexId(String outVertexId) {
        this.outVertexId = outVertexId;
    }

    public void setLabel(String label) {
        this.label = label;
    }
}
