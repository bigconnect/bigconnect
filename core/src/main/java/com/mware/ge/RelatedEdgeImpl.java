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

public class RelatedEdgeImpl implements RelatedEdge {
    private final String edgeId;
    private final String label;
    private final String outVertexId;
    private final String inVertexId;

    public RelatedEdgeImpl(String edgeId, String label, String outVertexId, String inVertexId) {
        this.edgeId = edgeId;
        this.label = label;
        this.outVertexId = outVertexId;
        this.inVertexId = inVertexId;
    }

    @Override
    public String getEdgeId() {
        return edgeId;
    }

    @Override
    public String getInVertexId() {
        return inVertexId;
    }

    @Override
    public String getOutVertexId() {
        return outVertexId;
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public String toString() {
        return "RelatedEdgeImpl{" +
                "edgeId='" + edgeId + '\'' +
                ", label='" + label + '\'' +
                ", inVertexId='" + inVertexId + '\'' +
                ", outVertexId='" + outVertexId + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof RelatedEdge)) {
            return false;
        }

        RelatedEdge that = (RelatedEdge) o;

        if (!edgeId.equals(that.getEdgeId())) {
            return false;
        }
        if (!label.equals(that.getLabel())) {
            return false;
        }
        if (!inVertexId.equals(that.getInVertexId())) {
            return false;
        }
        if (!outVertexId.equals(that.getOutVertexId())) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return edgeId.hashCode();
    }
}
