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
package com.mware.ge.accumulo.iterator.model;

import com.mware.ge.store.EdgesWithEdgeInfo;
import org.apache.hadoop.io.Text;
import com.mware.ge.accumulo.iterator.util.DataOutputStreamUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

public class VertexElementData extends ElementData {
    public Text conceptType;
    public final EdgesWithEdgeInfo outEdges = new EdgesWithEdgeInfo();
    public final EdgesWithEdgeInfo inEdges = new EdgesWithEdgeInfo();
    public final Set<Text> hiddenEdges = new HashSet<>();
    public final List<SoftDeleteEdgeInfo> outSoftDeletes = new ArrayList<>();
    public final List<SoftDeleteEdgeInfo> inSoftDeletes = new ArrayList<>();

    @Override
    public void clear() {
        super.clear();
        conceptType = null;
        outEdges.clear();
        inEdges.clear();
        hiddenEdges.clear();
        outSoftDeletes.clear();
        inSoftDeletes.clear();
    }

    @Override
    protected void encode(DataOutputStream out, IteratorFetchHints fetchHints) throws IOException {
        super.encode(out, fetchHints);
        DataOutputStreamUtils.encodeEdges(
                out,
                outEdges,
                fetchHints.isIncludeEdgeLabelsAndCounts() && !(fetchHints.isIncludeAllEdgeRefs() || fetchHints.isIncludeOutEdgeRefs()),
                fetchHints.isIncludeEdgeIds(),
                fetchHints.isIncludeEdgeVertexIds()
        );

        DataOutputStreamUtils.encodeEdges(
                out,
                inEdges,
                fetchHints.isIncludeEdgeLabelsAndCounts() && !(fetchHints.isIncludeAllEdgeRefs() || fetchHints.isIncludeInEdgeRefs()),
                fetchHints.isIncludeEdgeIds(),
                fetchHints.isIncludeEdgeVertexIds()
        );

        DataOutputStreamUtils.encodeText(out, conceptType);
    }

    @Override
    protected byte getTypeId() {
        return TYPE_ID_VERTEX;
    }
}
