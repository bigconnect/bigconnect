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
package com.mware.ge.store.decoder;

import com.mware.ge.Authorizations;
import com.mware.ge.FetchHints;
import com.mware.ge.Visibility;
import com.mware.ge.collection.Pair;
import com.mware.ge.collection.PrefetchingIterator;
import com.mware.ge.store.*;
import com.mware.ge.store.util.SoftDeleteEdgeInfo;

import java.util.List;
import java.util.Set;

public class VertexDecoder extends ElementDecoder<VertexElementData> {
    public VertexDecoder(PrefetchingIterator<Pair<StoreKey, StoreValue>> storeIterable, StorableGraph graph, FetchHints fetchHints, Authorizations authorizations) {
        super(storeIterable, graph, fetchHints, authorizations);
    }

    @Override
    protected VertexElementData decode(List<Pair<StoreKey, StoreValue>> mutations) {
        VertexElementData data = super.decode(mutations);
        if (data != null) {
            removeHiddenAndSoftDeletes();
        }
        return data;
    }

    @Override
    protected VertexElementData createElementData(StorableGraph graph) {
        VertexElementData data = new VertexElementData();
        data.graph = graph;
        return data;
    }

    @Override
    protected String getVisibilitySignal() {
        return StorableVertex.CF_SIGNAL;
    }

    @Override
    protected boolean processColumn(Pair<StoreKey, StoreValue> keyValue) {
        StoreKey key = keyValue.first();
        StoreValue value = keyValue.other();

        if (key.cf().equals(StorableVertex.CF_OUT_EDGE)) {
            processOutEdge(keyValue);
            return true;
        }

        if (key.cf().equals(StorableVertex.CF_IN_EDGE)) {
            processInEdge(keyValue);
            return true;
        }

        if (key.cf().equals(StorableVertex.CF_OUT_EDGE_HIDDEN) || key.cf().equals(StorableVertex.CF_IN_EDGE_HIDDEN)) {
            String edgeId = key.cq();
            getElementData().hiddenEdges.add(edgeId);
            getElementData().hiddenEdgesVisibilities.put(edgeId, key.visibility());
            return true;
        }

        if (key.cf().equals(StorableVertex.CF_IN_EDGE_SOFT_DELETE)) {
            String edgeId = key.cq();
            getElementData().inSoftDeletes.add(new SoftDeleteEdgeInfo(edgeId, value.ts()));
            return true;
        }

        if (key.cf().equals(StorableVertex.CF_OUT_EDGE_SOFT_DELETE)) {
            String edgeId = key.cq();
            getElementData().outSoftDeletes.add(new SoftDeleteEdgeInfo(edgeId, value.ts()));
            return true;
        }

        return false;
    }

    private void processOutEdge(Pair<StoreKey, StoreValue> keyValue) {
        StorableEdgeInfo edgeInfo = new StorableEdgeInfo(keyValue.other().value(), keyValue.other().ts());
        if (authorizations.canRead(keyValue.first().visibility()) && shouldIncludeOutEdge(edgeInfo)) {
            String edgeId = keyValue.first().cq();
            getElementData().outEdges.add(edgeId, edgeInfo);
        }
    }

    private void processInEdge(Pair<StoreKey, StoreValue> keyValue) {
        StorableEdgeInfo edgeInfo = new StorableEdgeInfo(keyValue.other().value(), keyValue.other().ts());
        if (authorizations.canRead(keyValue.first().visibility()) && shouldIncludeInEdge(edgeInfo)) {
            String edgeId = keyValue.first().cq();
            getElementData().inEdges.add(edgeId, edgeInfo);
        }
    }

    private boolean shouldIncludeOutEdge(StorableEdgeInfo edgeInfo) {
        Set<String> labels = getFetchHints().getEdgeLabelsOfEdgeRefsToInclude();
        if (labels != null && labels.contains(edgeInfo.getLabel())) {
            return true;
        }

        return getFetchHints().isIncludeAllEdgeRefs()
                || getFetchHints().isIncludeEdgeLabelsAndCounts()
                || getFetchHints().isIncludeOutEdgeRefs();
    }

    private boolean shouldIncludeInEdge(StorableEdgeInfo edgeInfo) {
        Set<String> labels = getFetchHints().getEdgeLabelsOfEdgeRefsToInclude();
        if (labels != null && labels.contains(edgeInfo.getLabel())) {
            return true;
        }

        return getFetchHints().isIncludeAllEdgeRefs()
                || getFetchHints().isIncludeEdgeLabelsAndCounts()
                || getFetchHints().isIncludeInEdgeRefs();
    }

    @Override
    protected void processSignalColumn(Pair<StoreKey, StoreValue> keyValue) {
        super.processSignalColumn(keyValue);
        getElementData().conceptType = keyValue.first().cq();
    }

    @Override
    protected boolean populateElementData(List<Pair<StoreKey, StoreValue>> mutations) {
        boolean ret = super.populateElementData(mutations);
        if (ret) {
            removeHiddenAndSoftDeletes();
        }
        return ret;
    }

    private void removeHiddenAndSoftDeletes() {
        if (!getFetchHints().isIncludeHidden()) {
            for (String edgeId : this.getElementData().hiddenEdges) {
                Visibility hiddenEdgeVisibility = this.getElementData().hiddenEdgesVisibilities.get(edgeId);
                if (authorizations.canRead(hiddenEdgeVisibility)) {
                    this.getElementData().inEdges.remove(edgeId);
                    this.getElementData().outEdges.remove(edgeId);
                }
            }
        }

        for (SoftDeleteEdgeInfo inSoftDelete : this.getElementData().inSoftDeletes) {
            StorableEdgeInfo inEdge = this.getElementData().inEdges.get(inSoftDelete.getEdgeId());
            if (inEdge != null && inSoftDelete.getTimestamp() >= inEdge.getTimestamp()) {
                this.getElementData().inEdges.remove(inSoftDelete.getEdgeId());
            }
        }

        for (SoftDeleteEdgeInfo outSoftDelete : this.getElementData().outSoftDeletes) {
            StorableEdgeInfo outEdge = this.getElementData().outEdges.get(outSoftDelete.getEdgeId());
            if (outEdge != null && outSoftDelete.getTimestamp() >= outEdge.getTimestamp()) {
                this.getElementData().outEdges.remove(outSoftDelete.getEdgeId());
            }
        }
    }
}
