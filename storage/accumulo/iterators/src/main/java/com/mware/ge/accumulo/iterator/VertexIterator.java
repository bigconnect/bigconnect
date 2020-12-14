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
package com.mware.ge.accumulo.iterator;

import com.mware.ge.accumulo.iterator.model.IteratorFetchHints;
import com.mware.ge.accumulo.iterator.model.SoftDeleteEdgeInfo;
import com.mware.ge.accumulo.iterator.model.VertexElementData;
import com.mware.ge.store.StorableEdgeInfo;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class VertexIterator extends ElementIterator<VertexElementData> {
    public static final String CF_SIGNAL_STRING = "V";
    public static final Text CF_SIGNAL = new Text(CF_SIGNAL_STRING);
    private static final byte[] CF_SIGNAL_BYTES = CF_SIGNAL.getBytes();

    public static final String CF_OUT_EDGE_STRING = "EOUT";
    public static final Text CF_OUT_EDGE = new Text(CF_OUT_EDGE_STRING);
    private static final byte[] CF_OUT_EDGE_BYTES = CF_OUT_EDGE.getBytes();

    public static final String CF_OUT_EDGE_HIDDEN_STRING = "EOUTH";
    public static final Text CF_OUT_EDGE_HIDDEN = new Text(CF_OUT_EDGE_HIDDEN_STRING);
    private static final byte[] CF_OUT_EDGE_HIDDEN_BYTES = CF_OUT_EDGE_HIDDEN.getBytes();

    public static final String CF_OUT_EDGE_SOFT_DELETE_STRING = "EOUTD";
    public static final Text CF_OUT_EDGE_SOFT_DELETE = new Text(CF_OUT_EDGE_SOFT_DELETE_STRING);
    private static final byte[] CF_OUT_EDGE_SOFT_DELETE_BYTES = CF_OUT_EDGE_SOFT_DELETE.getBytes();

    public static final String CF_IN_EDGE_STRING = "EIN";
    public static final Text CF_IN_EDGE = new Text(CF_IN_EDGE_STRING);
    private static final byte[] CF_IN_EDGE_BYTES = CF_IN_EDGE.getBytes();

    public static final String CF_IN_EDGE_HIDDEN_STRING = "EINH";
    public static final Text CF_IN_EDGE_HIDDEN = new Text(CF_IN_EDGE_HIDDEN_STRING);
    private static final byte[] CF_IN_EDGE_HIDDEN_BYTES = CF_IN_EDGE_HIDDEN.getBytes();

    public static final String CF_IN_EDGE_SOFT_DELETE_STRING = "EIND";
    public static final Text CF_IN_EDGE_SOFT_DELETE = new Text(CF_IN_EDGE_SOFT_DELETE_STRING);
    private static final byte[] CF_IN_EDGE_SOFT_DELETE_BYTES = CF_IN_EDGE_SOFT_DELETE.getBytes();

    public VertexIterator() {
        this(null, false);
    }

    public VertexIterator(IteratorFetchHints fetchHints, boolean compressTransfer) {
        super(null, fetchHints, compressTransfer);
    }

    public VertexIterator(SortedKeyValueIterator<Key, Value> source, IteratorFetchHints fetchHints, boolean compressTransfer) {
        super(source, fetchHints, compressTransfer);
    }

    @Override
    protected Text loadElement() throws IOException {
        Text ret = super.loadElement();
        if (ret != null) {
            removeHiddenAndSoftDeletes();
        }
        return ret;
    }

    @Override
    protected boolean populateElementData(List<Key> keys, List<Value> values) {
        boolean ret = super.populateElementData(keys, values);
        if (ret) {
            removeHiddenAndSoftDeletes();
        }
        return ret;
    }

    private void removeHiddenAndSoftDeletes() {
        if (!getFetchHints().isIncludeHidden()) {
            for (Text edgeId : this.getElementData().hiddenEdges) {
                this.getElementData().inEdges.remove(edgeId.toString());
                this.getElementData().outEdges.remove(edgeId.toString());
            }
        }

        for (SoftDeleteEdgeInfo inSoftDelete : this.getElementData().inSoftDeletes) {
            StorableEdgeInfo inEdge = this.getElementData().inEdges.get(inSoftDelete.getEdgeId().toString());
            if (inEdge != null && inSoftDelete.getTimestamp() >= inEdge.getTimestamp()) {
                this.getElementData().inEdges.remove(inSoftDelete.getEdgeId().toString());
            }
        }

        for (SoftDeleteEdgeInfo outSoftDelete : this.getElementData().outSoftDeletes) {
            StorableEdgeInfo outEdge = this.getElementData().outEdges.get(outSoftDelete.getEdgeId().toString());
            if (outEdge != null && outSoftDelete.getTimestamp() >= outEdge.getTimestamp()) {
                this.getElementData().outEdges.remove(outSoftDelete.getEdgeId().toString());
            }
        }
    }

    @Override
    protected void processSignalColumn(KeyValue keyValue) {
        super.processSignalColumn(keyValue);
        getElementData().conceptType = keyValue.takeColumnQualifier();
    }

    @Override
    protected boolean processColumn(KeyValue keyValue) {
        if (keyValue.columnFamilyEquals(CF_OUT_EDGE_BYTES)) {
            processOutEdge(keyValue);
            return true;
        }

        if (keyValue.columnFamilyEquals(CF_IN_EDGE_BYTES)) {
            processInEdge(keyValue);
            return true;
        }

        if (keyValue.columnFamilyEquals(CF_OUT_EDGE_HIDDEN_BYTES) || keyValue.columnFamilyEquals(CF_IN_EDGE_HIDDEN_BYTES)) {
            Text edgeId = keyValue.takeColumnQualifier();
            getElementData().hiddenEdges.add(edgeId);
            return true;
        }

        if (keyValue.columnFamilyEquals(CF_IN_EDGE_SOFT_DELETE_BYTES)) {
            Text edgeId = keyValue.takeColumnQualifier();
            getElementData().inSoftDeletes.add(new SoftDeleteEdgeInfo(edgeId, keyValue.getTimestamp()));
            return true;
        }

        if (keyValue.columnFamilyEquals(CF_OUT_EDGE_SOFT_DELETE_BYTES)) {
            Text edgeId = keyValue.takeColumnQualifier();
            getElementData().outSoftDeletes.add(new SoftDeleteEdgeInfo(edgeId, keyValue.getTimestamp()));
            return true;
        }

        return false;
    }

    private void processOutEdge(KeyValue keyValue) {
        StorableEdgeInfo edgeInfo = new StorableEdgeInfo(keyValue.takeValue().get(), keyValue.getTimestamp());
        if (shouldIncludeOutEdge(edgeInfo)) {
            Text edgeId = keyValue.takeColumnQualifier();
            getElementData().outEdges.add(edgeId.toString(), edgeInfo);
        }
    }

    private void processInEdge(KeyValue keyValue) {
        StorableEdgeInfo edgeInfo = new StorableEdgeInfo(keyValue.takeValue().get(), keyValue.getTimestamp());
        if (shouldIncludeInEdge(edgeInfo)) {
            Text edgeId = keyValue.takeColumnQualifier();
            getElementData().inEdges.add(edgeId.toString(), edgeInfo);
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
    protected byte[] getVisibilitySignal() {
        return CF_SIGNAL_BYTES;
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        if (getSourceIterator() != null) {
            return new VertexIterator(getSourceIterator().deepCopy(env), getFetchHints(), isCompressTransfer());
        }
        return new VertexIterator(getFetchHints(), isCompressTransfer());
    }

    @Override
    protected String getDescription() {
        return "This iterator encapsulates an entire Vertex into a single Key/Value pair.";
    }

    @Override
    protected VertexElementData createElementData() {
        return new VertexElementData();
    }
}
