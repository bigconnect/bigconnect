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

import com.mware.ge.accumulo.iterator.model.EdgeElementData;
import com.mware.ge.accumulo.iterator.model.IteratorFetchHints;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

public class EdgeIterator extends ElementIterator<EdgeElementData> {
    public static final String CF_SIGNAL_STRING = "E";
    public static final Text CF_SIGNAL = new Text(CF_SIGNAL_STRING);
    private static final byte[] CF_SIGNAL_BYTES = CF_SIGNAL.getBytes();

    public static final String CF_OUT_VERTEX_STRING = "EOUT";
    public static final Text CF_OUT_VERTEX = new Text(CF_OUT_VERTEX_STRING);
    private static final byte[] CF_OUT_VERTEX_BYTES = CF_OUT_VERTEX.getBytes();

    public static final String CF_IN_VERTEX_STRING = "EIN";
    public static final Text CF_IN_VERTEX = new Text(CF_IN_VERTEX_STRING);
    private static final byte[] CF_IN_VERTEX_BYTES = CF_IN_VERTEX.getBytes();

    public EdgeIterator() {
        this(null, false);
    }

    public EdgeIterator(IteratorFetchHints fetchHints, boolean compressTransfer) {
        super(null, fetchHints, compressTransfer);
    }

    public EdgeIterator(SortedKeyValueIterator<Key, Value> source, IteratorFetchHints fetchHints, boolean compressTransfer) {
        super(source, fetchHints, compressTransfer);
    }

    @Override
    protected boolean processColumn(KeyValue keyValue) {
        if (keyValue.columnFamilyEquals(CF_IN_VERTEX_BYTES)) {
            if (getElementData().inVertexIdTimestamp == null || keyValue.getTimestamp() > getElementData().inVertexIdTimestamp) {
                getElementData().inVertexId = keyValue.takeColumnQualifier();
                getElementData().inVertexIdTimestamp = keyValue.getTimestamp();
            }
            return true;
        }

        if (keyValue.columnFamilyEquals(CF_OUT_VERTEX_BYTES)) {
            if (getElementData().outVertexIdTimestamp == null || keyValue.getTimestamp() > getElementData().outVertexIdTimestamp) {
                getElementData().outVertexId = keyValue.takeColumnQualifier();
                getElementData().outVertexIdTimestamp = keyValue.getTimestamp();
            }
            return true;
        }

        return false;
    }

    @Override
    protected void processSignalColumn(KeyValue keyValue) {
        super.processSignalColumn(keyValue);
        getElementData().label = keyValue.takeColumnQualifier();
    }

    @Override
    protected byte[] getVisibilitySignal() {
        return CF_SIGNAL_BYTES;
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        if (getSourceIterator() != null) {
            return new EdgeIterator(getSourceIterator().deepCopy(env), getFetchHints(), isCompressTransfer());
        }
        return new EdgeIterator(getFetchHints(), isCompressTransfer());
    }

    @Override
    protected String getDescription() {
        return "This iterator encapsulates an entire Edge into a single Key/Value pair.";
    }

    @Override
    protected EdgeElementData createElementData() {
        return new EdgeElementData();
    }
}
