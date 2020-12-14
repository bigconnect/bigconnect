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
package com.mware.ge.accumulo.iterator.util;

import com.mware.ge.accumulo.iterator.model.IteratorMetadataEntry;
import com.mware.ge.store.EdgesWithEdgeInfo;
import com.mware.ge.store.StorableEdgeInfo;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.hadoop.io.Text;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

public class DataOutputStreamUtils {
    public static final Charset CHARSET = Charset.forName("utf8");
    public static final byte EDGE_LABEL_ONLY_MARKER = 1;
    public static final byte EDGE_LABEL_WITH_REFS_MARKER = 2;

    public static void encodeTextList(DataOutputStream out, Collection<Text> texts) throws IOException {
        if (texts == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(texts.size());
        for (Text text : texts) {
            encodeText(out, text);
        }
    }

    public static void encodeByteSequenceList(DataOutputStream out, Collection<ByteSequence> byteSequences) throws IOException {
        if (byteSequences == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(byteSequences.size());
        for (ByteSequence byteSequence : byteSequences) {
            encodeByteSequence(out, byteSequence);
        }
    }

    public static void encodeStringSet(DataOutputStream out, Set<String> set) throws IOException {
        if (set == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(set.size());
        for (String item : set) {
            encodeString(out, item);
        }
    }

    public static void encodeText(DataOutputStream out, Text text) throws IOException {
        if (text == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(text.getLength());
        out.write(text.getBytes(), 0, text.getLength());
    }

    public static void encodeByteSequence(DataOutputStream out, ByteSequence byteSequence) throws IOException {
        if (byteSequence == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(byteSequence.length());
        out.write(byteSequence.getBackingArray(), byteSequence.offset(), byteSequence.length());
    }

    public static void encodeByteArray(DataOutputStream out, byte[] bytes) throws IOException {
        if (bytes == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    public static void encodeIntArray(DataOutputStream out, Collection<Integer> integers) throws IOException {
        if (integers == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(integers.size());
        for (Integer i : integers) {
            out.writeInt(i);
        }
    }

    public static void encodeString(DataOutputStream out, String text) throws IOException {
        if (text == null) {
            out.writeInt(-1);
            return;
        }
        byte[] bytes = text.getBytes(CHARSET);
        out.writeInt(bytes.length);
        out.write(bytes, 0, bytes.length);
    }

    public static void encodePropertyMetadataEntry(DataOutputStream out, List<IteratorMetadataEntry> metadataEntries) throws IOException {
        if (metadataEntries == null) {
            out.writeInt(0);
            return;
        }
        out.writeInt(metadataEntries.size());
        for (IteratorMetadataEntry metadataEntry : metadataEntries) {
            encodeByteSequence(out, metadataEntry.metadataKey);
            encodeByteSequence(out, metadataEntry.metadataVisibility);
            out.writeInt(metadataEntry.value.length);
            out.write(metadataEntry.value);
        }
    }

    public static void encodeEdges(
            DataOutputStream out,
            EdgesWithEdgeInfo edges,
            boolean edgeLabelsOnly,
            boolean includeEdgeIds,
            boolean includeEdgeVertexIds
    ) throws IOException {
        out.write(edgeLabelsOnly ? EDGE_LABEL_ONLY_MARKER : EDGE_LABEL_WITH_REFS_MARKER);

        Map<ByteArrayWrapper, List<Map.Entry<String, StorableEdgeInfo>>> edgesByLabels = getEdgesByLabel(edges);
        out.writeInt(edgesByLabels.size());
        for (Map.Entry<ByteArrayWrapper, List<Map.Entry<String, StorableEdgeInfo>>> entry : edgesByLabels.entrySet()) {
            encodeByteArray(out, entry.getKey().getData());
            out.writeInt(entry.getValue().size());
            if (!edgeLabelsOnly) {
                for (Map.Entry<String, StorableEdgeInfo> edgeEntry : entry.getValue()) {
                    if (includeEdgeIds) {
                        encodeText(out, new Text(edgeEntry.getKey()));
                    }
                    out.writeLong(edgeEntry.getValue().getTimestamp());
                    if (includeEdgeVertexIds) {
                        encodeString(out, edgeEntry.getValue().getVertexId());
                    }
                }
            }
        }
    }

    private static Map<ByteArrayWrapper, List<Map.Entry<String, StorableEdgeInfo>>> getEdgesByLabel(EdgesWithEdgeInfo edges) throws IOException {
        Map<ByteArrayWrapper, List<Map.Entry<String, StorableEdgeInfo>>> edgesByLabels = new HashMap<>();
        for (Map.Entry<String, StorableEdgeInfo> edgeEntry : edges.getEntries()) {
            ByteArrayWrapper label = new ByteArrayWrapper(edgeEntry.getValue().getLabelBytes());
            List<Map.Entry<String, StorableEdgeInfo>> edgesByLabel = edgesByLabels.get(label);
            if (edgesByLabel == null) {
                edgesByLabel = new ArrayList<>();
                edgesByLabels.put(label, edgesByLabel);
            }
            edgesByLabel.add(edgeEntry);
        }
        return edgesByLabels;
    }

    public static void encodeSetOfStrings(DataOutputStream out, Set<String> strings) throws IOException {
        out.writeInt(strings.size());
        for (String string : strings) {
            encodeString(out, string);
        }
    }
}
