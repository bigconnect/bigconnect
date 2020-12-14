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
package com.mware.ge.tools;

import com.beust.jcommander.Parameter;
import com.mware.ge.*;
import com.mware.ge.serializer.GeSerializer;
import com.mware.ge.serializer.kryo.quickSerializers.QuickKryoGeSerializer;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.Value;
import org.apache.commons.codec.binary.Base64;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;

public class GraphBackup extends GraphToolBase {
    private static GeLogger LOGGER = GeLoggerFactory.getLogger(GraphRestore.class);

    @Parameter(names = {"--out", "-o"}, description = "Output filename prefix")
    protected String outputFileNamePrefix = null;

    @Parameter(names = {"--split", "-s"}, description = "Split file when reaching this number of elements")
    protected long batchSize = 300_000;

    protected long batchCount = 0;
    protected long backupFileIndex = 1;
    protected GeSerializer serializer = new QuickKryoGeSerializer(true);

    public GraphBackup() {
    }

    public GraphBackup(String rootDir, String outputFileNamePrefix) {
        super(rootDir);
        this.outputFileNamePrefix = outputFileNamePrefix;
    }

    public static void main(String[] args) throws Exception {
        GraphBackup graphBackup = new GraphBackup();
        graphBackup.run(args);
    }

    protected void run(String[] args) throws Exception {
        super.run(args);

        OutputStream out = createOutputStream();
        try {
            out = save(getGraph(), out, getAuthorizations());
        } finally {
            out.flush();
            out.close();
            System.out.println("Created backup file: "+getBackupFileName(backupFileIndex)+" with "+batchCount+" elements");
        }
    }

    private File createBackupFile() throws IOException {
        File backupRootDir = new File(rootDir);
        if (!backupRootDir.exists()) {
            backupRootDir.mkdir();
        }
        File backupFile = new File(backupRootDir, getBackupFileName(backupFileIndex));
        if (!backupFile.exists()) {
            backupFile.createNewFile();
        }
        return backupFile;
    }

    private String getBackupFileName(long index) {
        return outputFileNamePrefix + "-" + index + DEFAULT_GRAPH_BACKUP_EXT;
    }

    public OutputStream createOutputStream() throws IOException {
        if (outputFileNamePrefix == null) {
            return System.out;
        }
        File file = createBackupFile();
        return new FileOutputStream(file);
    }

    public OutputStream save(Graph graph, OutputStream out, Authorizations authorizations) throws IOException {
        FetchHints fetchHints = FetchHints.ALL_INCLUDING_HIDDEN;
        return save(graph.getVertices(fetchHints, authorizations), graph.getEdges(fetchHints, authorizations), out);
    }

    public OutputStream save(Iterable<Vertex> vertices, Iterable<Edge> edges, OutputStream out) throws IOException {
        out = saveVertices(vertices, out);
        return saveEdges(edges, out);
    }

    public OutputStream saveVertices(Iterable<Vertex> vertices, OutputStream out) throws IOException {
        for (Vertex vertex : vertices) {
            try {
                saveVertex(vertex, out);
                out = shouldNextBackupFile(out);
            } catch (Exception ex) {
                System.err.println("Cannot save vertex: "+vertex.getId()+", cause: "+ex.getMessage());
                ex.printStackTrace();
            }
        }
        return out;
    }

    private OutputStream shouldNextBackupFile(OutputStream out) throws IOException {
        batchCount++;

        if (batchCount == batchSize) {
            out.flush();
            out.close();
            backupFileIndex++;
            out = createOutputStream();
            System.out.println("Created backup file: "+getBackupFileName(backupFileIndex-1)+" with "+batchCount+" elements");
            batchCount = 0;

            return out;
        } else {
            return out;
        }
    }

    public void saveVertex(Vertex vertex, OutputStream out) throws IOException {
        JSONObject json = vertexToJson(vertex, true);
        out.write('V');
        out.write(json.toString().getBytes());
        out.write('\n');
        saveStreamingPropertyValues(out, vertex, true);
    }

    public OutputStream saveEdges(Iterable<Edge> edges, OutputStream out) throws IOException {
        for (Edge edge : edges) {
            try {
                saveEdge(edge, out);
                out = shouldNextBackupFile(out);
            } catch (Exception ex) {
                System.err.println("Cannot save edge: "+edge.getId()+", cause: "+ex.getMessage());
                ex.printStackTrace();
            }
        }
        return out;
    }

    public void saveEdge(Edge edge, OutputStream out) throws IOException {
        JSONObject json = edgeToJson(edge, true);
        out.write('E');
        out.write(json.toString().getBytes());
        out.write('\n');
        saveStreamingPropertyValues(out, edge, true);
    }

    public JSONObject vertexToJson(Vertex vertex, boolean encoded) {
        JSONObject json = elementToJson(vertex, encoded);
        json.put("conceptType", vertex.getConceptType());
        return json;
    }

    public JSONObject edgeToJson(Edge edge, boolean encoded) {
        JSONObject json = elementToJson(edge, encoded);
        json.put("outVertexId", edge.getVertexId(Direction.OUT));
        json.put("inVertexId", edge.getVertexId(Direction.IN));
        json.put("label", edge.getLabel());
        return json;
    }

    public JSONObject elementToJson(Element element, boolean encoded) {
        JSONObject json = new JSONObject();
        json.put("id", element.getId());
        json.put("visibility", element.getVisibility().getVisibilityString());
        json.put("properties", propertiesToJson(element.getProperties(), encoded));
        return json;
    }

    public JSONArray propertiesToJson(Iterable<Property> properties, boolean encoded) {
        JSONArray json = new JSONArray();
        for (Property property : properties) {
            if (property.getValue() instanceof StreamingPropertyValue) {
                continue;
            }
            json.put(propertyToJson(property, encoded));
        }
        return json;
    }

    public JSONObject propertyToJson(Property property, boolean encoded) {
        JSONObject json = new JSONObject();
        json.put("key", property.getKey());
        json.put("name", property.getName());
        json.put("visibility", property.getVisibility().getVisibilityString());
        Value value = property.getValue();
        if (!(value instanceof StreamingPropertyValue)) {
            json.put("value", objectToJsonString(value, encoded));
        }
        Metadata metadata = property.getMetadata();
        if (metadata != null) {
            json.put("metadata", metadataToJson(metadata, encoded));
        }
        return json;
    }

    public JSONObject metadataToJson(Metadata metadata, boolean encoded) {
        JSONObject json = new JSONObject();
        for (Metadata.Entry m : metadata.entrySet()) {
            json.put(m.getKey(), metadataItemToJson(m, encoded));
        }
        return json;
    }

    public JSONObject metadataItemToJson(Metadata.Entry entry, boolean encoded) {
        JSONObject json = new JSONObject();
        json.put("value", objectToJsonString(entry.getValue(), encoded));
        json.put("visibility", entry.getVisibility().getVisibilityString());
        return json;
    }

    public void saveStreamingPropertyValues(OutputStream out, Element element, boolean encoded) throws IOException {
        for (Property property : element.getProperties()) {
            if (property.getValue() instanceof StreamingPropertyValue) {
                saveStreamingProperty(out, property, encoded);
            }
        }
    }

    public void saveStreamingProperty(OutputStream out, Property property, boolean encoded) throws IOException {
        StreamingPropertyValue spv = (StreamingPropertyValue) property.getValue();
        JSONObject json = propertyToJson(property, encoded);
        json.put("valueType", spv.getValueType().getName());
        json.put("searchIndex", spv.isSearchIndex());
        out.write('D');
        out.write(json.toString().getBytes());
        out.write('\n');
        InputStream in = spv.getInputStream();
        byte[] buffer = new byte[10 * 1024];
        int read;
        while ((read = in.read(buffer)) > 0) {
            out.write(Integer.toString(read).getBytes());
            out.write('\n');
            out.write(buffer, 0, read);
            out.write('\n');
        }
        out.write('0');
        out.write('\n');
    }

    private String objectToJsonString(Value value, boolean encoded) {
        byte[] serialized = serializer.objectToBytes(value);
        return encoded ? Base64.encodeBase64String(serialized) : value.asObjectCopy().toString();
    }
}
