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
import com.mware.core.model.schema.SchemaConstants;
import com.mware.ge.*;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.serializer.GeSerializer;
import com.mware.ge.serializer.kryo.quickSerializers.QuickKryoGeSerializer;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.util.IOUtils;
import com.mware.ge.util.JavaSerializableUtils;
import com.mware.ge.values.storable.*;
import org.apache.commons.codec.binary.Base64;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.atomic.LongAdder;

public class GraphRestore extends GraphToolBase {
    private static GeLogger LOGGER = GeLoggerFactory.getLogger(GraphRestore.class);

    @Parameter(names = {"--in", "-i"}, description = "Input folder where .ge backup files are located", required = true)
    private final File inputFolder = null;

    @Parameter(names = {"--skip", "-s"}, description = "Concepts to skip (comma separated)")
    private final String conceptsToSkip = null;

    LongAdder vertices = new LongAdder();
    LongAdder edges = new LongAdder();
    protected GeSerializer serializer = new QuickKryoGeSerializer(true);
    protected int currentBackupFileIndex = 0;
    protected File[] availableBackupFiles;
    protected Set<String> toSkip = new HashSet<>();
    protected long skipped = 0;

    static int COMMIT_BATCH = 1000;

    public GraphRestore() {
    }

    public GraphRestore(String rootDir) {
        super(rootDir);
    }

    public static void main(String[] args) throws Exception {
        GraphRestore graphRestore = new GraphRestore();
        graphRestore.run(args);
    }

    protected void run(String[] args) throws Exception {
        super.run(args);

        if (!inputFolder.isDirectory())
            throw new IllegalArgumentException("The provided input folder is not a directory");

        if (conceptsToSkip != null) {
            toSkip.addAll(Arrays.asList(conceptsToSkip.split(",")));
        }

        availableBackupFiles = listBackupFiles();
        InputStream in = createInputStream();
        try {
            long nrRestored = restore(getGraph(), in, getAuthorizations(), 0);
            System.out.println("TOTAL: "+nrRestored+" elements were restored");
        } finally {
            in.close();
        }
    }

    private File[] listBackupFiles() {
        return Arrays.stream(Objects.requireNonNull(inputFolder.listFiles((File f) -> f.getName().endsWith(DEFAULT_GRAPH_BACKUP_EXT))))
                .sorted(Comparator.comparing(File::getName))
                .toArray(File[]::new);
    }

    public InputStream createInputStream() throws FileNotFoundException {
        File bkpFile = availableBackupFiles[currentBackupFileIndex];
        System.out.println("Loading backup file: "+bkpFile.getName());
        return new FileInputStream(bkpFile);
    }

    public long restore(Graph graph, InputStream in, Authorizations authorizations, long nrRestored) throws IOException {
        String line;
        char lastType = 'V';
        boolean elementCreated = false;
        Element element = null;
        // We can't use a BufferedReader here because when we need to read the streaming property values we need raw bytes not converted bytes
        while ((line = readLine(in)) != null) {
            try {
                char type = line.charAt(0);
                switch (type) {
                    case 'V':
                        JSONObject json;
                        try {
                            json = new JSONObject(line.substring(1));
                            element = restoreVertex(graph, json, authorizations);
                            if (element == null) {
                                skipToNextElement(in);
                                skipped++;
                                break;
                            }

                            elementCreated = true;
                            nrRestored++;
                            if (nrRestored % COMMIT_BATCH == 0) {
                                System.out.println("Restored: "+nrRestored+" elements");
                                graph.flush();
                            }
                        } catch (Exception ex) {
                            System.err.println("Cannot create vertex: " +ex.getMessage());
                            ex.printStackTrace();
                            elementCreated = false;
                            skipToNextElement(in);
                        }
                        break;
                    case 'E':
                        // flush when we make the transition to edges so that we have vertices to lookup and link to.
                        if (type != lastType) {
                            graph.flush();
                        }
                        try {
                            json = new JSONObject(line.substring(1));
                            element = restoreEdge(graph, json, authorizations);
                            elementCreated = true;
                            nrRestored++;
                            if (nrRestored % COMMIT_BATCH == 0) {
                                System.out.println("Restored: " + nrRestored + " elements");
                                graph.flush();
                            }
                        }  catch (Exception ex) {
                            LOGGER.warn("Cannot create edge: "+ex.getMessage());
                            ex.printStackTrace();
                            elementCreated = false;
                            skipToNextElement(in);
                        }

                        break;
                    case 'D':
                        json = new JSONObject(line.substring(1));
                        restoreStreamingPropertyValue(in, graph, json, element, authorizations, elementCreated);
                        break;
                    default:
                        throw new RuntimeException("Unexpected line: " + line);
                }
                lastType = type;
            } catch (Exception ex) {
                LOGGER.warn("Invalid line: " + line, ex);
            }
        }

        currentBackupFileIndex++;
        if (currentBackupFileIndex < availableBackupFiles.length) {
            in = createInputStream();
            nrRestored = restore(graph, in, authorizations, nrRestored);
        }

        return nrRestored;
    }

    private void skipToNextElement(InputStream in) throws Exception {
        String line;
        while ((line = readLine(in)) != null) {
            char type = line.charAt(0);
            if (type == 'D') {
                JSONObject json = new JSONObject(line.substring(1));
                restoreStreamingPropertyValue(in, getGraph(), json, null, getAuthorizations(), false);
                return;
            }
        }
    }

    private String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        while (true) {
            int b = in.read();
            if (b < 0) {
                if (buffer.size() == 0) {
                    return null;
                }
                break;
            }
            if (b == '\n') {
                break;
            }
            buffer.write(b);
        }
        return new String(buffer.toByteArray());
    }

    private Element restoreVertex(Graph graph, JSONObject json, Authorizations authorizations) {
        Visibility visibility = jsonToVisibility(json);
        String vertexId = json.getString("id");
        VertexBuilder v = graph.prepareVertex(vertexId, visibility, SchemaConstants.CONCEPT_TYPE_THING);
        jsonToProperties(json, v);
        if (toSkip.contains(v.getConceptType()))
            return null;

        vertices.increment();
        return v.save(authorizations);
    }

    private Element restoreEdge(Graph graph, JSONObject json, Authorizations authorizations) {
        Visibility visibility = jsonToVisibility(json);
        String edgeId = json.getString("id");
        String outVertexId = json.getString("outVertexId");
        String inVertexId = json.getString("inVertexId");
        String label = json.getString("label");
        Vertex outVertex = graph.getVertex(outVertexId, authorizations);
        Vertex inVertex = graph.getVertex(inVertexId, authorizations);
        EdgeBuilder e = graph.prepareEdge(edgeId, outVertex, inVertex, label, visibility);
        jsonToProperties(json, e);
        edges.increment();
        return e.save(authorizations);
    }

    protected Visibility jsonToVisibility(JSONObject jsonObject) {
        String visibility = jsonObject.optString("visibility", "");
        return new Visibility(visibility);
    }

    protected void jsonToProperties(JSONObject jsonObject, ElementBuilder e) {
        JSONArray propertiesJson = jsonObject.getJSONArray("properties");
        for (int i = 0; i < propertiesJson.length(); i++) {
            JSONObject propertyJson = propertiesJson.getJSONObject(i);
            if ("conceptType".equals(propertyJson.getString("name"))) {
                ((VertexBuilder) e).setConceptType(propertyJson.getString("value"));
            } else {
                jsonToProperty(propertyJson, e);
            }
        }
    }

    private void jsonToProperty(JSONObject propertyJson, ElementBuilder e) {
        String key = propertyJson.getString("key");
        String name = propertyJson.getString("name");
        Value value = jsonStringToObject(propertyJson.getString("value"));
        Metadata metadata = jsonToPropertyMetadata(propertyJson.optJSONObject("metadata"));
        Visibility visibility = new Visibility(propertyJson.getString("visibility"));
        e.addPropertyValue(key, name, value, metadata, visibility);
    }

    private void restoreStreamingPropertyValue(InputStream in, Graph graph, JSONObject propertyJson, Element element, Authorizations authorizations, boolean elementCreated) throws ClassNotFoundException, IOException {
        String key = propertyJson.getString("key");
        String name = propertyJson.getString("name");
        Metadata metadata = jsonToPropertyMetadata(propertyJson.optJSONObject("metadata"));
        Visibility visibility = new Visibility(propertyJson.getString("visibility"));
        Class valueType = Class.forName(propertyJson.getString("valueType"));
        if (String.class.equals(valueType))
            valueType = StringValue.class;

        if (byte[].class.equals(valueType))
            valueType = ByteArray.class;

        InputStream spvin = new StreamingPropertyValueInputStream(in);
        if (elementCreated) {
            StreamingPropertyValue value = StreamingPropertyValue.create(spvin, valueType);
            value.searchIndex(TextValue.class.isAssignableFrom(valueType));
            element.addPropertyValue(key, name, value, metadata, visibility, authorizations);
        } else {
            IOUtils.copy(spvin, new ByteArrayOutputStream());
        }
    }

    private Metadata jsonToPropertyMetadata(JSONObject metadataJson) {
        Metadata metadata = Metadata.create();
        if (metadataJson == null) {
            return metadata;
        }
        for (Object key : metadataJson.keySet()) {
            String keyString = (String) key;
            if ("confidence".equals(keyString) || "http://bigconnect#confidence".equals(keyString))
                continue;

            JSONObject metadataItemJson = metadataJson.getJSONObject(keyString);
            Value val = jsonStringToObject(metadataItemJson.getString("value"));
            Visibility visibility = new Visibility(metadataItemJson.getString("visibility"));
            metadata.add(keyString, val, visibility);
        }
        return metadata;
    }

    public static final String BASE64_PREFIX = "base64/java:";

    private Value jsonStringToObject(String str) {
        if (str.startsWith(BASE64_PREFIX)) {
            str = str.substring(BASE64_PREFIX.length());
            Object obj = JavaSerializableUtils.bytesToObject(Base64.decodeBase64(str));
            if (obj instanceof Date) {
                ZonedDateTime zdt = ZonedDateTime.ofInstant(((Date)obj).toInstant(), ZoneOffset.systemDefault());
                return DateTimeValue.datetime(zdt);
            }
            Value v = Values.of(obj);
            return v;
        } else {
            return Values.stringValue(str);
        }
    }

    private class StreamingPropertyValueInputStream extends InputStream {
        private final InputStream in;
        private int segmentLength;
        private boolean done;

        public StreamingPropertyValueInputStream(InputStream in) throws IOException {
            this.in = in;
            readSegmentLengthLine();
        }

        private void readSegmentLengthLine() throws IOException {
            String line = readLine(this.in);
            this.segmentLength = Integer.parseInt(line);
            if (this.segmentLength == 0) {
                this.done = true;
            }
        }

        @Override
        public int read() throws IOException {
            if (this.done) {
                return -1;
            }
            if (this.segmentLength == 0) {
                this.in.read(); // throw away new line character
                readSegmentLengthLine();
                if (this.done) {
                    return -1;
                }
            }
            int ret = this.in.read();
            this.segmentLength--;
            return ret;
        }
    }

    public InputStream createInputStream(String fileName) throws FileNotFoundException {
        return new FileInputStream(new File(rootDir, fileName));
    }

    public Optional<String> getLastBackupFile(String backupFilePrefix) {
        return Arrays.stream(new File(rootDir).listFiles((File f) -> f.getName().startsWith(backupFilePrefix)))
                .sorted((f1, f2) -> {
                    String date1Value = f1.getName().replace(backupFilePrefix, "").replace(DEFAULT_GRAPH_BACKUP_EXT, "");
                    LocalDateTime date1 = LocalDateTime.parse(date1Value, BACKUP_DATETIME_FORMATTER);
                    String date2Value = f2.getName().replace(backupFilePrefix, "").replace(DEFAULT_GRAPH_BACKUP_EXT, "");
                    LocalDateTime date2 = LocalDateTime.parse(date2Value, BACKUP_DATETIME_FORMATTER);
                    return date2.compareTo(date1);
                })
                .map(f -> f.getName())
                .findFirst();
    }
}
