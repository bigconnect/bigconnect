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
package com.mware.ge.store;

import com.mware.ge.GeException;
import com.mware.ge.Graph;
import com.mware.ge.GraphConfiguration;
import com.mware.ge.Property;
import com.mware.ge.store.mutations.ElementMutationBuilder;
import com.mware.ge.store.util.StreamingPropertyValueStorageStrategy;
import com.mware.ge.util.IOUtils;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.StreamingPropertyValueRef;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class FilesystemSPVStorageStrategy implements StreamingPropertyValueStorageStrategy {
    private final StorableGraph graph;
    private final Path dataFolder;

    public FilesystemSPVStorageStrategy(Graph graph, GraphConfiguration configuration) {
        if (!(configuration instanceof StorableGraphConfiguration)) {
            throw new GeException("Expected " + StorableGraphConfiguration.class.getName() + " found " + configuration.getClass().getName());
        }
        if (!(graph instanceof StorableGraph)) {
            throw new GeException("Expected " + StorableGraph.class.getName() + " found " + graph.getClass().getName());
        }

        this.graph = (StorableGraph) graph;
        this.dataFolder = ((StorableGraphConfiguration)configuration).createSPVFolder();
    }

    @Override
    public StreamingPropertyValueRef saveStreamingPropertyValue(ElementMutationBuilder elementMutationBuilder, String rowKey, Property property, StreamingPropertyValue streamingPropertyValue) {
        try {
            File filePath = createFile(rowKey, property);
            FileOutputStream out = new FileOutputStream(filePath);

            try {
                IOUtils.copy(streamingPropertyValue.getInputStream(), out);
            } finally {
                out.close();
            }

            return new StreamingPropertyValueFileRef(filePath, streamingPropertyValue);
        } catch (IOException ex) {
            throw new GeException(ex);
        }
    }

    @Override
    public void close() {
        // nothing to do
    }

    @Override
    public List<InputStream> getInputStreams(List<StreamingPropertyValue> streamingPropertyValues) {
        return streamingPropertyValues.stream()
                .map(StreamingPropertyValue::getInputStream)
                .collect(Collectors.toList());
    }

    protected File createFile(String rowKey, Property property) {
        String relativeFileName = createFileName(rowKey, property);
        Path path = Paths.get(dataFolder.toString(), relativeFileName);

        try {
            if(!path.getParent().toFile().exists())
                Files.createDirectory(path.getParent());
        } catch (IOException e) {
            throw new GeException(String.format("Could not create SPV folder for rowkey=%s, property=%s", rowKey, property), e);
        }

        if (path.toFile().exists()) {
            try {
                Files.delete(path);
            } catch (IOException e) {
                throw new GeException(String.format("Could not delete SPV folder for rowkey=%s, property=%s", rowKey, property), e);
            }
        }
        return path.toFile();
    }

    private String createFileName(String rowKey, Property property) {
        String fileName = encodeFileName(property.getName() + "_" + property.getKey() + "_" + property.getTimestamp());
        return rowKey + File.separator + fileName;
    }

    private static String encodeFileName(String fileName) {
        StringBuilder result = new StringBuilder();
        for (char ch : fileName.toCharArray()) {
            if ((ch >= '0' && ch <= '9')
                    || (ch >= 'A' && ch <= 'Z')
                    || (ch >= 'a' && ch <= 'z')) {
                result.append(ch);
            } else if (ch == ' ') {
                result.append('_');
            } else {
                String hex = "0000" + Integer.toHexString((int) ch);
                result.append(hex.substring(hex.length() - 4));
            }
        }
        return result.toString();
    }
}
