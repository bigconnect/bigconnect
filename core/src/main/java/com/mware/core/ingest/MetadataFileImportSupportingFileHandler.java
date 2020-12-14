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
package com.mware.core.ingest;

import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.properties.BcSchema;
import com.mware.ge.VertexBuilder;
import com.mware.ge.Visibility;
import com.mware.ge.values.storable.ByteArray;
import com.mware.ge.values.storable.DefaultStreamingPropertyValue;
import com.mware.ge.values.storable.StreamingPropertyValue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

@Name("Metadata Import")
@Description("Imports a .metadata.json file and assigns it to a metadata JSON property on that vertex")
public class MetadataFileImportSupportingFileHandler extends FileImportSupportingFileHandler {
    private static final String METADATA_JSON_FILE_NAME_SUFFIX = ".metadata.json";

    @Override
    public boolean isSupportingFile(File f) {
        return f.getName().endsWith(METADATA_JSON_FILE_NAME_SUFFIX);
    }

    @Override
    public AddSupportingFilesResult addSupportingFiles(VertexBuilder vertexBuilder, File f, Visibility visibility) throws FileNotFoundException {
        File mappingJsonFile = getMetadataFile(f);
        if (mappingJsonFile.exists()) {
            final FileInputStream mappingJsonInputStream = new FileInputStream(mappingJsonFile);
            StreamingPropertyValue mappingJsonValue = new DefaultStreamingPropertyValue(mappingJsonInputStream, ByteArray.class);
            mappingJsonValue.searchIndex(false);
            BcSchema.METADATA_JSON.setProperty(vertexBuilder, mappingJsonValue, visibility);
            return new AddSupportingFilesResult() {
                @Override
                public void close() throws IOException {
                    mappingJsonInputStream.close();
                }
            };
        }
        return null;
    }

    public static File getMetadataFile(File f) {
        return new File(f.getParentFile(), f.getName() + METADATA_JSON_FILE_NAME_SUFFIX);
    }
}
