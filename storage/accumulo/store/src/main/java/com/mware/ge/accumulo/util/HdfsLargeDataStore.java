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
package com.mware.ge.accumulo.util;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.mware.ge.Property;

import java.io.IOException;
import java.io.OutputStream;

public class HdfsLargeDataStore extends LimitOutputStream.LargeDataStore {
    private final FileSystem fs;
    private final String dataDir;
    private final String rowKey;
    private final Property property;
    private Path hdfsPath;
    private String relativeFileName;

    public HdfsLargeDataStore(FileSystem fs, String dataDir, String rowKey, Property property) {
        this.fs = fs;
        this.dataDir = dataDir;
        this.rowKey = rowKey;
        this.property = property;
    }

    @Override
    public OutputStream createOutputStream() throws IOException {
        this.hdfsPath = createFileName();
        return this.fs.create(this.hdfsPath);
    }

    protected Path createFileName() throws IOException {
        this.relativeFileName = createHdfsFileName(rowKey, property);
        Path path = new Path(dataDir, this.relativeFileName);
        if (!this.fs.mkdirs(path.getParent())) {
            throw new IOException("Could not create directory " + path.getParent());
        }
        if (this.fs.exists(path)) {
            this.fs.delete(path, true);
        }
        return path;
    }

    public Path getFullHdfsPath() {
        return hdfsPath;
    }

    public String getRelativeFileName() {
        return relativeFileName;
    }

    private String createHdfsFileName(String rowKey, Property property) throws IOException {
        String fileName = HdfsLargeDataStore.encodeFileName(property.getName() + "_" + property.getKey() + "_" + property.getTimestamp());
        return rowKey + "/" + fileName;
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
