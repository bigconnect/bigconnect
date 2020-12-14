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
package com.mware.ge.util;

import com.google.common.annotations.VisibleForTesting;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * An AutoDeleteFileInputStream deletes its underlying file when the stream is closed.
 */
public class AutoDeleteFileInputStream extends FileInputStream {
    private final File file;

    /**
     * Create an AutoDeleteFileInputStream from an existing file.
     */
    public AutoDeleteFileInputStream(File file) throws FileNotFoundException {
        super(file);
        this.file = file;
    }

    /**
     * Create an AutoDeleteFileInputStream by copying the contents of another InputStream into a temporary file.
     * copyFromStream is closed immediately after being copied. The temporary file will be deleted when this
     * AutoDeleteFileInputStream is closed.
     */
    public AutoDeleteFileInputStream(InputStream copyFromStream) throws IOException {
        this(copyToTempFile(copyFromStream));
    }

    private static File copyToTempFile(InputStream inputStream) throws IOException {
        try {
            Path tempPath = Files.createTempFile(AutoDeleteFileInputStream.class.getSimpleName(), null);
            Files.copy(inputStream, tempPath, StandardCopyOption.REPLACE_EXISTING);
            File tempFile = tempPath.toFile();
            tempFile.deleteOnExit();
            return tempFile;
        } finally {
            inputStream.close();
        }
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            if (file.exists()) {
                file.delete();
            }
        }
    }

    public long getFileLength() {
        return file.length();
    }

    @VisibleForTesting
    File getFile() {
        return file;
    }
}
