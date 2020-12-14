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
package com.mware.core.util;

import com.mware.core.util.RowKeyHelper;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

public class HdfsLimitOutputStream extends OutputStream {
    private static Random random = new Random();
    private final FileSystem fs;
    private final int maxSizeToStore;
    private final ByteArrayOutputStream smallOutputStream;
    private final MessageDigest digest;
    private OutputStream largeOutputStream;
    private Path hdfsPath;
    private long length;

    public HdfsLimitOutputStream(FileSystem fs, long maxSizeToStore) throws NoSuchAlgorithmException {
        this.digest = MessageDigest.getInstance("SHA-256");
        this.fs = fs;
        this.maxSizeToStore = (int) maxSizeToStore;
        this.smallOutputStream = new ByteArrayOutputStream((int) maxSizeToStore);
        this.length = 0;
    }

    private synchronized OutputStream getLargeOutputStream() throws IOException {
        if (largeOutputStream == null) {
            hdfsPath = createTempPath();
            largeOutputStream = fs.create(hdfsPath);
            largeOutputStream.write(smallOutputStream.toByteArray());
        }
        return largeOutputStream;
    }

    private Path createTempPath() {
        return new Path("/tmp/hdfsLimitOutputStream-" + random.nextLong());
    }

    @Override
    public synchronized void write(int b) throws IOException {
        this.digest.update((byte) b);
        if (this.smallOutputStream.size() <= maxSizeToStore - 1) {
            this.smallOutputStream.write(b);
        } else {
            getLargeOutputStream().write(b);
        }
        length++;
    }

    @Override
    public synchronized void write(byte[] b) throws IOException {
        this.digest.update(b);
        if (this.smallOutputStream.size() <= maxSizeToStore - b.length) {
            this.smallOutputStream.write(b);
        } else {
            getLargeOutputStream().write(b);
        }
        length += b.length;
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        this.digest.update(b, off, len);
        if (this.smallOutputStream.size() <= maxSizeToStore - len) {
            this.smallOutputStream.write(b, off, len);
        } else {
            getLargeOutputStream().write(b, off, len);
        }
        length += len;
    }

    public synchronized boolean hasExceededSizeLimit() {
        return this.largeOutputStream != null;
    }

    public Path getHdfsPath() {
        return hdfsPath;
    }

    public byte[] getSmall() {
        if (hasExceededSizeLimit()) {
            return null;
        }
        return this.smallOutputStream.toByteArray();
    }

    public String getRowKey() {
        byte[] sha = this.digest.digest();
        return "urn" + RowKeyHelper.FIELD_SEPARATOR + "sha256" + RowKeyHelper.FIELD_SEPARATOR + Hex.encodeHexString(sha);
    }

    @Override
    public synchronized void flush() throws IOException {
        if (this.largeOutputStream != null) {
            this.largeOutputStream.flush();
        }
        super.close();
    }

    @Override
    public synchronized void close() throws IOException {
        if (this.largeOutputStream != null) {
            this.largeOutputStream.close();
        }
        this.smallOutputStream.close();
        super.close();
    }

    public long getLength() {
        return this.length;
    }
}
