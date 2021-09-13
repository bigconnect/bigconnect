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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class LimitInputStream extends FilterInputStream {
    private long left;
    private long mark = -1L;

    public LimitInputStream(InputStream in, long limit) {
        super(in);
        checkNotNull(in);
        checkArgument(limit >= 0L, "limit must be non-negative");
        this.left = limit;
    }

    public int available() throws IOException {
        return (int) Math.min((long) this.in.available(), this.left);
    }

    public synchronized void mark(int readLimit) {
        this.in.mark(readLimit);
        this.mark = this.left;
    }

    public int read() throws IOException {
        if (this.left == 0L) {
            return -1;
        } else {
            int result = this.in.read();
            if (result != -1) {
                --this.left;
            }

            return result;
        }
    }

    public int read(byte[] b, int off, int len) throws IOException {
        if (this.left == 0L) {
            return -1;
        } else {
            len = (int) Math.min((long) len, this.left);
            int result = this.in.read(b, off, len);
            if (result != -1) {
                this.left -= (long) result;
            }

            return result;
        }
    }

    public synchronized void reset() throws IOException {
        if (!this.in.markSupported()) {
            throw new IOException("Mark not supported");
        } else if (this.mark == -1L) {
            throw new IOException("Mark not set");
        } else {
            this.in.reset();
            this.left = this.mark;
        }
    }

    public long skip(long n) throws IOException {
        n = Math.min(n, this.left);
        long skipped = this.in.skip(n);
        this.left -= skipped;
        return skipped;
    }
}

