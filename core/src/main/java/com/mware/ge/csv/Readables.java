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
/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.mware.ge.csv;

import com.mware.ge.collection.RawIterator;
import com.mware.ge.function.IOFunction;
import com.mware.ge.function.ThrowingFunction;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Means of instantiating common {@link CharReadable} instances.
 * <p>
 * There are support for compressed files as well for those methods accepting a {@link File} argument.
 * <ol>
 * <li>ZIP: is both an archive and a compression format. In many cases the order of files
 * is important and for a ZIP archive with multiple files, the order of the files are whatever the order
 * set by the tool that created the ZIP archive. Therefore only single-file-zip files are supported.
 * The single file in the given ZIP archive will be decompressed on the fly, while reading.</li>
 * <li>GZIP: is only a compression format and so will be decompressed on the fly, while reading.</li>
 * </ol>
 */
public class Readables {
    private Readables() {
        throw new AssertionError("No instances allowed");
    }

    public static final CharReadable EMPTY = new CharReadable.Adapter() {
        @Override
        public SectionedCharBuffer read(SectionedCharBuffer buffer, int from) {
            return buffer;
        }

        @Override
        public void close() {   // Nothing to close
        }

        @Override
        public String sourceDescription() {
            return "EMPTY";
        }

        @Override
        public int read(char[] into, int offset, int length) {
            return -1;
        }

        @Override
        public long length() {
            return 0;
        }
    };

    public static CharReadable wrap(final InputStream stream, final String sourceName, Charset charset)
            throws IOException {
        return wrap(stream, sourceName, charset, 0);
    }

    /**
     * Wraps a {@link InputStream} in a {@link CharReadable}.
     *
     * @param stream     {@link Reader} to wrap.
     * @param sourceName name or description of the source of the stream.
     * @param charset    {@link Charset} to use for reading.
     * @param length     total number of bytes provided by the reader.
     * @return a {@link CharReadable} for the {@link Reader}.
     * @throws IOException on I/O error.
     */
    public static CharReadable wrap(final InputStream stream, final String sourceName, Charset charset, long length)
            throws IOException {
        byte[] bytes = new byte[Magic.longest()];
        PushbackInputStream pushbackStream = new PushbackInputStream(stream, bytes.length);
        Charset usedCharset = charset;
        int read = stream.read(bytes);
        if (read >= 0) {
            bytes = read < bytes.length ? Arrays.copyOf(bytes, read) : bytes;
            Magic magic = Magic.of(bytes);
            int excessiveBytes = read;
            if (magic.impliesEncoding()) {
                // Unread the diff between the BOM and the longest magic we gathered bytes for
                excessiveBytes -= magic.length();
                usedCharset = magic.encoding();
            }
            pushbackStream.unread(bytes, read - excessiveBytes, excessiveBytes);
        }
        return wrap(new InputStreamReader(pushbackStream, usedCharset) {
            @Override
            public String toString() {
                return sourceName;
            }
        }, length);
    }

    public static CharReadable wrap(String data) {
        return wrap(new StringReader(data), data.length());
    }

    /**
     * Wraps a {@link Reader} in a {@link CharReadable}.
     * Remember that the {@link Reader#toString()} must provide a description of the data source.
     *
     * @param reader {@link Reader} to wrap.
     * @param length total number of bytes provided by the reader.
     * @return a {@link CharReadable} for the {@link Reader}.
     */
    public static CharReadable wrap(final Reader reader, long length) {
        return new WrappedCharReadable(length, reader);
    }

    private static class FromFile implements IOFunction<File, CharReadable> {
        private final Charset charset;

        FromFile(Charset charset) {
            this.charset = charset;
        }

        @Override
        public CharReadable apply(final File file) throws IOException {
            Magic magic = Magic.of(file);
            if (magic == Magic.ZIP) {   // ZIP file
                ZipFile zipFile = new ZipFile(file);
                ZipEntry entry = getSingleSuitableEntry(zipFile);
                return wrap(new InputStreamReader(zipFile.getInputStream(entry), charset) {
                    @Override
                    public String toString() {
                        return file.getPath();
                    }
                }, file.length());
            } else if (magic == Magic.GZIP) {   // GZIP file. GZIP isn't an archive like ZIP, so this is purely data that is compressed.
                // Although a very common way of compressing with GZIP is to use TAR which can combine many
                // files into one blob, which is then compressed. If that's the case then
                // the data will look like garbage and the reader will fail for whatever it will be used for.
                // TODO add tar support
                GZIPInputStream zipStream = new GZIPInputStream(new FileInputStream(file));
                return wrap(new InputStreamReader(zipStream, charset) {
                    @Override
                    public String toString() {
                        return file.getPath();
                    }
                }, file.length());
            } else {
                InputStream in = new FileInputStream(file);
                Charset usedCharset = this.charset;
                if (magic.impliesEncoding()) {
                    // Read (and skip) the magic (BOM in this case) from the file we're returning out
                    in.skip(magic.length());
                    usedCharset = magic.encoding();
                }
                return wrap(new InputStreamReader(in, usedCharset) {
                    @Override
                    public String toString() {
                        return file.getPath();
                    }
                }, file.length());
            }
        }

        private ZipEntry getSingleSuitableEntry(ZipFile zipFile) throws IOException {
            List<String> unsuitableEntries = new ArrayList<>();
            Enumeration<? extends ZipEntry> enumeration = zipFile.entries();
            ZipEntry found = null;
            while (enumeration.hasMoreElements()) {
                ZipEntry entry = enumeration.nextElement();
                if (entry.isDirectory() || invalidZipEntry(entry.getName())) {
                    unsuitableEntries.add(entry.getName());
                    continue;
                }

                if (found != null) {
                    throw new IOException("Multiple suitable files found in zip file " + zipFile.getName() +
                            ", at least " + found.getName() + " and " + entry.getName() +
                            ". Only a single file per zip file is supported");
                }
                found = entry;
            }

            if (found == null) {
                throw new IOException("No suitable file found in zip file " + zipFile.getName() + "." +
                        (!unsuitableEntries.isEmpty() ?
                                " Although found these unsuitable entries " + unsuitableEntries : ""));
            }
            return found;
        }
    }

    private static boolean invalidZipEntry(String name) {
        return name.contains("__MACOSX") ||
                name.startsWith(".") ||
                name.contains("/.");
    }

    public static RawIterator<CharReadable, IOException> individualFiles(Charset charset, File... files) {
        return iterator(new FromFile(charset), files);
    }

    public static CharReadable files(Charset charset, File... files) throws IOException {
        IOFunction<File, CharReadable> opener = new FromFile(charset);
        switch (files.length) {
            case 0:
                return EMPTY;
            case 1:
                return opener.apply(files[0]);
            default:
                return new MultiReadable(iterator(opener, files));
        }
    }

    @SafeVarargs
    public static <IN, OUT> RawIterator<OUT, IOException> iterator(ThrowingFunction<IN, OUT, IOException> converter, IN... items) {
        if (items.length == 0) {
            throw new IllegalStateException("No source items specified");
        }

        return new RawIterator<OUT, IOException>() {
            private int cursor;

            @Override
            public boolean hasNext() {
                return cursor < items.length;
            }

            @Override
            public OUT next() throws IOException {
                if (!hasNext()) {
                    throw new IllegalStateException();
                }
                return converter.apply(items[cursor++]);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Extracts the first line, i.e characters until the first newline or end of stream.
     * Reads one character at a time to be sure not to read too far ahead. The stream is left
     * in a state of either exhausted or at the beginning of the next line of data.
     *
     * @param source {@link CharReadable} to read from.
     * @return char[] containing characters until the first newline character or end of stream.
     * @throws IOException on I/O reading error.
     */
    public static char[] extractFirstLineFrom(CharReadable source) throws IOException {
        char[] result = new char[100];
        int cursor = 0;
        int read;
        boolean foundEol = false;
        do {
            // Grow on demand
            if (cursor >= result.length) {
                result = Arrays.copyOf(result, cursor * 2);
            }

            // Read one character
            read = source.read(result, cursor, 1);
            if (read > 0) {
                foundEol = BufferedCharSeeker.isEolChar(result[cursor]);
                if (!foundEol) {
                    cursor++;
                }
            }
        }
        while (read > 0 && !foundEol);
        return Arrays.copyOf(result, cursor);
    }
}
