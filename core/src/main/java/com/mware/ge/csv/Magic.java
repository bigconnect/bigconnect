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

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.nio.charset.Charset.forName;

/**
 * Logic for detecting and matching magic numbers in file headers.
 */
public class Magic {
    public static final Magic NONE = new Magic("NONE", null, new byte[0]);

    private static final List<Magic> DEFINITIONS = new ArrayList<>();
    private static int LONGEST;

    /**
     * First 4 bytes of a ZIP file have this signature.
     */
    public static final Magic ZIP = Magic.define("ZIP", null, 0x50, 0x4b, 0x03, 0x04);
    /**
     * First 2 bytes of a GZIP file have this signature.
     */
    public static final Magic GZIP = Magic.define("GZIP", null, 0x1f, 0x8b);

    /**
     * A couple of BOM magics
     */
    public static final Magic BOM_UTF_32_BE = define("BOM_UTF_32_BE", forName("UTF-32"), 0x0, 0x0, 0xFE, 0xFF);
    public static final Magic BOM_UTF_32_LE = define("BOM_UTF_32_LE", forName("UTF-32"), 0xFF, 0xFE, 0x0, 0x0);
    public static final Magic BOM_UTF_16_BE = define("BOM_UTF_16_BE", StandardCharsets.UTF_16BE, 0xFE, 0xFF);
    public static final Magic BOM_UTF_16_LE = define("BOM_UTF_16_LE", StandardCharsets.UTF_16LE, 0xFF, 0xFE);
    public static final Magic BOM_UTF_8 = define("BOM_UTF8", StandardCharsets.UTF_8, 0xEF, 0xBB, 0xBF);

    /**
     * Defines a magic signature which can later be detected in {@link #of(File)} and {@link #of(byte[])}.
     *
     * @param description               description of the magic, typically which file it is.
     * @param impliesEncoding           if a match for this to-be-defined magic implies that the contents in
     *                                  this file has a certain encoding. Typically used for byte-order-mark.
     * @param bytesAsIntsForConvenience bytes that makes up the magic signature. Here specified as
     *                                  an {@code int[]} for convenience of specifying those.
     * @return the defined {@link Magic} instance.
     */
    public static Magic define(String description, Charset impliesEncoding, int... bytesAsIntsForConvenience) {
        byte[] bytes = new byte[bytesAsIntsForConvenience.length];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) bytesAsIntsForConvenience[i];
        }

        Magic magic = new Magic(description, impliesEncoding, bytes);
        DEFINITIONS.add(magic);
        LONGEST = Math.max(LONGEST, bytes.length);
        return magic;
    }

    /**
     * Extracts and matches the magic of the header in the given {@code file}. If no magic matches
     * then {@link #NONE} is returned.
     *
     * @param file {@link File} to extract the magic from.
     * @return matching {@link Magic}, or {@link #NONE} if no match.
     * @throws IOException for errors reading from the file.
     */
    public static Magic of(File file) throws IOException {
        try (InputStream in = new FileInputStream(file)) {
            byte[] bytes = new byte[LONGEST];
            int read = in.read(bytes);
            if (read > 0) {
                bytes = Arrays.copyOf(bytes, read);
                return of(bytes);
            }
        } catch (EOFException e) {   // This is OK
        }
        return Magic.NONE;
    }

    /**
     * Matches the magic bytes with all defined and returns a match or {@link #NONE}.
     *
     * @param bytes magic bytes extracted from a file header.
     * @return matching {@link Magic}, or {@link #NONE} if no match.
     */
    public static Magic of(byte[] bytes) {
        for (Magic candidate : DEFINITIONS) {
            if (candidate.matches(bytes)) {
                return candidate;
            }
        }
        return NONE;
    }

    public static int longest() {
        return LONGEST;
    }

    private final String description;
    private final Charset encoding;
    private final byte[] bytes;

    private Magic(String description, Charset encoding, byte[] bytes) {
        this.description = description;
        this.encoding = encoding;
        this.bytes = bytes;
    }

    /**
     * Tests whether or not a set of magic bytes matches this {@link Magic} signature.
     *
     * @param candidateBytes magic bytes to test.
     * @return {@code true} if the candidate bytes matches this signature, otherwise {@code false}.
     */
    public boolean matches(byte[] candidateBytes) {
        if (candidateBytes.length < bytes.length) {
            return false;
        }
        for (int i = 0; i < bytes.length; i++) {
            if (candidateBytes[i] != bytes[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return number of bytes making up this magic signature.
     */
    public int length() {
        return bytes.length;
    }

    /**
     * @return whether or not the presence of this {@link Magic} implies the contents of the file being
     * of a certain encoding. If {@code true} then {@link #encoding()} may be called to get the implied encoding.
     */
    public boolean impliesEncoding() {
        return encoding != null;
    }

    /**
     * @return the encoding this magic signature implies, if {@link #impliesEncoding()} is {@code true},
     * otherwise throws {@link IllegalStateException}.
     */
    public Charset encoding() {
        if (encoding == null) {
            throw new IllegalStateException(this + " doesn't imply any specific encoding");
        }
        return encoding;
    }

    byte[] bytes() {
        // Defensive copy
        return Arrays.copyOf(bytes, bytes.length);
    }

    @Override
    public String toString() {
        return "Magic[" + description + ", " + Arrays.toString(bytes) + "]";
    }
}
