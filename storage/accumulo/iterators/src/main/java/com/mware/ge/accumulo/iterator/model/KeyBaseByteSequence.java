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
package com.mware.ge.accumulo.iterator.model;

import com.mware.ge.accumulo.iterator.util.ByteSequenceUtils;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;

import java.nio.ByteBuffer;

import static com.mware.ge.accumulo.iterator.util.ByteSequenceUtils.indexOf;
import static com.mware.ge.accumulo.iterator.util.ByteSequenceUtils.subSequence;


public abstract class KeyBaseByteSequence {
    public static final byte VALUE_SEPARATOR = 0x1f;

    public static ByteSequence[] splitOnValueSeparator(ByteSequence bytes, int partCount) {
        ByteSequence[] results = new ByteSequence[partCount];
        int last = 0;
        int i = indexOf(bytes, VALUE_SEPARATOR);
        int partIndex = 0;
        while (true) {
            if (i > 0) {
                results[partIndex++] = subSequence(bytes, last, i);
                if (partIndex >= partCount) {
                    throw new GeAccumuloIteratorException("Invalid number of parts for '" + bytes + "'. Expected " + partCount + " found " + partIndex);
                }
                last = i + 1;
                i = indexOf(bytes, VALUE_SEPARATOR, last);
            } else {
                results[partIndex++] = subSequence(bytes, last);
                break;
            }
        }
        if (partIndex != partCount) {
            throw new GeAccumuloIteratorException("Invalid number of parts for '" + bytes + "'. Expected " + partCount + " found " + partIndex);
        }
        return results;
    }

    public static void assertNoValueSeparator(ByteSequence bytes) {
        if (indexOf(bytes, VALUE_SEPARATOR) >= 0) {
            throw new GeInvalidKeyException("String cannot contain '" + VALUE_SEPARATOR + "' (0x1f): " + bytes);
        }
    }

    public static ByteSequence getDiscriminator(ByteSequence propertyName, ByteSequence propertyKey, ByteSequence visibilityString, long timestamp) {
        assertNoValueSeparator(propertyName);
        assertNoValueSeparator(propertyKey);
        assertNoValueSeparator(visibilityString);
        int length = propertyName.length() + propertyKey.length() + visibilityString.length() + 8;
        byte[] bytes = new byte[length];
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        ByteSequenceUtils.putIntoByteBuffer(propertyName, bb);
        ByteSequenceUtils.putIntoByteBuffer(propertyKey, bb);
        ByteSequenceUtils.putIntoByteBuffer(visibilityString, bb);
        bb.putLong(timestamp);
        return new ArrayByteSequence(bytes);
    }
}
