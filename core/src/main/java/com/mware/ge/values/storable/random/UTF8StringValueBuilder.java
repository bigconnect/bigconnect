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
package com.mware.ge.values.storable.random;

import com.mware.ge.values.storable.TextValue;
import com.mware.ge.values.storable.UTF8StringValue;
import com.mware.ge.values.storable.Values;

import java.util.Arrays;

/**
 * Helper class for dynamically building {@link UTF8StringValue}.
 * <p>
 * Either add individual bytes via {@link #add(byte)} or add a unicode code point via {@link #addCodePoint(int)}. The
 * builder maintains an internal {@code byte[]} and grows it as necessary.
 */
class UTF8StringValueBuilder
{
    private static final int DEFAULT_SIZE = 8;
    private byte[] bytes;
    private int length;

    UTF8StringValueBuilder()
    {
        this( DEFAULT_SIZE );
    }

    UTF8StringValueBuilder(int initialCapacity )
    {
        this.bytes = new byte[initialCapacity];
    }

    /**
     * Add a single byte to the builder.
     *
     * @param b the byte to add.
     */
    void add( byte b )
    {
        if ( bytes.length == length )
        {
            ensureCapacity();
        }
        bytes[length++] = b;
    }

    private void ensureCapacity()
    {
        int newCapacity = bytes.length << 1;
        if ( newCapacity < 0 )
        {
            throw new IllegalStateException( "Fail to increase capacity." );
        }
        this.bytes = Arrays.copyOf( bytes, newCapacity );
    }

    TextValue build()
    {
        return Values.utf8Value( bytes, 0, length );
    }

    /**
     * Add a single code point to the builder.
     * <p>
     * In UTF8 a code point use one to four bytes depending on the code point at hand. So we will have one of the
     * following cases (x marks the bits of the codepoint):
     * <ul>
     * <li>One byte (asciii): {@code 0xxx xxxx}</li>
     * <li>Two bytes: {@code 110xxxxx 10xxxxxx}</li>
     * <li>Three bytes: {@code 1110xxxx 10xxxxxx 10xxxxxx}</li>
     * <li>Four bytes: {@code 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx}</li>
     * </ul>
     *
     * @param codePoint the code point to add
     */
    void addCodePoint( int codePoint )
    {
        assert codePoint >= 0;
        if ( codePoint < 0x80 )
        {
            //one byte is all it takes
            add( (byte) codePoint );
        }
        else if ( codePoint < 0x800 )
        {
            //Require two bytes - will be laid out like:
            //b1       b2
            //110xxxxx 10xxxxxx
            add( (byte) (0b1100_0000 | (0b0001_1111 & (codePoint >> 6))) );
            add( (byte) (0b1000_0000 | (0b0011_1111 & codePoint)) );
        }
        else if ( codePoint < 0x10000 )
        {
            //Require three bytes - will be laid out like:
            //b1       b2       b3
            //1110xxxx 10xxxxxx 10xxxxxx
            add( (byte) (0b1110_0000 | (0b0000_1111 & (codePoint >> 12))) );
            add( (byte) (0b1000_0000 | (0b0011_1111 & (codePoint >> 6))) );
            add( (byte) (0b1000_0000 | (0b0011_1111 & codePoint)) );
        }
        else
        {
            //Require four bytes - will be laid out like:
            //b1       b2       b3       b4
            //11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
            add( (byte) (0b1111_0000 | (0b0001_1111 & (codePoint >> 18))) );
            add( (byte) (0b1000_0000 | (0b0011_1111 & (codePoint >> 12))) );
            add( (byte) (0b1000_0000 | (0b0011_1111 & (codePoint >> 6))) );
            add( (byte) (0b1000_0000 | (0b0011_1111 & codePoint)) );
        }
    }
}
