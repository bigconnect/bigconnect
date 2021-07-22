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
package com.mware.ge.values.storable;

import com.mware.ge.util.IOUtils;
import com.mware.ge.GeException;
import com.mware.ge.values.ValueMapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

public abstract class StreamingPropertyValue extends StreamingPropertyValueBase implements Serializable {
    private static final long serialVersionUID = -8009009221695795406L;
    private final Class<? extends Value> valueType;

    public StreamingPropertyValue(Class<? extends Value> valueType) {
        this.valueType = valueType;
    }

    public Class<? extends Value> getValueType() {
        return valueType;
    }

    public abstract Long getLength();

    public abstract InputStream getInputStream();

    public String readToString() {
        try (InputStream in = getInputStream()) {
            if (in.markSupported()) {
                in.reset();
            }
            return IOUtils.toString(in);
        } catch (IOException e) {
            throw new GeException("Could not read streaming property value into string", e);
        }
    }

    public String readToString(long offset, long limit) {
        try (InputStream in = getInputStream()) {
            if (in.markSupported()) {
                in.reset();
            }
            return IOUtils.toString(in, offset, limit);
        } catch (IOException e) {
            throw new GeException("Could not read streaming property value into string", e);
        }
    }

    public static StreamingPropertyValue create(String value) {
        InputStream data = new ByteArrayInputStream(value.getBytes());
        return new DefaultStreamingPropertyValue(data, TextValue.class);
    }

    public static StreamingPropertyValue create(InputStream inputStream, Class<? extends Value> type, Long length) {
        return new DefaultStreamingPropertyValue(inputStream, type, length);
    }

    public static StreamingPropertyValue create(InputStream inputStream, Class<? extends Value> type) {
        return new DefaultStreamingPropertyValue(inputStream, type, null);
    }

    @Override
    public <E extends Exception> void writeTo(ValueWriter<E> writer) throws E {
        writer.writeString(readToString());
    }

    @Override
    public <T> T map(ValueMapper<T> mapper) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    int unsafeCompareTo(Value other) {
        throw new IllegalArgumentException("Cannot compare streaming properties");
    }

    @Override
    public Object asObject() {
        return getInputStream();
    }

    @Override
    public Object asObjectCopy() {
        return asObject();
    }

    @Override
    public String getTypeName() {
        return "StreamingPropertyValue";
    }

    @Override
    public String prettyPrint() {
        return toString();
    }

    @Override
    public String toString() {
        return "StreamingPropertyValue{" +
                "valueType=" + getValueType() +
                ", length=" + getLength() +
                '}';
    }
}
