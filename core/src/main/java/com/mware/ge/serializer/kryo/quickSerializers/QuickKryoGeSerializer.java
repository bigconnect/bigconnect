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
package com.mware.ge.serializer.kryo.quickSerializers;

import com.mware.ge.GeException;
import com.mware.ge.GraphConfiguration;
import com.mware.ge.serializer.GeSerializer;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.values.storable.*;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import static com.mware.ge.serializer.kryo.quickSerializers.QuickTypeSerializer.*;

public class QuickKryoGeSerializer implements GeSerializer {
    private GeLogger LOGGER = GeLoggerFactory.getLogger(QuickKryoGeSerializer.class);

    private static final byte[] EMPTY = new byte[0];
    public static final String CONFIG_COMPRESS = GraphConfiguration.SERIALIZER + ".enableCompression";
    public static final boolean CONFIG_COMPRESS_DEFAULT = false;
    private final boolean enableCompression;
    private QuickTypeSerializer defaultQuickTypeSerializer = new KryoQuickTypeSerializer();

    private Map<Class, QuickTypeSerializer> quickTypeSerializersByClass = new HashMap<Class, QuickTypeSerializer>() {{
        put(BooleanValue.class, new BooleanValueSerializer());
        put(BooleanArray.class, new BooleanArraySerializer());
        put(ByteValue.class, new ByteValueSerializer());
        put(ByteArray.class, new ByteArraySerializer());
        put(CharValue.class, new CharValueSerializer());
        put(CharArray.class, new CharArraySerializer());
        put(DoubleValue.class, new DoubleValueSerializer());
        put(DoubleArray.class, new DoubleArraySerializer());
        put(FloatValue.class, new FloatValueSerializer());
        put(FloatArray.class, new FloatArraySerializer());
        put(IntValue.class, new IntValueSerializer());
        put(IntArray.class, new IntArraySerializer());
        put(ShortValue.class, new ShortValueSerializer());
        put(ShortArray.class, new ShortArraySerializer());
        put(LongValue.class, new LongValueSerializer());
        put(LongArray.class, new LongArraySerializer());

        put(StringValue.class, new StringValueSerializer());
        put(StringArray.class, new StringArraySerializer());

        put(DateTimeValue.class, new DateTimeValueSerializer());
        put(DateTimeArray.class, new DateTimeArraySerializer());
        put(DateValue.class, new DateValueSerializer());
        put(DateArray.class, new DateArraySerializer());
        put(LocalDateTimeValue.class, new LocalDateTimeValueSerializer());
        put(LocalDateTimeArray.class, new LocalDateTimeArraySerializer());
        put(TimeValue.class, new TimeValueSerializer());
        put(TimeArray.class, new TimeArraySerializer());
        put(LocalTimeValue.class, new LocalTimeValueSerializer());
        put(LocalTimeArray.class, new LocalTimeArraySerializer());

        put(GeoPointValue.class, new GeoPointValueSerializer());
        put(GeoCircleValue.class, new GeoCircleValueSerializer());
        put(GeoLineValue.class, new GeoLineValueSerializer());
        put(GeoRectValue.class, new GeoRectValueSerializer());
        put(GeoHashValue.class, new GeoHashValueSerializer());
        put(GeoPolygonValue.class, new GeoPolygonValueSerializer());
    }};

    private Map<Byte, QuickTypeSerializer> quickTypeSerializersByMarker = new HashMap<Byte, QuickTypeSerializer>() {{
        put(MARKER_BOOLEANVALUE, new BooleanValueSerializer());
        put(MARKER_BOOLEANARRAY, new BooleanArraySerializer());
        put(MARKER_BYTEVALUE, new ByteValueSerializer());
        put(MARKER_BYTEARRAY, new ByteArraySerializer());
        put(MARKER_CHARVALUE, new CharValueSerializer());
        put(MARKER_CHARARRAY, new CharArraySerializer());
        put(MARKER_DOUBLEVALUE, new DoubleValueSerializer());
        put(MARKER_DOUBLEARRAY, new DoubleArraySerializer());
        put(MARKER_FLOATVALUE, new FloatValueSerializer());
        put(MARKER_FLOATARRAY, new FloatArraySerializer());
        put(MARKER_INTVALUE, new IntValueSerializer());
        put(MARKER_INTARRAY, new IntArraySerializer());
        put(MARKER_SHORTVALUE, new ShortValueSerializer());
        put(MARKER_SHORTARRAY, new ShortArraySerializer());
        put(MARKER_LONGVALUE, new LongValueSerializer());
        put(MARKER_LONGARRAY, new LongArraySerializer());
        put(MARKER_STRINGVALUE, new StringValueSerializer());
        put(MARKER_STRINGARRAY, new StringArraySerializer());
        put(MARKER_DATETIMEVALUE, new DateTimeValueSerializer());
        put(MARKER_DATETIMEARRAY, new DateTimeArraySerializer());
        put(MARKER_DATEVALUE, new DateValueSerializer());
        put(MARKER_DATEARRAY, new DateArraySerializer());
        put(MARKER_LOCALDATETIMEVALUE, new LocalDateTimeValueSerializer());
        put(MARKER_LOCALDATETIMEARRAY, new LocalDateTimeArraySerializer());
        put(MARKER_TIMEVALUE, new TimeValueSerializer());
        put(MARKER_TIMEARRAY, new TimeArraySerializer());
        put(MARKER_LOCALTIMEVALUE, new LocalTimeValueSerializer());
        put(MARKER_LOCALTIMEARRAY, new LocalTimeArraySerializer());
        put(MARKER_GEOPOINTVALUE, new GeoPointValueSerializer());
        put(MARKER_GEOCIRCLEVALUE, new GeoCircleValueSerializer());
        put(MARKER_GEOLINEVALUE, new GeoLineValueSerializer());
        put(MARKER_GEORECTVALUE, new GeoRectValueSerializer());
        put(MARKER_GEOHASHVALUE, new GeoHashValueSerializer());
        put(MARKER_GEOPOLYGONVALUE, new GeoPolygonValueSerializer());
        put(MARKER_KRYO, new KryoQuickTypeSerializer());
    }};

    public QuickKryoGeSerializer(GraphConfiguration config) {
        this(config.getBoolean(CONFIG_COMPRESS, CONFIG_COMPRESS_DEFAULT));
    }

    public QuickKryoGeSerializer(boolean enableCompression) {
        this.enableCompression = enableCompression;
    }

    @Override
    public byte[] objectToBytes(Object object) {
        if (object == null) {
            return EMPTY;
        }
        QuickTypeSerializer quickTypeSerializer = quickTypeSerializersByClass.get(object.getClass());

        // for inner classes
        if (quickTypeSerializer == null)
            quickTypeSerializer = quickTypeSerializersByClass.get(object.getClass().getSuperclass());

        if (object.getClass().getName().contains("GeoCollection"))
            LOGGER.warn("### No optimized serializer for GeoCollection implemented");

        byte[] bytes;
        if (quickTypeSerializer != null) {
            bytes = quickTypeSerializer.objectToBytes(object);
        } else {
            bytes = defaultQuickTypeSerializer.objectToBytes(object);
        }
        return compress(bytes);
    }

    @Override
    public <T> T bytesToObject(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        bytes = expand(bytes);
        QuickTypeSerializer quickTypeSerializer = quickTypeSerializersByMarker.get(bytes[0]);
        if (quickTypeSerializer != null) {
            return (T) quickTypeSerializer.valueToObject(bytes);
        }
        throw new GeException("Invalid marker: " + Integer.toHexString(bytes[0]));
    }

    protected byte[] compress(byte[] bytes) {
        if (!enableCompression) {
            return bytes;
        }

        Deflater deflater = new Deflater(Deflater.BEST_COMPRESSION);
        try {
            deflater.setInput(bytes);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(bytes.length);
            deflater.finish();
            byte[] buffer = new byte[1024];
            while (!deflater.finished()) {
                int count = deflater.deflate(buffer);
                outputStream.write(buffer, 0, count);
            }
            outputStream.close();
            return outputStream.toByteArray();
        } catch (Exception ex) {
            throw new GeException("Could not compress bytes", ex);
        } finally {
            deflater.end();
        }
    }

    protected byte[] expand(byte[] bytes) {
        if (!enableCompression) {
            return bytes;
        }

        Inflater inflater = new Inflater();
        try {
            inflater.setInput(bytes);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(bytes.length);
            byte[] buffer = new byte[1024];
            while (!inflater.finished()) {
                int count = inflater.inflate(buffer);
                outputStream.write(buffer, 0, count);
            }
            outputStream.close();

            return outputStream.toByteArray();
        } catch (Exception ex) {
            throw new GeException("Could not decompress bytes", ex);
        } finally {
            inflater.end();
        }
    }
}
