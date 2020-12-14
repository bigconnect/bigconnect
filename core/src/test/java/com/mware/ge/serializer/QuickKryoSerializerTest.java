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
package com.mware.ge.serializer;

import com.mware.ge.serializer.kryo.quickSerializers.QuickKryoGeSerializer;
import com.mware.ge.type.*;
import com.mware.ge.values.storable.Value;
import com.mware.ge.values.storable.Values;
import org.junit.Test;

import java.time.*;
import java.util.Arrays;

import static com.mware.ge.util.GeAssert.assertEquals;

public class QuickKryoSerializerTest {
    GeSerializer geSerializer = new QuickKryoGeSerializer(false);

    @Test
    public void testValues() {
        test( Values.booleanValue(true) );
        test( Values.booleanArray(new boolean[] { true, false, true, false, true, true }) );
        test( Values.byteValue((byte) 18) );
        test( Values.byteArray("Marry had a little lamb".getBytes()));
        test( Values.charValue('y') );
        test( Values.charArray("Marry had a little lamb".toCharArray()) );
        test( Values.doubleValue(3490834545222.82987237489d) );
        test( Values.doubleArray(new double[] { Double.MIN_VALUE, Double.MAX_VALUE, 2098409234.203123123d, 39089038452.10923123d }) );
        test( Values.floatValue(349083454.82000189f) );
        test( Values.floatArray(new float[] { Float.MAX_VALUE, Float.MIN_VALUE, 203434.2309432f, 3045345.0000001f } ) );
        test( Values.intValue(3904851) );
        test( Values.intArray(new int[] { Integer.MAX_VALUE, Integer.MIN_VALUE, 290348234, 2987123, 13214, 343456, 46456 } ) );
        test( Values.shortValue((short) 334) );
        test( Values.shortArray(new short[]{ Short.MIN_VALUE, Short.MAX_VALUE, 123, 343, 11, 5, 55, 33 }) );
        test( Values.longValue( 2903234L ));
        test( Values.longArray( new long[] { Long.MAX_VALUE, Long.MIN_VALUE, 903234L, 29034934L, 12093213L, 9012839018203923L  }));
        test( Values.stringValue("Marry had a little lamb" ));
        test( Values.stringArray("Marry had a little lamb", "Flavius has a little dog", "John had a little kitty"));

        test( Values.temporalValue( ZonedDateTime.now() ));
        test( Values.temporalValue( ZonedDateTime.now(ZoneOffset.ofHours(4)) ));
        test( Values.temporalValue( OffsetDateTime.now()) );
        test( Values.dateTimeArray( new ZonedDateTime[] { ZonedDateTime.now(), ZonedDateTime.now().plusDays(1), ZonedDateTime.now().plusMinutes(5), ZonedDateTime.now().plusNanos(12)}));

        test( Values.temporalValue( LocalDate.now() ));
        test( Values.dateArray( new LocalDate[] { LocalDate.now(), LocalDate.now().plusDays(1), LocalDate.now().plusMonths(1), LocalDate.now().plusYears(1) }));

        test( Values.temporalValue( LocalDateTime.now() ));
        test( Values.localDateTimeArray( new LocalDateTime[] { LocalDateTime.now(), LocalDateTime.now().plusDays(1), LocalDateTime.now().plusMonths(5) } ));

        test( Values.temporalValue( LocalTime.now() ));
        test( Values.localTimeArray( new LocalTime[] { LocalTime.now(), LocalTime.now().plusHours(5), LocalTime.now().plusMinutes(11), LocalTime.now().plusSeconds(33) }));

        test( Values.temporalValue( OffsetTime.now() ));
        test( Values.timeArray( new OffsetTime[] { OffsetTime.now(), OffsetTime.now().plusHours(1), OffsetTime.now().plusSeconds(55), OffsetTime.now().plusMinutes(5)} ));

        test (Values.geoPointValue(new GeoPoint(11.11, 12.12, 13.13, 14.14)));
        test (Values.geoCircleValue(new GeoCircle(11.11, 12.12, 13.13)));
        test (Values.geoLineValue(new GeoLine(new GeoPoint(11, 8), new GeoPoint(8, 11))));
        test (Values.geoRectValue(new GeoRect(new GeoPoint(2, -4), new GeoPoint(-2, -2))));
        test (Values.geoHashValue(new GeoHash(48.669, -4.329, 5)));


        GeoPolygon geoPolygonWithHole = new GeoPolygon(
                Arrays.asList(
                        new GeoPoint(0, 0),
                        new GeoPoint(0, 10),
                        new GeoPoint(5, 10),
                        new GeoPoint(10, 5),
                        new GeoPoint(5, 0),
                        new GeoPoint(0, 0)
                ),
                Arrays.asList(
                        Arrays.asList(
                                new GeoPoint(1, 3),
                                new GeoPoint(5, 3),
                                new GeoPoint(5, 7),
                                new GeoPoint(1, 7),
                                new GeoPoint(1, 3)
                        )
                ));
        test (Values.geoPolygonValue(geoPolygonWithHole));
    }

    private void test(Value sourceValue) {
        Value targetValue = geSerializer.bytesToObject(geSerializer.objectToBytes(sourceValue));
        assertEquals(sourceValue, targetValue);
    }
}
