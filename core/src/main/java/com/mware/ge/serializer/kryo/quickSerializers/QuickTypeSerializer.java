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

interface QuickTypeSerializer<T> {
    byte MARKER_KRYO = 0;

    byte MARKER_BOOLEANVALUE = 1;
    byte MARKER_BOOLEANARRAY = 2;

    byte MARKER_BYTEVALUE = 3;
    byte MARKER_BYTEARRAY = 4;

    byte MARKER_CHARVALUE = 5;
    byte MARKER_CHARARRAY = 6;

    byte MARKER_DATEARRAY = 7;
    byte MARKER_DATEVALUE = 8;

    byte MARKER_DATETIMEARRAY = 9;
    byte MARKER_DATETIMEVALUE = 10;

    byte MARKER_DOUBLEARRAY = 11;
    byte MARKER_DOUBLEVALUE = 12;

    byte MARKER_DURATIONARRAY = 13;
    byte MARKER_DURATIONVALUE = 14;

    byte MARKER_EDGEVERTEXIDS = 15;

    byte MARKER_FLOATARRAY = 16;
    byte MARKER_FLOATVALUE = 17;

    byte MARKER_GEOPOINTVALUE = 18;
    byte MARKER_GEOCIRCLEVALUE = 19;
    byte MARKER_GEOCOLLECTIONVALUE = 20;
    byte MARKER_GEORECTVALUE = 21;
    byte MARKER_GEOPOLYGONVALUE = 22;
    byte MARKER_GEOHASHVALUE = 23;
    byte MARKER_GEOLINEVALUE = 38;

    byte MARKER_INTARRAY = 24;
    byte MARKER_INTVALUE = 25;

    byte MARKER_LONGARRAY = 26;
    byte MARKER_LONGVALUE = 27;

    byte MARKER_SHORTARRAY = 28;
    byte MARKER_SHORTVALUE = 29;

    byte MARKER_STRINGARRAY = 30;
    byte MARKER_STRINGVALUE = 31;

    byte MARKER_LOCALDATETIMEVALUE = 32;
    byte MARKER_LOCALDATETIMEARRAY = 35;

    byte MARKER_TIMEVALUE = 33;
    byte MARKER_TIMEARRAY = 36;

    byte MARKER_LOCALTIMEVALUE = 34;
    byte MARKER_LOCALTIMEARRAY = 37;

    byte[] objectToBytes(T value);

    T valueToObject(byte[] data);
}
