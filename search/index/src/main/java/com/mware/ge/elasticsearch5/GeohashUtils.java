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
package com.mware.ge.elasticsearch5;

import com.mware.ge.GeException;

class GeohashUtils {
    private static final int[] BITS = {16, 8, 4, 2, 1};

    public static void decodeCell(String key, org.elasticsearch.common.geo.GeoPoint northWest, org.elasticsearch.common.geo.GeoPoint southEast) {
        try {
            double[] interval = decodeCell(key);
            northWest.reset(interval[1], interval[2]);
            southEast.reset(interval[0], interval[3]);
        } catch (Exception e) {
            throw new GeException("Could not decode cell", e);
        }
    }

    private static double[] decodeCell(String geohash) {
        double[] interval = {-90.0, 90.0, -180.0, 180.0};
        boolean isEven = true;

        for (int i = 0; i < geohash.length(); i++) {
            final int cd = decode(geohash.charAt(i));

            for (int mask : BITS) {
                if (isEven) {
                    if ((cd & mask) != 0) {
                        interval[2] = (interval[2] + interval[3]) / 2D;
                    } else {
                        interval[3] = (interval[2] + interval[3]) / 2D;
                    }
                } else {
                    if ((cd & mask) != 0) {
                        interval[0] = (interval[0] + interval[1]) / 2D;
                    } else {
                        interval[1] = (interval[0] + interval[1]) / 2D;
                    }
                }
                isEven = !isEven;
            }
        }
        return interval;
    }

    private static int decode(char geo) {
        switch (geo) {
            case '0':
                return 0;
            case '1':
                return 1;
            case '2':
                return 2;
            case '3':
                return 3;
            case '4':
                return 4;
            case '5':
                return 5;
            case '6':
                return 6;
            case '7':
                return 7;
            case '8':
                return 8;
            case '9':
                return 9;
            case 'b':
                return 10;
            case 'c':
                return 11;
            case 'd':
                return 12;
            case 'e':
                return 13;
            case 'f':
                return 14;
            case 'g':
                return 15;
            case 'h':
                return 16;
            case 'j':
                return 17;
            case 'k':
                return 18;
            case 'm':
                return 19;
            case 'n':
                return 20;
            case 'p':
                return 21;
            case 'q':
                return 22;
            case 'r':
                return 23;
            case 's':
                return 24;
            case 't':
                return 25;
            case 'u':
                return 26;
            case 'v':
                return 27;
            case 'w':
                return 28;
            case 'x':
                return 29;
            case 'y':
                return 30;
            case 'z':
                return 31;
            default:
                throw new GeException("the character '" + geo + "' is not a valid geohash character");
        }
    }
}
