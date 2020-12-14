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
package com.mware.ge.type;

import java.util.HashMap;
import java.util.Map;

public class GeoHash extends GeoShapeBase {
    private static final char[] BASE32 = {
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g',
            'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'
    };
    private static final int[] BITS = {16, 8, 4, 2, 1};
    private static final Map<Character, Integer> DECODE_MAP = new HashMap<>();
    private final String hash;

    static {
        int sz = BASE32.length;
        for (int i = 0; i < sz; i++) {
            DECODE_MAP.put(BASE32[i], i);
        }
    }

    public GeoHash(String hash) {
        this.hash = hash;
    }

    public GeoHash(double latitude, double longitude, int precision) {
        this.hash = encode(latitude, longitude, precision);
    }

    public String getHash() {
        return hash;
    }

    @Override
    public String toString() {
        return "GeoHash{" +
                "hash='" + hash + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GeoHash geoHash = (GeoHash) o;

        if (!hash.equals(geoHash.hash)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return hash.hashCode();
    }

    private String encode(double latitude, double longitude, int precision) {
        double[] latInterval = {-90.0, 90.0};
        double[] lngInterval = {-180.0, 180.0};

        final StringBuilder geohash = new StringBuilder();
        boolean isEven = true;

        int bit = 0;
        int ch = 0;

        while (geohash.length() < precision) {
            double mid;
            if (isEven) {
                mid = (lngInterval[0] + lngInterval[1]) / 2D;
                if (longitude > mid) {
                    ch |= BITS[bit];
                    lngInterval[0] = mid;
                } else {
                    lngInterval[1] = mid;
                }
            } else {
                mid = (latInterval[0] + latInterval[1]) / 2D;
                if (latitude > mid) {
                    ch |= BITS[bit];
                    latInterval[0] = mid;
                } else {
                    latInterval[1] = mid;
                }
            }

            isEven = !isEven;

            if (bit < 4) {
                bit++;
            } else {
                geohash.append(BASE32[ch]);
                bit = 0;
                ch = 0;
            }
        }

        return geohash.toString();
    }

    public GeoRect toGeoRect() {
        final double[] latInterval = {-90.0, 90.0};
        final double[] lngInterval = {-180.0, 180.0};

        boolean isEven = true;

        for (int i = 0; i < hash.length(); i++) {
            final int cd = DECODE_MAP.get(hash.charAt(i));

            for (int mask : BITS) {
                if (isEven) {
                    if ((cd & mask) != 0) {
                        lngInterval[0] = (lngInterval[0] + lngInterval[1]) / 2D;
                    } else {
                        lngInterval[1] = (lngInterval[0] + lngInterval[1]) / 2D;
                    }
                } else {
                    if ((cd & mask) != 0) {
                        latInterval[0] = (latInterval[0] + latInterval[1]) / 2D;
                    } else {
                        latInterval[1] = (latInterval[0] + latInterval[1]) / 2D;
                    }
                }
                isEven = !isEven;
            }

        }
        GeoPoint nw = new GeoPoint(latInterval[1], lngInterval[0]);
        GeoPoint se = new GeoPoint(latInterval[0], lngInterval[1]);
        return new GeoRect(nw, se);
    }

    @Override
    public boolean within(GeoShape geoShape) {
        return toGeoRect().within(geoShape);
    }

    @Override
    public boolean intersects(GeoShape geoShape) {
        return toGeoRect().intersects(geoShape);
    }
}
