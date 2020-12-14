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

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.mware.ge.util.Preconditions.checkArgument;
import static com.mware.ge.util.Preconditions.checkNotNull;

public class GeoPoint extends GeoShapeBase implements Comparable<GeoPoint> {
    private static final long serialVersionUID = 1L;
    private static final double COMPARE_TOLERANCE = 0.0001;
    private static final Pattern HOUR_MIN_SECOND_PATTERN = Pattern.compile("\\s*(-)?([0-9\\.]+)°(\\s*([0-9\\.]+)'(\\s*([0-9\\.]+)\")?)?");
    public static final double EQUALS_TOLERANCE_KM = 0.001;
    private final double latitude;
    private final double longitude;
    private double altitude = 0.0d;
    private double accuracy = 0.0d;

    /**
     * Create a geopoint at 0, 0 with an altitude of 0
     */
    protected GeoPoint() {
        this(0.0, 0.0);
    }

    /**
     * @param latitude  latitude is specified in decimal degrees
     * @param longitude longitude is specified in decimal degrees
     * @param altitude  altitude is specified in kilometers
     */
    public GeoPoint(double latitude, double longitude, double altitude, double accuracy) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.altitude = altitude;
        this.accuracy = accuracy;
    }

    /**
     * @param latitude  latitude is specified in decimal degrees
     * @param longitude longitude is specified in decimal degrees
     * @param altitude  altitude is specified in kilometers
     */
    public GeoPoint(double latitude, double longitude, double altitude) {
        this(latitude, longitude, altitude, 0.0);
    }


    public GeoPoint(double latitude, double longitude) {
        this(latitude, longitude, 0.0);
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public Double getAltitude() {
        return altitude;
    }

    public Double getAccuracy() {
        return accuracy;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        sb.append(getLatitude()).append(", ").append(getLongitude());
        if (getAltitude() != null) {
            sb.append(", ").append(getAltitude());
        }
        if (getAccuracy() != null) {
            sb.append(", ~").append(getAccuracy());
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public boolean intersects(GeoShape geoShape) {
        if (geoShape instanceof GeoPoint) {
            return this.equals(geoShape);
        }
        return geoShape.intersects(this);
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 47 * hash + (int) (Double.doubleToLongBits(this.latitude) ^ (Double.doubleToLongBits(this.latitude) >>> 32));
        hash = 47 * hash + (int) (Double.doubleToLongBits(this.longitude) ^ (Double.doubleToLongBits(this.longitude) >>> 32));
        hash = 47 * hash + Double.hashCode(this.altitude);
        hash = 47 * hash + Double.hashCode(this.accuracy);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        GeoPoint other = (GeoPoint) obj;
        double distanceBetween = distanceBetween(this, other);
        if (Double.isNaN(distanceBetween)) {
            return false;
        }
        if (Math.abs(distanceBetween) > EQUALS_TOLERANCE_KM) {
            return false;
        }


        if (Math.abs(this.altitude - other.altitude) > EQUALS_TOLERANCE_KM) {
            return false;
        }


        if (Math.abs(this.accuracy - other.accuracy) > EQUALS_TOLERANCE_KM) {
            return false;
        }
        return true;
    }

    public static double distanceBetween(GeoPoint geoPoint1, GeoPoint geoPoint2) {
        return geoPoint1.distanceFrom(geoPoint2);
    }

    public double distanceFrom(GeoPoint geoPoint) {
        return GeoUtils.distanceBetween(
                this.getLatitude(), this.getLongitude(),
                geoPoint.getLatitude(), geoPoint.getLongitude()
        );
    }

    @Override
    public int compareTo(GeoPoint other) {
        int i;
        if ((i = compare(getLatitude(), other.getLatitude())) != 0) {
            return i;
        }
        if ((i = compare(getLongitude(), other.getLongitude())) != 0) {
            return i;
        }
        if (getAltitude() != null && other.getAltitude() != null) {
            return compare(getAltitude(), other.getAltitude());
        }
        if (getAltitude() != null) {
            return 1;
        }
        if (other.getAltitude() != null) {
            return -1;
        }
        return 0;
    }

    private static int compare(double d1, double d2) {
        if (Math.abs(d1 - d2) < COMPARE_TOLERANCE) {
            return 0;
        }
        if (d1 < d2) {
            return -1;
        }
        if (d1 > d2) {
            return 1;
        }
        return 0;
    }

    public static GeoPoint parse(String str) {
        String[] parts = str.split(",");
        if (parts.length < 2) {
            throw new GeInvalidShapeException("Too few parts to GeoPoint string. Expected at least 2 found " + parts.length + " for string: " + str);
        }
        if (parts.length >= 5) {
            throw new GeInvalidShapeException("Too many parts to GeoPoint string. Expected less than or equal to 4 found " + parts.length + " for string: " + str);
        }
        int part = 0;
        double latitude = parsePart(parts[part++]);
        double longitude = parsePart(parts[part++]);
        Double altitude = null;
        Double accuracy = null;
        while (part < parts.length) {
            String p = parts[part].trim();
            if (p.startsWith("~")) {
                if (accuracy != null) {
                    throw new GeInvalidShapeException("Cannot specify two accuracies (~) in GeoPoint string.");
                }
                accuracy = Double.parseDouble(p.substring(1));
            } else {
                if (altitude != null) {
                    throw new GeInvalidShapeException("Cannot specify two altitudes in GeoPoint string.");
                }
                altitude = Double.parseDouble(p);
            }
            part++;
        }
        return new GeoPoint(latitude, longitude,
                altitude == null ? 0 : altitude, accuracy == null ? 0 : accuracy);
    }

    private static double parsePart(String part) {
        Matcher m = HOUR_MIN_SECOND_PATTERN.matcher(part);
        if (m.matches()) {
            String deg = m.group(2);
            double result = Double.parseDouble(deg);
            if (m.groupCount() >= 4) {
                String minutes = m.group(4);
                result += Double.parseDouble(minutes) / 60.0;
                if (m.groupCount() >= 6) {
                    String seconds = m.group(6);
                    result += Double.parseDouble(seconds) / (60.0 * 60.0);
                }
            }
            if (m.group(1) != null && m.group(1).equals("-")) {
                result = -result;
            }
            return result;
        }
        return Double.parseDouble(part);
    }

    public boolean isSouthEastOf(GeoPoint pt) {
        return isSouthOf(pt) && isEastOf(pt);
    }

    private boolean isEastOf(GeoPoint pt) {
        return longitudinalDistanceTo(pt) > 0;
    }

    public boolean isSouthOf(GeoPoint pt) {
        return getLatitude() < pt.getLatitude();
    }

    public boolean isNorthWestOf(GeoPoint pt) {
        return isNorthOf(pt) && isWestOf(pt);
    }

    private boolean isWestOf(GeoPoint pt) {
        return longitudinalDistanceTo(pt) < 0;
    }

    public double longitudinalDistanceTo(GeoPoint pt) {
        double me = getLongitude();
        double them = pt.getLongitude();
        double result = Math.abs(me - them) > 180.0 ? (them - me) : (me - them);
        if (result > 180.0) {
            result -= 360.0;
        }
        if (result < -180.0) {
            result += 360.0;
        }
        return result;
    }

    public boolean isNorthOf(GeoPoint pt) {
        return getLatitude() > pt.getLatitude();
    }

    /**
     * For large distances center point calculation has rounding errors
     */
    public static GeoPoint calculateCenter(List<GeoPoint> geoPoints) {
        checkNotNull(geoPoints, "geoPoints cannot be null");
        checkArgument(geoPoints.size() > 0, "must have at least 1 geoPoints");
        if (geoPoints.size() == 1) {
            return geoPoints.get(0);
        }

        double x = 0.0;
        double y = 0.0;
        double z = 0.0;
        double totalAlt = 0.0;
        int altitudeCount = 0;
        for (GeoPoint geoPoint : geoPoints) {
            double latRad = Math.toRadians(geoPoint.getLatitude());
            double lonRad = Math.toRadians(geoPoint.getLongitude());
            x += Math.cos(latRad) * Math.cos(lonRad);
            y += Math.cos(latRad) * Math.sin(lonRad);
            z += Math.sin(latRad);

            if (geoPoint.getAltitude() != null) {
                totalAlt += geoPoint.getAltitude();
                altitudeCount++;
            }
        }

        x = x / (double) geoPoints.size();
        y = y / (double) geoPoints.size();
        z = z / (double) geoPoints.size();

        return new GeoPoint(
                Math.toDegrees(Math.atan2(z, Math.sqrt(x * x + y * y))),
                Math.toDegrees(Math.atan2(y, x)),
                altitudeCount == geoPoints.size() ? (totalAlt / (double) altitudeCount) : null
        );
    }
}
