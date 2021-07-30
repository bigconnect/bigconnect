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

import com.mware.ge.GeException;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import org.apache.commons.lang3.ArrayUtils;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.operation.valid.IsValidOp;
import org.locationtech.jts.operation.valid.TopologyValidationError;

import java.util.ArrayList;
import java.util.List;

public class GeoUtils {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(GeoUtils.class);
    public static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    public static double EARTH_RADIUS = 6371; // km
    public static double EARTH_CIRCUMFERENCE = 2 * Math.PI * EARTH_RADIUS;
    public static final double MIN_LAT = Math.toRadians(-90d); // -PI/2
    public static final double MAX_LAT = Math.toRadians(90d); // PI/2
    public static final double MIN_LON = Math.toRadians(-180d); // -PI
    public static final double MAX_LON = Math.toRadians(180d); // PI

    public static boolean intersects(GeoShape left, GeoShape right) {
        if (left instanceof GeoCircle) {
            GeoCircle geoCircle = (GeoCircle) left;
            if (right instanceof GeoPoint) {
                return within(right, geoCircle);
            } else if (right instanceof GeoCircle) {
                GeoCircle circle = (GeoCircle) right;
                double centerDistance = distanceBetween(geoCircle.getLatitude(), geoCircle.getLongitude(), circle.getLatitude(), circle.getLongitude());
                return centerDistance < (geoCircle.getRadius() + circle.getRadius());
            }
        } else if (right instanceof GeoCircle) {
            return intersects(right, left);
        }
        return toJtsGeometry(left, false).intersects(toJtsGeometry(right, false));
    }

    public static boolean within(GeoShape left, GeoShape right) {
        if (right instanceof GeoCircle) {
            GeoCircle geoCircle = (GeoCircle) right;
            if (left instanceof GeoPoint) {
                GeoPoint pt = (GeoPoint) left;
                return distanceBetween(geoCircle.getLatitude(), geoCircle.getLongitude(), pt.getLatitude(), pt.getLongitude()) <= geoCircle.getRadius();
            } else if (left instanceof GeoCircle) {
                GeoCircle circle = (GeoCircle) left;
                double distance = distanceBetween(geoCircle.getLatitude(), geoCircle.getLongitude(), circle.getLatitude(), circle.getLongitude());
                return (distance + circle.getRadius()) <= geoCircle.getRadius();
            } else if (left instanceof GeoRect) {
                GeoRect rect = (GeoRect) left;
                return within(rect.getNorthWest(), geoCircle) && within(rect.getSouthEast(), geoCircle);
            } else if (left instanceof GeoHash) {
                return within(((GeoHash) left).toGeoRect(), geoCircle);
            } else if (left instanceof GeoLine) {
                return ((GeoLine) left).getGeoPoints().stream().allMatch(linePoint -> within(linePoint, geoCircle));
            }
            throw new GeException("Not implemented for argument type " + left.getClass().getName());
        } else if (left instanceof GeoCircle) {
            throw new GeException("Not implemented for argument type " + left.getClass().getName());
        }
        return toJtsGeometry(left, false).within(toJtsGeometry(right, false));
    }

    public static GeoShape getEnvelope(GeoShape geoShape) {
        if (geoShape instanceof GeoCircle) {
            // see http://janmatuschek.de/LatitudeLongitudeBoundingCoordinates
            GeoCircle geoCircle = (GeoCircle) geoShape;
            double radDist = geoCircle.getRadius() / EARTH_RADIUS;
            double radLat = Math.toRadians(geoCircle.getLatitude());
            double radLon = Math.toRadians(geoCircle.getLongitude());

            double minLat = radLat - radDist;
            double maxLat = radLat + radDist;

            double minLon, maxLon;
            if (minLat > MIN_LAT && maxLat < MAX_LAT) {
                double deltaLon = Math.asin(Math.sin(radDist) /
                        Math.cos(radLat));
                minLon = radLon - deltaLon;
                if (minLon < MIN_LON) {
                    minLon += 2d * Math.PI;
                }
                maxLon = radLon + deltaLon;
                if (maxLon > MAX_LON) {
                    maxLon -= 2d * Math.PI;
                }
            } else {
                // a pole is within the distance
                minLat = Math.max(minLat, MIN_LAT);
                maxLat = Math.min(maxLat, MAX_LAT);
                minLon = MIN_LON;
                maxLon = MAX_LON;
            }

            return new GeoRect(
                    new GeoPoint(Math.toDegrees(maxLat), Math.toDegrees(minLon)),
                    new GeoPoint(Math.toDegrees(minLat), Math.toDegrees(maxLon))
            );
        }

        return toGeoShape(toJtsGeometry(geoShape, false).getEnvelope());
    }

    public static GeoShape repair(GeoShape geoShape) {
        return toGeoShape(toJtsGeometry(geoShape, true));
    }

    public static GeoShape toGeoShape(Geometry geometry) {
        if (geometry instanceof GeometryCollection) {
            GeoCollection geoCollection = new GeoCollection();
            GeometryCollection geometryCollection = (GeometryCollection) geometry;
            for (int i = 0; i < geometryCollection.getNumGeometries(); i++) {
                geoCollection.addShape(toGeoShape(geometryCollection.getGeometryN(i)));
            }
            return geoCollection;
        }

        if (geometry instanceof Polygon) {
            Polygon polygon = (Polygon) geometry;
            List<GeoPoint> outerRing = toGeoPoints(polygon.getExteriorRing().getCoordinates());
            List<List<GeoPoint>> holes = new ArrayList<>();
            for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
                holes.add(toGeoPoints(polygon.getInteriorRingN(i).getCoordinates()));
            }
            return new GeoPolygon(outerRing, holes);
        }

        if (geometry instanceof Point) {
            Point point = (Point) geometry;
            return new GeoPoint(point.getY(), point.getX());
        }

        if (geometry instanceof LineString) {
            LineString lineString = (LineString) geometry;
            return new GeoLine(toGeoPoints(lineString.getCoordinates()));
        }

        throw new GeInvalidShapeException("Unknown geometry type: " + geometry.toString());
    }

    public static double distanceBetween(double latitude1, double longitude1, double latitude2, double longitude2) {
        latitude1 = Math.toRadians(latitude1);
        longitude1 = Math.toRadians(longitude1);
        latitude2 = Math.toRadians(latitude2);
        longitude2 = Math.toRadians(longitude2);

        double cosLat1 = Math.cos(latitude1);
        double cosLat2 = Math.cos(latitude2);
        double sinLat1 = Math.sin(latitude1);
        double sinLat2 = Math.sin(latitude2);
        double deltaLon = longitude2 - longitude1;
        double cosDeltaLon = Math.cos(deltaLon);
        double sinDeltaLon = Math.sin(deltaLon);

        double a = cosLat2 * sinDeltaLon;
        double b = cosLat1 * sinLat2 - sinLat1 * cosLat2 * cosDeltaLon;
        double c = sinLat1 * sinLat2 + cosLat1 * cosLat2 * cosDeltaLon;

        double rads = Math.atan2(Math.sqrt(a * a + b * b), c);
        double percent = rads / (2 * Math.PI);
        return percent * EARTH_CIRCUMFERENCE;
    }

    private static Geometry toJtsGeometry(GeoShape geoShape, boolean lenient) {
        if (geoShape instanceof GeoCollection) {
            GeoCollection geoCollection = (GeoCollection) geoShape;
            Geometry[] geometries = geoCollection.getGeoShapes().stream()
                    .map(shape -> toJtsGeometry(shape, lenient))
                    .toArray(Geometry[]::new);
            return GEOMETRY_FACTORY.createGeometryCollection(geometries);
        } else if (geoShape instanceof GeoPolygon) {
            GeoPolygon geoPolygon = (GeoPolygon) geoShape;
            return toJtsPolygon(geoPolygon.getOuterBoundary(), geoPolygon.getHoles(), lenient);
        } else if (geoShape instanceof GeoRect) {
            GeoRect geoRect = (GeoRect) geoShape;
            Coordinate[] coordinates = new Coordinate[]{
                    new Coordinate(geoRect.getNorthWest().getLongitude(), geoRect.getNorthWest().getLatitude()),
                    new Coordinate(geoRect.getNorthWest().getLongitude(), geoRect.getSouthEast().getLatitude()),
                    new Coordinate(geoRect.getSouthEast().getLongitude(), geoRect.getSouthEast().getLatitude()),
                    new Coordinate(geoRect.getSouthEast().getLongitude(), geoRect.getNorthWest().getLatitude()),
                    new Coordinate(geoRect.getNorthWest().getLongitude(), geoRect.getNorthWest().getLatitude())
            };
            return GEOMETRY_FACTORY.createPolygon(coordinates);
        } else if (geoShape instanceof GeoPoint) {
            GeoPoint geoPoint = (GeoPoint) geoShape;
            return GEOMETRY_FACTORY.createPoint(new Coordinate(geoPoint.getLongitude(), geoPoint.getLatitude()));
        } else if (geoShape instanceof GeoLine) {
            GeoLine geoLine = (GeoLine) geoShape;
            Coordinate[] coordinates = geoLine.getGeoPoints().stream()
                    .map(point -> new Coordinate(point.getLongitude(), point.getLatitude()))
                    .toArray(Coordinate[]::new);
            return GEOMETRY_FACTORY.createLineString(coordinates);
        } else if (geoShape instanceof GeoHash) {
            return toJtsGeometry(((GeoHash) geoShape).toGeoRect(), lenient);
        }
        throw new GeInvalidShapeException("Unsupported shape type: " + geoShape.getClass());
    }

    private static List<GeoPoint> toGeoPoints(Coordinate[] coordinates) {
        List<GeoPoint> geoPoints = new ArrayList<>(coordinates.length);
        for (Coordinate coordinate : coordinates) {
            geoPoints.add(new GeoPoint(coordinate.y, coordinate.x));
        }
        return geoPoints;
    }

    public static Geometry toJtsPolygon(List<GeoPoint> outerBoundary, List<List<GeoPoint>> holeBoundaries, boolean lenient) {
        LinearRing shell = toJtsLinearRing(outerBoundary, true, lenient);
        LinearRing[] holes = holeBoundaries == null ? new LinearRing[0] : holeBoundaries.stream()
                .map(holeBoundary -> toJtsLinearRing(holeBoundary, false, lenient))
                .toArray(LinearRing[]::new);
        Geometry polygon = GEOMETRY_FACTORY.createPolygon(shell, holes);
        TopologyValidationError validationError = new IsValidOp(polygon).getValidationError();
        if (validationError != null) {
            if (lenient) {
                LOGGER.info("Attempting to repair and normalize an invalid polygon.");
                // NOTE: there seems to be a bug in JTS where normalizing puts the paths backwards. Reverse as a hack for now.
                polygon = polygon.buffer(0).norm().reverse();
            } else {
                throw new GeInvalidShapeException(validationError.toString());
            }
        }
        return polygon;
    }

    public static LinearRing toJtsLinearRing(List<GeoPoint> geoPoints, boolean counterClockwise, boolean lenient) {
        if (geoPoints.size() < 4) {
            throw new GeInvalidShapeException("A polygon must specify at least 4 points for each boundary and hole.");
        }

        Coordinate[] shellCoordinates = geoPoints.stream()
                .map(geoPoint -> new Coordinate(geoPoint.getLongitude(), geoPoint.getLatitude()))
                .toArray(Coordinate[]::new);

        if (!shellCoordinates[0].equals(shellCoordinates[shellCoordinates.length - 1])) {
            if (lenient) {
                LOGGER.info("Closing an unclosed GeoShape by appending the beginning coordinate");
                shellCoordinates = org.apache.commons.lang3.ArrayUtils.add(shellCoordinates, shellCoordinates[0].copy());
            } else {
                throw new GeInvalidShapeException("All polygon boundaries and holes must begin and end at the same point.");
            }
        }
        if (Orientation.isCCW(shellCoordinates) != counterClockwise) {
            if (lenient) {
                LOGGER.info("Reversing the coordinates of a ring that has a backwards orientation");
                ArrayUtils.reverse(shellCoordinates);
            } else {
                throw new GeInvalidShapeException("The outer shell of a polygon must be specified in counter-clockwise " +
                        "orientation and all holes must be specified in the clockwise direction.");
            }
        }
        return GEOMETRY_FACTORY.createLinearRing(shellCoordinates);
    }
}
