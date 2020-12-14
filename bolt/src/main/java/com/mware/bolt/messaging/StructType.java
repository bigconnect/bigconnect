/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.mware.bolt.messaging;

import com.mware.bolt.v1.messaging.BigConnectPackV1;
import com.mware.bolt.v2.messaging.BigConnectPackV2;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

public enum StructType {
    NODE(BigConnectPackV1.NODE, "Node"),
    RELATIONSHIP(BigConnectPackV1.RELATIONSHIP, "Relationship"),
    UNBOUND_RELATIONSHIP(BigConnectPackV1.UNBOUND_RELATIONSHIP, "Relationship"),
    PATH(BigConnectPackV1.PATH, "Path"),
    POINT_2D( BigConnectPackV2.POINT_2D, "Point" ),
    DATE( BigConnectPackV2.DATE, "LocalDate" ),
    TIME( BigConnectPackV2.TIME, "OffsetTime" ),
    LOCAL_TIME( BigConnectPackV2.LOCAL_TIME, "LocalTime" ),
    LOCAL_DATE_TIME( BigConnectPackV2.LOCAL_DATE_TIME, "LocalDateTime" ),
    DATE_TIME_WITH_ZONE_OFFSET( BigConnectPackV2.DATE_TIME_WITH_ZONE_OFFSET, "OffsetDateTime" ),
    DATE_TIME_WITH_ZONE_NAME( BigConnectPackV2.DATE_TIME_WITH_ZONE_NAME, "ZonedDateTime" ),
    DURATION( BigConnectPackV2.DURATION, "Duration" );

    private final byte signature;
    private final String description;

    StructType(byte signature, String description) {
        this.signature = signature;
        this.description = description;
    }

    public byte signature() {
        return signature;
    }

    public String description() {
        return description;
    }

    private static Map<Byte, StructType> knownTypesBySignature = knownTypesBySignature();

    public static StructType valueOf(byte signature) {
        return knownTypesBySignature.get(signature);
    }

    public static StructType valueOf(char signature) {
        return knownTypesBySignature.get((byte) signature);
    }

    private static Map<Byte, StructType> knownTypesBySignature() {
        StructType[] types = StructType.values();
        Map<Byte, StructType> result = new HashMap<>(types.length * 2);
        for (StructType type : types) {
            result.put(type.signature, type);
        }
        return unmodifiableMap(result);
    }
}

