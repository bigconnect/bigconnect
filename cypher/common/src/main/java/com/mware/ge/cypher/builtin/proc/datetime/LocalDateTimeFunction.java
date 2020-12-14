/*
 * Copyright (c) 2013-2020 "BigConnect,"
 * MWARE SOLUTIONS SRL
 *
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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
package com.mware.ge.cypher.builtin.proc.datetime;

import com.mware.ge.cypher.procedure.Description;
import com.mware.ge.values.AnyValue;
import com.mware.ge.values.storable.LocalDateTimeValue;
import com.mware.ge.values.storable.TemporalValue;
import com.mware.ge.values.storable.TextValue;
import com.mware.ge.values.virtual.MapValue;

import java.time.Clock;
import java.time.ZoneId;
import java.time.temporal.TemporalUnit;
import java.util.function.Supplier;

import static com.mware.ge.cypher.procedure.impl.Neo4jTypes.NTLocalDateTime;

@Description("Create a LocalDateTime instant.")
class LocalDateTimeFunction extends TemporalFunction<LocalDateTimeValue> {
    LocalDateTimeFunction(Supplier<ZoneId> defaultZone) {
        super(NTLocalDateTime, defaultZone);
    }

    @Override
    protected LocalDateTimeValue now(Clock clock, String timezone, Supplier<ZoneId> defaultZone) {
        return timezone == null ? LocalDateTimeValue.now(clock, defaultZone) : LocalDateTimeValue.now(clock, timezone);
    }

    @Override
    protected LocalDateTimeValue parse(TextValue value, Supplier<ZoneId> defaultZone) {
        return LocalDateTimeValue.parse(value);
    }

    @Override
    protected LocalDateTimeValue build(MapValue map, Supplier<ZoneId> defaultZone) {
        return LocalDateTimeValue.build(map, defaultZone);
    }

    @Override
    protected LocalDateTimeValue select(AnyValue from, Supplier<ZoneId> defaultZone) {
        return LocalDateTimeValue.select(from, defaultZone);
    }

    @Override
    protected LocalDateTimeValue truncate(TemporalUnit unit, TemporalValue input, MapValue fields, Supplier<ZoneId> defaultZone) {
        return LocalDateTimeValue.truncate(unit, input, fields, defaultZone);
    }
}
