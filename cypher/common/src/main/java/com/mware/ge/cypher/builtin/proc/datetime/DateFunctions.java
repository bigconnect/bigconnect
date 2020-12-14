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
import com.mware.ge.cypher.procedure.Name;
import com.mware.ge.cypher.procedure.UserFunction;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class DateFunctions {
    public static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String UTC_ZONE_ID = "UTC";

    @UserFunction(name = "date.parse")
    @Description("date.parse('2012-12-23','ms|s|m|h|d','yyyy-MM-dd') parse date string using the specified format into the specified time unit")
    public Long parse(@Name("time") String time, @Name(value = "unit", defaultValue = "ms") String unit, @Name(value = "format",defaultValue = DEFAULT_FORMAT) String format, final @Name(value = "timezone", defaultValue = "") String timezone) {
        Long value = parseOrThrow(time, getFormat(format, timezone));
        return value == null ? null : unit(unit).convert(value, TimeUnit.MILLISECONDS);
    }

    private static DateFormat getFormat(final String pattern, final String timezone) {
        String actualPattern = getPattern(pattern);
        SimpleDateFormat format = null;
        try {
            format = new SimpleDateFormat(actualPattern);
        } catch(Exception e){
            throw new IllegalArgumentException("The pattern: "+pattern+" is not correct");
        }
        if (timezone != null && !"".equals(timezone)) {
            format.setTimeZone(TimeZone.getTimeZone(timezone));
        } else if (!(containsTimeZonePattern(actualPattern))) {
            format.setTimeZone(TimeZone.getTimeZone(UTC_ZONE_ID));
        }
        return format;
    }

    private static boolean containsTimeZonePattern(final String pattern) {
        return pattern.matches("[XxZzVO]{1,3}");	// doesn't account for strings escaped with "'" (TODO?)
    }

    private static String getPattern(final String pattern) {
        return pattern == null || "".equals(pattern) ? DEFAULT_FORMAT : pattern;
    }

    private static Long parseOrThrow(final String date, final DateFormat format) {
        if (date == null) return null;
        try {
            return format.parse(date).getTime();
        } catch (ParseException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private TimeUnit unit(String unit) {
        if (unit == null) return TimeUnit.MILLISECONDS;

        switch (unit.toLowerCase()) {
            case "ms": case "milli":  case "millis": case "milliseconds": return TimeUnit.MILLISECONDS;
            case "s":  case "second": case "seconds": return TimeUnit.SECONDS;
            case "m":  case "minute": case "minutes": return TimeUnit.MINUTES;
            case "h":  case "hour":   case "hours":   return TimeUnit.HOURS;
            case "d":  case "day":    case "days":    return TimeUnit.DAYS;
//			case "month":case "months": return TimeUnit.MONTHS;
//			case "years":case "year": return TimeUnit.YEARS;
        }

        throw new IllegalArgumentException("The unit: "+ unit + " is not correct");

        //return TimeUnit.MILLISECONDS;
    }

    public static class FieldResult {
        public final Map<String,Object> value = new LinkedHashMap<>();
        public long years, months, days, weekdays, hours, minutes, seconds;
        public String zoneid;

        public Map<String, Object> asMap() {
            return value;
        }
    }

}
