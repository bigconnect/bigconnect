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
package com.mware.ge.values.storable;

import com.mware.ge.values.UnsupportedTemporalUnitException;

import static com.mware.ge.values.utils.TemporalUtil.NANOS_PER_SECOND;

/**
 * Defines all valid field accessors for durations
 */
public enum DurationFields {
    YEARS("years") {
        @Override
        public long asTimeStamp(long months, long days, long seconds, long nanos) {
            return months / 12;
        }
    },
    MONTHS("months") {
        @Override
        public long asTimeStamp(long months, long days, long seconds, long nanos) {
            return months;
        }
    },
    MONTHS_OF_YEAR("monthsofyear") {
        @Override
        public long asTimeStamp(long months, long days, long seconds, long nanos) {
            return months % 12;
        }
    },
    MONTHS_OF_QUARTER("monthsofquarter") {
        @Override
        public long asTimeStamp(long months, long days, long seconds, long nanos) {
            return months % 3;
        }
    },
    QUARTERS("quarters") {
        @Override
        public long asTimeStamp(long months, long days, long seconds, long nanos) {
            return months / 3;
        }
    },
    QUARTERS_OF_YEAR("quartersofyear") {
        @Override
        public long asTimeStamp(long months, long days, long seconds, long nanos) {
            return (months / 3) % 4;
        }
    },
    WEEKS("weeks") {
        @Override
        public long asTimeStamp(long months, long days, long seconds, long nanos) {
            return days / 7;
        }
    },
    DAYS_OF_WEEK("daysofweek") {
        @Override
        public long asTimeStamp(long months, long days, long seconds, long nanos) {
            return days % 7;
        }
    },
    DAYS("days") {
        @Override
        public long asTimeStamp(long months, long days, long seconds, long nanos) {
            return days;
        }
    },
    HOURS("hours") {
        @Override
        public long asTimeStamp(long months, long days, long seconds, long nanos) {
            return seconds / 3600;
        }
    },
    MINUTES_OF_HOUR("minutesofhour") {
        @Override
        public long asTimeStamp(long months, long days, long seconds, long nanos) {
            return (seconds / 60) % 60;
        }
    },
    MINUTES("minutes") {
        @Override
        public long asTimeStamp(long months, long days, long seconds, long nanos) {
            return seconds / 60;
        }
    },
    SECONDS_OF_MINUTE("secondsofminute") {
        @Override
        public long asTimeStamp(long months, long days, long seconds, long nanos) {
            return seconds % 60;
        }
    },
    SECONDS("seconds") {
        @Override
        public long asTimeStamp(long months, long days, long seconds, long nanos) {
            return seconds;
        }
    },
    MILLISECONDS_OF_SECOND("millisecondsofsecond") {
        @Override
        public long asTimeStamp(long months, long days, long seconds, long nanos) {
            return nanos / 1000_000;
        }
    },
    MILLISECONDS("milliseconds") {
        @Override
        public long asTimeStamp(long months, long days, long seconds, long nanos) {
            return seconds * 1000 + nanos / 1000_000;
        }
    },
    MICROSECONDS_OF_SECOND("microsecondsofsecond") {
        @Override
        public long asTimeStamp(long months, long days, long seconds, long nanos) {
            return nanos / 1000;
        }
    },
    MICROSECONDS("microseconds") {
        @Override
        public long asTimeStamp(long months, long days, long seconds, long nanos) {
            return seconds * 1000_000 + nanos / 1000;
        }
    },
    NANOSECONDS_OF_SECOND("nanosecondsofsecond") {
        @Override
        public long asTimeStamp(long months, long days, long seconds, long nanos) {
            return nanos;
        }
    },
    NANOSECONDS("nanoseconds") {
        @Override
        public long asTimeStamp(long months, long days, long seconds, long nanos) {
            return seconds * NANOS_PER_SECOND + nanos;
        }
    };

    public String propertyKey;

    DurationFields(String propertyKey) {
        this.propertyKey = propertyKey;
    }

    public abstract long asTimeStamp(long months, long days, long seconds, long nanos);

    public static DurationFields fromName(String fieldName) {
        switch (fieldName.toLowerCase()) {
            case "years":
                return YEARS;
            case "months":
                return MONTHS;
            case "monthsofyear":
                return MONTHS_OF_YEAR;
            case "monthsofquarter":
                return MONTHS_OF_QUARTER;
            case "quarters":
                return QUARTERS;
            case "quartersofyear":
                return QUARTERS_OF_YEAR;
            case "weeks":
                return WEEKS;
            case "daysofweek":
                return DAYS_OF_WEEK;
            case "days":
                return DAYS;
            case "hours":
                return HOURS;
            case "minutesofhour":
                return MINUTES_OF_HOUR;
            case "minutes":
                return MINUTES;
            case "secondsofminute":
                return SECONDS_OF_MINUTE;
            case "seconds":
                return SECONDS;
            case "millisecondsofsecond":
                return MILLISECONDS_OF_SECOND;
            case "milliseconds":
                return MILLISECONDS;
            case "microsecondsofsecond":
                return MICROSECONDS_OF_SECOND;
            case "microseconds":
                return MICROSECONDS;
            case "nanosecondsofsecond":
                return NANOSECONDS_OF_SECOND;
            case "nanoseconds":
                return NANOSECONDS;
            default:
                throw new UnsupportedTemporalUnitException("No such field: " + fieldName);
        }
    }
}
