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
package com.mware.ge.values.utils;

import java.time.OffsetTime;
import java.time.ZoneOffset;

import static java.time.temporal.ChronoUnit.DAYS;

public final class TemporalUtil {
    public static final long NANOS_PER_SECOND = 1_000_000_000L;
    public static final long AVG_NANOS_PER_MONTH = 2_629_746_000_000_000L;

    public static final long SECONDS_PER_DAY = DAYS.getDuration().getSeconds();
    public static final long AVG_SECONDS_PER_MONTH = 2_629_746;

    /**
     * 30.4375 days = 30 days, 10 hours, 30 minutes
     */
    public static final double AVG_DAYS_PER_MONTH = 365.2425 / 12;

    private TemporalUtil() {
    }

    public static OffsetTime truncateOffsetToMinutes(OffsetTime value) {
        int offsetMinutes = value.getOffset().getTotalSeconds() / 60;
        ZoneOffset truncatedOffset = ZoneOffset.ofTotalSeconds(offsetMinutes * 60);
        return value.withOffsetSameInstant(truncatedOffset);
    }

    public static long nanosOfDayToUTC(long nanosOfDayLocal, int offsetSeconds) {
        return nanosOfDayLocal - offsetSeconds * NANOS_PER_SECOND;
    }

    public static long nanosOfDayToLocal(long nanosOfDayUTC, int offsetSeconds) {
        return nanosOfDayUTC + (long) offsetSeconds * NANOS_PER_SECOND;
    }

    public static long getNanosOfDayUTC(OffsetTime value) {
        long secondsOfDayLocal = value.toLocalTime().toSecondOfDay();
        long secondsOffset = value.getOffset().getTotalSeconds();
        return (secondsOfDayLocal - secondsOffset) * NANOS_PER_SECOND + value.getNano();
    }
}
