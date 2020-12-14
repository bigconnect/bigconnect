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
package com.mware.core.util;

import com.mware.core.exception.BcException;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Duration {
    private static final Pattern BASIC_PATTERN = Pattern.compile("([-0-9\\.]+)[\\s*](.*)");
    private double time;
    private TimeUnit timeUnit;

    public Duration(double time, TimeUnit timeUnit) {
        this.time = time;
        this.timeUnit = timeUnit;
    }

    public Date addTo(Date date) {
        return new Date((long) (date.getTime() + getTimeInMillis()));
    }

    public double getTimeInMillis() {
        switch (timeUnit) {
            case NANOSECONDS:
                return time / 1000.0 / 1000.0;
            case MICROSECONDS:
                return time / 1000.0;
            case MILLISECONDS:
                return time;
            case SECONDS:
                return time * 1000.0;
            case MINUTES:
                return time * 1000.0 * 60.0;
            case HOURS:
                return time * 1000.0 * 60.0 * 60.0;
            case DAYS:
                return time * 1000.0 * 60.0 * 60.0 * 24.0;
        }
        throw new BcException("Unhandled time unit: " + timeUnit);
    }

    @Override
    public String toString() {
        return "Duration{" + time + " " + timeUnit + "}";
    }

    public static Duration parse(String str) {
        str = str.trim();
        if (str.equals("0")) {
            return new Duration(0, TimeUnit.MILLISECONDS);
        }

        Matcher m = BASIC_PATTERN.matcher(str);
        if (!m.matches()) {
            throw new BcException("Could not parse duration string: " + str);
        }
        try {
            double time = Double.parseDouble(m.group(1));
            TimeUnit timeUnit = parseTimeUnit(m.group(2));
            return new Duration(time, timeUnit);
        } catch (Exception ex) {
            throw new BcException("Could not parse duration string: " + str, ex);
        }
    }

    private static TimeUnit parseTimeUnit(String timeUnitString) {
        if (!timeUnitString.endsWith("s")) {
            timeUnitString = timeUnitString + "s";
        }
        timeUnitString = timeUnitString.toUpperCase();
        return TimeUnit.valueOf(timeUnitString);
    }
}
