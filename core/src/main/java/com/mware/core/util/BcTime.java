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

public class BcTime {
    private final String hours;
    private final String minutes;
    private final String seconds;
    private final String milliseconds;

    public BcTime(Integer hours, Integer minutes, Integer seconds, Integer milliseconds) {
        this(
                hours == null ? null : hours.toString(),
                minutes == null ? null : minutes.toString(),
                seconds == null ? null : seconds.toString(),
                milliseconds == null ? null : milliseconds.toString()
        );
    }

    public BcTime(String hours, String minutes, String seconds, String milliseconds) {
        this.hours = cleanHoursString(hours);
        this.minutes = cleanMinutesString(minutes);
        this.seconds = cleanSecondsString(seconds);
        this.milliseconds = cleanMillisecondsString(milliseconds);
    }

    private static String cleanMillisecondsString(String milliseconds) {
        milliseconds = milliseconds == null ? "???" : milliseconds;
        if (milliseconds.length() == 1) {
            if (milliseconds.charAt(0) == '?') {
                milliseconds = "?" + milliseconds;
            } else {
                milliseconds = "0" + milliseconds;
            }
        }
        if (milliseconds.length() == 2) {
            if (milliseconds.charAt(0) == '?') {
                milliseconds = "?" + milliseconds;
            } else {
                milliseconds = "0" + milliseconds;
            }
        }
        return milliseconds;
    }

    private static String cleanHoursString(String hours) {
        hours = hours == null ? "??" : hours;
        if (hours.length() == 1) {
            if (hours.charAt(0) == '?') {
                hours = "?" + hours;
            } else {
                hours = "0" + hours;
            }
        }
        return hours;
    }

    private static String cleanMinutesString(String minutes) {
        minutes = minutes == null ? "??" : minutes;
        if (minutes.length() == 1) {
            if (minutes.charAt(0) == '?') {
                minutes = "?" + minutes;
            } else {
                minutes = "0" + minutes;
            }
        }
        return minutes;
    }

    private static String cleanSecondsString(String seconds) {
        seconds = seconds == null ? "??" : seconds;
        if (seconds.length() == 1) {
            if (seconds.charAt(0) == '?') {
                seconds = "?" + seconds;
            } else {
                seconds = "0" + seconds;
            }
        }
        return seconds;
    }

    public String getHours() {
        return hours;
    }

    public int getHoursInt() {
        return Integer.parseInt(getHours());
    }

    public String getMinutes() {
        return minutes;
    }

    public int getMinutesInt() {
        return Integer.parseInt(getMinutes());
    }

    public String getSeconds() {
        return seconds;
    }

    public int getSecondsInt() {
        return Integer.parseInt(getSeconds());
    }

    public String getMilliseconds() {
        return milliseconds;
    }

    public int getMillisecondsInt() {
        return Integer.parseInt(getMilliseconds());
    }

    @Override
    public String toString() {
        return getHours() + ":" + getMinutes() + ":" + getSeconds() + "." + getMilliseconds();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BcTime that = (BcTime) o;

        if (!hours.equals(that.hours)) {
            return false;
        }
        if (!minutes.equals(that.minutes)) {
            return false;
        }
        if (!seconds.equals(that.seconds)) {
            return false;
        }
        return milliseconds.equals(that.milliseconds);

    }

    @Override
    public int hashCode() {
        int result = hours.hashCode();
        result = 31 * result + minutes.hashCode();
        result = 31 * result + seconds.hashCode();
        result = 31 * result + milliseconds.hashCode();
        return result;
    }
}
