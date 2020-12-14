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

import javax.xml.bind.DatatypeConverter;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class BcDateTime implements Comparable<BcDateTime> {
    public static final String DATE_TIME_NO_TIME_ZONE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";
    private static final TimeZone GMT = TimeZone.getTimeZone("GMT");
    private final BcTime time;
    private final BcDate date;
    private final String timeZone;

    public BcDateTime(Integer year, Integer month, Integer date, Integer hour, Integer minutes, Integer seconds, Integer milliseconds, String timeZone) {
        this(
                new BcDate(year, month, date),
                new BcTime(hour, minutes, seconds, milliseconds),
                timeZone
        );
    }

    public BcDateTime(String year, String month, String date, String hour, String minutes, String seconds, String milliseconds, String timeZone) {
        this(new BcDate(year, month, date), new BcTime(hour, minutes, seconds, milliseconds), timeZone);
    }

    public BcDateTime(BcDate date, BcTime time, String timeZone) {
        this.date = date;
        this.time = time;
        this.timeZone = timeZone;
    }

    public Date toDateGMT() {
        return toDate(GMT);
    }

    public Date toDate(TimeZone destTimeZone) {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(timeZone));
        cal.setTimeInMillis(0);
        cal.set(
                getDate().getYearInt(), getDate().getMonthInt() - 1, getDate().getDateInt(),
                getTime().getHoursInt(), getTime().getMinutesInt(), getTime().getSecondsInt()
        );
        cal.set(Calendar.MILLISECOND, getTime().getMillisecondsInt());

        Calendar destCal = Calendar.getInstance(destTimeZone);
        destCal.setTimeInMillis(cal.getTimeInMillis());
        return destCal.getTime();
    }

    public long getEpoch() {
        return toDateGMT().getTime();
    }

    public static BcDateTime create(Object obj) {
        return create(obj, TimeZone.getDefault());
    }

    public static BcDateTime create(Object obj, TimeZone defaultTimeZone) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof String) {
            return parse((String) obj, defaultTimeZone);
        }
        if (obj instanceof Date) {
            return create((Date) obj, defaultTimeZone);
        }
        if (obj instanceof Calendar) {
            return create(((Calendar) obj).getTime(), ((Calendar) obj).getTimeZone());
        }
        throw new BcException("Invalid object type to convert to " + BcDateTime.class.getSimpleName() + ": " + obj.getClass().getName());
    }

    public static BcDateTime create(Date date, String timeZoneString) {
        TimeZone timeZone = TimeZone.getTimeZone(timeZoneString);
        return create(date, timeZone);
    }

    public static BcDateTime create(Date date, TimeZone timeZone) {
        Calendar cal = Calendar.getInstance(timeZone);
        cal.setTime(date);
        return new BcDateTime(
                cal.get(Calendar.YEAR),
                cal.get(Calendar.MONTH) + 1,
                cal.get(Calendar.DATE),
                cal.get(Calendar.HOUR_OF_DAY),
                cal.get(Calendar.MINUTE),
                cal.get(Calendar.SECOND),
                cal.get(Calendar.MILLISECOND),
                timeZone.getID()
        );
    }

    public static BcDateTime parse(String str, TimeZone defaultTimeZone) {
        return create(DatatypeConverter.parseDateTime(str), defaultTimeZone);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BcDateTime that = (BcDateTime) o;
        return this.getEpoch() == that.getEpoch();
    }

    @Override
    public int hashCode() {
        int result = time != null ? time.hashCode() : 0;
        result = 31 * result + (date != null ? date.hashCode() : 0);
        result = 31 * result + (timeZone != null ? timeZone.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return getDate().toString() + "T" + getTime().toString() + timeZoneToISO8601(getTimeZone());
    }

    private String timeZoneToISO8601(String timeZone) {
        if (timeZone.equals("GMT")) {
            return "Z";
        }
        TimeZone tz = TimeZone.getTimeZone(timeZone);
        int rawOffset = tz.getOffset(getEpoch());
        int totalMinutes = rawOffset / 1000 / 60;
        boolean negative = totalMinutes < 0;
        String negPosPrefix = negative ? "-" : "+";
        totalMinutes = Math.abs(totalMinutes);
        int hours = totalMinutes / 60;
        int minutes = totalMinutes % 60;
        return String.format("%s%02d:%02d", negPosPrefix, hours, minutes);
    }

    public BcTime getTime() {
        return time;
    }

    public BcDate getDate() {
        return date;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public Date getJavaDate() {
        return new Date(getEpoch());
    }

    public BcDateTime add(int amount, BcDate.Unit units) {
        switch (units) {
            case DAY:
                Calendar cal = Calendar.getInstance();
                cal.setTime(getJavaDate());
                cal.add(Calendar.DATE, amount);
                return BcDateTime.create(cal.getTime(), getTimeZone());
            default:
                throw new BcException("Unhandled unit: " + units);
        }
    }

    @Override
    public int compareTo(BcDateTime o) {
        return this.getJavaDate().compareTo(o.getJavaDate());
    }

    public static String getHumanTimeAgo(Date now, Date date) {
        long ago = now.getTime() - date.getTime();
        return getHumanTimeAgo(ago);
    }

    public static String getHumanTimeAgo(long agoMillis) {
        String agoUnits = "ms ago";
        if (agoMillis >= 1000) {
            agoMillis = agoMillis / 1000;
            agoUnits = "seconds ago";
            if (agoMillis >= 60) {
                agoMillis = agoMillis / 60;
                agoUnits = "minutes ago";
                if (agoMillis >= 60) {
                    agoMillis = agoMillis / 60;
                    agoUnits = "hours ago";
                    if (agoMillis >= 24) {
                        agoMillis = agoMillis / 24;
                        agoUnits = "days ago";
                    }
                }
            }
        }
        return String.format("%d %s", agoMillis, agoUnits);
    }

    public static String getHumanTimeAgo(Date date) {
        return getHumanTimeAgo(new Date(), date);
    }
}
