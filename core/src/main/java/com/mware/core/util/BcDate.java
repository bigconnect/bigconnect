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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class BcDate {
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final TimeZone GMT = TimeZone.getTimeZone("GMT");

    public enum Unit {
        DAY
    }

    private final String year;
    private final String month;
    private final String date;

    public BcDate(String year, String month, String date) {
        this.year = cleanYearString(year);
        this.month = cleanMonthString(month);
        this.date = cleanDateString(date);
    }

    private static String cleanDateString(String date) {
        date = date == null ? "??" : date;
        if (date.length() == 1) {
            if (date.charAt(0) == '?') {
                date = "?" + date;
            } else {
                date = "0" + date;
            }
        }
        return date;
    }

    private static String cleanMonthString(String month) {
        month = month == null ? "??" : month;
        if (month.length() == 1) {
            if (month.charAt(0) == '?') {
                month = "?" + month;
            } else {
                month = "0" + month;
            }
        }
        return month;
    }

    private static String cleanYearString(String year) {
        year = year == null ? "????" : year;
        if (year.length() == 2) {
            year = "20" + year;
        }
        return year;
    }

    public BcDate(Integer year, Integer month, Integer date) {
        this(
                year == null ? null : year.toString(),
                month == null ? null : month.toString(),
                date == null ? null : date.toString()
        );
    }

    public static BcDate create(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof String) {
            return parse((String) obj);
        }
        if (obj instanceof Date) {
            return create((Date) obj);
        }
        throw new BcException("Invalid object type to convert to " + BcDate.class.getSimpleName() + ": " + obj.getClass().getName());
    }

    public static BcDate create(Date date) {
        Calendar cal = Calendar.getInstance(GMT);
        cal.setTime(date);
        return new BcDate(
                cal.get(Calendar.YEAR),
                cal.get(Calendar.MONTH) + 1,
                cal.get(Calendar.DATE)
        );
    }

    private static BcDate parse(String str) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
            sdf.setTimeZone(GMT);
            return create(sdf.parse(str));
        } catch (ParseException e) {
            throw new BcException("Could not parse date: " + str, e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BcDate that = (BcDate) o;

        if (!year.equals(that.year)) {
            return false;
        }
        if (!month.equals(that.month)) {
            return false;
        }
        return date.equals(that.date);

    }

    @Override
    public int hashCode() {
        int result = year.hashCode();
        result = 31 * result + month.hashCode();
        result = 31 * result + date.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return getYear() + "-" + getMonth() + "-" + getDate();
    }

    public Date toDate() {
        Calendar cal = Calendar.getInstance(GMT);
        cal.setTimeInMillis(0);
        cal.set(getYearInt(), getMonthInt() - 1, getDateInt(), 0, 0, 0);
        return cal.getTime();
    }

    public int getDateInt() {
        return Integer.parseInt(getDate());
    }

    public int getMonthInt() {
        return Integer.parseInt(getMonth());
    }

    public int getYearInt() {
        return Integer.parseInt(getYear());
    }

    public String getYear() {
        return year;
    }

    public String getMonth() {
        return month;
    }

    public String getDate() {
        return date;
    }

    public long getEpoch() {
        return toDate().getTime();
    }
}
