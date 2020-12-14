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

import java.time.Year;
import java.time.temporal.*;
import java.util.Locale;

import static java.time.temporal.ChronoUnit.*;

enum Neo4JTemporalField implements TemporalField {
    YEAR_OF_DECADE("Year of decade", YEARS, DECADES, 10),
    YEAR_OF_CENTURY("Year of century", YEARS, CENTURIES, 100),
    YEAR_OF_MILLENNIUM("Millennium", YEARS, MILLENNIA, 1000);

    private final String name;
    private final TemporalUnit baseUnit;
    private final TemporalUnit rangeUnit;
    private final int years;
    private final ValueRange range;

    Neo4JTemporalField(String name, TemporalUnit baseUnit, TemporalUnit rangeUnit, int years) {
        this.name = name;
        this.baseUnit = baseUnit;
        this.rangeUnit = rangeUnit;
        this.years = years;
        this.range = ValueRange.of(Year.MIN_VALUE / years, Year.MAX_VALUE / years);
    }

    @Override
    public String getDisplayName(Locale locale) {
        return name;
    }

    @Override
    public TemporalUnit getBaseUnit() {
        return baseUnit;
    }

    @Override
    public TemporalUnit getRangeUnit() {
        return rangeUnit;
    }

    @Override
    public ValueRange range() {
        return range;
    }

    @Override
    public boolean isDateBased() {
        return true;
    }

    @Override
    public boolean isTimeBased() {
        return false;
    }

    @Override
    public boolean isSupportedBy(TemporalAccessor temporal) {
        return false;
    }

    @Override
    public ValueRange rangeRefinedBy(TemporalAccessor temporal) {
        // Always identical
        return range();
    }

    @Override
    public long getFrom(TemporalAccessor temporal) {
        throw new UnsupportedOperationException("Getting a " + this.name + " from temporal values is not supported.");
    }

    @SuppressWarnings("unchecked")
    @Override
    public <R extends Temporal> R adjustInto(R temporal, long newValue) {
        int newVal = range.checkValidIntValue(newValue, this);
        int oldYear = temporal.get(ChronoField.YEAR);
        return (R) temporal.with(ChronoField.YEAR, (oldYear / years) * years + newVal)
                .with(TemporalAdjusters.firstDayOfYear());
    }

    @Override
    public String toString() {
        return name;
    }

}
