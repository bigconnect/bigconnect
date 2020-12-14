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

import com.mware.ge.collection.Pair;
import com.mware.ge.values.*;
import com.mware.ge.values.virtual.MapValue;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalUnit;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.mware.ge.values.storable.DateTimeValue.parseZoneName;
import static com.mware.ge.values.storable.DateValue.DATE_PATTERN;
import static com.mware.ge.values.storable.DateValue.parseDate;
import static com.mware.ge.values.storable.IntegralValue.safeCastIntegral;
import static com.mware.ge.values.storable.LocalTimeValue.TIME_PATTERN;
import static com.mware.ge.values.storable.LocalTimeValue.parseTime;
import static java.time.Instant.ofEpochSecond;
import static java.time.LocalDateTime.ofInstant;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;

public final class LocalDateTimeValue extends TemporalValue<LocalDateTime, LocalDateTimeValue> {
    public static final LocalDateTimeValue MIN_VALUE = new LocalDateTimeValue(LocalDateTime.MIN);
    public static final LocalDateTimeValue MAX_VALUE = new LocalDateTimeValue(LocalDateTime.MAX);

    public static LocalDateTimeValue localDateTime(DateValue date, LocalTimeValue time) {
        return new LocalDateTimeValue(LocalDateTime.of(date.temporal(), time.temporal()));
    }

    public static LocalDateTimeValue localDateTime(
            int year, int month, int day, int hour, int minute, int second, int nanoOfSecond) {
        return new LocalDateTimeValue(assertValidArgument(() -> LocalDateTime.of(year, month, day, hour, minute, second, nanoOfSecond)));
    }

    public static LocalDateTimeValue localDateTime(LocalDateTime value) {
        return new LocalDateTimeValue(requireNonNull(value, "LocalDateTime"));
    }

    public static LocalDateTimeValue localDateTime(long epochSecond, long nano) {
        return new LocalDateTimeValue(localDateTimeRaw(epochSecond, nano));
    }

    public static LocalDateTime localDateTimeRaw(long epochSecond, long nano) {
        return assertValidArgument(() -> ofInstant(ofEpochSecond(epochSecond, nano), UTC));
    }

    public static LocalDateTimeValue parse(CharSequence text) {
        return parse(LocalDateTimeValue.class, PATTERN, LocalDateTimeValue::parse, text);
    }

    public static LocalDateTimeValue parse(TextValue text) {
        return parse(LocalDateTimeValue.class, PATTERN, LocalDateTimeValue::parse, text);
    }

    public static LocalDateTimeValue now(Clock clock) {
        return new LocalDateTimeValue(LocalDateTime.now(clock));
    }

    public static LocalDateTimeValue now(Clock clock, String timezone) {
        return now(clock.withZone(parseZoneName(timezone)));
    }

    public static LocalDateTimeValue now(Clock clock, Supplier<ZoneId> defaultZone) {
        return now(clock.withZone(defaultZone.get()));
    }

    public static LocalDateTimeValue build(MapValue map, Supplier<ZoneId> defaultZone) {
        return StructureBuilder.build(builder(defaultZone), map);
    }

    public static LocalDateTimeValue select(AnyValue from, Supplier<ZoneId> defaultZone) {
        return builder(defaultZone).selectDateTime(from);
    }

    public static LocalDateTimeValue truncate(
            TemporalUnit unit,
            TemporalValue input,
            MapValue fields,
            Supplier<ZoneId> defaultZone) {
        Pair<LocalDate, LocalTime> pair = getTruncatedDateAndTime(unit, input, "local date time");

        LocalDate truncatedDate = pair.first();
        LocalTime truncatedTime = pair.other();

        LocalDateTime truncatedLDT = LocalDateTime.of(truncatedDate, truncatedTime);

        if (fields.size() == 0) {
            return localDateTime(truncatedLDT);
        } else {
            return updateFieldMapWithConflictingSubseconds(fields, unit, truncatedLDT,
                    (mapValue, localDateTime) -> {
                        if (mapValue.size() == 0) {
                            return localDateTime(localDateTime);
                        } else {
                            return build(mapValue.updatedWith("datetime", localDateTime(localDateTime)),
                                    defaultZone);
                        }
                    });
        }
    }

    static final LocalDateTime DEFAULT_LOCAL_DATE_TIME =
            LocalDateTime.of(TemporalFields.year.defaultValue, TemporalFields.month.defaultValue,
                    TemporalFields.day.defaultValue, TemporalFields.hour.defaultValue,
                    TemporalFields.minute.defaultValue);

    static DateTimeValue.DateTimeBuilder<LocalDateTimeValue> builder(Supplier<ZoneId> defaultZone) {
        return new DateTimeValue.DateTimeBuilder<LocalDateTimeValue>(defaultZone) {
            @Override
            protected boolean supportsTimeZone() {
                return false;
            }

            @Override
            protected boolean supportsEpoch() {
                return false;
            }

            @Override
            public LocalDateTimeValue buildInternal() {
                boolean selectingDate = fields.containsKey(TemporalFields.date);
                boolean selectingTime = fields.containsKey(TemporalFields.time);
                boolean selectingDateTime = fields.containsKey(TemporalFields.datetime);
                LocalDateTime result;
                if (selectingDateTime) {
                    AnyValue dtField = fields.get(TemporalFields.datetime);
                    if (!(dtField instanceof TemporalValue)) {
                        throw new InvalidValuesArgumentException(String.format("Cannot construct local date time from: %s", dtField));
                    }
                    TemporalValue dt = (TemporalValue) dtField;
                    result = LocalDateTime.of(dt.getDatePart(), dt.getLocalTimePart());
                } else if (selectingTime || selectingDate) {
                    LocalTime time;
                    if (selectingTime) {
                        AnyValue timeField = fields.get(TemporalFields.time);
                        if (!(timeField instanceof TemporalValue)) {
                            throw new InvalidValuesArgumentException(String.format("Cannot construct local time from: %s", timeField));
                        }
                        TemporalValue t = (TemporalValue) timeField;
                        time = t.getLocalTimePart();
                    } else {
                        time = LocalTimeValue.DEFAULT_LOCAL_TIME;
                    }
                    LocalDate date;
                    if (selectingDate) {
                        AnyValue dateField = fields.get(TemporalFields.date);
                        if (!(dateField instanceof TemporalValue)) {
                            throw new InvalidValuesArgumentException(String.format("Cannot construct date from: %s", dateField));
                        }
                        TemporalValue t = (TemporalValue) dateField;
                        date = t.getDatePart();
                    } else {
                        date = DateValue.DEFAULT_CALENDER_DATE;
                    }

                    result = LocalDateTime.of(date, time);
                } else {
                    result = DEFAULT_LOCAL_DATE_TIME;
                }

                if (fields.containsKey(TemporalFields.week) && !selectingDate && !selectingDateTime) {
                    // Be sure to be in the start of the week based year (which can be later than 1st Jan)
                    result = result
                            .with(IsoFields.WEEK_BASED_YEAR, safeCastIntegral(TemporalFields.year.name(), fields.get(TemporalFields.year),
                                    TemporalFields.year.defaultValue))
                            .with(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 1)
                            .with(ChronoField.DAY_OF_WEEK, 1);
                }

                result = assignAllFields(result);
                return localDateTime(result);
            }

            private LocalDateTime getLocalDateTimeOf(AnyValue temporal) {
                if (temporal instanceof TemporalValue) {
                    TemporalValue v = (TemporalValue) temporal;
                    LocalDate datePart = v.getDatePart();
                    LocalTime timePart = v.getLocalTimePart();
                    return LocalDateTime.of(datePart, timePart);
                }
                throw new InvalidValuesArgumentException(String.format("Cannot construct date from: %s", temporal));
            }

            @Override
            protected LocalDateTimeValue selectDateTime(AnyValue datetime) {
                if (datetime instanceof LocalDateTimeValue) {
                    return (LocalDateTimeValue) datetime;
                }
                return localDateTime(getLocalDateTimeOf(datetime));
            }
        };
    }

    private final LocalDateTime value;
    private final long epochSecondsInUTC;

    private LocalDateTimeValue(LocalDateTime value) {
        this.value = value;
        this.epochSecondsInUTC = this.value.toEpochSecond(UTC);
    }

    @Override
    int unsafeCompareTo(Value other) {
        LocalDateTimeValue that = (LocalDateTimeValue) other;
        int cmp = Long.compare(epochSecondsInUTC, that.epochSecondsInUTC);
        if (cmp == 0) {
            cmp = value.getNano() - that.value.getNano();
        }
        return cmp;
    }

    @Override
    public String getTypeName() {
        return "LocalDateTime";
    }

    @Override
    LocalDateTime temporal() {
        return value;
    }

    @Override
    LocalDate getDatePart() {
        return value.toLocalDate();
    }

    @Override
    LocalTime getLocalTimePart() {
        return value.toLocalTime();
    }

    @Override
    OffsetTime getTimePart(Supplier<ZoneId> defaultZone) {
        ZoneOffset currentOffset = assertValidArgument(() -> ZonedDateTime.ofInstant(Instant.now(), defaultZone.get())).getOffset();
        return OffsetTime.of(value.toLocalTime(), currentOffset);
    }

    @Override
    ZoneId getZoneId(Supplier<ZoneId> defaultZone) {
        return defaultZone.get();
    }

    @Override
    ZoneId getZoneId() {
        throw new UnsupportedTemporalUnitException(String.format("Cannot get the timezone of: %s", this));
    }

    @Override
    ZoneOffset getZoneOffset() {
        throw new UnsupportedTemporalUnitException(String.format("Cannot get the offset of: %s", this));
    }

    @Override
    public boolean supportsTimeZone() {
        return false;
    }

    @Override
    boolean hasTime() {
        return true;
    }

    @Override
    public boolean equals(Value other) {
        return other instanceof LocalDateTimeValue && value.equals(((LocalDateTimeValue) other).value);
    }

    @Override
    public <E extends Exception> void writeTo(ValueWriter<E> writer) throws E {
        writer.writeLocalDateTime(value);
    }

    @Override
    public String prettyPrint() {
        return assertPrintable(() -> value.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
    }

    @Override
    public ValueGroup valueGroup() {
        return ValueGroup.LOCAL_DATE_TIME;
    }

    @Override
    protected int computeHash() {
        return value.toInstant(UTC).hashCode();
    }

    @Override
    public <T> T map(ValueMapper<T> mapper) {
        return mapper.mapLocalDateTime(this);
    }

    @Override
    public LocalDateTimeValue add(DurationValue duration) {
        return replacement(assertValidArithmetic(() -> value.plus(duration)));
    }

    @Override
    public LocalDateTimeValue sub(DurationValue duration) {
        return replacement(assertValidArithmetic(() -> value.minus(duration)));
    }

    @Override
    LocalDateTimeValue replacement(LocalDateTime dateTime) {
        return dateTime == value ? this : new LocalDateTimeValue(dateTime);
    }

    private static final Pattern PATTERN = Pattern.compile(
            DATE_PATTERN + "(?<time>T" + TIME_PATTERN + ")?",
            Pattern.CASE_INSENSITIVE);

    private static LocalDateTimeValue parse(Matcher matcher) {
        return localDateTime(LocalDateTime.of(parseDate(matcher), optTime(matcher)));
    }

    static LocalTime optTime(Matcher matcher) {
        return matcher.group("time") != null ? parseTime(matcher) : LocalTime.MIN;
    }
}
