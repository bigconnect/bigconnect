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

import com.mware.ge.csv.CSVHeaderInformation;
import com.mware.ge.values.*;
import com.mware.ge.values.utils.TemporalUtil;
import com.mware.ge.values.virtual.MapValue;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalUnit;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.mware.ge.values.storable.DateTimeValue.parseZoneName;
import static com.mware.ge.values.storable.LocalTimeValue.optInt;
import static com.mware.ge.values.storable.LocalTimeValue.parseTime;
import static com.mware.ge.values.storable.Values.NO_VALUE;
import static java.lang.Integer.parseInt;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;

public final class TimeValue extends TemporalValue<OffsetTime, TimeValue> {
    public static final TimeValue MIN_VALUE = new TimeValue(OffsetTime.MIN);
    public static final TimeValue MAX_VALUE = new TimeValue(OffsetTime.MAX);

    public static TimeValue time(OffsetTime time) {
        return new TimeValue(requireNonNull(time, "OffsetTime"));
    }

    public static TimeValue time(int hour, int minute, int second, int nanosOfSecond, String offset) {
        return time(hour, minute, second, nanosOfSecond, parseOffset(offset));
    }

    public static TimeValue time(int hour, int minute, int second, int nanosOfSecond, ZoneOffset offset) {
        return new TimeValue(
                OffsetTime.of(assertValidArgument(() -> LocalTime.of(hour, minute, second, nanosOfSecond)), offset));
    }

    public static TimeValue time(long nanosOfDayUTC, ZoneOffset offset) {
        return new TimeValue(timeRaw(nanosOfDayUTC, offset));
    }

    public static OffsetTime timeRaw(long nanosOfDayUTC, ZoneOffset offset) {
        return OffsetTime.ofInstant(assertValidArgument(() -> Instant.ofEpochSecond(0, nanosOfDayUTC)), offset);
    }

    @Override
    public String getTypeName() {
        return "Time";
    }

    public static TimeValue parse(CharSequence text, Supplier<ZoneId> defaultZone, CSVHeaderInformation fieldsFromHeader) {
        if (fieldsFromHeader != null) {
            if (!(fieldsFromHeader instanceof TimeCSVHeaderInformation)) {
                throw new IllegalStateException("Wrong header information type: " + fieldsFromHeader);
            }
            // Override defaultZone
            defaultZone = ((TimeCSVHeaderInformation) fieldsFromHeader).zoneSupplier(defaultZone);
        }
        return parse(TimeValue.class, PATTERN, TimeValue::parse, text, defaultZone);
    }

    public static TimeValue parse(CharSequence text, Supplier<ZoneId> defaultZone) {
        return parse(TimeValue.class, PATTERN, TimeValue::parse, text, defaultZone);
    }

    public static TimeValue parse(TextValue text, Supplier<ZoneId> defaultZone) {
        return parse(TimeValue.class, PATTERN, TimeValue::parse, text, defaultZone);
    }

    public static TimeValue now(Clock clock) {
        return new TimeValue(OffsetTime.now(clock));
    }

    public static TimeValue now(Clock clock, String timezone) {
        return now(clock.withZone(parseZoneName(timezone)));
    }

    public static TimeValue now(Clock clock, Supplier<ZoneId> defaultZone) {
        return now(clock.withZone(defaultZone.get()));
    }

    public static TimeValue build(MapValue map, Supplier<ZoneId> defaultZone) {
        return StructureBuilder.build(builder(defaultZone), map);
    }

    public static TimeValue select(AnyValue from, Supplier<ZoneId> defaultZone) {
        return builder(defaultZone).selectTime(from);
    }

    @Override
    boolean hasTime() {
        return true;
    }

    public static TimeValue truncate(
            TemporalUnit unit,
            TemporalValue input,
            MapValue fields,
            Supplier<ZoneId> defaultZone) {
        OffsetTime time = input.getTimePart(defaultZone);
        OffsetTime truncatedOT = assertValidUnit(() -> time.truncatedTo(unit));
        if (fields.size() == 0) {
            return time(truncatedOT);
        } else {
            // Timezone needs some special handling, since the builder will shift keeping the instant instead of the local time
            AnyValue timezone = fields.get("timezone");
            if (timezone != NO_VALUE) {
                ZonedDateTime currentDT =
                        assertValidArgument(() -> ZonedDateTime.ofInstant(Instant.now(), timezoneOf(timezone)));
                ZoneOffset currentOffset = currentDT.getOffset();
                truncatedOT = truncatedOT.withOffsetSameLocal(currentOffset);
            }

            return updateFieldMapWithConflictingSubseconds(fields, unit, truncatedOT,
                    (mapValue, offsetTime) -> {
                        if (mapValue.size() == 0) {
                            return time(offsetTime);
                        } else {
                            return build(mapValue.updatedWith("time", time(offsetTime)), defaultZone);

                        }
                    });

        }
    }

    static OffsetTime defaultTime(ZoneId zoneId) {
        return OffsetTime.of(TemporalFields.hour.defaultValue, TemporalFields.minute.defaultValue,
                TemporalFields.second.defaultValue, TemporalFields.nanosecond.defaultValue,
                assertValidZone(() -> ZoneOffset.of(zoneId.toString())));
    }

    static TimeBuilder<TimeValue> builder(Supplier<ZoneId> defaultZone) {
        return new TimeBuilder<TimeValue>(defaultZone) {
            @Override
            protected boolean supportsTimeZone() {
                return true;
            }

            @Override
            public TimeValue buildInternal() {
                boolean selectingTime = fields.containsKey(TemporalFields.time);
                boolean selectingTimeZone;
                OffsetTime result;
                if (selectingTime) {
                    AnyValue time = fields.get(TemporalFields.time);
                    if (!(time instanceof TemporalValue)) {
                        throw new InvalidValuesArgumentException(String.format("Cannot construct time from: %s", time));
                    }
                    TemporalValue t = (TemporalValue) time;
                    result = t.getTimePart(defaultZone);
                    selectingTimeZone = t.supportsTimeZone();
                } else {
                    ZoneId timezone = timezone();
                    if (!(timezone instanceof ZoneOffset)) {
                        timezone = assertValidArgument(() -> ZonedDateTime.ofInstant(Instant.now(), timezone())).getOffset();
                    }

                    result = defaultTime(timezone);
                    selectingTimeZone = false;
                }

                result = assignAllFields(result);
                if (timezone != null) {
                    ZoneOffset currentOffset = assertValidArgument(() -> ZonedDateTime.ofInstant(Instant.now(), timezone())).getOffset();
                    if (selectingTime && selectingTimeZone) {
                        result = result.withOffsetSameInstant(currentOffset);
                    } else {
                        result = result.withOffsetSameLocal(currentOffset);
                    }
                }
                return time(result);
            }

            @Override
            protected TimeValue selectTime(
                    AnyValue temporal) {
                if (!(temporal instanceof TemporalValue)) {
                    throw new InvalidValuesArgumentException(String.format("Cannot construct time from: %s", temporal));
                }
                if (temporal instanceof TimeValue &&
                        timezone == null) {
                    return (TimeValue) temporal;
                }

                TemporalValue v = (TemporalValue) temporal;
                OffsetTime time = v.getTimePart(defaultZone);
                if (timezone != null) {
                    ZoneOffset currentOffset = assertValidArgument(() -> ZonedDateTime.ofInstant(Instant.now(), timezone())).getOffset();
                    time = time.withOffsetSameInstant(currentOffset);
                }
                return time(time);
            }
        };
    }

    private final OffsetTime value;
    private final long nanosOfDayUTC;

    private TimeValue(OffsetTime value) {
        this.value = value;
        this.nanosOfDayUTC = TemporalUtil.getNanosOfDayUTC(this.value);
    }

    @Override
    int unsafeCompareTo(Value otherValue) {
        TimeValue other = (TimeValue) otherValue;
        int compare = Long.compare(nanosOfDayUTC, other.nanosOfDayUTC);
        if (compare == 0) {
            compare = Integer.compare(value.getOffset().getTotalSeconds(), other.value.getOffset().getTotalSeconds());
        }
        return compare;
    }

    @Override
    OffsetTime temporal() {
        return value;
    }

    @Override
    LocalDate getDatePart() {
        throw new UnsupportedTemporalUnitException(String.format("Cannot get the date of: %s", this));
    }

    @Override
    LocalTime getLocalTimePart() {
        return value.toLocalTime();
    }

    @Override
    OffsetTime getTimePart(Supplier<ZoneId> defaultZone) {
        return value;
    }

    @Override
    ZoneId getZoneId(Supplier<ZoneId> defaultZone) {
        return value.getOffset();
    }

    @Override
    ZoneId getZoneId() {
        throw new UnsupportedTemporalTypeException("Cannot get the timezone of" + this);
    }

    @Override
    ZoneOffset getZoneOffset() {
        return value.getOffset();
    }

    @Override
    public boolean supportsTimeZone() {
        return true;
    }

    @Override
    public boolean equals(Value other) {
        return other instanceof TimeValue && value.equals(((TimeValue) other).value);
    }

    @Override
    public <E extends Exception> void writeTo(ValueWriter<E> writer) throws E {
        writer.writeTime(value);
    }

    @Override
    public String prettyPrint() {
        return assertPrintable(() -> value.format(DateTimeFormatter.ISO_TIME));
    }

    @Override
    public ValueGroup valueGroup() {
        return ValueGroup.ZONED_TIME;
    }

    @Override
    protected int computeHash() {
        return Long.hashCode(value.toLocalTime().toNanoOfDay() - value.getOffset().getTotalSeconds() * 1000_000_000L);
    }

    @Override
    public <T> T map(ValueMapper<T> mapper) {
        return mapper.mapTime(this);
    }

    @Override
    public TimeValue add(DurationValue duration) {
        return replacement(assertValidArithmetic(() -> value.plusNanos(duration.nanosOfDay())));
    }

    @Override
    public TimeValue sub(DurationValue duration) {
        return replacement(assertValidArithmetic(() -> value.minusNanos(duration.nanosOfDay())));
    }

    @Override
    TimeValue replacement(OffsetTime time) {
        return time == value ? this : new TimeValue(time);
    }

    private static final String OFFSET_PATTERN = "(?<zone>Z|[+-](?<zoneHour>[0-9]{2})(?::?(?<zoneMinute>[0-9]{2}))?)";
    static final String TIME_PATTERN = LocalTimeValue.TIME_PATTERN + "(?:" + OFFSET_PATTERN + ")?";
    private static final Pattern PATTERN = Pattern.compile("(?:T)?" + TIME_PATTERN);
    static final Pattern OFFSET = Pattern.compile(OFFSET_PATTERN);

    static ZoneOffset parseOffset(String offset) {
        Matcher matcher = OFFSET.matcher(offset);
        if (matcher.matches()) {
            return parseOffset(matcher);
        }
        throw new InvalidValuesArgumentException("Not a valid offset: " + offset);
    }

    static ZoneOffset parseOffset(Matcher matcher) {
        String zone = matcher.group("zone");
        if (null == zone) {
            return null;
        }
        if ("Z".equalsIgnoreCase(zone)) {
            return UTC;
        }
        int factor = zone.charAt(0) == '+' ? 1 : -1;
        int hours = parseInt(matcher.group("zoneHour"));
        int minutes = optInt(matcher.group("zoneMinute"));
        return assertValidZone(() -> ZoneOffset.ofHoursMinutes(factor * hours, factor * minutes));
    }

    private static TimeValue parse(Matcher matcher, Supplier<ZoneId> defaultZone) {
        return new TimeValue(OffsetTime.of(parseTime(matcher), parseOffset(matcher, defaultZone)));
    }

    private static ZoneOffset parseOffset(Matcher matcher, Supplier<ZoneId> defaultZone) {
        ZoneOffset offset = parseOffset(matcher);
        if (offset == null) {
            ZoneId zoneId = defaultZone.get();
            offset = zoneId instanceof ZoneOffset ? (ZoneOffset) zoneId : zoneId.getRules().getOffset(Instant.now());
        }
        return offset;
    }

    abstract static class TimeBuilder<Result> extends Builder<Result> {
        TimeBuilder(Supplier<ZoneId> defaultZone) {
            super(defaultZone);
        }

        @Override
        protected final boolean supportsDate() {
            return false;
        }

        @Override
        protected final boolean supportsTime() {
            return true;
        }

        @Override
        protected boolean supportsEpoch() {
            return false;
        }

        protected abstract Result selectTime(AnyValue time);
    }
}
