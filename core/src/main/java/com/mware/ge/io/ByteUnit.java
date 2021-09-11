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
package com.mware.ge.io;

import com.mware.ge.collection.Pair;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Stream;

import static java.lang.String.format;

/**
 * A ByteUnit is a unit for a quantity of bytes.
 * <p>
 * The unit knows how to convert between other units in its class, so you for instance can turn a number of KiBs into
 * an accurate quantity of bytes. Precision can be lost when converting smaller units into larger units, because of
 * integer division.
 * <p>
 * These units all follow the EIC (International Electrotechnical Commission) standard, which uses a multiplier of
 * 1.024. This system is also known as the binary system, and has been accepted as part of the International System of
 * Quantities. It is therefor the recommended choice when communicating quantities of information, and the only one
 * available in this implementation.
 */
public enum ByteUnit {
    /*
    XXX Future notes: This class can potentially replace some of the functionality in org.neo4j.helpers.Format.
     */

    Byte(0, "B"),
    KibiByte(1, "KiB", "KB", "K", "kB", "kb", "k"),
    MebiByte(2, "MiB", "MB", "M", "mB", "mb", "m"),
    GibiByte(3, "GiB", "GB", "G", "gB", "gb", "g"),
    TebiByte(4, "TiB", "TB"),
    PebiByte(5, "PiB", "PB"),
    ExbiByte(6, "EiB", "EB");

    public static final long ONE_KIBI_BYTE = ByteUnit.KibiByte.toBytes(1);
    public static final long ONE_MEBI_BYTE = ByteUnit.MebiByte.toBytes(1);
    public static final long ONE_GIBI_BYTE = ByteUnit.GibiByte.toBytes(1);

    private static final long EIC_MULTIPLIER = 1024;

    private final long factor;
    private final String shortName;
    private final String[] names;

    ByteUnit(long power, String... names) {
        this.factor = factorFromPower(power);
        this.shortName = names[0];
        this.names = names;
    }

    /**
     * Compute the increment factor from the given power.
     * <p>
     * Giving zero always produces 1. Giving 1 will produce 1000 or 1024, for SI and EIC respectively, and so on.
     */
    private long factorFromPower(long power) {
        if (power == 0) {
            return 1;
        }
        long product = EIC_MULTIPLIER;
        for (int i = 0; i < power - 1; i++) {
            product = product * EIC_MULTIPLIER;
        }
        return product;
    }

    /**
     * Get the short or abbreviated name of this unit, e.g. KiB or MiB.
     *
     * @return The short unit name.
     */
    public String abbreviation() {
        return shortName;
    }

    /**
     * Convert the given value of this unit, to a value in the given unit.
     *
     * @param value  The value to convert from this unit.
     * @param toUnit The unit of the resulting value.
     * @return The value in the given result unit.
     */
    public long convert(long value, ByteUnit toUnit) {
        return toBytes(value) / toUnit.factor;
    }

    public long toBytes(long value) {
        return factor * value;
    }

    public long toKibiBytes(long value) {
        return convert(value, KibiByte);
    }

    public long toMebiBytes(long value) {
        return convert(value, MebiByte);
    }

    public long toGibiBytes(long value) {
        return convert(value, GibiByte);
    }

    public long toTebiBytes(long value) {
        return convert(value, TebiByte);
    }

    public long toPebiBytes(long value) {
        return convert(value, PebiByte);
    }

    public long toExbiBytes(long value) {
        return convert(value, ExbiByte);
    }

    public static long bytes(long bytes) {
        return bytes;
    }

    public static long kibiBytes(long kibibytes) {
        return KibiByte.toBytes(kibibytes);
    }

    public static long mebiBytes(long mebibytes) {
        return MebiByte.toBytes(mebibytes);
    }

    public static long gibiBytes(long gibibytes) {
        return GibiByte.toBytes(gibibytes);
    }

    public static long tebiBytes(long tebibytes) {
        return TebiByte.toBytes(tebibytes);
    }

    public static long pebiBytes(long pebibytes) {
        return PebiByte.toBytes(pebibytes);
    }

    public static long exbiBytes(long exbibytes) {
        return ExbiByte.toBytes(exbibytes);
    }

    public static String bytesToString(long bytes) {
        if (bytes > ONE_GIBI_BYTE) {
            return format(Locale.ROOT, "%.4g%s", bytes / (double) ONE_GIBI_BYTE, GibiByte.shortName);
        } else if (bytes > ONE_MEBI_BYTE) {
            return format(Locale.ROOT, "%.4g%s", bytes / (double) ONE_MEBI_BYTE, MebiByte.shortName);
        } else if (bytes > ONE_KIBI_BYTE) {
            return format(Locale.ROOT, "%.4g%s", bytes / (double) ONE_KIBI_BYTE, KibiByte.shortName);
        } else {
            return bytes + Byte.shortName;
        }
    }

    public static long parse(String text) {
        long result = 0;
        int len = text.length();
        int unitCharacter = 0;
        int digitCharacters = 0;
        Stream<Pair<String, ByteUnit>> unitsStream = listUnits();

        for (int i = 0; i < len; i++) {
            char ch = text.charAt(i);
            int digit = Character.digit(ch, 10);
            if (digit != -1) {
                if (unitCharacter != 0) {
                    throw invalidFormat(text);
                }
                if (result != 0) {
                    result *= 10;
                }
                result += digit;
                digitCharacters++;
            } else if (!Character.isWhitespace(ch)) {
                int idx = unitCharacter;
                unitsStream = unitsStream.filter(p -> p.first().length() > idx && p.first().charAt(idx) == ch);
                unitCharacter++;
            }
        }

        if (digitCharacters == 0) {
            throw invalidFormat(text);
        }

        if (unitCharacter > 0) {
            ByteUnit byteUnit = unitsStream.map(Pair::other).findFirst().orElse(null);
            if (byteUnit == null) {
                throw invalidFormat(text);
            }
            result = byteUnit.toBytes(result);
        }

        return result;
    }

    private static IllegalArgumentException invalidFormat(String text) {
        return new IllegalArgumentException("Invalid number format: '" + text + "'");
    }

    private static Stream<Pair<String, ByteUnit>> listUnits() {
        return Arrays.stream(values()).flatMap(
                b -> Stream.of(b.names).map(n -> Pair.of(n, b)));
    }
}
