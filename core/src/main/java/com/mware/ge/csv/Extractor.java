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
/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.mware.ge.csv;


/**
 * Extracts a value from a part of a {@code char[]} into any type of value, f.ex. a {@link Extractors#string()},
 * {@link Extractors#long_() long} or {@link Extractors#intArray()}.
 * <p>
 * An {@link Extractor} is mutable for the single purpose of ability to reuse its value instance. Consider extracting
 * a primitive int -
 * <p>
 * Sub-interfaces and implementations can and should specify specific accessors for the purpose
 * of performance and less garbage, f.ex. where an IntExtractor could have an accessor method for
 * getting the extracted value as primitive int, to avoid auto-boxing which would arise from calling {@link #value()}.
 *
 * @see Extractors for a collection of very common extractors.
 */
public interface Extractor<T> extends Cloneable {
    /**
     * Extracts value of type {@code T} from the given character data.
     *
     * @param data         characters in a buffer.
     * @param offset       offset into the buffer where the value starts.
     * @param length       number of characters from the offset to extract.
     * @param hadQuotes    whether or not there were skipped characters, f.ex. quotation.
     * @param optionalData optional data to be used for spatial or temporal values or null if csv header did not use it
     * @return {@code true} if a value was extracted, otherwise {@code false}.
     */
    boolean extract(char[] data, int offset, int length, boolean hadQuotes, CSVHeaderInformation optionalData);

    /**
     * Extracts value of type {@code T} from the given character data.
     *
     * @param data      characters in a buffer.
     * @param offset    offset into the buffer where the value starts.
     * @param length    number of characters from the offset to extract.
     * @param hadQuotes whether or not there were skipped characters, f.ex. quotation.
     * @return {@code true} if a value was extracted, otherwise {@code false}.
     */
    boolean extract(char[] data, int offset, int length, boolean hadQuotes);

    /**
     * @return the most recently extracted value.
     */
    T value();

    /**
     * @return string representation of what type of value of produces. Also used as key in {@link Extractors}.
     */
    String name();

    Extractor<T> clone();
}
