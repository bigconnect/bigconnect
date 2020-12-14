/*
 * Copyright (c) 2013-2020 "BigConnect,"
 * MWARE SOLUTIONS SRL
 *
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package com.mware.bigconnect.driver;

import com.mware.bigconnect.driver.exceptions.ClientException;
import com.mware.bigconnect.driver.exceptions.NoSuchRecordException;
import com.mware.bigconnect.driver.internal.value.NullValue;
import com.mware.bigconnect.driver.types.MapAccessorWithDefaultValue;
import com.mware.bigconnect.driver.util.Immutable;
import com.mware.bigconnect.driver.util.Pair;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Container for Cypher result values.
 * <p>
 * Streams of records are returned from Cypher statement execution, contained
 * within a {@link StatementResult}.
 * <p>
 * A record is a form of ordered map and, as such, contained values can be
 * accessed by either positional {@link #get(int) index} or textual
 * {@link #get(String) key}.
 *
 * @since 1.0
 */
@Immutable
public interface Record extends MapAccessorWithDefaultValue
{
    /**
     * Retrieve the keys of the underlying map
     *
     * @return all field keys in order
     */
    List<String> keys();

    /**
     * Retrieve the values of the underlying map
     *
     * @return all field keys in order
     */
    List<Value> values();

    /**
     * Check if the list of keys contains the given key
     *
     * @param key the key
     * @return {@code true} if this map keys contains the given key otherwise {@code false}
     */
    boolean containsKey(String key);

    /**
     * Retrieve the index of the field with the given key
     *
     * @throws java.util.NoSuchElementException if the given key is not from {@link #keys()}
     * @param key the give key
     * @return the index of the field as used by {@link #get(int)}
     */
    int index(String key);

    /**
     * Retrieve the value of the property with the given key
     *
     * @param key the key of the property
     * @return the property's value or a {@link NullValue} if no such key exists
     * @throws NoSuchRecordException if the associated underlying record is not available
     */
    Value get(String key);

    /**
     * Retrieve the value at the given field index
     *
     * @param index the index of the value
     * @return the value or a {@link com.mware.bigconnect.driver.internal.value.NullValue} if the index is out of bounds
     * @throws ClientException if record has not been initialized
     */
    Value get(int index);

    /**
     * Retrieve the number of fields in this record
     *
     * @return the number of fields in this record
     */
    int size();

    /**
     * Return this record as a map, where each value has been converted to a default
     * java object using {@link Value#asObject()}.
     *
     * This is equivalent to calling {@link #asMap(Function)} with {@link Values#ofObject()}.
     *
     * @return this record as a map
     */
    Map<String, Object> asMap();

    /**
     * Return this record as a map, where each value has been converted using the provided
     * mapping function. You can find a library of common mapping functions in {@link Values}.
     *
     * @see Values for a long list of built-in conversion functions
     * @param mapper the mapping function
     * @param <T> the type to convert to
     * @return this record as a map
     */
    <T> Map<String, T> asMap(Function<Value, T> mapper);

    /**
     * Retrieve all record fields
     *
     * @return all fields in key order
     * @throws NoSuchRecordException if the associated underlying record is not available
     */
    List<Pair<String, Value>> fields();

}
