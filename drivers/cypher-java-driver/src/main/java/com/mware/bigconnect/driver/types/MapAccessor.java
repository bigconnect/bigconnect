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
package com.mware.bigconnect.driver.types;

import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.Values;
import com.mware.bigconnect.driver.exceptions.ClientException;
import com.mware.bigconnect.driver.internal.value.NullValue;

import java.util.Map;
import java.util.function.Function;

/**
 * Access the keys, properties and values of an underlying unordered map by key
 *
 * This provides only read methods. Subclasses may chose to provide additional methods
 * for changing the underlying map.
 * @since 1.0
 */
public interface MapAccessor
{
    /**
     * Retrieve the keys of the underlying map
     *
     * @return all map keys in unspecified order
     */
    Iterable<String> keys();

    /**
     * Check if the list of keys contains the given key
     *
     * @param key the key
     * @return {@code true} if this map keys contains the given key otherwise {@code false}
     */
    boolean containsKey(String key);

    /**
     * Retrieve the value of the property with the given key
     *
     * @param key the key of the property
     * @return the property's value or a {@link NullValue} if no such key exists
     * @throws ClientException if record has not been initialized
     */
    Value get(String key);

    /**
     * Retrieve the number of entries in this map
     *
     * @return the number of entries in this map
     */
    int size();

    /**
     * Retrieve all values of the underlying collection
     *
     * @return all values in unspecified order
     */
    Iterable<Value> values();

    /**
     * Map and retrieve all values of the underlying collection
     *
     * @param mapFunction a function to map from Value to T. See {@link Values} for some predefined functions, such
     * as {@link Values#ofBoolean()}, {@link Values#ofList(Function)}.
     * @param <T> the target type of mapping
     * @return the result of mapping all values in unspecified order
     */
    <T> Iterable<T> values(Function<Value, T> mapFunction);

    /**
     * Return the underlying map as a map of string keys and values converted using
     * {@link Value#asObject()}.
     *
     * This is equivalent to calling {@link #asMap(Function)} with {@link Values#ofObject()}.
     *
     * @return the value as a Java map
     */
    Map<String, Object> asMap();

    /**
     * @param mapFunction a function to map from Value to T. See {@link Values} for some predefined functions, such
     * as {@link Values#ofBoolean()}, {@link Values#ofList(Function)}.
     * @param <T> the type of map values
     * @see Values for a long list of built-in conversion functions
     * @return the value as a map from string keys to values of type T obtained from mapping he original map values, if possible
     */
    <T> Map<String, T> asMap(Function<Value, T> mapFunction);
}
