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
import com.mware.bigconnect.driver.exceptions.value.LossyCoercion;
import com.mware.bigconnect.driver.exceptions.value.Uncoercible;
import com.mware.bigconnect.driver.internal.value.NullValue;
import com.mware.bigconnect.driver.types.*;
import com.mware.bigconnect.driver.util.Experimental;
import com.mware.bigconnect.driver.util.Immutable;

import java.io.Serializable;
import java.time.*;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A unit of data that adheres to the BigConnect type system.
 *
 * This interface describes a number of <code>isType</code> methods along with
 * <code>typeValue</code> methods. The first set of these correlate with types from
 * the BigConnect Type System and are used to determine which BigConnect type is represented.
 * The second set of methods perform coercions to Java types (wherever possible).
 * For example, a common String value should be tested for using <code>isString</code>
 * and extracted using <code>stringValue</code>.
 *
 * <h2>Navigating a tree structure</h2>
 *
 * Because BigConnect often handles dynamic structures, this interface is designed to help
 * you handle such structures in Java. Specifically, {@link Value} lets you navigate arbitrary tree
 * structures without having to resort to type casting.
 *
 * Given a tree structure like:
 *
 * <pre>
 * {@code
 * {
 *   users : [
 *     { name : "Anders" },
 *     { name : "John" }
 *   ]
 * }
 * }
 * </pre>
 *
 * You can retrieve the name of the second user, John, like so:
 *
 * <pre class="docTest:ValueDocIT#classDocTreeExample">
 * {@code
 * String username = value.get("users").get(1).get("name").asString();
 * }
 * </pre>
 *
 * You can also easily iterate over the users:
 *
 * <pre class="docTest:ValueDocIT#classDocIterationExample">
 * {@code
 * List<String> names = new LinkedList<>();
 * for(Value user : value.get("users").values() )
 * {
 *     names.add(user.get("name").asString());
 * }
 * }
 * </pre>
 * @since 1.0
 */
@Immutable
public interface Value extends MapAccessor, MapAccessorWithDefaultValue, Serializable
{
    /**
     * If the underlying value is a collection type, return the number of values in the collection.
     * <p>
     * For {@link TypeSystem#LIST()}  list} values, this will return the size of the list.
     * <p>
     * For {@link TypeSystem#MAP() map} values, this will return the number of entries in the map.
     * <p>
     * For {@link TypeSystem#NODE() node} and {@link TypeSystem#RELATIONSHIP()}  relationship} values,
     * this will return the number of properties.
     * <p>
     * For {@link TypeSystem#PATH() path} values, this returns the length (number of relationships) in the path.
     *
     * @return the number of values in an underlying collection
     */
    int size();

    /**
     * If this value represents a list or map, test if the collection is empty.
     *
     * @return {@code true} if size() is 0, otherwise {@code false}
     */
    boolean isEmpty();

    /**
     * If the underlying value supports {@link #get(String) key-based indexing}, return an iterable of the keys in the
     * map, this applies to {@link TypeSystem#MAP() map}, {@link #asNode() node} and {@link
     * TypeSystem#RELATIONSHIP()}  relationship} values.
     *
     * @return the keys in the value
     */
    @Override
    Iterable<String> keys();

    /**
     * Retrieve the value at the given index
     *
     * @param index the index of the value
     * @return the value or a {@link com.mware.bigconnect.driver.internal.value.NullValue} if the index is out of bounds
     * @throws ClientException if record has not been initialized
     */
    Value get(int index);

    /** @return The type of this value as defined in the BigConnect type system */
    @Experimental
    Type type();

    /**
     * Test if this value is a value of the given type
     *
     * @param type the given type
     * @return type.isTypeOf( this )
     */
    @Experimental
    boolean hasType(Type type);

    /**
     * @return {@code true} if the value is a Boolean value and has the value True.
     */
    boolean isTrue();

    /**
     * @return {@code true} if the value is a Boolean value and has the value False.
     */
    boolean isFalse();

    /**
     * @return {@code true} if the value is a Null, otherwise {@code false}
     */
    boolean isNull();

    /**
     * This returns a java standard library representation of the underlying value,
     * using a java type that is "sensible" given the underlying type. The mapping
     * for common types is as follows:
     *
     * <ul>
     *     <li>{@link TypeSystem#INTEGER()} - {@link Long}</li>
     *     <li>{@link TypeSystem#FLOAT()} - {@link Double}</li>
     *     <li>{@link TypeSystem#NUMBER()} - {@link Number}</li>
     *     <li>{@link TypeSystem#STRING()} - {@link String}</li>
     *     <li>{@link TypeSystem#BOOLEAN()} - {@link Boolean}</li>
     *     <li>{@link TypeSystem#NULL()} - {@code null}</li>
     *     <li>{@link TypeSystem#NODE()} - {@link Node}</li>
     *     <li>{@link TypeSystem#RELATIONSHIP()} - {@link Relationship}</li>
     *     <li>{@link TypeSystem#PATH()} - {@link Path}</li>
     *     <li>{@link TypeSystem#MAP()} - {@link Map}</li>
     *     <li>{@link TypeSystem#LIST()} - {@link List}</li>
     * </ul>
     *
     * Note that the types in {@link TypeSystem} refers to the BigConnect type system
     * where {@link TypeSystem#INTEGER()} and {@link TypeSystem#FLOAT()} are both
     * 64-bit precision. This is why these types return java {@link Long} and
     * {@link Double}, respectively.
     *
     * @return the value as a Java Object
     */
    Object asObject();

    /**
     * Apply the mapping function on the value if the value is not a {@link NullValue}, or the default value if the value is a {@link NullValue}.
     * @param mapper The mapping function defines how to map a {@link Value} to T.
     * @param defaultValue the value to return if the value is a {@link NullValue}
     * @param <T> The return type
     * @return The value after applying the given mapping function or the default value if the value is {@link NullValue}.
     */
    <T>T computeOrDefault(Function<Value, T> mapper, T defaultValue);

    /**
     * @return the value as a Java boolean, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    boolean asBoolean();

    /**
     * @param defaultValue return this value if the value is a {@link NullValue}.
     * @return the value as a Java boolean, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    boolean asBoolean(boolean defaultValue);

    /**
     *  @return the value as a Java byte array, if possible.
     *  @throws Uncoercible if value types are incompatible.
     */
    byte[] asByteArray();

    /**
     *  @param defaultValue default to this value if the original value is a {@link NullValue}
     *  @return the value as a Java byte array, if possible.
     *  @throws Uncoercible if value types are incompatible.
     */
    byte[] asByteArray(byte[] defaultValue);

    /**
     *  @return the value as a Java String, if possible.
     *  @throws Uncoercible if value types are incompatible.
     */
    String asString();

    /**
     * @param defaultValue return this value if the value is null.
     * @return the value as a Java String, if possible
     * @throws Uncoercible if value types are incompatible.
     */
    String asString(String defaultValue);

    /**
     * @return the value as a Java Number, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    Number asNumber();

    /**
     * Returns a Java long if no precision is lost in the conversion.
     *
     * @return the value as a Java long.
     * @throws LossyCoercion if it is not possible to convert the value without loosing precision.
     * @throws Uncoercible if value types are incompatible.
     */
    long asLong();

    /**
     * Returns a Java long if no precision is lost in the conversion.
     * @param defaultValue return this default value if the value is a {@link NullValue}.
     * @return the value as a Java long.
     * @throws LossyCoercion if it is not possible to convert the value without loosing precision.
     * @throws Uncoercible if value types are incompatible.
     */
    long asLong(long defaultValue);

    /**
     * Returns a Java int if no precision is lost in the conversion.
     *
     * @return the value as a Java int.
     * @throws LossyCoercion if it is not possible to convert the value without loosing precision.
     * @throws Uncoercible if value types are incompatible.
     */
    int asInt();

    /**
     * Returns a Java int if no precision is lost in the conversion.
     * @param defaultValue return this default value if the value is a {@link NullValue}.
     * @return the value as a Java int.
     * @throws LossyCoercion if it is not possible to convert the value without loosing precision.
     * @throws Uncoercible if value types are incompatible.
     */
    int asInt(int defaultValue);

    /**
     * Returns a Java double if no precision is lost in the conversion.
     *
     * @return the value as a Java double.
     * @throws LossyCoercion if it is not possible to convert the value without loosing precision.
     * @throws Uncoercible if value types are incompatible.
     */
    double asDouble();

    /**
     * Returns a Java double if no precision is lost in the conversion.
     * @param defaultValue default to this value if the value is a {@link NullValue}.
     * @return the value as a Java double.
     * @throws LossyCoercion if it is not possible to convert the value without loosing precision.
     * @throws Uncoercible if value types are incompatible.
     */
    double asDouble(double defaultValue);

    /**
     * Returns a Java float if no precision is lost in the conversion.
     *
     * @return the value as a Java float.
     * @throws LossyCoercion if it is not possible to convert the value without loosing precision.
     * @throws Uncoercible if value types are incompatible.
     */
    float asFloat();

    /**
     * Returns a Java float if no precision is lost in the conversion.
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a Java float.
     * @throws LossyCoercion if it is not possible to convert the value without loosing precision.
     * @throws Uncoercible if value types are incompatible.
     */
    float asFloat(float defaultValue);

    /**
     * If the underlying type can be viewed as a list, returns a java list of
     * values, where each value has been converted using {@link #asObject()}.
     *
     * @see #asObject()
     * @return the value as a Java list of values, if possible
     */
    List<Object> asList();


    /**
     * If the underlying type can be viewed as a list, returns a java list of
     * values, where each value has been converted using {@link #asObject()}.
     *
     * @see #asObject()
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a Java list of values, if possible
     */
    List<Object> asList(List<Object> defaultValue);

    /**
     * @param mapFunction a function to map from Value to T. See {@link Values} for some predefined functions, such
     * as {@link Values#ofBoolean()}, {@link Values#ofList(Function)}.
     * @param <T> the type of target list elements
     * @see Values for a long list of built-in conversion functions
     * @return the value as a list of T obtained by mapping from the list elements, if possible
     */
    <T> List<T> asList(Function<Value, T> mapFunction);

    /**
     * @param mapFunction a function to map from Value to T. See {@link Values} for some predefined functions, such
     * as {@link Values#ofBoolean()}, {@link Values#ofList(Function)}.
     * @param <T> the type of target list elements
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @see Values for a long list of built-in conversion functions
     * @return the value as a list of T obtained by mapping from the list elements, if possible
     */
    <T> List<T> asList(Function<Value, T> mapFunction, List<T> defaultValue);

    /**
     * @return the value as a {@link Entity}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    Entity asEntity();

    /**
     * @return the value as a {@link Node}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    Node asNode();

    /**
     * @return the value as a {@link Relationship}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    Relationship asRelationship();

    /**
     * @return the value as a {@link Path}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    Path asPath();

    /**
     * @return the value as a {@link LocalDate}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    LocalDate asLocalDate();

    /**
     * @return the value as a {@link OffsetTime}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    OffsetTime asOffsetTime();

    /**
     * @return the value as a {@link LocalTime}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    LocalTime asLocalTime();

    /**
     * @return the value as a {@link LocalDateTime}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    LocalDateTime asLocalDateTime();

    /**
     * @return the value as a {@link java.time.OffsetDateTime}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    OffsetDateTime asOffsetDateTime();

    /**
     * @return the value as a {@link ZonedDateTime}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    ZonedDateTime asZonedDateTime();

    /**
     * @return the value as a {@link IsoDuration}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    IsoDuration asIsoDuration();

    /**
     * @return the value as a {@link Point}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    Point asPoint();

    /**
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a {@link LocalDate}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    LocalDate asLocalDate( LocalDate defaultValue );

    /**
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a {@link OffsetTime}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    OffsetTime asOffsetTime(OffsetTime defaultValue);

    /**
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a {@link LocalTime}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    LocalTime asLocalTime(LocalTime defaultValue);

    /**
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a {@link LocalDateTime}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    LocalDateTime asLocalDateTime(LocalDateTime defaultValue);

    /**
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a {@link OffsetDateTime}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    OffsetDateTime asOffsetDateTime(OffsetDateTime defaultValue);

    /**
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a {@link ZonedDateTime}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    ZonedDateTime asZonedDateTime(ZonedDateTime defaultValue);

    /**
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a {@link IsoDuration}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    IsoDuration asIsoDuration(IsoDuration defaultValue);

    /**
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a {@link Point}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    Point asPoint(Point defaultValue);

    /**
     * Return as a map of string keys and values converted using
     * {@link Value#asObject()}.
     *
     * This is equivalent to calling {@link #asMap(Function, Map)} with {@link Values#ofObject()}.
     *
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a Java map
     */
    Map<String, Object> asMap(Map<String, Object> defaultValue);

    /**
     * @param mapFunction a function to map from Value to T. See {@link Values} for some predefined functions, such
     * as {@link Values#ofBoolean()}, {@link Values#ofList(Function)}.
     * @param <T> the type of map values
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @see Values for a long list of built-in conversion functions
     * @return the value as a map from string keys to values of type T obtained from mapping he original map values, if possible
     */
    <T> Map<String, T> asMap(Function<Value, T> mapFunction, Map<String, T> defaultValue);

    @Override
    boolean equals(Object other);

    @Override
    int hashCode();

    @Override
    String toString();
}
