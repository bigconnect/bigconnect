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
package com.mware.ge.serializer.kryo.customserializers;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.*;

import java.util.EnumMap;
import java.util.Map;

public class GuavaSerializers {

    public static void register(final Kryo kryo) {
        registerImmutableListSerializers(kryo);
        registerImmutableSetSerializers(kryo);
        registerImmutableMapSerializers(kryo);
    }

    private static void registerImmutableListSerializers(final Kryo kryo) {

        // ImmutableList (abstract class)
        //  +- RegularImmutableList
        //  |   RegularImmutableList
        //  +- SingletonImmutableList
        //  |   Optimized for List with only 1 element.
        //  +- SubList
        //  |   Representation for part of ImmutableList
        //  +- ReverseImmutableList
        //  |   For iterating in reverse order
        //  +- StringAsImmutableList
        //  |   Used by Lists#charactersOf
        //  +- Values (ImmutableTable values)
        //      Used by return value of #values() when there are multiple cells

        final ImmutableListSerializer serializer = new ImmutableListSerializer();

        kryo.register(ImmutableList.class, serializer);

        // Note:
        //  Only registering above is good enough for serializing/deserializing.
        //  but if using Kryo#copy, following is required.

        kryo.register(ImmutableList.of().getClass(), serializer);
        kryo.register(ImmutableList.of(1).getClass(), serializer);
        kryo.register(ImmutableList.of(1, 2, 3, 4).subList(1, 3).getClass(), serializer);
        kryo.register(ImmutableList.of(1, 2).reverse().getClass(), serializer);

        kryo.register(Lists.charactersOf("KryoRocks").getClass(), serializer);

        Table<Integer,Integer,Integer> baseTable = HashBasedTable.create();
        baseTable.put(1, 2, 3);
        baseTable.put(4, 5, 6);
        Table<Integer, Integer, Integer> table = ImmutableTable.copyOf(baseTable);
        kryo.register(table.values().getClass(), serializer);

    }

    private static void registerImmutableSetSerializers(final Kryo kryo) {

        // ImmutableList (abstract class)
        //  +- EmptyImmutableSet
        //  |   EmptyImmutableSet
        //  +- SingletonImmutableSet
        //  |   Optimized for Set with only 1 element.
        //  +- RegularImmutableSet
        //  |   RegularImmutableList
        //  +- EnumImmutableSet
        //  |   EnumImmutableSet

        final ImmutableSetSerializer serializer = new ImmutableSetSerializer();

        kryo.register(ImmutableSet.class, serializer);

        // Note:
        //  Only registering above is good enough for serializing/deserializing.
        //  but if using Kryo#copy, following is required.

        kryo.register(ImmutableSet.of().getClass(), serializer);
        kryo.register(ImmutableSet.of(1).getClass(), serializer);
        kryo.register(ImmutableSet.of(1,2,3).getClass(), serializer);

        kryo.register(Sets.immutableEnumSet(Value.VALUE1, Value.VALUE2, Value.VALUE3).getClass(), serializer);
    }


    private static void registerImmutableMapSerializers(final Kryo kryo) {

        final ImmutableMapSerializer serializer = new ImmutableMapSerializer();

        kryo.register(ImmutableMap.class, serializer);
        kryo.register(ImmutableMap.of().getClass(), serializer);

        Object o1 = new Object();
        Object o2 = new Object();

        kryo.register(ImmutableMap.of(o1, o1).getClass(), serializer);
        kryo.register(ImmutableMap.of(o1, o1, o2, o2).getClass(), serializer);

        Map<Value,Object> enumMap = new EnumMap<>(Value.class);
        for (Value e : Value.values()) {
            enumMap.put(e, o1);
        }

        kryo.register(ImmutableMap.copyOf(enumMap).getClass(), serializer);
    }
}
