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
package com.mware.ge.values.virtual;


import com.mware.ge.values.AnyValue;
import com.mware.ge.values.storable.ArrayValue;
import com.mware.ge.values.storable.TextArray;
import com.mware.ge.values.storable.TextValue;

import java.util.HashMap;
import java.util.List;

/**
 * Entry point to the virtual values library.
 */
@SuppressWarnings("WeakerAccess")
public final class VirtualValues {
    public static final MapValue EMPTY_MAP = MapValue.EMPTY;
    public static final ListValue EMPTY_LIST = new ListValue.ArrayListValue(new AnyValue[0]);

    private VirtualValues() {
    }

    // DIRECT FACTORY METHODS

    public static ListValue list(AnyValue... values) {
        return new ListValue.ArrayListValue(values);
    }

    public static ListValue fromList(List<AnyValue> values) {
        return new ListValue.JavaListListValue(values);
    }

    public static ListValue range(long start, long end, long step) {
        return new ListValue.IntegralRangeListValue(start, end, step);
    }

    public static ListValue fromArray(ArrayValue arrayValue) {
        return new ListValue.ArrayValueListValue(arrayValue);
    }

    /*
    TOMBSTONE: TransformedListValue & FilteredListValue

    This list value variant would lazily apply a transform/filter on a inner list. The lazy behavior made it hard
    to guarantee that the transform/filter was still evaluable and correct on reading the transformed list, so
    this was removed. If we want lazy values again, remember the problems of

       - returning results out of Cypher combined with auto-closing iterators
       - reading modified tx-state which was not visible at TransformedListValue creation

    */

    public static ListValue concat(ListValue... lists) {
        return new ListValue.ConcatList(lists);
    }

    public static MapValue emptyMap() {
        return EMPTY_MAP;
    }

    public static MapValue map(String[] keys, AnyValue[] values) {
        assert keys.length == values.length;
        HashMap<String, AnyValue> map = new HashMap<>(keys.length);
        for (int i = 0; i < keys.length; i++) {
            map.put(keys[i], values[i]);
        }
        return new MapValue.MapWrappingMapValue(map);
    }

    public static ErrorValue error(Exception e) {
        return new ErrorValue(e);
    }

    public static NodeReference node(String id) {
        return new NodeReference(id);
    }

    public static RelationshipReference relationship(String id) {
        return new RelationshipReference(id);
    }

    public static PathValue path(NodeValue[] nodes, RelationshipValue[] relationships) {
        assert nodes != null;
        assert relationships != null;
        if ((nodes.length + relationships.length) % 2 == 0) {
            throw new IllegalArgumentException(
                    "Tried to construct a path that is not built like a path: even number of elements");
        }
        return new PathValue.DirectPathValue(nodes, relationships);
    }

    public static NodeValue nodeValue(String id, TextArray labels, MapValue properties) {
        return new NodeValue.DirectNodeValue(id, labels, properties);
    }

    public static RelationshipValue relationshipValue(String id, NodeValue startNode, NodeValue endNode, TextValue type,
                                                      MapValue properties) {
        return new RelationshipValue.DirectRelationshipValue(id, startNode, endNode, type, properties);
    }
}
