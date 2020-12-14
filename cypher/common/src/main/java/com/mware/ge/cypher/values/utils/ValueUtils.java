package com.mware.ge.cypher.values.utils;

import com.mware.ge.Edge;
import com.mware.ge.Element;
import com.mware.ge.Vertex;
import com.mware.ge.cypher.Path;
import com.mware.ge.cypher.values.virtual.GeEdgeWrappingValue;
import com.mware.ge.cypher.values.virtual.GeVertexWrappingNodeValue;
import com.mware.ge.cypher.values.virtual.GeWrappingPathValue;
import com.mware.ge.values.AnyValue;
import com.mware.ge.values.storable.Value;
import com.mware.ge.values.storable.Values;
import com.mware.ge.values.virtual.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ValueUtils {
    private ValueUtils() {
        throw new UnsupportedOperationException("do not instantiate");
    }

    /**
     * Creates an AnyValue by doing type inspection. Do not use in production code where performance is important.
     *
     * @param object the object to turned into a AnyValue
     * @return the AnyValue corresponding to object.
     */
    @SuppressWarnings("unchecked")
    public static AnyValue of(Object object) {
        Value value = Values.unsafeOf(object, true);
        if (value != null) {
            return value;
        } else {
            if (object instanceof Element) {
                if (object instanceof Vertex) {
                    return fromNodeProxy((Vertex) object);
                } else if (object instanceof Edge) {
                    return fromRelationshipProxy((Edge) object);
                } else {
                    throw new IllegalArgumentException("Unknown entity + " + object.getClass().getName());
                }
            } else if (object instanceof Iterable<?>) {
                if (object instanceof Path) {
                    return fromPath((Path) object);
                } else if (object instanceof List<?>) {
                    return asListValue((List<?>) object);
                } else {
                    return asListValue((Iterable<?>) object);
                }
            } else if (object instanceof Map<?, ?>) {
                return asMapValue((Map<String, Object>) object);
            } else if (object instanceof Iterator<?>) {
                ArrayList<Object> objects = new ArrayList<>();
                Iterator<?> iterator = (Iterator<?>) object;
                while (iterator.hasNext()) {
                    objects.add(iterator.next());
                }
                return asListValue(objects);
            } else if (object instanceof Object[]) {
                Object[] array = (Object[]) object;
                AnyValue[] anyValues = new AnyValue[array.length];
                for (int i = 0; i < array.length; i++) {
                    anyValues[i] = ValueUtils.of(array[i]);
                }
                return VirtualValues.list(anyValues);
            } else if (object instanceof Stream<?>) {
                return asListValue(((Stream<Object>) object).collect(Collectors.toList()));
            } else if (object instanceof VirtualNodeValue || object instanceof VirtualRelationshipValue) {
                return (AnyValue) object;
            } else {
                throw new IllegalArgumentException(
                        String.format("Cannot convert %s to AnyValue", object.getClass().getName()));
            }
        }
    }

    public static ListValue asListValue(List<?> collection) {
        ArrayList<AnyValue> values = new ArrayList<>(collection.size());
        for (Object o : collection) {
            values.add(ValueUtils.of(o));
        }
        return VirtualValues.fromList(values);
    }

    public static ListValue asListValue(Iterable<?> collection) {
        ArrayList<AnyValue> values = new ArrayList<>();
        for (Object o : collection) {
            values.add(ValueUtils.of(o));
        }
        return VirtualValues.fromList(values);
    }

    public static MapValue asMapValue(Map<String, Object> map) {
        MapValueBuilder builder = new MapValueBuilder(map.size());
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            builder.add(entry.getKey(), ValueUtils.of(entry.getValue()));
        }
        return builder.build();
    }

    public static RelationshipValue fromRelationshipProxy(Edge relationship) {
        return new GeEdgeWrappingValue(relationship);
    }

    public static NodeValue fromNodeProxy(Vertex node) {
        return new GeVertexWrappingNodeValue(node);
    }

    public static PathValue fromPath(Path path) {
        return new GeWrappingPathValue(path);
    }

    public static ListValue asListOfEdges(Iterable<RelationshipValue> rels) {
        return VirtualValues.list(StreamSupport.stream(rels.spliterator(), false)
                .toArray(RelationshipValue[]::new));
    }

    public static AnyValue asNodeOrEdgeValue(Element container) {
        if (container instanceof Vertex) {
            return fromNodeProxy((Vertex) container);
        } else if (container instanceof Edge) {
            return fromRelationshipProxy((Edge) container);
        } else {
            throw new IllegalArgumentException(
                    "Cannot produce a node or edge from " + container.getClass().getName());
        }
    }

    public static MapValue asParameterMapValue(Map<String, Object> map) {
        MapValueBuilder builder = new MapValueBuilder(map.size());
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            try {
                builder.add(entry.getKey(), ValueUtils.of(entry.getValue()));
            } catch (IllegalArgumentException e) {
                builder.add(entry.getKey(), VirtualValues.error(e));
            }
        }

        return builder.build();
    }
}
