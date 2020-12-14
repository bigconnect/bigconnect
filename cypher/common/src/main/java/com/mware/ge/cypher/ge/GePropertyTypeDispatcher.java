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
package com.mware.ge.cypher.ge;

import com.mware.ge.Element;
import com.mware.ge.Property;
import com.mware.ge.collection.ArrayIterator;
import com.mware.ge.values.storable.DateTimeValue;
import com.mware.ge.values.storable.LongValue;
import com.mware.ge.values.storable.Values;
import com.mware.ge.values.storable.StreamingPropertyValue;

import java.sql.Timestamp;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.util.Iterator;

public abstract class GePropertyTypeDispatcher<K, T> {
    public abstract static class PropertyArray<A, T> implements Iterable<T> {
        private PropertyArray() {
        }

        public abstract int length();

        public abstract A getClonedArray();

        public abstract Class<?> getType();
    }

    public static void consumeProperties(GePropertyTypeDispatcher<String, Void> dispatcher,
                                         Element entity) {
        for (Property property : entity.getProperties()) {
            dispatcher.dispatch(property.getValue(), property.getName());
        }
    }

    @SuppressWarnings("boxing")
    public final T dispatch(Object property, K param) {
        if (property == null) {
            return dispatchNullProperty(param);
        } else if (property instanceof String) {
            return dispatchStringProperty((String) property, param);
        } else if (property instanceof Number) {
            return dispatchNumberProperty((Number) property, param);
        } else if (property instanceof Boolean) {
            return dispatchBooleanProperty((Boolean) property, param);
        } else if (property instanceof Character) {
            return dispatchCharacterProperty((Character) property, param);
        } else if (property instanceof Temporal) {
            return dispatchTemporalProperty((Temporal) property, param);
        } else if (property instanceof TemporalAmount) {
            return dispatchTemporalAmountProperty((TemporalAmount) property, param);
        } else if (property instanceof String[]) {
            return dispatchStringArrayProperty((String[]) property, param);
        } else if (property instanceof Temporal[]) {
            return dispatchTemporalArrayProperty((Temporal[]) property, param);
        } else if (property instanceof TemporalAmount[]) {
            return dispatchTemporalAmountArrayProperty((TemporalAmount[]) property, param);
        } else if (property instanceof Object[]) {
            return dispatchOtherArray((Object[]) property, param);
        } else if (property instanceof Timestamp) {
            LongValue millis = Values.longValue(((Timestamp) property).getTime());
            return dispatchTemporalProperty(DateTimeValue.ofEpochMillis(millis), param);
        } else if (property instanceof java.util.Date) {
            LongValue millis = Values.longValue(((java.util.Date) property).getTime());
            return dispatchTemporalProperty(DateTimeValue.ofEpochMillis(millis), param);
        } else if (property instanceof StreamingPropertyValue) {
            return dispatchStringProperty(((StreamingPropertyValue) property).readToString(), param);
        } else {
            Class<?> propertyType = property.getClass();
            if (propertyType.isArray() && propertyType.getComponentType().isPrimitive()) {
                return dispatchPrimitiveArray(property, param);
            } else {
                return dispatchOtherProperty(property, param);
            }
        }
    }

    private T dispatchPrimitiveArray(Object property, K param) {
        if (property instanceof byte[]) {
            return dispatchByteArrayProperty((byte[]) property, param);
        } else if (property instanceof char[]) {
            return dispatchCharacterArrayProperty((char[]) property, param);
        } else if (property instanceof boolean[]) {
            return dispatchBooleanArrayProperty((boolean[]) property, param);
        } else if (property instanceof long[]) {
            return dispatchLongArrayProperty((long[]) property, param);
        } else if (property instanceof double[]) {
            return dispatchDoubleArrayProperty((double[]) property, param);
        } else if (property instanceof int[]) {
            return dispatchIntegerArrayProperty((int[]) property, param);
        } else if (property instanceof short[]) {
            return dispatchShortArrayProperty((short[]) property, param);
        } else if (property instanceof float[]) {
            return dispatchFloatArrayProperty((float[]) property, param);
        } else {
            throw new Error("Unsupported primitive array type: " + property.getClass());
        }
    }

    protected T dispatchOtherArray(Object[] property, K param) {
        if (property instanceof Byte[]) {
            return dispatchByteArrayProperty((Byte[]) property, param);
        } else if (property instanceof Character[]) {
            return dispatchCharacterArrayProperty((Character[]) property, param);
        } else if (property instanceof Boolean[]) {
            return dispatchBooleanArrayProperty((Boolean[]) property, param);
        } else if (property instanceof Long[]) {
            return dispatchLongArrayProperty((Long[]) property, param);
        } else if (property instanceof Double[]) {
            return dispatchDoubleArrayProperty((Double[]) property, param);
        } else if (property instanceof Integer[]) {
            return dispatchIntegerArrayProperty((Integer[]) property, param);
        } else if (property instanceof Short[]) {
            return dispatchShortArrayProperty((Short[]) property, param);
        } else if (property instanceof Float[]) {
            return dispatchFloatArrayProperty((Float[]) property, param);
        } else {
            throw new IllegalArgumentException("Unsupported property array type: "
                    + property.getClass());
        }
    }

    @SuppressWarnings("boxing")
    protected T dispatchNumberProperty(Number property, K param) {
        if (property instanceof Long) {
            return dispatchLongProperty((Long) property, param);
        } else if (property instanceof Integer) {
            return dispatchIntegerProperty((Integer) property, param);
        } else if (property instanceof Double) {
            return dispatchDoubleProperty((Double) property, param);
        } else if (property instanceof Float) {
            return dispatchFloatProperty((Float) property, param);
        } else if (property instanceof Short) {
            return dispatchShortProperty((Short) property, param);
        } else if (property instanceof Byte) {
            return dispatchByteProperty((Byte) property, param);
        } else {
            throw new IllegalArgumentException("Unsupported property type: " + property.getClass());
        }
    }

    protected T dispatchNullProperty(K param) {
        return null;
    }

    @SuppressWarnings("boxing")
    protected abstract T dispatchByteProperty(byte property, K param);

    @SuppressWarnings("boxing")
    protected abstract T dispatchCharacterProperty(char property, K param);

    @SuppressWarnings("boxing")
    protected abstract T dispatchShortProperty(short property, K param);

    @SuppressWarnings("boxing")
    protected abstract T dispatchIntegerProperty(int property, K param);

    @SuppressWarnings("boxing")
    protected abstract T dispatchLongProperty(long property, K param);

    @SuppressWarnings("boxing")
    protected abstract T dispatchFloatProperty(float property, K param);

    @SuppressWarnings("boxing")
    protected abstract T dispatchDoubleProperty(double property, K param);

    @SuppressWarnings("boxing")
    protected abstract T dispatchBooleanProperty(boolean property, K param);

    //not abstract in order to not break existing code, since this was fixed in point release
    protected T dispatchTemporalProperty(Temporal property, K param) {
        return dispatchOtherProperty(property, param);
    }

    //not abstract in order to not break existing code, since this was fixed in point release
    protected T dispatchTemporalAmountProperty(TemporalAmount property, K param) {
        return dispatchOtherProperty(property, param);
    }

    protected T dispatchOtherProperty(Object property, K param) {
        throw new IllegalArgumentException("Unsupported property type: "
                + property.getClass());
    }

    protected T dispatchByteArrayProperty(final byte[] property, K param) {
        return dispatchByteArrayProperty(new PrimitiveArray<byte[], Byte>() {
            @Override
            public byte[] getClonedArray() {
                return property.clone();
            }

            @Override
            public int length() {
                return property.length;
            }

            @Override
            @SuppressWarnings("boxing")
            protected Byte item(int offset) {
                return property[offset];
            }

            @Override
            public Class<?> getType() {
                return property.getClass();
            }
        }, param);
    }

    protected T dispatchCharacterArrayProperty(final char[] property, K param) {
        return dispatchCharacterArrayProperty(new PrimitiveArray<char[], Character>() {
            @Override
            public char[] getClonedArray() {
                return property.clone();
            }

            @Override
            public int length() {
                return property.length;
            }

            @Override
            @SuppressWarnings("boxing")
            protected Character item(int offset) {
                return property[offset];
            }

            @Override
            public Class<?> getType() {
                return property.getClass();
            }
        }, param);
    }

    protected T dispatchShortArrayProperty(final short[] property, K param) {
        return dispatchShortArrayProperty(new PrimitiveArray<short[], Short>() {
            @Override
            public short[] getClonedArray() {
                return property.clone();
            }

            @Override
            public int length() {
                return property.length;
            }

            @Override
            @SuppressWarnings("boxing")
            protected Short item(int offset) {
                return property[offset];
            }

            @Override
            public Class<?> getType() {
                return property.getClass();
            }
        }, param);
    }

    protected T dispatchIntegerArrayProperty(final int[] property, K param) {
        return dispatchIntegerArrayProperty(new PrimitiveArray<int[], Integer>() {
            @Override
            public int[] getClonedArray() {
                return property.clone();
            }

            @Override
            public int length() {
                return property.length;
            }

            @Override
            @SuppressWarnings("boxing")
            protected Integer item(int offset) {
                return property[offset];
            }

            @Override
            public Class<?> getType() {
                return property.getClass();
            }
        }, param);
    }

    protected T dispatchLongArrayProperty(final long[] property, K param) {
        return dispatchLongArrayProperty(new PrimitiveArray<long[], Long>() {
            @Override
            public long[] getClonedArray() {
                return property.clone();
            }

            @Override
            public int length() {
                return property.length;
            }

            @Override
            @SuppressWarnings("boxing")
            protected Long item(int offset) {
                return property[offset];
            }

            @Override
            public Class<?> getType() {
                return property.getClass();
            }
        }, param);
    }

    protected T dispatchFloatArrayProperty(final float[] property, K param) {
        return dispatchFloatArrayProperty(new PrimitiveArray<float[], Float>() {
            @Override
            public float[] getClonedArray() {
                return property.clone();
            }

            @Override
            public int length() {
                return property.length;
            }

            @Override
            @SuppressWarnings("boxing")
            protected Float item(int offset) {
                return property[offset];
            }

            @Override
            public Class<?> getType() {
                return property.getClass();
            }
        }, param);
    }

    protected T dispatchDoubleArrayProperty(final double[] property, K param) {
        return dispatchDoubleArrayProperty(new PrimitiveArray<double[], Double>() {
            @Override
            public double[] getClonedArray() {
                return property.clone();
            }

            @Override
            public int length() {
                return property.length;
            }

            @Override
            @SuppressWarnings("boxing")
            protected Double item(int offset) {
                return property[offset];
            }

            @Override
            public Class<?> getType() {
                return property.getClass();
            }
        }, param);
    }

    protected T dispatchBooleanArrayProperty(final boolean[] property, K param) {
        return dispatchBooleanArrayProperty(new PrimitiveArray<boolean[], Boolean>() {
            @Override
            public boolean[] getClonedArray() {
                return property.clone();
            }

            @Override
            public int length() {
                return property.length;
            }

            @Override
            @SuppressWarnings("boxing")
            protected Boolean item(int offset) {
                return property[offset];
            }

            @Override
            public Class<?> getType() {
                return property.getClass();
            }
        }, param);
    }

    protected T dispatchByteArrayProperty(final Byte[] property, K param) {
        return dispatchByteArrayProperty(new BoxedArray<byte[], Byte>(property) {
            @Override
            @SuppressWarnings("boxing")
            public byte[] getClonedArray() {
                byte[] result = new byte[property.length];
                for (int i = 0; i < result.length; i++) {
                    result[i] = property[i];
                }
                return result;
            }
        }, param);
    }

    protected T dispatchCharacterArrayProperty(final Character[] property, K param) {
        return dispatchCharacterArrayProperty(new BoxedArray<char[], Character>(property) {
            @Override
            @SuppressWarnings("boxing")
            public char[] getClonedArray() {
                char[] result = new char[property.length];
                for (int i = 0; i < result.length; i++) {
                    result[i] = property[i];
                }
                return result;
            }
        }, param);
    }

    protected T dispatchShortArrayProperty(final Short[] property, K param) {
        return dispatchShortArrayProperty(new BoxedArray<short[], Short>(property) {
            @Override
            @SuppressWarnings("boxing")
            public short[] getClonedArray() {
                short[] result = new short[property.length];
                for (int i = 0; i < result.length; i++) {
                    result[i] = property[i];
                }
                return result;
            }
        }, param);
    }

    protected T dispatchIntegerArrayProperty(final Integer[] property, K param) {
        return dispatchIntegerArrayProperty(new BoxedArray<int[], Integer>(property) {
            @Override
            @SuppressWarnings("boxing")
            public int[] getClonedArray() {
                int[] result = new int[property.length];
                for (int i = 0; i < result.length; i++) {
                    result[i] = property[i];
                }
                return result;
            }
        }, param);
    }

    protected T dispatchLongArrayProperty(final Long[] property, K param) {
        return dispatchLongArrayProperty(new BoxedArray<long[], Long>(property) {
            @Override
            @SuppressWarnings("boxing")
            public long[] getClonedArray() {
                long[] result = new long[property.length];
                for (int i = 0; i < result.length; i++) {
                    result[i] = property[i];
                }
                return result;
            }
        }, param);
    }

    protected T dispatchFloatArrayProperty(final Float[] property, K param) {
        return dispatchFloatArrayProperty(new BoxedArray<float[], Float>(property) {
            @Override
            @SuppressWarnings("boxing")
            public float[] getClonedArray() {
                float[] result = new float[property.length];
                for (int i = 0; i < result.length; i++) {
                    result[i] = property[i];
                }
                return result;
            }
        }, param);
    }

    protected T dispatchDoubleArrayProperty(final Double[] property, K param) {
        return dispatchDoubleArrayProperty(new BoxedArray<double[], Double>(property) {
            @Override
            @SuppressWarnings("boxing")
            public double[] getClonedArray() {
                double[] result = new double[property.length];
                for (int i = 0; i < result.length; i++) {
                    result[i] = property[i];
                }
                return result;
            }
        }, param);
    }

    protected T dispatchBooleanArrayProperty(final Boolean[] property, K param) {
        return dispatchBooleanArrayProperty(new BoxedArray<boolean[], Boolean>(property) {
            @Override
            @SuppressWarnings("boxing")
            public boolean[] getClonedArray() {
                boolean[] result = new boolean[property.length];
                for (int i = 0; i < result.length; i++) {
                    result[i] = property[i];
                }
                return result;
            }
        }, param);
    }

    protected abstract T dispatchStringProperty(String property, K param);

    protected T dispatchStringArrayProperty(final String[] property, K param) {
        return dispatchStringArrayProperty(new BoxedArray<String[], String>(property) {
            @Override
            public String[] getClonedArray() {
                return property.clone();
            }
        }, param);
    }

    protected T dispatchStringArrayProperty(PropertyArray<String[], String> array, K param) {
        return dispatchArray(array, param);
    }


    protected T dispatchTemporalArrayProperty(PropertyArray<Temporal[], Temporal> array, K param) {
        return dispatchArray(array, param);
    }

    protected T dispatchTemporalArrayProperty(final Temporal[] property, K param) {
        return dispatchTemporalArrayProperty(new BoxedArray<Temporal[], Temporal>(property) {
            @Override
            public Temporal[] getClonedArray() {
                return property.clone();
            }
        }, param);
    }

    protected T dispatchTemporalAmountArrayProperty(PropertyArray<TemporalAmount[], TemporalAmount> array, K param) {
        return dispatchArray(array, param);
    }

    protected T dispatchTemporalAmountArrayProperty(final TemporalAmount[] property, K param) {
        return dispatchTemporalAmountArrayProperty(new BoxedArray<TemporalAmount[], TemporalAmount>(property) {
            @Override
            public TemporalAmount[] getClonedArray() {
                return property.clone();
            }
        }, param);
    }

    protected T dispatchByteArrayProperty(PropertyArray<byte[], Byte> array, K param) {
        return dispatchNumberArray(array, param);
    }

    protected T dispatchCharacterArrayProperty(PropertyArray<char[], Character> array, K param) {
        return dispatchArray(array, param);
    }

    protected T dispatchShortArrayProperty(PropertyArray<short[], Short> array, K param) {
        return dispatchNumberArray(array, param);
    }

    protected T dispatchIntegerArrayProperty(PropertyArray<int[], Integer> array, K param) {
        return dispatchNumberArray(array, param);
    }

    protected T dispatchLongArrayProperty(PropertyArray<long[], Long> array, K param) {
        return dispatchNumberArray(array, param);
    }

    protected T dispatchFloatArrayProperty(PropertyArray<float[], Float> array, K param) {
        return dispatchNumberArray(array, param);
    }

    protected T dispatchDoubleArrayProperty(PropertyArray<double[], Double> array, K param) {
        return dispatchNumberArray(array, param);
    }

    protected T dispatchBooleanArrayProperty(PropertyArray<boolean[], Boolean> array, K param) {
        return dispatchArray(array, param);
    }

    protected T dispatchNumberArray(PropertyArray<?, ? extends Number> array, K param) {
        return dispatchArray(array, param);
    }

    protected T dispatchArray(PropertyArray<?, ?> array, K param) {
        throw new UnsupportedOperationException("Unhandled array type: " + array.getType());
    }

    private abstract static class BoxedArray<A, T> extends PropertyArray<A, T> {
        private final T[] array;

        BoxedArray(T[] array) {
            this.array = array;
        }

        @Override
        public int length() {
            return array.length;
        }

        @Override
        public Iterator<T> iterator() {
            return new ArrayIterator<>(array);
        }

        @Override
        public Class<?> getType() {
            return array.getClass();
        }
    }

    private abstract static class PrimitiveArray<A, T> extends PropertyArray<A, T> {
        @Override
        public Iterator<T> iterator() {
            return new Iterator<T>() {
                final int size = length();
                int pos;

                @Override
                public boolean hasNext() {
                    return pos < size;
                }

                @Override
                public T next() {
                    return item(pos++);
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException(
                            "Cannot remove element from primitive array.");
                }
            };
        }

        protected abstract T item(int offset);
    }
}
