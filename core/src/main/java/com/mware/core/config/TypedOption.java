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
package com.mware.core.config;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.mware.ge.util.ConfigurationUtils;
import com.mware.ge.util.Preconditions;
import org.apache.commons.configuration.PropertyConverter;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;
import java.util.Set;

public class TypedOption<T, R> {
    private static final Set<Class<?>> ACCEPTED_DATA_TYPES;
    private static final String ACCEPTED_DATA_TYPES_STRING;

    static {
        ACCEPTED_DATA_TYPES = ImmutableSet.of(
                Boolean.class,
                Short.class,
                Integer.class,
                Byte.class,
                Long.class,
                Float.class,
                Double.class,
                String.class,
                String[].class,
                Class.class,
                List.class,
                Duration.class
        );

        ACCEPTED_DATA_TYPES_STRING = Joiner.on(", ").join(ACCEPTED_DATA_TYPES);
    }

    private final String name;
    private final String desc;
    private final boolean required;
    private final Class<T> dataType;
    private T defaultValue;
    private final Predicate<T> checkFunc;

    @SuppressWarnings("unchecked")
    public TypedOption(String name, boolean required, String desc,
                       Predicate<T> pred, Class<T> type, T value) {
        Preconditions.checkNotNull(name, "name");
        Preconditions.checkNotNull(type, "dataType");

        this.name = name;
        this.dataType = (Class<T>) this.checkAndAssignDataType(type);
        this.defaultValue = value;
        this.required = required;
        this.desc = desc;
        this.checkFunc = pred;

        this.check(this.defaultValue);
    }

    private Class<?> checkAndAssignDataType(Class<T> dataType) {
        for (Class<?> clazz : ACCEPTED_DATA_TYPES) {
            if (clazz.isAssignableFrom(dataType)) {
                return clazz;
            }
        }

        String msg = String.format("Input data type '%s' doesn't belong " +
                        "to acceptable type set: [%s]",
                dataType, ACCEPTED_DATA_TYPES_STRING);
        throw new IllegalArgumentException(msg);
    }

    public String name() {
        return this.name;
    }

    public Class<T> dataType() {
        return this.dataType;
    }

    public String desc() {
        return this.desc;
    }

    public boolean required() {
        return this.required;
    }

    public R defaultValue() {
        return this.convert(this.defaultValue);
    }

    public void overrideDefaultValue(T defaultValue) {
        this.defaultValue = defaultValue;
    }

    public R parseConvert(String value) {
        T parsed = this.parse(value);
        this.check(parsed);
        return this.convert(parsed);
    }

    @SuppressWarnings("unchecked")
    protected T parse(String value) {
        return (T) this.parse(value, this.dataType);
    }

    protected Object parse(String value, Class<?> dataType) {
        if (dataType.equals(String.class)) {
            return value;
        } else if (dataType.equals(Duration.class)) {
            return ConfigurationUtils.parseDuration(value);
        } else if (dataType.equals(Class.class)) {
            try {
                return Class.forName(value);
            } catch (ClassNotFoundException e) {
                throw new ConfigException(
                        "Failed to parse Class from String '%s'", e, value);
            }
        } else if (List.class.isAssignableFrom(dataType)) {
            Preconditions.checkState(this.forList(),
                    "List option can't be registered with class %s",
                    this.getClass().getSimpleName());
        }

        // Use PropertyConverter method `toXXX` convert value
        String methodTo = "to" + dataType.getSimpleName();
        try {
            Method method = PropertyConverter.class.getMethod(
                    methodTo, Object.class);
            return method.invoke(null, value);
        } catch (ReflectiveOperationException e) {
            throw new ConfigException(
                    "Invalid type of value '%s' for option '%s', " +
                            "expect '%s' type",
                    value, this.name, dataType.getSimpleName());
        }
    }

    protected void check(Object value) {
        Preconditions.checkNotNull(value, "value", this.name);
        if (!this.dataType.isInstance(value)) {
            throw new ConfigException(
                    "Invalid type of value '%s' for option '%s', " +
                            "expect type %s but got %s", value, this.name,
                    this.dataType.getSimpleName(),
                    value.getClass().getSimpleName());
        }

        if (this.checkFunc != null) {
            @SuppressWarnings("unchecked")
            T result = (T) value;
            if (!this.checkFunc.apply(result)) {
                throw new ConfigException("Invalid option value for '%s': %s",
                        this.name, value);
            }
        }
    }

    @SuppressWarnings("unchecked")
    protected R convert(T value) {
        return (R) value;
    }

    protected boolean forList() {
        return false;
    }

    @Override
    public String toString() {
        return String.format("[%s]%s=%s", this.dataType.getSimpleName(),
                this.name, this.defaultValue);
    }
}
