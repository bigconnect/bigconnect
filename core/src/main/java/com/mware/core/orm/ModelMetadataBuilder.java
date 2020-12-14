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
package com.mware.core.orm;

import org.json.JSONObject;

import java.lang.reflect.Constructor;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class ModelMetadataBuilder {
    public static <T> ModelMetadata<T> build(Class<T> rowClass) {
        String discriminatorColumnFamily, discriminatorColumnName;
        Map<String, ModelMetadata.Type> types = new HashMap<>();
        AnnotationAndClass<EntitySubTypes> subTypes = findAnnotation(rowClass, EntitySubTypes.class);
        Class entityRowClass;
        if (subTypes == null) {
            discriminatorColumnFamily = null;
            discriminatorColumnName = null;
            entityRowClass = rowClass;
            ModelMetadata.Type t = new ModelMetadata.Type(
                    rowClass,
                    getConstructor(rowClass),
                    getFields(rowClass)
            );
            types.put(ModelMetadata.DEFAULT_DISCRIMINATOR, t);
        } else {
            discriminatorColumnFamily = subTypes.getAnnotation().discriminatorColumnFamily();
            discriminatorColumnName = subTypes.getAnnotation().discriminatorColumnName();
            entityRowClass = subTypes.getClazz();
            for (EntitySubTypes.Type subType : subTypes.getAnnotation().types()) {
                Class<?> subTypeClass = subType.value();
                ModelMetadata.Type t = new ModelMetadata.Type(
                        subTypeClass,
                        getConstructor(subTypeClass),
                        getFields(subTypeClass)
                );
                types.put(subType.name(), t);
            }
        }

        return new ModelMetadata<>(
                entityRowClass,
                getIdField(rowClass),
                types,
                discriminatorColumnFamily,
                discriminatorColumnName,
                getTableName(rowClass)
        );
    }

    private static <T> Constructor<T> getConstructor(Class<T> rowClass) {
        try {
            return rowClass.getDeclaredConstructor();
        } catch (Exception ex) {
            throw new SimpleOrmException("Could not get constructor for class: " + rowClass.getName());
        }
    }

    private static String getTableName(Class rowClass) {
        AnnotationAndClass<Entity> entityAnnotation = findAnnotation(rowClass, Entity.class);
        checkNotNull(entityAnnotation, "Could not find " + Entity.class.getName() + " on class " + rowClass.getName());
        return entityAnnotation.getAnnotation().tableName();
    }

    @SuppressWarnings("unchecked")
    private static <T> AnnotationAndClass<T> findAnnotation(Class clazz, Class<T> annotationType) {
        T annotation = (T) clazz.getAnnotation(annotationType);
        while (annotation == null && clazz.getSuperclass() != null) {
            clazz = clazz.getSuperclass();
            annotation = (T) clazz.getAnnotation(annotationType);
        }
        if (annotation == null) {
            return null;
        }
        return new AnnotationAndClass<>(annotation, clazz);
    }

    private static class AnnotationAndClass<T> {
        private final T annotation;
        private final Class clazz;

        private AnnotationAndClass(T annotation, Class clazz) {
            this.annotation = annotation;
            this.clazz = clazz;
        }

        public T getAnnotation() {
            return annotation;
        }

        public Class getClazz() {
            return clazz;
        }
    }

    private static Map<String, Map<String, ModelMetadata.Field>> getFields(Class rowClass) {
        Map<String, Map<String, ModelMetadata.Field>> fields = new HashMap<>();
        getFields(rowClass, fields);
        return fields;
    }

    private static <T> ModelMetadata.IdField getIdField(Class<T> clazz) {
        for (java.lang.reflect.Field field : clazz.getDeclaredFields()) {
            Id idAnnotation = field.getAnnotation(Id.class);
            if (idAnnotation == null) {
                continue;
            }
            return new ModelMetadata.IdField(field);
        }
        if (clazz.getSuperclass() != null) {
            return getIdField(clazz.getSuperclass());
        }
        return null;
    }

    private static void getFields(Class clazz, Map<String, Map<String, ModelMetadata.Field>> fields) {
        for (java.lang.reflect.Field field : clazz.getDeclaredFields()) {
            getField(field, fields);
        }
        if (clazz.getSuperclass() != null) {
            getFields(clazz.getSuperclass(), fields);
        }
    }

    private static void getField(java.lang.reflect.Field field, Map<String, Map<String, ModelMetadata.Field>> fields) {
        Field fieldAnnotation = field.getAnnotation(Field.class);
        if (fieldAnnotation == null) {
            return;
        }
        String fieldColumnFamily = fieldAnnotation.columnFamily();
        Map<String, ModelMetadata.Field> columnFamilyFields = fields.get(fieldColumnFamily);
        if (columnFamilyFields == null) {
            columnFamilyFields = new HashMap<>();
            fields.put(fieldColumnFamily, columnFamilyFields);
        }

        String fieldColumnName = fieldAnnotation.columnName();
        if (fieldColumnName.length() == 0) {
            fieldColumnName = field.getName();
        }

        if (columnFamilyFields.get(fieldColumnName) != null) {
            throw new SimpleOrmException("Multiple columns map to same field: " + fieldColumnFamily + " " + fieldColumnName);
        }
        columnFamilyFields.put(fieldColumnName, createField(field, fieldColumnFamily, fieldColumnName));
    }

    private static ModelMetadata.Field createField(java.lang.reflect.Field field, String columnFamily, String columnName) {
        Class<?> fieldType = field.getType();
        if (fieldType.isEnum()) {
            return new ModelMetadata.EnumField(field, columnFamily, columnName);
        } else if (fieldType == String.class) {
            return new ModelMetadata.StringField(field, columnFamily, columnName);
        } else if (fieldType == Date.class) {
            return new ModelMetadata.DateField(field, columnFamily, columnName);
        } else if (fieldType == Map.class) {
            return new ModelMetadata.ObjectField(field, columnFamily, columnName);
        } else if (fieldType == JSONObject.class) {
            return new ModelMetadata.JSONObjectField(field, columnFamily, columnName);
        } else if (fieldType == Integer.class || fieldType == Integer.TYPE) {
            return new ModelMetadata.IntegerField(field, columnFamily, columnName);
        } else if (fieldType == Long.class || fieldType == Long.TYPE) {
            return new ModelMetadata.LongField(field, columnFamily, columnName);
        } else if (fieldType == Boolean.class || fieldType == Boolean.TYPE) {
            return new ModelMetadata.BooleanField(field, columnFamily, columnName);
        } else if (fieldType.isArray() && fieldType.getComponentType() == Byte.TYPE) {
            return new ModelMetadata.ByteArrayField(field, columnFamily, columnName);
        } else {
            throw new SimpleOrmException("Unhandled field type: " + fieldType);
        }
    }
}
