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
package com.mware.ge.store.util;

import com.mware.ge.*;
import com.mware.ge.id.NameSubstitutionStrategy;
import com.mware.ge.mutation.ExtendedDataMutationBase;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

public class StorableKeyHelper {
    public static ThreadLocal<CharsetEncoder> ENCODER_FACTORY = ThreadLocal.withInitial(() ->
            StandardCharsets.UTF_8
                    .newEncoder()
                    .onMalformedInput(CodingErrorAction.REPLACE)
                    .onUnmappableCharacter(CodingErrorAction.REPLACE));

    public static String createExtendedDataRowKey(ExtendedDataRowId rowId) {
        return createExtendedDataRowKey(rowId.getElementType(), rowId.getElementId(), rowId.getTableName(), rowId.getRowId());
    }

    public static String createExtendedDataRowKey(ElementType elementType, String elementId, String tableName, String row) {
        StringBuilder sb = new StringBuilder();
        if (elementType != null) {
            String elementTypePrefix = getExtendedDataRowKeyElementTypePrefix(elementType);
            sb.append(elementTypePrefix);
            if (elementId != null) {
                sb.append(elementId);
                if (tableName != null) {
                    sb.append(KeyBase.VALUE_SEPARATOR);
                    sb.append(tableName);
                    sb.append(KeyBase.VALUE_SEPARATOR);
                    if (row != null) {
                        sb.append(row);
                    }
                } else if (row != null) {
                    throw new GeException("Cannot create partial key with missing inner value");
                }
            } else if (tableName != null || row != null) {
                throw new GeException("Cannot create partial key with missing inner value");
            }
        } else if (elementId != null || tableName != null || row != null) {
            throw new GeException("Cannot create partial key with missing inner value");
        }
        return sb.toString();
    }

    public static String createExtendedDataTableKey(ElementType elementType, String elementId, String tableName) {
        String elementTypePrefix = getExtendedDataRowKeyElementTypePrefix(elementType);
        return elementTypePrefix + elementId + KeyBase.VALUE_SEPARATOR + tableName;
    }

    public static String getExtendedDataRowKeyElementTypePrefix(ElementType elementType) {
        switch (elementType) {
            case VERTEX:
                return "V";
            case EDGE:
                return "E";
            default:
                throw new GeException("Unhandled element type: " + elementType);
        }
    }

    public static IdRange createExtendedDataRowKeyRange(ElementType elementType, IdRange elementIdRange) {
        String elementTypePrefix = getExtendedDataRowKeyElementTypePrefix(elementType);
        String start = elementIdRange.getStart();
        if (start == null) {
            start = "";
        }
        start = elementTypePrefix + start;

        String end = elementIdRange.getEnd();
        if (end != null) {
            end = elementTypePrefix + end;
        } else if (elementType == ElementType.EDGE) {
            end = getExtendedDataRowKeyElementTypePrefix(ElementType.VERTEX);
        }
        return new IdRange(start, elementIdRange.isInclusiveStart(), end, elementIdRange.isInclusiveEnd());
    }

    public static String createExtendedDataColumnQualifier(ExtendedDataMutationBase edm) {
        if (edm.getKey() == null) {
            return edm.getColumnName();
        } else {
            return edm.getColumnName() + KeyBase.VALUE_SEPARATOR + edm.getKey();
        }
    }

    public static String getColumnQualifierFromPropertyColumnQualifier(com.mware.ge.Property property, NameSubstitutionStrategy nameSubstitutionStrategy) {
        return getColumnQualifierFromPropertyColumnQualifier(property.getKey(), property.getName(), nameSubstitutionStrategy);
    }

    public static String getColumnQualifierFromPropertyColumnQualifier(String propertyKey, String propertyName, NameSubstitutionStrategy nameSubstitutionStrategy) {
        String name = nameSubstitutionStrategy.deflate(propertyName);
        String key = nameSubstitutionStrategy.deflate(propertyKey);
        KeyBase.assertNoValueSeparator(name);
        KeyBase.assertNoValueSeparator(key);
        return new StringBuilder(name.length() + 1 + key.length())
                .append(name)
                .append(KeyBase.VALUE_SEPARATOR)
                .append(key)
                .toString();
    }

    public static String getColumnQualifierFromPropertyMetadataColumnQualifier(String propertyName, String propertyKey, String visibilityString, String metadataKey, NameSubstitutionStrategy nameSubstitutionStrategy) {
        String name = nameSubstitutionStrategy.deflate(propertyName);
        String key = nameSubstitutionStrategy.deflate(propertyKey);
        metadataKey = nameSubstitutionStrategy.deflate(metadataKey);
        KeyBase.assertNoValueSeparator(name);
        KeyBase.assertNoValueSeparator(key);
        KeyBase.assertNoValueSeparator(visibilityString);
        KeyBase.assertNoValueSeparator(metadataKey);

        int charCount = name.length() + key.length() + visibilityString.length() + metadataKey.length() + 3;
        java.nio.CharBuffer qualifierChars = CharBuffer.allocate(charCount);
        qualifierChars
                .put(name).put(KeyBase.VALUE_SEPARATOR)
                .put(key).put(KeyBase.VALUE_SEPARATOR)
                .put(visibilityString).put(KeyBase.VALUE_SEPARATOR)
                .put(metadataKey)
                .flip();

        CharsetEncoder encoder = ENCODER_FACTORY.get();
        encoder.reset();

        try {
            ByteBuffer encodedQualifier = encoder.encode(qualifierChars);
            return new String(encodedQualifier.array(), 0, encodedQualifier.limit(), StandardCharsets.UTF_8);
        } catch (CharacterCodingException cce) {
            throw new RuntimeException("This should never happen", cce);
        }
    }

    public static String getColumnQualifierForDeleteAllPropertyMetadata(com.mware.ge.Property property, NameSubstitutionStrategy nameSubstitutionStrategy) {
        String name = nameSubstitutionStrategy.deflate(property.getName());
        String key = nameSubstitutionStrategy.deflate(property.getKey());
        KeyBase.assertNoValueSeparator(name);
        KeyBase.assertNoValueSeparator(key);
        return new StringBuilder(name)
                .append(KeyBase.VALUE_SEPARATOR)
                .append(key)
                .append(KeyBase.VALUE_SEPARATOR)
                .toString();
    }

    public static String getColumnQualifierFromPropertyHiddenColumnQualifier(com.mware.ge.Property property, NameSubstitutionStrategy nameSubstitutionStrategy) {
        return getColumnQualifierFromPropertyHiddenColumnQualifier(property.getKey(), property.getName(), property.getVisibility().getVisibilityString(), nameSubstitutionStrategy);
    }

    public static String getColumnQualifierFromPropertyHiddenColumnQualifier(String propertyKey, String propertyName, String visibilityString, NameSubstitutionStrategy nameSubstitutionStrategy) {
        String name = nameSubstitutionStrategy.deflate(propertyName);
        String key = nameSubstitutionStrategy.deflate(propertyKey);
        KeyBase.assertNoValueSeparator(name);
        KeyBase.assertNoValueSeparator(key);
        //noinspection StringBufferReplaceableByString
        return new StringBuilder(name.length() + 1 + key.length() + 1 + visibilityString.length())
                .append(name)
                .append(KeyBase.VALUE_SEPARATOR)
                .append(key)
                .append(KeyBase.VALUE_SEPARATOR)
                .append(visibilityString)
                .toString();
    }

    public static ExtendedDataRowId parseExtendedDataRowId(String rowKey) {
        ElementType elementType = extendedDataRowIdElementTypePrefixToElementType(rowKey.charAt(0));
        String[] parts = rowKey.substring(1).split("" + KeyBase.VALUE_SEPARATOR);
        if (parts.length != 3) {
            throw new GeException("Invalid row key found for extended data, expected 3 parts found " + parts.length + ": " + rowKey);
        }
        return new ExtendedDataRowId(elementType, parts[0], parts[1], parts[2]);
    }

    private static ElementType extendedDataRowIdElementTypePrefixToElementType(char c) {
        switch (c) {
            case 'V':
                return ElementType.VERTEX;
            case 'E':
                return ElementType.EDGE;
            default:
                throw new GeException("Invalid element type prefix: " + c);
        }
    }
}
