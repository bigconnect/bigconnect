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
package com.mware.ge.values.storable;


import static com.mware.ge.values.storable.ValueCategory.*;

/**
 * The ValueGroup is the logical group or type of a Value. For example byte, short, int and long are all attempting
 * to represent mathematical integers, meaning that for comparison purposes they should be treated the same.
 * <p>
 * The order here is defined in <a href="https://github.com/opencypher/openCypher/blob/master/cip/1.accepted/CIP2016-06-14-Define-comparability-and-equality-as-well-as-orderability-and-equivalence.adoc">
 * The Cypher CIP defining orderability
 * </a>
 * <p>
 * Each ValueGroup belong to some larger grouping called {@link ValueCategory}.
 */
public enum ValueGroup {
    UNKNOWN(ValueCategory.UNKNOWN),
    GEOMETRY_ARRAY(ValueCategory.GEOMETRY_ARRAY),
    ZONED_DATE_TIME_ARRAY(TEMPORAL_ARRAY),
    LOCAL_DATE_TIME_ARRAY(TEMPORAL_ARRAY),
    DATE_ARRAY(TEMPORAL_ARRAY),
    ZONED_TIME_ARRAY(TEMPORAL_ARRAY),
    LOCAL_TIME_ARRAY(TEMPORAL_ARRAY),
    DURATION_ARRAY(TEMPORAL_ARRAY),
    TEXT_ARRAY(ValueCategory.TEXT_ARRAY),
    BOOLEAN_ARRAY(ValueCategory.BOOLEAN_ARRAY),
    NUMBER_ARRAY(ValueCategory.NUMBER_ARRAY),
    GEOMETRY(ValueCategory.GEOMETRY),
    ZONED_DATE_TIME(TEMPORAL),
    LOCAL_DATE_TIME(TEMPORAL),
    DATE(TEMPORAL),
    ZONED_TIME(TEMPORAL),
    LOCAL_TIME(TEMPORAL),
    DURATION(TEMPORAL),
    TEXT(ValueCategory.TEXT),
    BOOLEAN(ValueCategory.BOOLEAN),
    NUMBER(ValueCategory.NUMBER),
    STREAMING(ValueCategory.STREAMING),
    NO_VALUE(NO_CATEGORY);

    private final ValueCategory category;

    ValueGroup(ValueCategory category) {
        this.category = category;
    }

    public ValueCategory category() {
        return category;
    }
}
