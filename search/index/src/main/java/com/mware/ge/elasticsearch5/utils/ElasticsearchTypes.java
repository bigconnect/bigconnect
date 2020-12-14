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
package com.mware.ge.elasticsearch5.utils;

import com.mware.ge.GeException;
import com.mware.ge.type.GeoShape;
import com.mware.ge.values.storable.*;

public class ElasticsearchTypes {
    public static String fromJavaClass(Class<? extends Value> clazz) {
        if (TextValue.class.isAssignableFrom(clazz)) {
            return "text";
        } else if (FloatValue.class.isAssignableFrom(clazz)) {
            return "float";
        } else if (DoubleValue.class.isAssignableFrom(clazz)) {
            return "double";
        } else if (ByteValue.class.isAssignableFrom(clazz)) {
            return "byte";
        } else if (ShortValue.class.isAssignableFrom(clazz)) {
            return "short";
        } else if (IntValue.class.isAssignableFrom(clazz)) {
            return "integer";
        } else if (LongValue.class.isAssignableFrom(clazz)) {
            return "long";
        } else if (TemporalValue.class.isAssignableFrom(clazz)) {
            return "date";
        } else if (BooleanValue.class.isAssignableFrom(clazz)) {
            return "boolean";
        } else if (GeoPointValue.class.isAssignableFrom(clazz)) {
            return "geo_point";
        } else if (GeoShape.class.isAssignableFrom(clazz)) {
            return "geo_shape";
        } else if (NumberValue.class.isAssignableFrom(clazz)) {
            return "double";
        } else {
            throw new GeException("Unexpected value type for property: " + clazz.getName());
        }
    }
}
