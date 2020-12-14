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
package com.mware.ge.values;

import com.mware.ge.values.storable.Values;
import com.mware.ge.values.virtual.VirtualValueGroup;

import java.util.Comparator;

@SuppressWarnings("WeakerAccess")
public final class AnyValues {
    /**
     * Default AnyValue comparator. Will correctly compare all storable and virtual values.
     *
     * <h1>
     * Orderability
     *
     * <a href="https://github.com/opencypher/openCypher/blob/master/cip/1.accepted/CIP2016-06-14-Define-comparability-and-equality-as-well-as-orderability-and-equivalence.adoc">
     * The Cypher CIP defining orderability
     * </a>
     *
     * <p>
     * Ascending global sort order of disjoint types:
     *
     * <ul>
     *   <li> MAP types
     *   <ul>
     *     <li> Regular map
     *
     *     <li> NODE
     *
     *     <li> RELATIONSHIP
     *   </ul>
     *
     *  <li> LIST OF ANY?
     *
     *  <li> PATH
     *
     *  <li> POINT
     *
     *  <li> STRING
     *
     *  <li> BOOLEAN
     *
     *  <li> NUMBER
     *    <ul>
     *      <li> NaN values are treated as the largest numbers in orderability only (i.e. they are put after positive infinity)
     *    </ul>
     *  <li> VOID (i.e. the type of null)
     * </ul>
     */
    private static final AnyValueComparator comp = new AnyValueComparator(Values.COMPARATOR, VirtualValueGroup::compareTo);
    public static final Comparator<AnyValue> COMPARATOR = comp;
    public static final TernaryComparator<AnyValue> TERNARY_COMPARATOR = comp;

}
