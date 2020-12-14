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
package com.mware.ge.accumulo;

import com.mware.ge.accumulo.iterator.ElementIterator;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.io.Serializable;

public interface AccumuloElement extends Serializable {
    Text CF_PROPERTY = ElementIterator.CF_PROPERTY;
    Text CF_PROPERTY_METADATA = ElementIterator.CF_PROPERTY_METADATA;
    Text CF_PROPERTY_SOFT_DELETE = ElementIterator.CF_PROPERTY_SOFT_DELETE;
    Text CF_EXTENDED_DATA = ElementIterator.CF_EXTENDED_DATA;
    Value SOFT_DELETE_VALUE = ElementIterator.SOFT_DELETE_VALUE;
    Value HIDDEN_VALUE = ElementIterator.HIDDEN_VALUE;
    Text CF_PROPERTY_HIDDEN = ElementIterator.CF_PROPERTY_HIDDEN;
    Value HIDDEN_VALUE_DELETED = ElementIterator.HIDDEN_VALUE_DELETED;
    Text DELETE_ROW_COLUMN_FAMILY = ElementIterator.DELETE_ROW_COLUMN_FAMILY;
    Text DELETE_ROW_COLUMN_QUALIFIER = ElementIterator.DELETE_ROW_COLUMN_QUALIFIER;
    Text CF_SOFT_DELETE = ElementIterator.CF_SOFT_DELETE;
    Text CQ_SOFT_DELETE = ElementIterator.CQ_SOFT_DELETE;
    Text CF_HIDDEN = ElementIterator.CF_HIDDEN;
    Text CQ_HIDDEN = ElementIterator.CQ_HIDDEN;
    Text METADATA_COLUMN_FAMILY = ElementIterator.METADATA_COLUMN_FAMILY;
    Text METADATA_COLUMN_QUALIFIER = ElementIterator.METADATA_COLUMN_QUALIFIER;
}
