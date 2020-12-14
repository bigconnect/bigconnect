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
package com.mware.core.util;

import com.mware.ge.values.storable.Value;
import com.mware.ge.values.storable.Values;
import org.json.JSONObject;
import com.mware.ge.Metadata;
import com.mware.ge.Visibility;

public class GeMetadataUtil {
    public static Metadata metadataStringToMap(String metadataString, Visibility visibility) {
        Metadata metadata = Metadata.create();
        if (metadataString != null && metadataString.length() > 0) {
            JSONObject metadataJson = new JSONObject(metadataString);
            for (Object keyObj : metadataJson.keySet()) {
                String key = "" + keyObj;
                Object valueObject = metadataJson.get(key);
                Value v = valueObject instanceof Value ? (Value) valueObject : Values.of(valueObject);
                metadata.add(key, v, visibility);
            }
        }
        return metadata;
    }

    public static Metadata mergeMetadata(Metadata... metadatas) {
        Metadata mergedMetadata = Metadata.create();
        for (Metadata metadata : metadatas) {
            for (Metadata.Entry entry : metadata.entrySet()) {
                mergedMetadata.add(entry.getKey(), entry.getValue(), entry.getVisibility());
            }
        }
        return mergedMetadata;
    }
}
