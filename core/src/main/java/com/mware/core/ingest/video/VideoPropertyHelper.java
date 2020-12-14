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
package com.mware.core.ingest.video;

import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.MediaBcSchema;
import com.mware.core.util.RowKeyHelper;
import com.mware.ge.Property;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VideoPropertyHelper {
    private static final Pattern START_TIME_AND_END_TIME_PATTERN = Pattern.compile("^(.*)" + RowKeyHelper.FIELD_SEPARATOR + MediaBcSchema.VIDEO_FRAME.getPropertyName() + RowKeyHelper.FIELD_SEPARATOR + "([0-9]+)" + RowKeyHelper.FIELD_SEPARATOR + "([0-9]+)");
    private static final Pattern START_TIME_ONLY_PATTERN = Pattern.compile("^(.*)" + RowKeyHelper.FIELD_SEPARATOR + MediaBcSchema.VIDEO_FRAME.getPropertyName() + RowKeyHelper.FIELD_SEPARATOR + "([0-9]+)");

    public static VideoFrameInfo getVideoFrameInfoFromProperty(Property property) {
        String mimeType = BcSchema.MIME_TYPE_METADATA.getMetadataValueOrDefault(property.getMetadata(), null);
        if (mimeType == null || !mimeType.equals("text/plain")) {
            return null;
        }
        return getVideoFrameInfo(property.getKey());
    }

    public static VideoFrameInfo getVideoFrameInfo(String propertyKey) {
        Matcher m = START_TIME_AND_END_TIME_PATTERN.matcher(propertyKey);
        if (m.find()) {
            return new VideoFrameInfo(Long.parseLong(m.group(2)), Long.parseLong(m.group(3)), m.group(1));
        }

        m = START_TIME_ONLY_PATTERN.matcher(propertyKey);
        if (m.find()) {
            return new VideoFrameInfo(Long.parseLong(m.group(2)), null, m.group(1));
        }
        return null;
    }
}
