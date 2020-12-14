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
package com.mware.core.model.properties.types;

import com.mware.core.exception.BcException;
import com.mware.core.ingest.video.VideoTranscript;
import com.mware.ge.values.storable.ByteArray;
import com.mware.ge.values.storable.DefaultStreamingPropertyValue;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.Value;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class VideoTranscriptProperty extends BcProperty<VideoTranscript> {
    public VideoTranscriptProperty(String inKey) {
        super(inKey);
    }

    @Override
    public StreamingPropertyValue wrap(VideoTranscript value) {
        InputStream in = new ByteArrayInputStream(value.toJson().toString().getBytes());
        StreamingPropertyValue result = new DefaultStreamingPropertyValue(in, ByteArray.class);
        result.searchIndex(false);
        return result;
    }

    @Override
    public VideoTranscript unwrap(Value value) {
        String strValue = null;
        if (value instanceof StreamingPropertyValue) {
            try {
                strValue = IOUtils.toString(((StreamingPropertyValue) value).getInputStream());
            } catch (IOException e) {
                throw new BcException("Could not read propery value", e);
            }
        } else if (value != null) {
            strValue = value.toString();
        }
        JSONObject json = new JSONObject(strValue);
        return new VideoTranscript(json);
    }
}
