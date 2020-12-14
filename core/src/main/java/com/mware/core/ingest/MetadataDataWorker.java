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
package com.mware.core.ingest;

import com.mware.core.ingest.dataworker.DataWorker;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.ingest.dataworker.ElementOrPropertyStatus;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.properties.BcSchema;
import com.mware.ge.Element;
import com.mware.ge.Metadata;
import com.mware.ge.Property;
import com.mware.ge.Visibility;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.Values;
import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;

import static com.google.common.base.Preconditions.checkNotNull;

@Name("Metadata Processor")
@Description("Adds properties to a vertex from a metadata JSON document")
public class MetadataDataWorker extends DataWorker {
    @Override
    public void execute(InputStream in, DataWorkerData data) throws Exception {
        JSONObject metadataJson = getMetadataJson(data);

        JSONArray propertiesJson = metadataJson.optJSONArray("properties");
        if (propertiesJson == null) {
            return;
        }

        for (int i = 0; i < propertiesJson.length(); i++) {
            JSONObject propertyJson = propertiesJson.getJSONObject(i);
            setProperty(propertyJson, data);
        }

        getGraph().flush();

        for (int i = 0; i < propertiesJson.length(); i++) {
            JSONObject propertyJson = propertiesJson.getJSONObject(i);
            queueProperty(propertyJson, data);
        }
    }

    public void queueProperty(JSONObject propertyJson, DataWorkerData data) {
        String propertyKey = propertyJson.optString("key");
        if (propertyKey == null) {
            propertyKey = ElementMutation.DEFAULT_KEY;
        }
        String propertyName = propertyJson.optString("name");

        if (getWebQueueRepository().shouldBroadcastGraphPropertyChange(propertyName, data.getPriority())) {
            getWebQueueRepository().broadcastPropertyChange(data.getElement(), propertyKey, propertyName, null);
        }

        getWorkQueueRepository().pushGraphPropertyQueue(
                data.getElement(),
                propertyKey,
                propertyName,
                null,
                null,
                data.getPriority(),
                ElementOrPropertyStatus.UPDATE,
                null
        );
    }

    private void setProperty(JSONObject propertyJson, DataWorkerData data) {
        String propertyKey = propertyJson.optString("key", null);
        if (propertyKey == null) {
            propertyKey = ElementMutation.DEFAULT_KEY;
        }

        String propertyName = propertyJson.optString("name", null);
        checkNotNull(propertyName, "name is required: " + propertyJson.toString());

        String propertyValue = propertyJson.optString("value", null);
        checkNotNull(propertyValue, "value is required: " + propertyJson.toString());

        String visibilitySource = propertyJson.optString("visibilitySource", null);
        Visibility visibility;
        if (visibilitySource == null) {
            visibility = data.getVisibility();
        } else {
            visibility = getVisibilityTranslator().toVisibility(visibilitySource).getVisibility();
        }

        Metadata metadata = Metadata.create();
        BcSchema.VISIBILITY_JSON_METADATA.setMetadata(metadata, VisibilityJson.updateVisibilitySource(null, visibilitySource), getVisibilityTranslator().getDefaultVisibility());

        data.getElement().addPropertyValue(propertyKey, propertyName, Values.stringValue(propertyValue), metadata, visibility, getAuthorizations());
    }

    public JSONObject getMetadataJson(DataWorkerData data) throws IOException {
        StreamingPropertyValue metadataJsonValue = BcSchema.METADATA_JSON.getPropertyValue(data.getElement());
        try (InputStream metadataJsonIn = metadataJsonValue.getInputStream()) {
            String metadataJsonString = IOUtils.toString(metadataJsonIn);
            return new JSONObject(metadataJsonString);
        }
    }

    @Override
    public boolean isHandled(Element element, Property property) {
        if (property != null) {
            return false;
        }

        StreamingPropertyValue mappingJson = BcSchema.METADATA_JSON.getPropertyValue(element);
        if (mappingJson == null) {
            return false;
        }

        return true;
    }
}
