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

import com.google.common.collect.Lists;
import com.mware.core.exception.BcException;
import com.mware.core.ingest.video.VideoFrameInfo;
import com.mware.core.ingest.video.VideoPropertyHelper;
import com.mware.core.model.clientapi.dto.*;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.MediaBcSchema;
import com.mware.core.security.BcVisibility;
import com.mware.ge.*;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.type.GeoPoint;
import com.mware.ge.type.GeoRect;
import com.mware.ge.util.IterableUtils;
import com.mware.ge.values.storable.TextValue;
import com.mware.ge.values.storable.Value;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.mware.core.util.StreamUtil.stream;

public class ClientApiConverter extends com.mware.core.model.clientapi.util.ClientApiConverter {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(ClientApiConverter.class);

    public static final FetchHints SEARCH_FETCH_HINTS = new FetchHintsBuilder()
            .setIncludeAllProperties(true)
            .setIncludeAllPropertyMetadata(true)
            .setIncludeEdgeLabelsAndCounts(true)
            .setIncludeExtendedDataTableNames(true)
            .build();

    private static final int HISTORICAL_PROPERTY_MAX_SPV_SIZE = 2000;

    public static List<? extends ClientApiGeObject> toClientApi(
            Iterable<? extends com.mware.ge.GeObject> geObjects,
            String workspaceId,
            Authorizations authorizations
    ) {
        return toClientApi(geObjects, workspaceId, false, authorizations);
    }

    public static List<? extends ClientApiGeObject> toClientApi(
            Iterable<? extends GeObject> geObjects,
            String workspaceId,
            boolean includeEdgeInfos,
            Authorizations authorizations
    ) {
        List<ClientApiGeObject> results = new ArrayList<>();
        for (GeObject geObject : geObjects) {
            results.add(toClientApi(geObject, workspaceId, includeEdgeInfos, authorizations));
        }
        return results;
    }

    public static List<ClientApiElement> toClientApiTermMention(
            Iterable<Property> elements,
            String workspaceId,
            boolean includeEdgeInfos,
            Authorizations authorizations
    ) {
        List<ClientApiElement> clientApiElements = new ArrayList<>();
        for (Property element : elements) {
            clientApiElements.add(toClientApiTermMention(element, workspaceId, includeEdgeInfos, authorizations));
        }
        return clientApiElements;
    }

    public static List<ClientApiVertex> toClientApiVertices(
            Iterable<? extends Vertex> vertices,
            String workspaceId,
            Authorizations authorizations
    ) {
        List<ClientApiVertex> clientApiElements = new ArrayList<>();
        for (Vertex v : vertices) {
            clientApiElements.add(toClientApiVertex(v, workspaceId, authorizations));
        }
        return clientApiElements;
    }

    public static ClientApiGeObject toClientApi(
            GeObject geObject,
            String workspaceId,
            Authorizations authorizations
    ) {
        return toClientApi(geObject, workspaceId, false, authorizations);
    }

    public static ClientApiGeObject toClientApi(
            GeObject geObject,
            String workspaceId,
            boolean includeEdgeInfos,
            Authorizations authorizations
    ) {
        checkNotNull(geObject, "geObject cannot be null");
        if (geObject  instanceof Vertex) {
            return toClientApiVertex((Vertex) geObject, workspaceId, null, includeEdgeInfos, authorizations);
        }
        if (geObject  instanceof Edge) {
            return toClientApiEdge((Edge) geObject, workspaceId);
        }
        if (geObject instanceof ExtendedDataRow) {
            return toClientApiExtendedDataRow((ExtendedDataRow) geObject, workspaceId);
        }
        throw new RuntimeException("Unexpected geObject type: " + geObject.getClass().getName());
    }

    public static ClientApiElement toClientApiTermMention(
            Property element,
            String workspaceId,
            boolean includeEdgeInfos,
            Authorizations authorizations
    ) {
        checkNotNull(element, "element cannot be null");

        throw new UnsupportedOperationException("Not implemented");
        //return toClientApiVertex((Vertex) element, workspaceId, null, includeEdgeInfos, authorizations);
    }

    public static ClientApiVertex toClientApiVertex(
            Vertex vertex,
            String workspaceId,
            Authorizations authorizations
    ) {
        return toClientApiVertex(vertex, workspaceId, null, authorizations);
    }

    public static ClientApiVertex toClientApiVertex(
            Vertex vertex,
            String workspaceId,
            Integer commonCount,
            Authorizations authorizations
    ) {
        return toClientApiVertex(vertex, workspaceId, commonCount, false, authorizations);
    }

    /**
     * @param commonCount the number of vertices this vertex has in common with other vertices.
     */
    public static ClientApiVertex toClientApiVertex(
            Vertex vertex,
            String workspaceId,
            Integer commonCount,
            boolean includeEdgeInfos,
            Authorizations authorizations
    ) {
        checkNotNull(vertex, "vertex is required");
        ClientApiVertex v = new ClientApiVertex();

        if (authorizations != null) {
            StreamUtil.stream(vertex.getEdgeLabels(Direction.BOTH, authorizations))
                    .forEach(v::addEdgeLabel);

            if (includeEdgeInfos) {
                StreamUtil.stream(vertex.getEdgeInfos(Direction.BOTH, authorizations))
                        .map(ClientApiConverter::toClientApi)
                        .forEach(v::addEdgeInfo);
            }
        }

        populateClientApiElement(v, vertex, workspaceId);

        // backwards compatibility - set conceptType as a property
        ClientApiProperty conceptType = new ClientApiProperty();
        conceptType.setKey("");
        conceptType.setName("conceptType");
        conceptType.setValue(vertex.getConceptType());

        v.getProperties().add(conceptType);
        v.setCommonCount(commonCount);
        return v;
    }

    private static ClientApiEdgeInfo toClientApi(EdgeInfo edgeInfo) {
        return new ClientApiEdgeInfo(
                edgeInfo.getEdgeId(),
                edgeInfo.getLabel(),
                edgeInfo.getVertexId()
        );
    }

    public static ClientApiEdge toClientApiEdge(Edge edge, String workspaceId) {
        ClientApiEdge e = new ClientApiEdge();
        populateClientApiEdge(e, edge, workspaceId);
        return e;
    }

    public static ClientApiEdgeWithVertexData toClientApiEdgeWithVertexData(
            Edge edge,
            Vertex source,
            Vertex target,
            String workspaceId,
            Authorizations authorizations
    ) {
        checkNotNull(source, "source vertex is required");
        checkNotNull(target, "target vertex is required");
        ClientApiEdgeWithVertexData e = new ClientApiEdgeWithVertexData();
        e.setSource(toClientApiVertex(source, workspaceId, authorizations));
        e.setTarget(toClientApiVertex(target, workspaceId, authorizations));
        populateClientApiEdge(e, edge, workspaceId);
        return e;
    }

    public static void populateClientApiEdge(ClientApiEdge e, Edge edge, String workspaceId) {
        e.setLabel(edge.getLabel());
        e.setOutVertexId(edge.getVertexId(Direction.OUT));
        e.setInVertexId(edge.getVertexId(Direction.IN));

        populateClientApiElement(e, edge, workspaceId);
    }

    private static void populateClientApiElement(
            ClientApiElement clientApiElement,
            com.mware.ge.Element element,
            String workspaceId
    ) {
        clientApiElement.setId(element.getId());
        clientApiElement.getProperties().addAll(toClientApiProperties(element.getProperties(), workspaceId));
        clientApiElement.getExtendedDataTableNames().addAll(element.getExtendedDataTableNames());
        clientApiElement.setSandboxStatus(SandboxStatusUtil.getSandboxStatus(element, workspaceId));

        VisibilityJson visibilityJson = BcSchema.VISIBILITY_JSON.getPropertyValue(element);
        if (visibilityJson != null) {
            clientApiElement.setVisibilitySource(visibilityJson.getSource());
        }

        if (clientApiElement instanceof ClientApiVertex) {
            ClientApiVertex clientApiVertex = (ClientApiVertex) clientApiElement;
            Vertex vertex = (Vertex) element;
            clientApiVertex.setConceptType(vertex.getConceptType());
        }
    }

    public static List<ClientApiProperty> toClientApiProperties(Iterable<Property> properties, String workspaceId) {
        List<ClientApiProperty> clientApiProperties = new ArrayList<>();
        List<Property> propertiesList = IterableUtils.toList(properties);
        SandboxStatus[] sandboxStatuses = SandboxStatusUtil.getPropertySandboxStatuses(propertiesList, workspaceId);
        for (int i = 0; i < propertiesList.size(); i++) {
            Property property = propertiesList.get(i);
            SandboxStatus sandboxStatus = sandboxStatuses[i];
            VideoFrameInfo videoFrameInfo;
            if ((videoFrameInfo = VideoPropertyHelper.getVideoFrameInfoFromProperty(property)) != null) {
                String textDescription = BcSchema.TEXT_DESCRIPTION_METADATA.getMetadataValueOrDefault(
                        property.getMetadata(),
                        null
                );
                addVideoFramePropertyToResults(
                        clientApiProperties,
                        videoFrameInfo.getPropertyKey(),
                        textDescription,
                        sandboxStatus
                );
            } else {
                ClientApiProperty clientApiProperty = toClientApiProperty(property);
                clientApiProperty.setSandboxStatus(sandboxStatus);
                clientApiProperties.add(clientApiProperty);
            }
        }
        return clientApiProperties;
    }

    public static ClientApiProperty toClientApiProperty(Property property) {
        ClientApiProperty clientApiProperty = new ClientApiProperty();
        clientApiProperty.setKey(property.getKey());
        clientApiProperty.setName(property.getName());

        Value propertyValue = property.getValue();
        if (propertyValue instanceof StreamingPropertyValue) {
            clientApiProperty.setStreamingPropertyValue(true);
        } else {
            clientApiProperty.setValue(toClientApiValue(propertyValue));
        }

        for (Metadata.Entry entry : property.getMetadata().entrySet()) {
            clientApiProperty.getMetadata().put(entry.getKey(), toClientApiValue(entry.getValue()));
        }

        return clientApiProperty;
    }

    private static void addVideoFramePropertyToResults(
            List<ClientApiProperty> clientApiProperties,
            String propertyKey,
            String textDescription,
            SandboxStatus sandboxStatus
    ) {
        ClientApiProperty clientApiProperty = findProperty(
                clientApiProperties,
                MediaBcSchema.VIDEO_TRANSCRIPT.getPropertyName(),
                propertyKey
        );
        if (clientApiProperty == null) {
            clientApiProperty = new ClientApiProperty();
            clientApiProperty.setKey(propertyKey);
            clientApiProperty.setName(MediaBcSchema.VIDEO_TRANSCRIPT.getPropertyName());
            clientApiProperty.setSandboxStatus(sandboxStatus);
            clientApiProperty.getMetadata().put(
                    BcSchema.TEXT_DESCRIPTION_METADATA.getMetadataKey(),
                    textDescription
            );
            clientApiProperty.setStreamingPropertyValue(true);
            clientApiProperties.add(clientApiProperty);
        }
    }

    private static ClientApiProperty findProperty(
            List<ClientApiProperty> clientApiProperties,
            String propertyName,
            String propertyKey
    ) {
        for (ClientApiProperty property : clientApiProperties) {
            if (property.getName().equals(propertyName) && property.getKey().equals(propertyKey)) {
                return property;
            }
        }
        return null;
    }

    /**
     * Sort HistoricalPropertyValue chronologically and generate events by looking at the
     * deltas between the historical values.
     */
    public static ClientApiHistoricalPropertyResults calculateHistoricalPropertyDeltas(
            Iterable<HistoricalPropertyValue> historicalPropertyValues, Locale locale, ResourceBundle resourceBundle,
            boolean withVisibility
    ) {

        ClientApiHistoricalPropertyResults result = new ClientApiHistoricalPropertyResults();

        // Sort chronologically
        List<HistoricalPropertyValue> sortedHistoricalValues = Lists.newArrayList(historicalPropertyValues);
        sortedHistoricalValues.sort(Collections.reverseOrder());

        Map<String, HistoricalPropertyValue> cachedValues = new HashMap<>();
        ClientApiHistoricalPropertyResults.Event event;
        HistoricalPropertyValue conceptTypeHpv;

        int i = 0;
        for (HistoricalPropertyValue hpv : sortedHistoricalValues) {
            String key = hpv.getPropertyKey() + hpv.getPropertyName();
            HistoricalPropertyValue cached = cachedValues.get(key);
            event = null;

            if (cached == null) { // Add
                if (hpv.getPropertyName().equals("conceptType")) {
                    conceptTypeHpv = hpv;
                    HistoricalPropertyValue modifiedByHpv = cachedValues.get(BcSchema.MODIFIED_BY.getPropertyName());

                    int j = i;
                    while (modifiedByHpv == null && j < sortedHistoricalValues.size()) {
                        HistoricalPropertyValue v = sortedHistoricalValues.get(j);
                        if (v.getPropertyName().equals(BcSchema.MODIFIED_BY.getPropertyName())) {
                            modifiedByHpv = v;
                        }
                        j++;
                    }

                    event = generatePropertyAddedEvent(conceptTypeHpv, locale, resourceBundle, withVisibility);

                    if (modifiedByHpv != null) {
                        // Use the ModifiedBy property to complete the ConceptType event
                        ClientApiHistoricalPropertyResults.Event modifiedByEvent = generatePropertyAddedEvent(modifiedByHpv, locale, resourceBundle, withVisibility);
                        event.modifiedBy = modifiedByEvent.fields.get("value");
                    }
                } else {
                    event = generatePropertyAddedEvent(hpv, locale, resourceBundle, withVisibility);
                    cachedValues.put(key, hpv);
                }
            } else if (hpv.isDeleted()) {  // Delete
                // Non-consecutive delete events
                if (hpv.isDeleted() != cached.isDeleted()) {
                    event = generatePropertyDeletedEvent(hpv, locale, resourceBundle, withVisibility);
                }
                cachedValues.remove(key);
            } else { // Check if modified
                if (hasHistoricalPropertyChanged(cached, hpv, withVisibility)) {
                    event = generatePropertyModifiedEvent(hpv, cached, locale, resourceBundle, withVisibility);
                } else {
                    LOGGER.debug("Historical property value did not change. Ignore");
                    LOGGER.debug("  was:" + hpv);
                }

                cachedValues.put(key, hpv);
            }

            if (event != null) {
                result.events.add(event);
            }

            i++;
        }

        return result;
    }

    private static ClientApiHistoricalPropertyResults.Event generateGenericEvent(HistoricalPropertyValue hpv) {
        ClientApiHistoricalPropertyResults.Event event = new ClientApiHistoricalPropertyResults.Event();
        event.timestamp = hpv.getTimestamp();
        event.propertyKey = hpv.getPropertyKey();
        event.propertyName = hpv.getPropertyName();
        Metadata.Entry modifiedByEntry = (hpv.getMetadata() != null) ? hpv.getMetadata().getEntry(BcSchema.MODIFIED_BY.getPropertyName()) : null;
        event.modifiedBy = ((modifiedByEntry != null) ? toClientApiValue(modifiedByEntry.getValue()).toString() : null);
        return event;
    }

    private static ClientApiHistoricalPropertyResults.Event generatePropertyAddedEvent(
            HistoricalPropertyValue hpv,
            Locale locale,
            ResourceBundle resourceBundle,
            boolean withVisibility
    ) {
        ClientApiHistoricalPropertyResults.Event event = generateGenericEvent(hpv);
        event.setEventType(ClientApiHistoricalPropertyResults.EventType.PROPERTY_ADDED);
        Map<String, String> fields =new HashMap<>();

        Object value = hpv.getValue();
        if (value instanceof StreamingPropertyValue) {
            value = readStreamingPropertyValueForHistory((StreamingPropertyValue) value, locale, resourceBundle);
        }
        fields.put("value", toClientApiValue(value).toString());

        if (withVisibility) {
            fields.put("visibility", removeWorkspaceVisibility(hpv.getPropertyVisibility().getVisibilityString()));
        }
        event.fields = fields;
        event.changed = null;
        return event;
    }

    private static ClientApiHistoricalPropertyResults.Event generatePropertyDeletedEvent(
            HistoricalPropertyValue hpv,
            Locale locale,
            ResourceBundle resourceBundle,
            boolean withVisibility
    ) {
        ClientApiHistoricalPropertyResults.Event event = generateGenericEvent(hpv);
        event.setEventType(ClientApiHistoricalPropertyResults.EventType.PROPERTY_DELETED);
        Map<String, String> fields = new HashMap<>();
        Map<String, String> changed = new HashMap<>();

        Object value = hpv.getValue();

        if (value != null) {
            if (value instanceof StreamingPropertyValue) {
                value = readStreamingPropertyValueForHistory((StreamingPropertyValue) value, locale, resourceBundle);
            }
            changed.put("value", toClientApiValue(value).toString());
        }

        if (withVisibility) {
            changed.put("visibility", removeWorkspaceVisibility(hpv.getPropertyVisibility().getVisibilityString()));
        }
        event.fields = null;
        event.changed = changed;

        return event;
    }

    private static ClientApiHistoricalPropertyResults.Event generatePropertyModifiedEvent(
            HistoricalPropertyValue hpv,
            HistoricalPropertyValue cached,
            Locale locale,
            ResourceBundle resourceBundle,
            boolean withVisibility
    ) {
        ClientApiHistoricalPropertyResults.Event event = generateGenericEvent(hpv);
        event.setEventType(ClientApiHistoricalPropertyResults.EventType.PROPERTY_MODIFIED);

        Map<String, String> fields = new HashMap<>();
        Map<String, String> changed = new HashMap<>();

        Object value = hpv.getValue();
        if (value instanceof StreamingPropertyValue) {
            value = readStreamingPropertyValueForHistory((StreamingPropertyValue) value, locale, resourceBundle);
        }
        fields.put("value", toClientApiValue(value).toString());
        if (!hpv.getValue().equals(cached.getValue())) {
            changed.put("value", toClientApiValue(cached.getValue()).toString());
        }

        if (withVisibility) {
            String currentVis = removeWorkspaceVisibility(hpv.getPropertyVisibility().getVisibilityString());
            String previousVis = removeWorkspaceVisibility(cached.getPropertyVisibility().getVisibilityString());
            fields.put("visibility", currentVis);
            if (!currentVis.equals(previousVis)) {
                changed.put("visibility", previousVis);
            }
        }
        event.fields = fields;
        event.changed = changed;

        return event;
    }

    private static boolean hasHistoricalPropertyChanged(
            HistoricalPropertyValue previous,
            HistoricalPropertyValue current,
            boolean withVisibility
    ) {
        if (!current.getValue().equals(previous.getValue())) {
            return true;
        }

        String currentVis = removeWorkspaceVisibility(current.getPropertyVisibility().getVisibilityString());
        String previousVis = removeWorkspaceVisibility(previous.getPropertyVisibility().getVisibilityString());
        if (withVisibility && !currentVis.equals(previousVis)) {
            return true;
        }

        return false;
    }

    public static ClientApiHistoricalPropertyResults toClientApi(
            Iterable<HistoricalPropertyValue> historicalPropertyValues,
            Locale locale,
            ResourceBundle resourceBundle,
            boolean withVisibility
    ) {
        return calculateHistoricalPropertyDeltas(historicalPropertyValues, locale, resourceBundle, withVisibility);
    };

    private static String readStreamingPropertyValueForHistory(
            StreamingPropertyValue spv,
            Locale locale,
            ResourceBundle resourceBundle
    ) {
        if (TextValue.class.isAssignableFrom(spv.getValueType())) {
            return readStreamingPropertyValueStringForHistory(spv);
        } else {
            return String.format(locale, resourceBundle.getString("history.nondisplayable"), spv.getLength());
        }
    }

    private static String readStreamingPropertyValueStringForHistory(StreamingPropertyValue spv) {
        try (InputStream in = spv.getInputStream()) {
            byte[] buffer = new byte[HISTORICAL_PROPERTY_MAX_SPV_SIZE];
            int bytesRead = in.read(buffer, 0, HISTORICAL_PROPERTY_MAX_SPV_SIZE);
            if (bytesRead < 0) {
                return "";
            }
            return new String(buffer, 0, bytesRead);
        } catch (IOException ex) {
            throw new BcException("Could not read StreamingPropertyValue", ex);
        }
    }

    public static ClientApiGeoPoint toClientApiGeoPoint(GeoPoint geoPoint) {
        return new ClientApiGeoPoint(geoPoint.getLatitude(), geoPoint.getLongitude());
    }

    public static ClientApiGeoRect toClientApiGeoRect(GeoRect rect) {
        return new ClientApiGeoRect(
                toClientApiGeoPoint(rect.getNorthWest()),
                toClientApiGeoPoint(rect.getSouthEast())
        );
    }

    public static String removeWorkspaceVisibility(String visibility) {
        return Arrays.stream(visibility.split("\\|"))
                .map(s -> s.replaceAll("&?\\(WORKSPACE_.*?\\)", "").replace(BcVisibility.SUPER_USER_VISIBILITY_STRING, ""))
                .filter(s -> s.length() > 0 && !s.equals("()"))
                .collect(Collectors.joining("|"));
    }

    public static List<ClientApiExtendedDataRow> toClientApiExtendedDataRows(Iterable<ExtendedDataRow> rows) {
        return stream(rows).map(ClientApiConverter::toClientApiExtendedDataRow).collect(Collectors.toList());
    }

    public static ClientApiExtendedDataRow toClientApiExtendedDataRow(ExtendedDataRow row) {
        return toClientApiExtendedDataRow(row, null);
    }

    public static ClientApiExtendedDataRow toClientApiExtendedDataRow(ExtendedDataRow row, String workspaceId) {
        ClientApiExtendedDataRow results = new ClientApiExtendedDataRow(toClientApiExtendedDataRowId(row.getId()));
        results.getProperties().addAll(toClientApiProperties(row.getProperties(), workspaceId));
        return results;
    }

    public static ClientApiExtendedDataRowId toClientApiExtendedDataRowId(ExtendedDataRowId id) {
        return new ClientApiExtendedDataRowId(
                id.getElementType().name(),
                id.getElementId(),
                id.getTableName(),
                id.getRowId()
        );
    }

    public static JSONObject clientApiToJSONObject(Object obj) {
        return new JSONObject(clientApiToString(obj));
    }

    public static Long toClientApiDate(Date date) {
        if (date == null) {
            return null;
        }
        return date.getTime();
    }

    public static Long toClientApiDate(ZonedDateTime date) {
        if (date == null) {
            return null;
        }
        return date.toInstant().toEpochMilli();
    }
}
