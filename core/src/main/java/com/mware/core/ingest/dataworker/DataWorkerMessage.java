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
package com.mware.core.ingest.dataworker;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.mware.core.exception.BcException;
import com.mware.core.model.workQueue.Priority;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class DataWorkerMessage {
    private static final ObjectMapper mapper;

    static {
        mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(SerializationFeature.WRITE_SINGLE_ELEM_ARRAYS_UNWRAPPED, true);
        mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
    }

    private String workspaceId;
    private String visibilitySource;
    private Priority priority;
    private boolean traceEnabled;
    private Property[] properties;
    private String[] graphVertexId;
    private String[] graphEdgeId;
    private String propertyKey;
    private String propertyName;
    private ElementOrPropertyStatus status;
    private Long beforeActionTimestamp;

    public String getWorkspaceId() {
        return workspaceId;
    }

    public DataWorkerMessage setWorkspaceId(String workspaceId) {
        this.workspaceId = workspaceId;
        return this;
    }

    public String getVisibilitySource() {
        return visibilitySource;
    }

    public DataWorkerMessage setVisibilitySource(String visibilitySource) {
        this.visibilitySource = visibilitySource;
        return this;
    }

    public Priority getPriority() {
        return priority;
    }

    public DataWorkerMessage setPriority(Priority priority) {
        checkNotNull(priority, "priority cannot be null");
        this.priority = priority;
        return this;
    }

    public boolean isTraceEnabled() {
        return traceEnabled;
    }

    public void setTraceEnabled(boolean traceEnabled) {
        this.traceEnabled = traceEnabled;
    }

    public String getPropertyKey() {
        return propertyKey;
    }

    public DataWorkerMessage setPropertyKey(String propertyKey) {
        this.propertyKey = propertyKey;
        return this;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public DataWorkerMessage setPropertyName(String propertyName) {
        this.propertyName = propertyName;
        return this;
    }

    public String[] getGraphEdgeId() {
        return graphEdgeId;
    }

    public DataWorkerMessage setGraphEdgeId(String[] graphEdgeId) {
        this.graphEdgeId = graphEdgeId;
        return this;
    }

    public String[] getGraphVertexId() {
        return graphVertexId;
    }

    public DataWorkerMessage setGraphVertexId(String[] graphVertexId) {
        this.graphVertexId = graphVertexId;
        return this;
    }

    public ElementOrPropertyStatus getStatus() {
        return status;
    }

    public DataWorkerMessage setStatus(ElementOrPropertyStatus status) {
        this.status = status;
        return this;
    }

    public Long getBeforeActionTimestamp() {
        return beforeActionTimestamp;
    }

    @JsonIgnore
    public long getBeforeActionTimestampOrDefault() {
        return getBeforeActionTimestamp() == null ? -1L : getBeforeActionTimestamp();
    }

    public DataWorkerMessage setBeforeActionTimestamp(Long beforeActionTimestamp) {
        this.beforeActionTimestamp = beforeActionTimestamp;
        return this;
    }

    public DataWorkerMessage.Property[] getProperties() {
        return properties;
    }

    public DataWorkerMessage setProperties(Property[] properties) {
        this.properties = properties;
        return this;
    }

    public static DataWorkerMessage create(byte[] data) {
        try {
            DataWorkerMessage message = mapper.readValue(data, DataWorkerMessage.class);
            checkNotNull(message.getPriority(), "priority cannot be null");
            return message;
        } catch (IOException e) {
            throw new BcException("Could not create " + DataWorkerMessage.class.getName() + " from " + new String(data), e);
        }
    }

    public String toJsonString() {
        try {
            checkNotNull(getPriority(), "priority cannot be null");
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new BcException("Could not write " + this.getClass().getName(), e);
        }
    }

    public byte[] toBytes() {
        try {
            checkNotNull(getPriority(), "priority cannot be null");
            return mapper.writeValueAsBytes(this);
        } catch (JsonProcessingException e) {
            throw new BcException("Could not write " + this.getClass().getName(), e);
        }
    }

    public static class Property {
        private String propertyKey;
        private String propertyName;
        private ElementOrPropertyStatus status;
        private Long beforeActionTimestamp;

        public String getPropertyKey() {
            return propertyKey;
        }

        public Property setPropertyKey(String propertyKey) {
            this.propertyKey = propertyKey;
            return this;
        }

        public String getPropertyName() {
            return propertyName;
        }

        public Property setPropertyName(String propertyName) {
            this.propertyName = propertyName;
            return this;
        }

        public ElementOrPropertyStatus getStatus() {
            return status;
        }

        public Property setStatus(ElementOrPropertyStatus status) {
            this.status = status;
            return this;
        }

        public Long getBeforeActionTimestamp() {
            return beforeActionTimestamp;
        }

        @JsonIgnore
        public long getBeforeActionTimestampOrDefault() {
            return getBeforeActionTimestamp() == null ? -1L : getBeforeActionTimestamp();
        }

        public Property setBeforeActionTimestamp(Long beforeActionTimestamp) {
            this.beforeActionTimestamp = beforeActionTimestamp;
            return this;
        }
    }
}
