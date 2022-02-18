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
 *
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
package com.mware.core.ingest.database.model;

import com.mware.core.model.clientapi.dto.ClientApiObject;
import com.mware.core.model.clientapi.util.ClientApiConverter;
import lombok.Getter;
import lombok.Setter;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class ClientApiDataSource implements ClientApiObject {
    private String dsId;
    private String dcId;
    private String name;
    private String description;
    private long maxRecords = -1;
    private String sqlSelect;
    private String lastImportDate;
    private boolean importRunning;
    private String type = "datasource-ingest";
    private List<EntityMapping> entityMappings = new ArrayList<>();
    private List<RelMapping> relMappings = new ArrayList<>();
    private ImportConfig importConfig;
    private String[] authorizations;
    private String userId;
    private String workspaceId;
    private boolean importEntitiesToDictionaries = true;
    private boolean canceled;
    private int commitBatchSize = 1000;

    public JSONObject toJson() {
        return new JSONObject(ClientApiConverter.clientApiToString(this));
    }

    @Getter
    @Setter
    public static class EntityMapping {
        public String colName;
        private String colType;
        private String colConcept;
        private String colEntityId;
        private String colProperty;
        private String colPropertyType;
        private boolean colIdentifier;
        private String colEntityVisibility;
        private String colPropVisibility;
        private String colPropTrueValues;
        private String colPropFalseValues;
        private String colPropDateFormat;
    }

    @Getter
    @Setter
    public static class RelMapping {
        private String id;
        private String rel;
        private String relVisibility;
        private String sourceId;
        private String sourceType;
        private String targetId;
        private String targetType;
    }

    @Getter
    @Setter
    public static class ImportConfig {
        private String jobId;
        private boolean incremental;
        private String incrementalMode;
        private String checkColumn;
        private String lastValue;
        private boolean runNow;
        private String source;
    }
}
