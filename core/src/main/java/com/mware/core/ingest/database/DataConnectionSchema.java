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
package com.mware.core.ingest.database;

import com.mware.core.model.properties.types.BooleanSingleValueBcProperty;
import com.mware.core.model.properties.types.DateSingleValueBcProperty;
import com.mware.core.model.properties.types.LongSingleValueBcProperty;
import com.mware.core.model.properties.types.StringSingleValueBcProperty;

public class DataConnectionSchema {
    public static final String DATA_CONNECTION_CONCEPT_NAME = "__dc";
    public static final String DATA_SOURCE_CONCEPT_NAME = "__ds";
    public static final String DATA_CONNECTION_TO_DATA_SOURCE_EDGE_NAME = "__dcHds";

    public static final StringSingleValueBcProperty DC_NAME = new StringSingleValueBcProperty("dataConnectionName");
    public static final StringSingleValueBcProperty DC_DESCRIPTION = new StringSingleValueBcProperty("dataConnectionDescription");
    public static final StringSingleValueBcProperty DC_DRIVER_CLASS = new StringSingleValueBcProperty("dataConnectionDriver");
    public static final StringSingleValueBcProperty DC_JDBC_URL = new StringSingleValueBcProperty("dataConnectionJdbcUrl");
    public static final StringSingleValueBcProperty DC_DRIVER_PROPS = new StringSingleValueBcProperty("dataConnectionDriverProps");
    public static final StringSingleValueBcProperty DC_USERNAME = new StringSingleValueBcProperty("dataConnectionUsername");
    public static final StringSingleValueBcProperty DC_PASSWORD = new StringSingleValueBcProperty("dataConnectionPassword");

    public static final StringSingleValueBcProperty DS_NAME = new StringSingleValueBcProperty("dataSourceName");
    public static final StringSingleValueBcProperty DS_DESCRIPTION = new StringSingleValueBcProperty("dataSourceDescription");
    public static final LongSingleValueBcProperty DS_MAX_RECORDS = new LongSingleValueBcProperty("dataSourceMaxRecords");
    public static final StringSingleValueBcProperty DS_SQL = new StringSingleValueBcProperty("dataSourceSql");
    public static final DateSingleValueBcProperty DS_LAST_IMPORT_DATE = new DateSingleValueBcProperty("dataSourceLastImportDate");
    public static final BooleanSingleValueBcProperty DS_IMPORT_RUNNING = new BooleanSingleValueBcProperty("dataSourceImportRunning");
    public static final StringSingleValueBcProperty DS_ENTITY_MAPPING = new StringSingleValueBcProperty("dataSourceEntityMapping");
    public static final StringSingleValueBcProperty DS_RELATIONSHIP_MAPPING = new StringSingleValueBcProperty("dataSourceRelMapping");
    public static final StringSingleValueBcProperty DS_IMPORT_CONFIG = new StringSingleValueBcProperty("dataSourceImportConfig");
}
