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
package com.mware.core.ping;

import com.google.common.base.Strings;
import com.mware.core.model.properties.types.LongSingleValueBcProperty;
import com.mware.core.model.properties.types.StringSingleValueBcProperty;
import com.mware.core.model.properties.types.DateSingleValueBcProperty;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.util.Date;

public class PingSchema {
    private static final String DATE_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    private static final String HOST_NAME = getHostName();

    public static final String BASE_NAME = "ping";

    public static final String CONCEPT_NAME_PING = "__p";

    public static final DateSingleValueBcProperty CREATE_DATE = new DateSingleValueBcProperty(BASE_NAME + "CreateDate");
    public static final StringSingleValueBcProperty CREATE_REMOTE_ADDR = new StringSingleValueBcProperty(BASE_NAME + "CreateRemoteAddr");
    public static final LongSingleValueBcProperty SEARCH_TIME_MS = new LongSingleValueBcProperty(BASE_NAME + "SearchTimeMs");
    public static final LongSingleValueBcProperty RETRIEVAL_TIME_MS = new LongSingleValueBcProperty(BASE_NAME + "RetrievalTimeMs");
    public static final DateSingleValueBcProperty GRAPH_PROPERTY_WORKER_DATE = new DateSingleValueBcProperty(BASE_NAME + "GpwDate");
    public static final StringSingleValueBcProperty GRAPH_PROPERTY_WORKER_HOSTNAME = new StringSingleValueBcProperty(BASE_NAME + "GpwHostname");
    public static final StringSingleValueBcProperty GRAPH_PROPERTY_WORKER_HOST_ADDRESS = new StringSingleValueBcProperty(BASE_NAME + "GpwHostAddress");
    public static final LongSingleValueBcProperty GRAPH_PROPERTY_WORKER_WAIT_TIME_MS = new LongSingleValueBcProperty(BASE_NAME + "GpwWaitTimeMs");
    public static final DateSingleValueBcProperty LONG_RUNNING_PROCESS_DATE = new DateSingleValueBcProperty(BASE_NAME + "LrpDate");
    public static final StringSingleValueBcProperty LONG_RUNNING_PROCESS_HOSTNAME = new StringSingleValueBcProperty(BASE_NAME + "LrpHostname");
    public static final StringSingleValueBcProperty LONG_RUNNING_PROCESS_HOST_ADDRESS = new StringSingleValueBcProperty(BASE_NAME + "LrpHostAddress");
    public static final LongSingleValueBcProperty LONG_RUNNING_PROCESS_WAIT_TIME_MS = new LongSingleValueBcProperty(BASE_NAME + "LrpWaitTimeMs");

    public static String getVertexId(ZonedDateTime date) {
        return "PING_" + new SimpleDateFormat(DATE_TIME_FORMAT).format(date) + "_" + HOST_NAME;
    }

    private static String getHostName() {
        // Windows
        String host = System.getenv("COMPUTERNAME");
        if (!Strings.isNullOrEmpty(host)) {
            return host;
        }

        // Unix'ish
        host = System.getenv("HOSTNAME");
        if (!Strings.isNullOrEmpty(host)) {
            return host;
        }

        // Java which requires DNS resolution
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException uhe) {
            return "Unknown";
        }
    }
}
