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
package com.mware.core.security;

import com.google.inject.Inject;
import com.mware.core.model.role.GeAuthorizationRepository;
import com.mware.core.model.user.UserRepository;
import com.mware.core.user.User;
import com.mware.ge.Authorizations;
import com.mware.ge.Graph;
import com.mware.ge.Visibility;
import com.mware.ge.time.Clocks;
import com.mware.ge.util.Preconditions;
import com.mware.ge.values.storable.LocalDateTimeValue;
import com.mware.ge.values.storable.Values;
import org.apache.commons.lang.StringUtils;

import java.util.Objects;
import java.util.UUID;

import static com.mware.core.model.schema.SchemaConstants.CONCEPT_TYPE_THING;

public class ExtendedDataAuditService implements AuditService {
    private final Graph graph;
    private final UserRepository userRepository;

    private static final String AUDIT_VERTEX_ID = "audit_vertex";
    private static final String AUDIT_TABLE = "audit_table";

    private static final String AUDIT_WORKSPACE_ID_COLUMN = "workspace_id";
    private static final String AUDIT_USERID_COLUMN = "user_id";
    private static final String AUDIT_USERNAME_COLUMN = "user_name";
    private static final String AUDIT_DATETIME_COLUMN = "time";
    private static final String AUDIT_EVENT_TYPE_COLUMN = "event_type";
    private static final String AUDIT_EVENT_KEY_COLUMN = "event_key";
    private static final String AUDIT_EVENT_VALUE_COLUMN = "event_value";

    private static final Authorizations AUTHORIZATIONS_ALL = new Authorizations(GeAuthorizationRepository.ADMIN_ROLE);
    private static final Visibility VISIBILITY_PUBLIC = new Visibility(GeAuthorizationRepository.ADMIN_ROLE);

    @Inject
    public ExtendedDataAuditService(Graph graph, UserRepository userRepository) {
        this.graph = graph;
        this.userRepository = userRepository;
        ensureAuditVertexExists();
    }

    private void ensureAuditVertexExists() {
        if (!this.graph.doesVertexExist(AUDIT_VERTEX_ID, AUTHORIZATIONS_ALL)) {
            this.graph
                    .prepareVertex(AUDIT_VERTEX_ID, VISIBILITY_PUBLIC, CONCEPT_TYPE_THING)
                    .save(AUTHORIZATIONS_ALL);
            this.graph.flush();
        }
    }

    @Override
    public void auditLogin(User user) {
        this.createAuditRow(StringUtils.EMPTY, user.getUserId(), user.getUsername(),
                AuditEventType.LOGIN, "", "");
    }

    @Override
    public void auditLogout(String userId) {
        User user = userRepository.findById(userId);
        this.createAuditRow(StringUtils.EMPTY, userId, user.getUsername(),
                AuditEventType.LOGOUT, "", "");
    }

    @Override
    public void auditAccessDenied(String message, User user, Object resourceId) {
        this.createAuditRow(StringUtils.EMPTY, user.getUserId(), user.getUsername(),
                AuditEventType.ACCESS_DENIED, Objects.toString(resourceId), message);
    }

    @Override
    public void auditGenericEvent(User user, String workspaceId, AuditEventType type, String key, String value) {
        this.createAuditRow(workspaceId, user.getUserId(), user.getUsername(), type, key, value);
    }

    private void createAuditRow(String workspaceId,
                                String userId,
                                String userName,
                                AuditEventType type,
                                String key,
                                String value) {

        Preconditions.checkNotNull(type);

        final String rowId = UUID.randomUUID().toString();
        this.graph.getVertex(AUDIT_VERTEX_ID, AUTHORIZATIONS_ALL).prepareMutation()
                .addExtendedData(AUDIT_TABLE, rowId,
                        AUDIT_WORKSPACE_ID_COLUMN, Values.stringValue(workspaceId), VISIBILITY_PUBLIC)
                .addExtendedData(AUDIT_TABLE, rowId,
                        AUDIT_USERID_COLUMN, Values.stringValue(userId), VISIBILITY_PUBLIC)
                .addExtendedData(AUDIT_TABLE, rowId,
                        AUDIT_USERNAME_COLUMN, Values.stringValue(userName), VISIBILITY_PUBLIC)
                .addExtendedData(AUDIT_TABLE, rowId,
                        AUDIT_DATETIME_COLUMN, LocalDateTimeValue.now(Clocks.systemClock()), VISIBILITY_PUBLIC)
                .addExtendedData(AUDIT_TABLE, rowId,
                        AUDIT_EVENT_TYPE_COLUMN, Values.stringValue(type.name()), VISIBILITY_PUBLIC)
                .addExtendedData(AUDIT_TABLE, rowId,
                        AUDIT_EVENT_KEY_COLUMN, Values.stringValue(key), VISIBILITY_PUBLIC)
                .addExtendedData(AUDIT_TABLE, rowId,
                        AUDIT_EVENT_VALUE_COLUMN, Values.stringValue(value), VISIBILITY_PUBLIC)
                .save(AUTHORIZATIONS_ALL);
        this.graph.flush();
    }
}
