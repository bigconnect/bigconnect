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
package com.mware.core.model.user;

import com.mware.core.time.TimeRepository;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

public abstract class MapUserSessionCounterRepositoryBase implements UserSessionCounterRepository {
    public final static int UNSEEN_SESSION_DURATION = 300000; // 5 minutes
    private final TimeRepository timeRepository;

    protected MapUserSessionCounterRepositoryBase(TimeRepository timeRepository) {
        this.timeRepository = timeRepository;
    }

    @Override
    public int updateSession(String userId, String sessionId, boolean autoDelete) {
        put(userId, sessionId, new SessionData(timeRepository.getNow(), autoDelete));
        return getSessionCount(userId);
    }

    protected abstract void put(String userId, String sessionId, SessionData sessionData);

    protected abstract void remove(String userId, String sessionId);

    protected abstract Map<String, SessionData> getRow(String userId);

    @Override
    public int getSessionCount(String userId) {
        for (Map.Entry<String, SessionData> sessionIdToSessionData : getRow(userId).entrySet()) {
            if (shouldDelete(sessionIdToSessionData.getValue())) {
                remove(userId, sessionIdToSessionData.getKey());
            }
        }
        return getRow(userId).size();
    }

    private boolean shouldDelete(SessionData sessionData) {
        if (!sessionData.isAutoDelete()) {
            return false;
        }

        long now = timeRepository.currentTimeMillis();
        return now - sessionData.getCreateDate().getTime() >= UNSEEN_SESSION_DURATION;

    }

    @Override
    public void deleteSessions(String userId) {
        Map<String, SessionData> sessions = getRow(userId);
        for (String sessionId : sessions.keySet()) {
            remove(userId, sessionId);
        }
    }

    @Override
    public int deleteSession(String userId, String sessionId) {
        remove(userId, sessionId);
        return getSessionCount(userId);
    }

    protected static class SessionData implements Serializable {
        private static final long serialVersionUID = -1883352978079887306L;
        private final Date createDate;
        private final boolean autoDelete;

        public SessionData(Date createDate, boolean autoDelete) {
            this.createDate = createDate;
            this.autoDelete = autoDelete;
        }

        public Date getCreateDate() {
            return createDate;
        }

        public boolean isAutoDelete() {
            return autoDelete;
        }

        @Override
        public String toString() {
            return "SessionData{" +
                    "createDate=" + createDate +
                    ", autoDelete=" + autoDelete +
                    '}';
        }
    }
}
