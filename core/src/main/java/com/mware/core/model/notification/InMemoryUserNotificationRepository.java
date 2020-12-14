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
package com.mware.core.model.notification;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.orm.SimpleOrmSession;
import com.mware.core.user.User;
import org.json.JSONObject;

import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

@Singleton
public class InMemoryUserNotificationRepository extends UserNotificationRepository {
    private Map<String, UserNotification> notifications = new HashMap<>();
    private WebQueueRepository webQueueRepository;

    @Inject
    public InMemoryUserNotificationRepository(
            SimpleOrmSession simpleOrmSession,
            WebQueueRepository webQueueRepository
    ) {
        super(simpleOrmSession, webQueueRepository);
        this.webQueueRepository = webQueueRepository;
    }

    @Override
    public Stream<UserNotification> findAll(User authUser) {
        return notifications.values().stream()
                .sorted(Comparator.comparing(UserNotification::getSentDate));
    }

    @Override
    public void saveNotification(UserNotification notification, User authUser) {
        notifications.put(notification.getId(), notification);
        webQueueRepository.pushUserNotification(notification);
    }

    @Override
    public UserNotification getNotification(String id, User user) {
        return notifications.get(id);
    }

    @Override
    public void markRead(String[] notificationIds, User user) {
        for (String notificationId : notificationIds) {
            notifications.get(notificationId).setMarkedRead(true);
        }
    }

    @Override
    public void markNotified(Iterable<String> notificationIds, User user) {
        for (String notificationId : notificationIds) {
            notifications.get(notificationId).setNotified(true);
        }
    }

    public UserNotification createNotification(
            String userId,
            String title,
            String message,
            String actionEvent,
            JSONObject actionPayload,
            Date sentTime,
            ExpirationAge expirationAge,
            User authUser
    ) {
        UserNotification notification = new UserNotification(
                userId,
                title,
                message,
                actionEvent,
                actionPayload,
                sentTime,
                expirationAge
        );
        saveNotification(notification, authUser);
        return notification;
    }
}