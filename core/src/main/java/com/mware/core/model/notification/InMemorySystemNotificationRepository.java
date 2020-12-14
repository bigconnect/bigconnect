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
import com.mware.core.model.user.UserRepository;
import com.mware.core.orm.SimpleOrmSession;
import com.mware.core.user.User;
import org.json.JSONObject;

import java.util.*;
import java.util.stream.Collectors;

@Singleton
public class InMemorySystemNotificationRepository extends SystemNotificationRepository {
    private final Map<String, SystemNotification> notifications = new HashMap<>();

    @Inject
    public InMemorySystemNotificationRepository(SimpleOrmSession simpleOrmSession) {
        super(simpleOrmSession);
    }

    @Override
    public List<SystemNotification> getActiveNotifications(User user) {
        Date now = new Date();
        return notifications.values().stream()
                .filter(n -> n.getStartDate().compareTo(now) <= 0 && (n.getEndDate() == null || n.getEndDate().compareTo(now) >= 0))
                .sorted(Comparator.comparing(SystemNotification::getStartDate))
                .collect(Collectors.toList());
    }

    @Override
    public SystemNotification createNotification(
            SystemNotificationSeverity severity,
            String title,
            String message,
            String actionEvent,
            JSONObject actionPayload,
            Date startDate,
            Date endDate,
            User user
    ) {
        if (startDate == null) {
            startDate = new Date();
        }
        SystemNotification notification = new SystemNotification(startDate, title, message, actionEvent, actionPayload);
        notification.setSeverity(severity);
        notification.setStartDate(startDate);
        notification.setEndDate(endDate);
        return updateNotification(notification, user);
    }

    @Override
    public SystemNotification getNotification(String id, User user) {
        return notifications.get(id);
    }

    @Override
    public SystemNotification updateNotification(SystemNotification notification, User user) {
        notifications.put(notification.getId(), notification);
        return notification;
    }

    @Override
    public List<SystemNotification> getFutureNotifications(Date maxDate, User user) {
        Date now = new Date();
        return notifications.values().stream()
                .filter(n -> n.getStartDate().compareTo(now) >= 0 && (n.getEndDate() == null || n.getEndDate().compareTo(maxDate) <= 0))
                .sorted(Comparator.comparing(SystemNotification::getStartDate))
                .collect(Collectors.toList());
    }
}
