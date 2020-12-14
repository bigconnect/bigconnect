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
import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.model.role.GeAuthorizationRepository;
import com.mware.core.orm.SimpleOrmSession;
import org.json.JSONObject;
import com.mware.core.model.user.UserRepository;
import com.mware.core.user.User;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Singleton
public class SystemNotificationRepository extends NotificationRepository {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(SystemNotificationRepository.class);
    private static final String VISIBILITY_STRING = "";
    private UserRepository userRepository;

    @Inject
    public SystemNotificationRepository(
            SimpleOrmSession simpleOrmSession
    ) {
        super(simpleOrmSession);
    }

    public List<SystemNotification> getActiveNotifications(User user) {
        Date now = new Date();
        List<SystemNotification> activeNotifications = new ArrayList<>();
        for (SystemNotification notification : getSimpleOrmSession().findAll(
                SystemNotification.class,
                getUserRepository().getSimpleOrmContext(GeAuthorizationRepository.ADMIN_ROLE)
        )) {
            if (notification.getStartDate().before(now)) {
                if (notification.getEndDate() == null || notification.getEndDate().after(now)) {
                    activeNotifications.add(notification);
                }
            }
        }
        LOGGER.debug("returning %d active system notifications", activeNotifications.size());
        return activeNotifications;
    }

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
        getSimpleOrmSession().save(notification, VISIBILITY_STRING, getUserRepository().getSimpleOrmContext(GeAuthorizationRepository.ADMIN_ROLE));
        return notification;
    }

    public SystemNotification createNotification(
            SystemNotificationSeverity severity,
            String title,
            String message,
            String externalUrl,
            Date startDate,
            Date endDate,
            User user
    ) {
        String actionEvent = null;
        JSONObject actionPayload = null;

        if (externalUrl != null) {
            actionEvent = Notification.ACTION_EVENT_EXTERNAL_URL;
            actionPayload = new JSONObject();
            actionPayload.put("url", externalUrl);
        }

        return createNotification(severity, title, message, actionEvent, actionPayload, startDate, endDate, user);
    }

    public SystemNotification getNotification(String rowKey, User user) {
        return getSimpleOrmSession().findById(
                SystemNotification.class,
                rowKey,
                getUserRepository().getSimpleOrmContext(GeAuthorizationRepository.ADMIN_ROLE)
        );
    }

    public SystemNotification updateNotification(SystemNotification notification, User user) {
        getSimpleOrmSession().save(notification, VISIBILITY_STRING, getUserRepository().getSimpleOrmContext(GeAuthorizationRepository.ADMIN_ROLE));
        return notification;
    }

    public void endNotification(SystemNotification notification, User user) {
        notification.setEndDate(new Date());
        updateNotification(notification, user);
    }

    public List<SystemNotification> getFutureNotifications(Date maxDate, User user) {
        Date now = new Date();
        List<SystemNotification> futureNotifications = new ArrayList<>();
        for (SystemNotification notification : getSimpleOrmSession().findAll(
                SystemNotification.class,
                getUserRepository().getSimpleOrmContext(GeAuthorizationRepository.ADMIN_ROLE)
        )) {
            if (notification.getStartDate().after(now) && notification.getStartDate().before(maxDate)) {
                futureNotifications.add(notification);
            }
        }
        LOGGER.debug("returning %d future system notifications", futureNotifications.size());
        return futureNotifications;
    }

    /**
     * Avoid circular reference with UserRepository
     */
    public UserRepository getUserRepository() {
        if (userRepository == null) {
            userRepository = InjectHelper.getInstance(UserRepository.class);
        }
        return userRepository;
    }
}
