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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mware.core.model.role.GeAuthorizationRepository;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.core.orm.SimpleOrmContext;
import com.mware.core.orm.SimpleOrmSession;
import org.json.JSONObject;
import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.model.user.UserRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.user.User;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.mware.core.util.StreamUtil.stream;

@Singleton
public class UserNotificationRepository extends NotificationRepository {
    private static final String VISIBILITY_STRING = "";
    private final WebQueueRepository webQueueRepository;
    private UserRepository userRepository;

    @Inject
    public UserNotificationRepository(
            SimpleOrmSession simpleOrmSession,
            WebQueueRepository webQueueRepository
    ) {
        super(simpleOrmSession);
        this.webQueueRepository = webQueueRepository;
    }

    @VisibleForTesting
    public UserNotificationRepository(
            SimpleOrmSession simpleOrmSession,
            WebQueueRepository webQueueRepository,
            UserRepository userRepository
    ) {
        this(simpleOrmSession, webQueueRepository);
        this.userRepository = userRepository;
    }

    public Stream<UserNotification> getActiveNotifications(User user) {
        Date now = new Date();
        return findAll(user)
                .filter(notification ->
                                user.getUserId().equals(notification.getUserId())
                                        && notification.getSentDate().before(now)
                                        && notification.isActive()
                );
    }

    public Stream<UserNotification> findAll(User user) {
        SimpleOrmContext ctx = getUserRepository().getSimpleOrmContext(user);
        return stream(getSimpleOrmSession().findAll(UserNotification.class, ctx));
    }

    public Stream<UserNotification> getActiveNotificationsOlderThan(int duration, TimeUnit timeUnit, User user) {
        Date now = new Date();
        return findAll(user)
                .filter(notification -> {
                            if (!notification.isActive()) {
                                return false;
                            }
                            Date t = new Date(notification.getSentDate().getTime() + timeUnit.toMillis(duration));
                            return t.before(now);
                        }
                );
    }

    public UserNotification createNotification(
            String userId,
            String title,
            String message,
            String actionEvent,
            JSONObject actionPayload,
            ExpirationAge expirationAge,
            User authUser
    ) {
        UserNotification notification = new UserNotification(
                userId,
                title,
                message,
                actionEvent,
                actionPayload,
                expirationAge
        );
        saveNotification(notification, authUser);
        return notification;
    }

    public UserNotification createNotification(
            String userId,
            String title,
            String message,
            String externalUrl,
            ExpirationAge expirationAge,
            User authUser
    ) {
        UserNotification notification = new UserNotification(userId, title, message, null, null, expirationAge);
        notification.setExternalUrl(externalUrl);
        saveNotification(notification, authUser);
        return notification;
    }

    public void saveNotification(UserNotification notification, User authUser) {
        getSimpleOrmSession().save(notification, VISIBILITY_STRING, getUserRepository().getSimpleOrmContext(GeAuthorizationRepository.ADMIN_ROLE));
        webQueueRepository.pushUserNotification(notification);
    }

    public UserNotification getNotification(String notificationId, User user) {
        return getSimpleOrmSession().findById(
                UserNotification.class,
                notificationId,
                getUserRepository().getSimpleOrmContext(GeAuthorizationRepository.ADMIN_ROLE)
        );
    }

    /**
     * This method only allows marking items read for the passed in user
     */
    public void markRead(String[] notificationIds, User user) {
        Collection<UserNotification> toSave = new ArrayList<>();
        for (String notificationId : notificationIds) {
            UserNotification notification = getNotification(notificationId, user);
            checkNotNull(notification, "Could not find notification with id " + notificationId);
            notification.setMarkedRead(true);
            toSave.add(notification);
        }
        getSimpleOrmSession().saveMany(toSave, VISIBILITY_STRING, getUserRepository().getSimpleOrmContext(GeAuthorizationRepository.ADMIN_ROLE));
    }

    public void markNotified(Iterable<String> notificationIds, User user) {
        Collection<UserNotification> toSave = new ArrayList<>();
        for (String notificationId : notificationIds) {
            UserNotification notification = getNotification(notificationId, user);
            checkNotNull(notification, "Could not find notification with id " + notificationId);
            notification.setNotified(true);
            toSave.add(notification);
        }
        getSimpleOrmSession().saveMany(toSave, VISIBILITY_STRING, getUserRepository().getSimpleOrmContext(GeAuthorizationRepository.ADMIN_ROLE));
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
