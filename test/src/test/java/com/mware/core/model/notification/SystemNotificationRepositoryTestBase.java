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

import com.mware.core.GraphTestBase;
import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;

public abstract class SystemNotificationRepositoryTestBase extends GraphTestBase {
    @Before
    public void before() throws Exception {
        super.before();
    }

    @Override
    protected abstract SystemNotificationRepository getSystemNotificationRepository();

    @Test
    public void testCreateAndUpdate() {
        Date startDate = Date.from(ZonedDateTime.of(2017, 11, 28, 10, 12, 13, 0, ZoneId.of("GMT")).toInstant());
        Date endDate = Date.from(ZonedDateTime.of(2017, 11, 29, 10, 12, 13, 0, ZoneId.of("GMT")).toInstant());
        SystemNotification notification = getSystemNotificationRepository().createNotification(
                SystemNotificationSeverity.INFORMATIONAL,
                "notification title",
                "notification message",
                "http://example.com/notification/test",
                startDate,
                endDate,
                getUserRepository().getSystemUser()
        );

        notification = getSystemNotificationRepository().getNotification(notification.getId(), getUserRepository().getSystemUser());
        assertEquals(SystemNotificationSeverity.INFORMATIONAL, notification.getSeverity());
        assertEquals("notification title", notification.getTitle());
        assertEquals("notification message", notification.getMessage());
        assertEquals("EXTERNAL_URL", notification.getActionEvent());
        assertEquals("{\"url\":\"http://example.com/notification/test\"}", notification.getActionPayload().toString());
        assertEquals(startDate, notification.getStartDate());
        assertEquals(endDate, notification.getEndDate());

        // Update
        notification.setSeverity(SystemNotificationSeverity.WARNING);
        notification.setTitle("notification title2");
        notification.setMessage("notification message2");
        getSystemNotificationRepository().updateNotification(notification, getUserRepository().getSystemUser());

        notification = getSystemNotificationRepository().getNotification(notification.getId(), getUserRepository().getSystemUser());
        assertEquals(SystemNotificationSeverity.WARNING, notification.getSeverity());
        assertEquals("notification title2", notification.getTitle());
        assertEquals("notification message2", notification.getMessage());
        assertEquals("EXTERNAL_URL", notification.getActionEvent());
        assertEquals("{\"url\":\"http://example.com/notification/test\"}", notification.getActionPayload().toString());
        assertEquals(startDate, notification.getStartDate());
        assertEquals(endDate, notification.getEndDate());
    }

    @Test
    public void testGetActiveNotifications() {
        Date startDate = Date.from(ZonedDateTime.of(2017, 11, 28, 10, 12, 13, 0, ZoneId.of("GMT")).toInstant());
        getSystemNotificationRepository().createNotification(
                SystemNotificationSeverity.INFORMATIONAL,
                "notification title",
                "notification message",
                "http://example.com/notification/test",
                startDate,
                null,
                getUserRepository().getSystemUser()
        );

        List<SystemNotification> notifications = getSystemNotificationRepository().getActiveNotifications(getUserRepository().getSystemUser());
        assertEquals(1, notifications.size());
        SystemNotification notification = notifications.get(0);
        assertEquals(SystemNotificationSeverity.INFORMATIONAL, notification.getSeverity());
        assertEquals("notification title", notification.getTitle());
        assertEquals("notification message", notification.getMessage());
        assertEquals("EXTERNAL_URL", notification.getActionEvent());
        assertEquals("{\"url\":\"http://example.com/notification/test\"}", notification.getActionPayload().toString());
        assertEquals(startDate, notification.getStartDate());
        assertEquals(null, notification.getEndDate());
    }

    @Test
    public void testGetFutureNotifications() {
        Date startDate = Date.from(ZonedDateTime.of(2025, 11, 28, 10, 12, 13, 0, ZoneId.of("GMT")).toInstant());
        getSystemNotificationRepository().createNotification(
                SystemNotificationSeverity.INFORMATIONAL,
                "notification title",
                "notification message",
                "http://example.com/notification/test",
                startDate,
                null,
                getUserRepository().getSystemUser()
        );

        List<SystemNotification> notifications = getSystemNotificationRepository().getFutureNotifications(null, getUserRepository().getSystemUser());
        assertEquals(1, notifications.size());
        SystemNotification notification = notifications.get(0);
        assertEquals(SystemNotificationSeverity.INFORMATIONAL, notification.getSeverity());
        assertEquals("notification title", notification.getTitle());
        assertEquals("notification message", notification.getMessage());
        assertEquals("EXTERNAL_URL", notification.getActionEvent());
        assertEquals("{\"url\":\"http://example.com/notification/test\"}", notification.getActionPayload().toString());
        assertEquals(startDate, notification.getStartDate());
        assertEquals(null, notification.getEndDate());
    }
}
