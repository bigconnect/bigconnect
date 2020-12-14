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

import com.mware.core.orm.Entity;
import com.mware.core.orm.Field;
import org.json.JSONObject;

import java.util.Date;
import java.util.UUID;

@Entity(tableName = "systemNotifications")
public class SystemNotification extends Notification {
    @Field
    private SystemNotificationSeverity severity;

    @Field
    private Date startDate;

    @Field
    private Date endDate;

    // Used by SimpleOrm to create instance
    @SuppressWarnings("UnusedDeclaration")
    protected SystemNotification() {
        super();
    }

    SystemNotification(Date startDate, String title, String message, String actionEvent, JSONObject actionPayload) {
        super(createId(startDate), title, message, actionEvent, actionPayload);
    }

    private static String createId(Date startDate) {
        return Long.toString(startDate.getTime()) + ":" + UUID.randomUUID().toString();
    }

    public SystemNotificationSeverity getSeverity() {
        return severity;
    }

    public void setSeverity(SystemNotificationSeverity severity) {
        this.severity = severity;
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        if (startDate == null) {
            startDate = new Date();
        }
        this.startDate = startDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public boolean isActive() {
        Date now = new Date();
        Date endDate = getEndDate();
        return getStartDate().before(now) && (endDate == null || endDate.after(now));
    }

    @Override
    protected String getType() {
        return "system";
    }

    @Override
    public void populateJSONObject(JSONObject json) {
        json.put("severity", getSeverity());
        Date startDate = getStartDate();
        if (startDate != null) {
            json.put("startDate", startDate.getTime());
        }
        Date endDate = getEndDate();
        if (endDate != null) {
            json.put("endDate", endDate.getTime());
        }
    }
}
