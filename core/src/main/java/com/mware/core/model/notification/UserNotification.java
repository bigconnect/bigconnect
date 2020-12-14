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
import com.mware.core.orm.Entity;
import com.mware.core.orm.Field;
import org.json.JSONObject;

import java.util.*;

@Entity(tableName = "userNotifications")
public class UserNotification extends Notification {
    @Field
    private String userId;

    @Field
    private Date sentDate;

    @Field
    private Integer expirationAgeAmount;

    @Field
    private ExpirationAgeUnit expirationAgeUnit;

    @Field
    private boolean markedRead;

    @Field
    private boolean notified;

    // Used by SimpleOrm to create instance
    @SuppressWarnings("UnusedDeclaration")
    protected UserNotification() {
        super();
    }

    @VisibleForTesting
    public UserNotification(
            String userId,
            String title,
            String message,
            String actionEvent,
            JSONObject actionPayload,
            ExpirationAge expirationAge
    ) {
        this(userId, title, message, actionEvent, actionPayload, new Date(), expirationAge);
    }

    @VisibleForTesting
    public UserNotification(
            String userId,
            String title,
            String message,
            String actionEvent,
            JSONObject actionPayload,
            Date sentDate,
            ExpirationAge expirationAge
    ) {
        super(createRowKey(sentDate), title, message, actionEvent, actionPayload);
        this.userId = userId;
        this.sentDate = sentDate;
        this.markedRead = false;
        this.notified = false;
        if (expirationAge != null) {
            this.expirationAgeAmount = expirationAge.getAmount();
            this.expirationAgeUnit = expirationAge.getExpirationAgeUnit();
        }
    }

    private static String createRowKey(Date date) {
        return Long.toString(date.getTime()) + ":" + UUID.randomUUID().toString();
    }

    public String getUserId() {
        return userId;
    }

    public Date getSentDate() {
        return sentDate;
    }

    public ExpirationAge getExpirationAge() {
        if (expirationAgeUnit != null && expirationAgeAmount != null) {
            return new ExpirationAge(expirationAgeAmount, expirationAgeUnit);
        }
        return null;
    }

    public boolean isMarkedRead() {
        return markedRead;
    }

    public void setMarkedRead(boolean markedRead) {
        this.markedRead = markedRead;
    }

    public boolean isNotified() {
        return notified;
    }

    public void setNotified(boolean notified) {
        this.notified = notified;
    }

    public boolean isActive() {
        if (isMarkedRead()) {
            return false;
        }
        Date now = new Date();
        Date expirationDate = getExpirationDate();
        Date sentDate = getSentDate();
        return sentDate.before(now) && (expirationDate == null || expirationDate.after(now));
    }

    public Date getExpirationDate() {
        ExpirationAge age = getExpirationAge();
        if (age == null) {
            return null;
        }

        Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        cal.setTime(getSentDate());
        cal.add(age.getExpirationAgeUnit().getCalendarUnit(), age.getAmount());
        return cal.getTime();
    }

    @Override
    protected String getType() {
        return "user";
    }

    @Override
    public void populateJSONObject(JSONObject json) {
        json.put("userId", getUserId());
        json.put("sentDate", getSentDate());
        json.put("expirationAge", getExpirationAge());
        json.put("markedRead", isMarkedRead());
        json.put("notified", isNotified());
    }

    @Override
    public String toString() {
        return "UserNotification{" +
                "userId='" + userId + '\'' +
                ", title=" + getTitle() +
                ", sentDate=" + sentDate +
                ", expirationAgeAmount=" + expirationAgeAmount +
                ", expirationAgeUnit=" + expirationAgeUnit +
                ", markedRead=" + markedRead +
                ", notified=" + notified +
                '}';
    }
}
