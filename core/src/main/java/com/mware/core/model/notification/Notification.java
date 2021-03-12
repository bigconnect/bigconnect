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

import com.mware.core.orm.Field;
import com.mware.core.orm.Id;
import com.mware.core.exception.BcException;
import org.apache.commons.codec.binary.Hex;
import org.json.JSONObject;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public abstract class Notification {
    static final String ACTION_EVENT_EXTERNAL_URL = "EXTERNAL_URL";
    static final String ACTION_EVENT_OBJECT_ID = "OBJECT_ID";

    @Id
    private String id;

    @Field
    private String title;

    @Field
    private String message;

    @Field
    private String actionEvent;

    @Field
    private JSONObject actionPayload;

    protected Notification(String id, String title, String message, String actionEvent, JSONObject actionPayload) {
        this.id = id;
        this.title = title;
        this.message = message;
        this.actionEvent = actionEvent;
        this.actionPayload = actionPayload;
    }

    protected Notification() {
    }

    public String getId() {
        return id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getActionEvent() {
        return actionEvent;
    }

    public void setActionEvent(String actionEvent) {
        this.actionEvent = actionEvent;
    }

    public JSONObject getActionPayload() {
        return actionPayload;
    }

    public void setActionPayload(JSONObject actionPayload) {
        this.actionPayload = actionPayload;
    }

    public void setExternalUrl(String externalUrl) {
        if (getActionPayload() != null || getActionEvent() != null) {
            throw new IllegalStateException("actionPayload or actionEvent is already assigned");
        }
        setActionEvent(ACTION_EVENT_EXTERNAL_URL);
        JSONObject payload = new JSONObject();
        payload.put("url", externalUrl);
        this.setActionPayload(payload);
    }

    public void setElementId(String id) {
        if (getActionPayload() != null || getActionEvent() != null) {
            throw new IllegalStateException("actionPayload or actionEvent is already assigned");
        }
        setActionEvent(ACTION_EVENT_OBJECT_ID);
        JSONObject payload = new JSONObject();
        payload.put("eid", id);
        this.setActionPayload(payload);
    }

    public final JSONObject toJSONObject() {
        JSONObject json = new JSONObject();
        json.put("id", getId());
        json.put("title", getTitle());
        json.put("type", getType());
        json.put("message", getMessage());
        json.put("actionEvent", getActionEvent());
        json.put("actionPayload", getActionPayload());
        populateJSONObject(json);
        json.put("hash", hashJson(json));
        return json;
    }

    protected abstract void populateJSONObject(JSONObject json);



    protected abstract String getType();

    private static String hashJson(JSONObject json) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] md5 = digest.digest(json.toString().getBytes());
            return Hex.encodeHexString(md5);
        } catch (NoSuchAlgorithmException e) {
            throw new BcException("Could not find MD5", e);
        }
    }
}
