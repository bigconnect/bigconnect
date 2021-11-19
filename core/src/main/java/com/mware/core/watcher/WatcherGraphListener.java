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
package com.mware.core.watcher;

import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.email.EmailRepository;
import com.mware.core.model.notification.ExpirationAge;
import com.mware.core.model.notification.ExpirationAgeUnit;
import com.mware.core.model.notification.Notification;
import com.mware.core.model.notification.UserNotificationRepository;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.schema.Schema;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.model.user.UserRepository;
import com.mware.core.model.watcher.Watch;
import com.mware.core.model.watcher.WatchlistRepository;
import com.mware.core.user.User;
import com.mware.ge.Authorizations;
import com.mware.ge.Edge;
import com.mware.ge.Vertex;
import com.mware.ge.event.AddPropertyEvent;
import com.mware.ge.event.GraphEvent;
import com.mware.ge.event.GraphEventListener;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class WatcherGraphListener extends GraphEventListener {
    private static final int WATCH_NOTIFICATION_DEFAULT_EXPIRATION_AGE = 10; //In minutes
    private static final String DEFAULT_EMAIL_FROM = "admin@localhost";
    private static final boolean ENABLE_MAIL_NOTIFICATION = false;
    private List<String> allowedProperties = null;
    private UserNotificationRepository userNotificationRepository;
    private EmailRepository emailRepository;
    private UserRepository userRepository;
    private WatchlistRepository watchlistRepository;
    private SchemaRepository schemaRepository;
    private User systemUser;

    @Override
    public void onGraphEvent(GraphEvent graphEvent) {
        if (graphEvent instanceof AddPropertyEvent) {
            if (((AddPropertyEvent) graphEvent).getProperty() == null)
                return;
        }

        final PropertyEvent propEvent = new PropertyEvent(graphEvent);
        if (propEvent.isValid()) {
            onPropertyEvent(propEvent);
        } else {
            final EdgeEvent edgeEvent = new EdgeEvent(graphEvent);
            if (edgeEvent.isValid()) {
                onEdgeEvent(edgeEvent);
            }
        }
    }

    private void onPropertyEvent(PropertyEvent event) {
        if (!isPropertyAllowed(event.getPropertyName()))
            return;

        Stream<Watch> watchStream = getWatchlistRepository().getElementWatches(event.getElement().getId());
        watchStream
                .filter(watch ->
                        watch.getPropertyName().equals(event.getPropertyName()))
                .forEach(watch ->
                        notifyUser(watch.getUserId(), event.getElement().getId(),
                                "Property change on element: " + event.getElement().getId(),
                                "Property: " + event.getPropertyName() + ", Event: " + event.getEventType())
                );
    }

    private boolean isPropertyAllowed(String propName) {
        if (allowedProperties == null) {
            allowedProperties = new ArrayList<>();
            getSchemaRepository().getProperties().forEach(sp -> {
                if (sp.getUserVisible()) {
                    allowedProperties.add(sp.getName());
                }
            });
        }

        return allowedProperties.stream().anyMatch(propName::endsWith);
    }

    private void onEdgeEvent(EdgeEvent event) {
        final Edge edge = event.getEdge();
        final Authorizations authorizations = edge.getAuthorizations();

        //Scan watches for source and destination vertices
        final Vertex inVertex = edge.getVertices(authorizations).getInVertex();
        final Vertex outVertex = edge.getVertices(authorizations).getOutVertex();
        List<Vertex> vertices = new ArrayList<>();
        vertices.add(inVertex);
        vertices.add(outVertex);

        for (Vertex vertex : vertices) {
            Stream<Watch> watchStream = getWatchlistRepository().getElementWatches(vertex.getId());
            watchStream
                    .filter(watch ->
                            watch.getPropertyName().equals(event.getTitle()))
                    .forEach(watch ->
                            notifyUser(watch.getUserId(), vertex.getId(),
                                    String.format("Element %s, Relationship %s", vertex.getId(), edge.getLabel()),
                                    "event: " + event.getEventType())
                    );
        }
    }

    private void notifyUser(String userId, String elementId, String title, String message) {
        User user = getUserRepository().findById(userId);

        JSONObject actionPayload = new JSONObject();
        actionPayload.put("id", elementId);

        getUserNotificationRepository()
                .createNotification(userId,
                        title,
                        message,
                        Notification.ACTION_EVENT_OBJECT_ID,
                        actionPayload,
                        new ExpirationAge(WATCH_NOTIFICATION_DEFAULT_EXPIRATION_AGE, ExpirationAgeUnit.MINUTE),
                        getSystemUser());

        if (ENABLE_MAIL_NOTIFICATION &&
                (EmailValidator.getInstance().validate(user.getUsername()) ||
                        EmailValidator.getInstance().validate((user.getEmailAddress())))) {//May impact performance as it's synchronous (maybe create a job for this)
            getEmailRepository()
                    .send(DEFAULT_EMAIL_FROM,
                            EmailValidator.getInstance().validate(user.getEmailAddress())
                                    ? user.getEmailAddress() : user.getUsername(), //If no email address then maybe username is the mail (SSO)
                            title,
                            message);
        }
    }

    private User getSystemUser() {
        if (systemUser == null) {
            systemUser = getUserRepository().getSystemUser();
        }

        return systemUser;
    }

    private WatchlistRepository getWatchlistRepository() {
        if (watchlistRepository == null) {
            watchlistRepository = InjectHelper.getInstance(WatchlistRepository.class);
        }

        return watchlistRepository;
    }

    private UserRepository getUserRepository() {
        if (userRepository == null) {
            userRepository = InjectHelper.getInstance(UserRepository.class);
        }

        return userRepository;
    }

    private UserNotificationRepository getUserNotificationRepository() {
        if (userNotificationRepository == null) {
            userNotificationRepository = InjectHelper.getInstance(UserNotificationRepository.class);
        }

        return userNotificationRepository;
    }

    private EmailRepository getEmailRepository() {
        if (emailRepository == null) {
            emailRepository = InjectHelper.getInstance(EmailRepository.class);
        }

        return emailRepository;
    }

    public SchemaRepository getSchemaRepository() {
        if (schemaRepository == null) {
            schemaRepository = InjectHelper.getInstance(SchemaRepository.class);
        }
        return schemaRepository;
    }
}
