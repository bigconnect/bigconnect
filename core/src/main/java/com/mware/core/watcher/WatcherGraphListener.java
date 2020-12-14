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
import com.mware.core.model.notification.UserNotificationRepository;
import com.mware.core.model.properties.BcSchema;
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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class WatcherGraphListener extends GraphEventListener {
    private static final int WATCH_NOTIFICATION_DEFAULT_EXPIRATION_AGE = 10; //In minutes
    private static final String DEFAULT_EMAIL_FROM = "amplexor@sparksc.com";
    private static final boolean ENABLE_MAIL_NOTIFICATION = true;
    private static final List<String> propertiesToIgnore = new ArrayList<>();
    private UserNotificationRepository userNotificationRepository;
    private EmailRepository emailRepository;
    private UserRepository userRepository;
    private WatchlistRepository watchlistRepository;
    private User systemUser;

    static {
        propertiesToIgnore.add("addRelatedConceptWhiteList");
        propertiesToIgnore.add("addable");
        propertiesToIgnore.add("boost");
        propertiesToIgnore.add("color");
        propertiesToIgnore.add("deleteable");
        propertiesToIgnore.add("dependentPropertyIri");
        propertiesToIgnore.add("dependentPropertyIris");
        propertiesToIgnore.add("displayFormula");
        propertiesToIgnore.add("displayName");
        propertiesToIgnore.add("displayType");
        propertiesToIgnore.add("glyphIconFileName");
        propertiesToIgnore.add("intent");
        propertiesToIgnore.add("mapGlyphIconFileName");
        propertiesToIgnore.add("objectPropertyDomain");
        propertiesToIgnore.add("possibleValues");
        propertiesToIgnore.add("propertyGroup");
        propertiesToIgnore.add("searchable");
        propertiesToIgnore.add("sortable");
        propertiesToIgnore.add("subtitleFormula");
        propertiesToIgnore.add("textIndexHints");
        propertiesToIgnore.add("timeFormula");
        propertiesToIgnore.add("titleFormula");
        propertiesToIgnore.add("updateable");
        propertiesToIgnore.add("userVisible");
        propertiesToIgnore.add("validationFormula");
        propertiesToIgnore.add("analyticsvisible");
        propertiesToIgnore.add("searchFacet");
        propertiesToIgnore.add("systemProperty");
        propertiesToIgnore.add("aggType");
        propertiesToIgnore.add("aggPrecision");
        propertiesToIgnore.add("aggInterval");
        propertiesToIgnore.add("aggMinDocumentCount");
        propertiesToIgnore.add("aggTimeZone");
        propertiesToIgnore.add("aggCalendarField");
        propertiesToIgnore.add("glyphIcon");
        propertiesToIgnore.add("ontologyFile");
        propertiesToIgnore.add("ontologyTitle");
        propertiesToIgnore.add("conceptType");
        propertiesToIgnore.add("dataType");
    }

    @Override
    public void onGraphEvent(GraphEvent graphEvent) {
        if(graphEvent instanceof AddPropertyEvent) {
            if(((AddPropertyEvent)graphEvent).getProperty() == null)
                return;
        }

        final PropertyEvent event = new PropertyEvent(graphEvent);
        if(event.isValid()) {
            onPropertyEvent(event);
        } else {
            final EdgeEvent eevent = new EdgeEvent(graphEvent);
            if (eevent.isValid()) {
                onEdgeEvent(eevent);
            }
        }
    }

    private void onPropertyEvent(PropertyEvent event) {
        String propertyName = event.getPropertyName();
        for(String prop : propertiesToIgnore) {
            if(propertyName.endsWith(prop))
                return;
        }

        Stream<Watch> watchStream = getWatchlistRepository().getElementWatches(event.getElement().getId());
        watchStream
                .filter(watch ->
                    watch.getPropertyName().equals(event.getPropertyName()))
                .forEach(watch ->
                    notifyUser(watch.getUserId(),
                            "Watch alert on element with title: "+ BcSchema.TITLE.getFirstPropertyValue(event.getElement()),
                            "Property: "+event.getPropertyName()+" changed with event: "+event.getEventType())
        );
    }

    private void onEdgeEvent(EdgeEvent event) {
        final Edge edge = event.getEdge();
        final Authorizations authorizations = edge.getAuthorizations();

        //Scan watches for source and destination vertices
        final Vertex inVertex = edge.getVertices(authorizations).getInVertex();
        final Vertex outVertex = edge.getVertices(authorizations).getOutVertex();
        List<Vertex> vertices = new ArrayList<Vertex>();
        vertices.add(inVertex);
        final String sourceTitle = BcSchema.TITLE.getFirstPropertyValue(inVertex);
        vertices.add(outVertex);
        final String destTitle = BcSchema.TITLE.getFirstPropertyValue(outVertex);

        for (Vertex vertex : vertices) {
            Stream<Watch> watchStream = getWatchlistRepository().getElementWatches(vertex.getId());
            watchStream
                    .filter(watch ->
                            watch.getPropertyName().equals(event.getTitle()))
                    .forEach(watch ->
                            notifyUser(watch.getUserId(),
                                    "Watch alert on relationship with title: " + event.getTitle(),
                                    "Relationship: " + event.getTitle() + " between " + (sourceTitle == null ? "-" : sourceTitle) + " and "+
                                            (destTitle == null ? "-" : destTitle) + " changed with event: " + event.getEventType())
                    );
        }
    }

    private void notifyUser(String userId, String title, String message) {
        User user = getUserRepository().findById(userId);
        getUserNotificationRepository()
                .createNotification(userId,
                        title,
                        message, null,
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
}
