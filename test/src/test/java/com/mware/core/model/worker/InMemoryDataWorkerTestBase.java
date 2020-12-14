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
package com.mware.core.model.worker;

import com.google.inject.Injector;
import com.mware.core.GraphTestBase;
import com.mware.core.exception.BcException;
import com.mware.core.ingest.dataworker.*;
import com.mware.core.model.clientapi.dto.PropertyType;
import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.schema.Concept;
import com.mware.core.model.schema.SchemaPropertyDefinition;
import com.mware.core.model.user.InMemoryUser;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.security.BcVisibility;
import com.mware.core.user.User;
import com.mware.ge.Authorizations;
import com.mware.ge.Element;
import com.mware.ge.Property;
import com.mware.ge.TextIndexHint;
import com.mware.ge.base.TestGraphFactory;
import com.mware.ge.values.storable.StreamingPropertyValue;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InMemoryDataWorkerTestBase extends GraphTestBase {
    private User user;

    @Override
    public void before() throws Exception {
        super.before();
        user = null;
    }

    @Override
    protected TestGraphFactory graphFactory() {
        return null;
    }

    protected void run(DataWorker gpw, DataWorkerPrepareData workerPrepareData, Element element) {
        run(gpw, workerPrepareData, element, null);
    }

    protected void run(
            DataWorker gpw,
            DataWorkerPrepareData workerPrepareData,
            Element element,
            String workspaceId
    ) {
        String visibilitySource = getVisibilitySource(element);
        run(gpw, workerPrepareData, element, null, null, workspaceId, null, visibilitySource);
        for (Property property : element.getProperties()) {
            InputStream in = null;
            if (property.getValue() instanceof StreamingPropertyValue) {
                StreamingPropertyValue spv = (StreamingPropertyValue) property.getValue();
                in = spv.getInputStream();
            }
            run(gpw, workerPrepareData, element, property, in, workspaceId, null, visibilitySource);
        }
    }

    protected boolean run(
            DataWorker gpw,
            DataWorkerPrepareData workerPrepareData,
            Element element,
            Property property,
            InputStream in
    ) {
        return run(gpw, workerPrepareData, element, property, in, null, null, null);
    }

    protected boolean run(
            DataWorker gpw,
            DataWorkerPrepareData workerPrepareData,
            Element element,
            Property property,
            InputStream in,
            String workspaceId,
            ElementOrPropertyStatus status
    ) {
        return run(gpw, workerPrepareData, element, property, in, workspaceId, status, null);
    }

    protected boolean run(
            DataWorker gpw,
            DataWorkerPrepareData workerPrepareData,
            Element element,
            Property property,
            InputStream in,
            String workspaceId,
            ElementOrPropertyStatus status,
            String visibilitySource
    ) {
        try {
            gpw.setSchemaRepository(getSchemaRepository());
            gpw.setWorkspaceRepository(getWorkspaceRepository());
            gpw.setConfiguration(getConfiguration());
            gpw.setGraph(getGraph());
            gpw.setVisibilityTranslator(getVisibilityTranslator());
            gpw.setWorkQueueRepository(getWorkQueueRepository());
            gpw.setGraphRepository(getGraphRepository());
            gpw.prepare(workerPrepareData);
        } catch (Exception ex) {
            throw new BcException("Failed to prepare: " + gpw.getClass().getName(), ex);
        }

        try {
            if (!(status == ElementOrPropertyStatus.HIDDEN && gpw.isHiddenHandled(element, property))
                    && !(status == ElementOrPropertyStatus.DELETION && gpw.isDeleteHandled(element, property))
                    && !gpw.isHandled(element, property)) {
                return false;
            }
        } catch (Exception ex) {
            throw new BcException("Failed isHandled: " + gpw.getClass().getName(), ex);
        }

        try {
            DataWorkerData workData = new DataWorkerData(
                    getVisibilityTranslator(),
                    element,
                    property,
                    workspaceId,
                    visibilitySource,
                    Priority.NORMAL,
                    false,
                    (property == null ? element.getTimestamp() : property.getTimestamp()) - 1,
                    status
            );
            if (gpw.isLocalFileRequired() && workData.getLocalFile() == null && in != null) {
                byte[] data = IOUtils.toByteArray(in);
                File tempFile = File.createTempFile("bcTest", "data");
                FileUtils.writeByteArrayToFile(tempFile, data);
                workData.setLocalFile(tempFile);
                in = new ByteArrayInputStream(data);
            }
            gpw.execute(in, workData);
        } catch (Exception ex) {
            throw new BcException("Failed to execute: " + gpw.getClass().getName(), ex);
        }
        return true;
    }

    private String getVisibilitySource(Element e) {
        String visibilitySource = null;
        if (e != null) {
            VisibilityJson visibilitySourceJson = BcSchema.VISIBILITY_JSON.getPropertyValue(e, null);
            if (visibilitySourceJson != null) {
                visibilitySource = visibilitySourceJson.getSource();
            }
        }
        return visibilitySource;
    }

    protected DataWorkerPrepareData createWorkerPrepareData() {
        return createWorkerPrepareData(null, null, null, null);
    }

    protected DataWorkerPrepareData createWorkerPrepareData(
            List<TermMentionFilter> termMentionFilters,
            User user,
            Authorizations authorizations,
            Injector injector
    ) {
        Map configuration = getConfigurationMap();
        if (termMentionFilters == null) {
            termMentionFilters = new ArrayList<>();
        }
        if (user == null) {
            user = getUser();
        }
        if (authorizations == null) {
            authorizations = getGraphAuthorizations(user, BcVisibility.SUPER_USER_VISIBILITY_STRING);
        }
        return new DataWorkerPrepareData(configuration, termMentionFilters, user, authorizations, injector);
    }

    protected User getUser() {
        if (user == null) {
            user = new InMemoryUser("test", "Test User", "test@example.org", null);
        }
        return user;
    }

    protected void addPropertyWithIntent(String propertyIri, String... intents) {
        getSchemaRepository().getOrCreateProperty(
                new SchemaPropertyDefinition(
                        new ArrayList<>(),
                        propertyIri,
                        propertyIri,
                        PropertyType.STRING
                )
                        .setIntents(intents)
                        .setTextIndexHints(TextIndexHint.ALL),
                getUserRepository().getSystemUser(),
                null
        );
    }

    protected void addConceptWithIntent(String conceptIri, String... intents) {
        Concept concept = getSchemaRepository().getOrCreateConcept(
                null,
                conceptIri,
                conceptIri,
                getUserRepository().getSystemUser(),
                null
        );
        for (String intent : intents) {
            concept.addIntent(intent, getUserRepository().getSystemUser(), getGraphAuthorizations());
        }
    }
}
