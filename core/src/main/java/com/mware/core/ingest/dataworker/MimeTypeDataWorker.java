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
package com.mware.core.ingest.dataworker;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.exception.BcException;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.Element;
import com.mware.ge.Metadata;
import com.mware.ge.Property;
import com.mware.ge.Vertex;
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.ge.values.storable.Values;
import org.apache.commons.lang3.StringUtils;

import java.io.InputStream;
import java.util.Collection;

/**
 * By default raw properties will be assigned a mime type.
 *
 * Configuration:
 *
 * <pre><code>
 * MimeTypeDataWorker.handled.myTextProperty.propertyName=myTextProperty
 * MimeTypeDataWorker.handled.myOtherTextProperty.propertyName=myOtherTextProperty
 * </code></pre>
 */
@Singleton
public abstract class MimeTypeDataWorker extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(MimeTypeDataWorker.class);
    public static final String MULTI_VALUE_KEY = MimeTypeDataWorker.class.getSimpleName();
    private final MimeTypeDataWorkerConfiguration configuration;
    private Collection<PostMimeTypeWorker> postMimeTypeWorkers;

    @Inject
    protected MimeTypeDataWorker(MimeTypeDataWorkerConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);
        postMimeTypeWorkers = getPostMimeTypeWorkers();
        for (PostMimeTypeWorker postMimeTypeWorker : postMimeTypeWorkers) {
            try {
                postMimeTypeWorker.prepare(workerPrepareData);
            } catch (Exception ex) {
                throw new BcException("Could not prepare post mime type worker " + postMimeTypeWorker.getClass().getName(), ex);
            }
        }
    }

    protected Collection<PostMimeTypeWorker> getPostMimeTypeWorkers() {
        return InjectHelper.getInjectedServices(PostMimeTypeWorker.class, getConfiguration());
    }

    @Override
    public boolean isHandled(Element element, Property property) {
        if (property == null) {
            return false;
        }

        if (BcSchema.MIME_TYPE.hasProperty(element, getMultiKey(property))) {
            return false;
        }

        return configuration.isHandled(element, property);
    }

    @Override
    public void execute(InputStream in, DataWorkerData data) throws Exception {
        String fileName = BcSchema.FILE_NAME.getOnlyPropertyValue(data.getElement());

        // check to see if the dataprep application set the mimeType and use this one if it did
        String dataPrepMimeType = BcSchema.MIME_TYPE.getPropertyValue(data.getElement(), "dataprep");

        String mimeType;

        if (!StringUtils.isEmpty(dataPrepMimeType))
            mimeType = dataPrepMimeType;
        else
            mimeType = getMimeType(in, fileName);

        if (mimeType == null) {
            return;
        }

        String propertyKey = getMultiKey(data.getProperty());
        ExistingElementMutation<Vertex> m = ((Vertex) refresh(data.getElement())).prepareMutation();
        Metadata mimeTypeMetadata = data.createPropertyMetadata(getUser());
        BcSchema.MIME_TYPE.addPropertyValue(m, propertyKey, mimeType, mimeTypeMetadata, data.getProperty().getVisibility());
        m.setPropertyMetadata(data.getProperty(), BcSchema.MIME_TYPE.getPropertyName(), Values.stringValue(mimeType), getVisibilityTranslator().getDefaultVisibility());
        Vertex element = m.save(getAuthorizations());
        getGraph().flush();

        runPostMimeTypeWorkers(mimeType, data);

        if (getWebQueueRepository().shouldBroadcastGraphPropertyChange(data.getProperty().getName(), data.getPriority())) {
            getWebQueueRepository().broadcastPropertyChange(element, data.getProperty().getKey(), data.getProperty().getName(), data.getWorkspaceId());
        }

        if (getWebQueueRepository().shouldBroadcastGraphPropertyChange(BcSchema.MIME_TYPE.getPropertyName(), data.getPriority())) {
            getWebQueueRepository().broadcastPropertyChange(element, propertyKey, BcSchema.MIME_TYPE.getPropertyName(), data.getWorkspaceId());
        }

        getWorkQueueRepository().pushOnDwQueue(
                element,
                propertyKey,
                BcSchema.MIME_TYPE.getPropertyName(),
                data.getWorkspaceId(),
                data.getVisibilitySource(),
                data.getPriority(),
                ElementOrPropertyStatus.UPDATE,
                null
        );
    }

    private String getMultiKey(Property property) {
        return MULTI_VALUE_KEY + property.getKey();
    }

    private void runPostMimeTypeWorkers(String mimeType, DataWorkerData data) {
        for (PostMimeTypeWorker postMimeTypeWorker : postMimeTypeWorkers) {
            try {
                LOGGER.debug("running PostMimeTypeWorker: %s on element: %s, mimeType: %s", postMimeTypeWorker.getClass().getName(), data.getElement().getId(), mimeType);
                postMimeTypeWorker.executeAndCleanup(mimeType, data, getAuthorizations());
            } catch (Exception ex) {
                throw new BcException("Failed running PostMimeTypeWorker " + postMimeTypeWorker.getClass().getName(), ex);
            }
        }
        if (postMimeTypeWorkers.size() > 0) {
            getGraph().flush();
        }
    }

    protected abstract String getMimeType(InputStream in, String fileName) throws Exception;
}
