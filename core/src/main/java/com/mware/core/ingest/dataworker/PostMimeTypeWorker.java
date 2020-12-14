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
import com.mware.core.exception.BcException;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.user.User;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.*;
import com.mware.ge.values.storable.StreamingPropertyValue;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

public abstract class PostMimeTypeWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(PostMimeTypeWorker.class);
    private Graph graph;
    private WorkQueueRepository workQueueRepository;
    private WebQueueRepository webQueueRepository;
    private File localFileForRaw;
    private DataWorkerPrepareData workerPrepareData;

    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        this.workerPrepareData = workerPrepareData;
    }

    public Element refresh(Element element, Authorizations authorizations) {
        if(element == null)
            return null;

        if(element instanceof Vertex)
            return getGraph().getVertex(element.getId(), authorizations);
        else if(element instanceof Edge)
            return getGraph().getEdge(element.getId(), authorizations);
        else
            throw new BcException("Asking to refresh an element of unknown type: "+element.getClass());
    }

    protected abstract void execute(String mimeType, DataWorkerData data, Authorizations authorizations) throws Exception;

    public void executeAndCleanup(String mimeType, DataWorkerData data, Authorizations authorizations) throws Exception {
        try {
            execute(mimeType, data, authorizations);
        } finally {
            if (localFileForRaw != null) {
                if (!localFileForRaw.delete()) {
                    LOGGER.warn("Could not delete local file: %s", localFileForRaw.getAbsolutePath());
                }
                localFileForRaw = null;
            }
        }
    }

    protected File getLocalFileForRaw(Element element) throws IOException {
        if (localFileForRaw != null) {
            return localFileForRaw;
        }
        StreamingPropertyValue rawValue = BcSchema.RAW.getPropertyValue(element);
        try (InputStream in = rawValue.getInputStream()) {
            String suffix = "-" + element.getId().replaceAll("\\W", "_");
            localFileForRaw = File.createTempFile(PostMimeTypeWorker.class.getName() + "-", suffix);
            try (FileOutputStream out = new FileOutputStream(localFileForRaw)) {
                IOUtils.copy(in, out);
                return localFileForRaw;
            }
        }
    }

    protected User getUser() {
        return this.workerPrepareData.getUser();
    }

    @Inject
    public final void setGraph(Graph graph) {
        this.graph = graph;
    }

    protected Graph getGraph() {
        return graph;
    }

    @Inject
    public final void setWorkQueueRepository(WorkQueueRepository workQueueRepository) {
        this.workQueueRepository = workQueueRepository;
    }

    @Inject
    public void setWebQueueRepository(WebQueueRepository webQueueRepository) {
        this.webQueueRepository = webQueueRepository;
    }

    public WebQueueRepository getWebQueueRepository() {
        return webQueueRepository;
    }

    protected WorkQueueRepository getWorkQueueRepository() {
        return workQueueRepository;
    }
}
