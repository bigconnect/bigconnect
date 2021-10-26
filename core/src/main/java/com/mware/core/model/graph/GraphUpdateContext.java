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
package com.mware.core.model.graph;

import com.mware.core.exception.BcException;
import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.security.VisibilityTranslator;
import com.mware.core.user.User;
import com.mware.ge.*;
import com.mware.ge.mutation.ElementMutation;

import java.time.ZonedDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Helper class to create or update graph Elements.
 * <p>
 * Example
 * <pre>
 * {@code
 * try (GraphUpdateContext ctx = graphRepository.beginGraphUpdate(Priority.NORMAL, user, authorizations)) {
 *   ElementMutation<Vertex> m = graph.prepareVertex("v1", visibility);
 *   ctx.update(m, updateContext -> {
 *     BcProperties.FILE_NAME.updateProperty(updateContext, "key", fileName, metadata, visibility);
 *   });
 * }
 * }
 * </pre>
 */
public abstract class GraphUpdateContext implements AutoCloseable {
    private static final int DEFAULT_SAVE_QUEUE_SIZE = 1000;
    private final Graph graph;
    private final WorkQueueRepository workQueueRepository;
    private final WebQueueRepository webQueueRepository;
    private final VisibilityTranslator visibilityTranslator;
    private final Priority priority;
    private final User user;
    private final Authorizations authorizations;
    private final Queue<UpdateFuture<? extends Element>> outstandingFutures = new LinkedList<>();
    private int saveQueueSize = DEFAULT_SAVE_QUEUE_SIZE;
    private boolean pushOnQueue = true;

    protected GraphUpdateContext(
            Graph graph,
            WorkQueueRepository workQueueRepository,
            WebQueueRepository webQueueRepository,
            VisibilityTranslator visibilityTranslator,
            Priority priority,
            User user,
            Authorizations authorizations
    ) {
        checkNotNull(graph, "graph cannot be null");
        checkNotNull(workQueueRepository, "workQueueRepository cannot be null");
        checkNotNull(visibilityTranslator, "visibilityTranslator cannot be null");
        checkNotNull(priority, "priority cannot be null");
        checkNotNull(user, "user cannot be null");
        checkNotNull(authorizations, "authorizations cannot be null");

        this.graph = graph;
        this.workQueueRepository = workQueueRepository;
        this.webQueueRepository = webQueueRepository;
        this.visibilityTranslator = visibilityTranslator;
        this.priority = priority;
        this.user = user;
        this.authorizations = authorizations;
    }

    /**
     * Saves, flushes, and pushes element on work queue.
     */
    @SuppressWarnings("unchecked")
    @Override
    public void close() {
        flushFutures();
    }

    /**
     * Saves, flushes, and pushes element on work queue.
     */
    public void flush() {
        flushFutures();
    }

    protected void flushFutures() {
        synchronized (outstandingFutures) {
            saveOutstandingUpdateFutures();
            graph.flush();
            if (isPushOnQueue()) {
                pushOutstandingUpdateFutures();
            }
            outstandingFutures.clear();
        }
    }

    private void pushOutstandingUpdateFutures() {
        outstandingFutures.forEach(f -> {
            try {
                Element element = f.get();
                webQueueRepository.broadcastPropertiesChange(
                        element,
                        f.getElementUpdateContext().getProperties(),
                        null,
                        priority
                );
                workQueueRepository.pushOnDwQueue(
                        element,
                        f.getElementUpdateContext().getProperties(),
                        null,
                        null,
                        priority
                );
            } catch (Exception ex) {
                throw new BcException("Could not push on queue", ex);
            }
        });
    }

    protected void saveOutstandingUpdateFutures() {
        List<UpdateFuture<?>> futures = outstandingFutures.stream()
                .filter(f -> !f.isDone())
                .collect(Collectors.toList());

        List<ElementMutation<? extends Element>> mutations = futures.stream()
                .map(f -> f.getElementUpdateContext().getMutation())
                .collect(Collectors.toList());

        Iterable<Element> results = graph.saveElementMutations(mutations, authorizations);
        int i = 0;
        for (Element result : results) {
            UpdateFuture future = futures.get(i);
            future.setElement(result);
            i++;
        }
    }

    /**
     * Similar to {@link GraphUpdateContext#update(ElementMutation, Update)} but
     * prepares the mutation from the element.
     */
    public <T extends Element> UpdateFuture<T> update(T element, Update<T> updateFn) {
        ZonedDateTime modifiedDate = null;
        VisibilityJson visibilityJson = null;
        String conceptType = null;
        return update(element, modifiedDate, visibilityJson, conceptType, updateFn);
    }

    /**
     * Similar to {@link GraphUpdateContext#update(Element, Update)} but calls
     * {@link ElementUpdateContext#updateBuiltInProperties(ZonedDateTime, VisibilityJson)} before calling
     * updateFn.
     */
    public <T extends Element> UpdateFuture<T> update(
            T element,
            ZonedDateTime modifiedDate,
            VisibilityJson visibilityJson,
            Update<T> updateFn
    ) {
        String conceptType = null;
        return update(element, modifiedDate, visibilityJson, conceptType, updateFn);
    }

    /**
     * Similar to {@link GraphUpdateContext#update(Element, Update)} but calls
     * {@link ElementUpdateContext#updateBuiltInProperties(ZonedDateTime, VisibilityJson)} and
     * {@link ElementUpdateContext#setConceptType(String)} before calling
     * updateFn.
     */
    public <T extends Element> UpdateFuture<T> update(
            T element,
            ZonedDateTime modifiedDate,
            VisibilityJson visibilityJson,
            String conceptType,
            Update<T> updateFn
    ) {
        checkNotNull(element, "element cannot be null");
        return update(element.prepareMutation(), modifiedDate, visibilityJson, conceptType, updateFn);
    }

    /**
     * Calls the update function, saves the element, and adds any updates to
     * the work queue.
     */
    public <T extends Element> UpdateFuture<T> update(ElementMutation<T> m, Update<T> updateFn) {
        ZonedDateTime modifiedDate = null;
        VisibilityJson visibilityJson = null;
        String conceptType = null;
        return update(m, modifiedDate, visibilityJson, conceptType, updateFn);
    }

    /**
     * Similar to {@link GraphUpdateContext#update(ElementMutation, Update)} but calls
     * {@link ElementUpdateContext#updateBuiltInProperties(ZonedDateTime, VisibilityJson)} before calling
     * updateFn.
     */
    public <T extends Element> UpdateFuture<T> update(
            ElementMutation<T> m,
            ZonedDateTime modifiedDate,
            VisibilityJson visibilityJson,
            Update<T> updateFn
    ) {
        String conceptType = null;
        return update(m, modifiedDate, visibilityJson, conceptType, updateFn);
    }

    /**
     * Similar to {@link GraphUpdateContext#update(ElementMutation, Update)} but calls
     * {@link ElementUpdateContext#updateBuiltInProperties(ZonedDateTime, VisibilityJson)} and
     * {@link ElementUpdateContext#setConceptType(String)} before calling
     * updateFn.
     */
    public <T extends Element> UpdateFuture<T> update(
            ElementMutation<T> m,
            ZonedDateTime modifiedDate,
            VisibilityJson visibilityJson,
            String conceptType,
            Update<T> updateFn
    ) {
        checkNotNull(m, "element cannot be null");
        checkNotNull(updateFn, "updateFn cannot be null");

        ElementUpdateContext<T> elementUpdateContext = new ElementUpdateContext<>(visibilityTranslator, m, user);
        if (modifiedDate != null || visibilityJson != null) {
            elementUpdateContext.updateBuiltInProperties(modifiedDate, visibilityJson);
        }
        if (conceptType != null) {
            elementUpdateContext.setConceptType(conceptType);
        }
        try {
            updateFn.update(elementUpdateContext);
        } catch (Exception ex) {
            throw new BcException("Could not update element", ex);
        }

        UpdateFuture<T> future = new UpdateFuture<>(elementUpdateContext);
        addToOutstandingFutures(future);
        return future;
    }

    /**
     * Similar to {@link GraphUpdateContext#getOrCreateVertexAndUpdate(String, Long, Visibility, String, Update)}
     * using the current time as the timestamp.
     */
    public UpdateFuture<Vertex> getOrCreateVertexAndUpdate(String vertexId, Visibility visibility, String conceptType, Update<Vertex> updateFn) {
        return getOrCreateVertexAndUpdate(vertexId, null, visibility, conceptType, updateFn);
    }

    /**
     * Gets a vertex by id from the graph. If the vertex does not exist prepares a new mutation and
     * calls update.
     *
     * @param vertexId   The existing vertex id, desired vertex id for new vertices, or null to generate a new id
     *                   and create a new vertex.
     * @param timestamp  The timestamp to assign new vertices or null to use the current time
     * @param visibility The visibility of the new vertex
     * @param updateFn   Closure in which all element updates should be made
     */
    public UpdateFuture<Vertex> getOrCreateVertexAndUpdate(
            String vertexId,
            Long timestamp,
            Visibility visibility,
            String conceptType,
            Update<Vertex> updateFn
    ) {
        Vertex existingVertex = vertexId == null ? null : graph.getVertex(vertexId, getAuthorizations());
        ElementMutation<Vertex> m = existingVertex == null
                ? graph.prepareVertex(vertexId, timestamp, visibility, conceptType)
                : existingVertex.prepareMutation();
        return update(m, updateFn);
    }

    /**
     * Similar to {@link GraphUpdateContext#getOrCreateEdgeAndUpdate(String, String, String, String, Long, Visibility, Update)}
     * using the current time as the timestamp.
     */
    public UpdateFuture<Edge> getOrCreateEdgeAndUpdate(
            String edgeId,
            String outVertexId,
            String inVertexId,
            String label,
            Visibility visibility,
            Update<Edge> updateFn
    ) {
        return getOrCreateEdgeAndUpdate(edgeId, outVertexId, inVertexId, label, null, visibility, updateFn);
    }

    /**
     * Gets a edge by id from the graph. If the edge does not exist prepares a new mutation and
     * calls update.
     *
     * @param edgeId      The existing edge id, desired edge id for new edges, or null to generate a new id
     *                    and create a new edge.
     * @param outVertexId the out vertex id of newly created edges
     * @param inVertexId  the in vertex id of newly created edges
     * @param label       the label to assign on newly created edges
     * @param timestamp   The timestamp to assign new edges or null to use the current time
     * @param visibility  The visibility of the new edge
     * @param updateFn    Closure in which all element updates should be made
     */
    public UpdateFuture<Edge> getOrCreateEdgeAndUpdate(
            String edgeId,
            String outVertexId,
            String inVertexId,
            String label,
            Long timestamp,
            Visibility visibility,
            Update<Edge> updateFn
    ) {
        Edge existingEdge = edgeId == null ? null : graph.getEdge(edgeId, getAuthorizations());
        ElementMutation<Edge> m = existingEdge == null
                ? graph.prepareEdge(edgeId, outVertexId, inVertexId, label, timestamp, visibility)
                : existingEdge.prepareMutation();
        return update(m, updateFn);
    }

    private <T extends Element> void addToOutstandingFutures(UpdateFuture<T> future) {
        synchronized (outstandingFutures) {
            outstandingFutures.add(future);
            if (outstandingFutures.size() > saveQueueSize) {
                flushFutures();
            }
        }
    }

    public interface Update<T extends Element> {
        void update(ElementUpdateContext<T> elemCtx) throws Exception;
    }

    public Priority getPriority() {
        return priority;
    }

    public User getUser() {
        return user;
    }

    public Authorizations getAuthorizations() {
        return authorizations;
    }

    public int getSaveQueueSize() {
        return saveQueueSize;
    }

    /**
     * Sets the maximum number of element updates to keep in memory before they are flushed
     * and added to the work queue.
     */
    public GraphUpdateContext setSaveQueueSize(int saveQueueSize) {
        this.saveQueueSize = saveQueueSize;
        return this;
    }

    public boolean isPushOnQueue() {
        return pushOnQueue;
    }

    public Graph getGraph() {
        return graph;
    }

    /**
     * By default updates are added to the work queue. If this is false updates will be
     * saved but not added to the work queue.
     */
    public GraphUpdateContext setPushOnQueue(boolean pushOnQueue) {
        this.pushOnQueue = pushOnQueue;
        return this;
    }

    public class UpdateFuture<T extends Element> implements Future<T> {
        private final ElementUpdateContext<T> elementUpdateContext;
        private T element;

        public UpdateFuture(ElementUpdateContext<T> elementUpdateContext) {
            this.elementUpdateContext = elementUpdateContext;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            throw new BcException("Not supported");
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return element != null;
        }

        @Override
        public T get() throws InterruptedException, ExecutionException {
            if (element == null) {
                element = this.elementUpdateContext.getMutation().save(authorizations);
            }
            return element;
        }

        @Override
        public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return get();
        }

        protected ElementUpdateContext<T> getElementUpdateContext() {
            return elementUpdateContext;
        }

        protected void setElement(T element) {
            this.element = element;
        }
    }
}
