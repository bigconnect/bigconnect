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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.config.Configuration;
import com.mware.core.exception.BcException;
import com.mware.core.model.WorkerBase;
import com.mware.core.model.plugin.PluginStateRepository;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.role.AuthorizationRepository;
import com.mware.core.model.user.UserRepository;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.security.VisibilityTranslator;
import com.mware.core.user.User;
import com.mware.core.util.*;
import com.mware.ge.*;
import com.mware.ge.util.IterableUtils;
import com.mware.ge.values.storable.StreamingPropertyValue;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.mware.core.model.workQueue.WorkQueueRepository.DW_DEFAULT_QUEUE_NAME;
import static com.mware.ge.util.IterableUtils.toList;

// Unlike many other injected classes, this is not a singleton
public class DataWorkerRunner extends WorkerBase<DataWorkerItem> {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(DataWorkerRunner.class);
    private final AuthorizationRepository authorizationRepository;
    private Graph graph;
    private Authorizations authorizations;
    private List<DataWorkerThreadedWrapper> workerWrappers = Lists.newArrayList();
    private User user;
    private UserRepository userRepository;
    private Configuration configuration;
    private VisibilityTranslator visibilityTranslator;
    private AtomicLong lastProcessedPropertyTime = new AtomicLong(0);
    private List<DataWorker> dataWorkers = Lists.newArrayList();
    private boolean prepareWorkersCalled;
    private final String queueName;
    private final PluginStateRepository pluginStateRepository;

    @Inject
    public DataWorkerRunner(
            WorkQueueRepository workQueueRepository,
            WebQueueRepository webQueueRepository,
            Configuration configuration,
            AuthorizationRepository authorizationRepository,
            Graph graph,
            PluginStateRepository pluginStateRepository
    ) {
        super(workQueueRepository, webQueueRepository, configuration, graph.getMetricsRegistry());
        this.authorizationRepository = authorizationRepository;
        this.queueName = configuration.get(Configuration.DW_QUEUE_NAME, DW_DEFAULT_QUEUE_NAME);
        this.pluginStateRepository = pluginStateRepository;
    }

    @Override
    protected DataWorkerItem tupleDataToWorkerItem(byte[] data) {
        DataWorkerMessage message = DataWorkerMessage.create(data);
        return new DataWorkerItem(message, getElements(message));
    }

    @Override
    protected String getQueueName() {
        return queueName;
    }

    @Override
    public void process(DataWorkerItem workerItem) throws Exception {
        DataWorkerMessage message = workerItem.getMessage();
        if (message.getProperties() != null && message.getProperties().length > 0) {
            safeExecuteHandlePropertiesOnElements(workerItem);
        } else if (message.getPropertyName() != null) {
            safeExecuteHandlePropertyOnElements(workerItem);
        } else {
            safeExecuteHandleAllEntireElements(workerItem);
        }
    }

    public void prepare(User user) {
        prepare(user, new DataWorkerInitializer());
    }

    public void prepare(User user, DataWorkerInitializer repository) {
        setUser(user);
        setAuthorizations(authorizationRepository.getGraphAuthorizations(user));
        prepareWorkers(repository);
        this.getWorkQueueRepository().setDataWorkerRunner(this);
    }

    public void prepareWorkers(DataWorkerInitializer initializer) {
        if (prepareWorkersCalled) {
            throw new BcException("prepareWorkers should be called only once");
        }
        prepareWorkersCalled = true;

        List<TermMentionFilter> termMentionFilters = loadTermMentionFilters();

        DataWorkerPrepareData workerPrepareData = new DataWorkerPrepareData(
                configuration.toMap(),
                termMentionFilters,
                this.user,
                this.authorizations,
                InjectHelper.getInjector()
        );
        Collection<DataWorker> workers = getAvailableWorkers();
        workers.forEach(worker -> {
            LOGGER.debug("registering state for: %s", worker.getClass().getName());
            pluginStateRepository.registerPlugin(worker.getClass().getName(), worker.systemPlugin(), user);
        });

        workers = workers.stream()
                .filter(worker -> pluginStateRepository.isEnabled(worker.getClass().getName()))
                .collect(Collectors.toList());

        for (DataWorker worker : workers) {
            try {
                LOGGER.debug("verifying: %s", worker.getClass().getName());
                VerifyResults verifyResults = worker.verify();
                if (verifyResults != null && verifyResults.getFailures().size() > 0) {
                    LOGGER.error("data worker %s had errors verifying", worker.getClass().getName());
                    for (VerifyResults.Failure failure : verifyResults.getFailures()) {
                        LOGGER.error("  %s", failure.getMessage());
                    }
                }

                if (initializer != null) {
                    initializer.initialize(worker);
                }
            } catch (Exception ex) {
                LOGGER.error("Could not verify data worker %s", worker.getClass().getName(), ex);
            }
        }

        List<DataWorkerThreadedWrapper> wrappers = Lists.newArrayList();
        for (DataWorker worker : workers) {
            try {
                LOGGER.debug("preparing: %s", worker.getClass().getName());
                worker.prepare(workerPrepareData);
                this.dataWorkers.add(worker);
            } catch (Exception ex) {
                LOGGER.error("Could not prepare data worker %s", worker.getClass().getName(), ex);
            }

            DataWorkerThreadedWrapper wrapper = new DataWorkerThreadedWrapper(worker, graph.getMetricsRegistry());
            setupWrapper(wrapper);
            wrappers.add(wrapper);
            Thread thread = new Thread(wrapper);
            String workerName = worker.getClass().getName();
            thread.setName(Thread.currentThread().getName()+"-dw-" + workerName);
            thread.start();
        }

        this.addDataWorkerThreadedWrappers(wrappers);
    }

    protected DataWorkerThreadedWrapper setupWrapper(DataWorkerThreadedWrapper wrapper) {
        return InjectHelper.inject(wrapper);
    }

    protected Collection<DataWorker> getAvailableWorkers() {
        return InjectHelper.getInjectedServices(
                DataWorker.class,
                configuration
        );
    }

    public void addDataWorkerThreadedWrappers(List<DataWorkerThreadedWrapper> wrappers) {
        this.workerWrappers.addAll(wrappers);
    }

    public void addDataWorkerThreadedWrappers(DataWorkerThreadedWrapper... wrappers) {
        this.workerWrappers.addAll(Lists.newArrayList(wrappers));
    }

    private List<TermMentionFilter> loadTermMentionFilters() {
        TermMentionFilterPrepareData termMentionFilterPrepareData = new TermMentionFilterPrepareData(
                configuration.toMap(),
                this.user,
                this.authorizations,
                InjectHelper.getInjector()
        );

        List<TermMentionFilter> termMentionFilters = toList(ServiceLoaderUtil.load(
                TermMentionFilter.class,
                configuration
        ));
        for (TermMentionFilter termMentionFilter : termMentionFilters) {
            try {
                termMentionFilter.prepare(termMentionFilterPrepareData);
            } catch (Exception ex) {
                throw new BcException(
                        "Could not initialize term mention filter: " + termMentionFilter.getClass().getName(),
                        ex
                );
            }
        }
        return termMentionFilters;
    }

    private void safeExecuteHandleAllEntireElements(DataWorkerItem workerItem) throws Exception {
        for (Element element : workerItem.getElements()) {
            safeExecuteHandleEntireElement(element, workerItem.getMessage());
        }
    }

    private void safeExecuteHandleEntireElement(Element element, DataWorkerMessage message) throws Exception {
        safeExecuteHandlePropertyOnElement(element, null, message);
        for (Property property : element.getProperties()) {
            safeExecuteHandlePropertyOnElement(element, property, message);
        }
    }

    private ImmutableList<Element> getVerticesFromMessage(DataWorkerMessage message) {
        ImmutableList.Builder<Element> vertices = ImmutableList.builder();

        for (String vertexId : message.getGraphVertexId()) {
            Vertex vertex;
            if (message.getStatus() == ElementOrPropertyStatus.DELETION || message.getStatus() == ElementOrPropertyStatus.HIDDEN) {
                vertex = graph.getVertex(
                        vertexId,
                        FetchHints.ALL,
                        message.getBeforeActionTimestamp(),
                        this.authorizations
                );
            } else {
                vertex = graph.getVertex(vertexId, FetchHints.ALL, this.authorizations);
            }
            if (doesExist(vertex)) {
                vertices.add(vertex);
            } else {
                LOGGER.warn("Could not find vertex with id %s", vertexId);
            }
        }
        return vertices.build();
    }

    private ImmutableList<Element> getEdgesFromMessage(DataWorkerMessage message) {
        ImmutableList.Builder<Element> edges = ImmutableList.builder();

        for (String edgeId : message.getGraphEdgeId()) {
            Edge edge;
            if (message.getStatus() == ElementOrPropertyStatus.DELETION || message.getStatus() == ElementOrPropertyStatus.HIDDEN) {
                edge = graph.getEdge(edgeId, FetchHints.ALL, message.getBeforeActionTimestamp(), this.authorizations);
            } else {
                edge = graph.getEdge(edgeId, FetchHints.ALL, this.authorizations);
            }
            if (doesExist(edge)) {
                edges.add(edge);
            } else {
                LOGGER.warn("Could not find edge with id %s", edgeId);
            }
        }
        return edges.build();
    }

    private boolean doesExist(Element element) {
        return element != null;
    }

    private void safeExecuteHandlePropertiesOnElements(DataWorkerItem workerItem) throws Exception {
        DataWorkerMessage message = workerItem.getMessage();
        for (Element element : workerItem.getElements()) {
            for (DataWorkerMessage.Property propertyMessage : message.getProperties()) {
                Property property = null;
                String propertyKey = propertyMessage.getPropertyKey();
                String propertyName = propertyMessage.getPropertyName();
                if (StringUtils.isNotEmpty(propertyKey) || StringUtils.isNotEmpty(propertyName)) {
                    if (propertyKey == null) {
                        property = element.getProperty(propertyName);
                    } else {
                        property = element.getProperty(propertyKey, propertyName);
                    }

                    if (property == null) {
                        LOGGER.debug(
                                "Could not find property [%s]:[%s] on vertex with id %s",
                                propertyKey,
                                propertyName,
                                element.getId()
                        );
                        continue;
                    }
                }

                safeExecuteHandlePropertyOnElement(
                        element,
                        property,
                        message.getWorkspaceId(),
                        message.getVisibilitySource(),
                        message.getPriority(),
                        message.isTraceEnabled(),
                        propertyMessage.getStatus(),
                        propertyMessage.getBeforeActionTimestampOrDefault()
                );
            }
        }
    }

    private void safeExecuteHandlePropertyOnElements(DataWorkerItem workerItem) throws Exception {
        DataWorkerMessage message = workerItem.getMessage();
        for (Element element : workerItem.getElements()) {
            Property property = getProperty(element, message);

            if (property != null) {
                safeExecuteHandlePropertyOnElement(element, property, message);
            } else {
                LOGGER.debug(
                        "Could not find property [%s]:[%s] on vertex with id %s",
                        message.getPropertyKey(),
                        message.getPropertyName(),
                        element.getId()
                );
            }
        }
    }

    private Property getProperty(Element element, DataWorkerMessage message) {
        if (message.getPropertyName() == null) {
            return null;
        }

        Iterable<Property> properties;

        if (message.getPropertyKey() == null) {
            properties = element.getProperties(message.getPropertyName());
        } else {
            properties = element.getProperties(message.getPropertyKey(), message.getPropertyName());
        }

        Property result = null;
        for (Property property : properties) {
            if (message.getWorkspaceId() != null && property.getVisibility().hasAuthorization(message.getWorkspaceId())) {
                result = property;
            } else if (result == null) {
                result = property;
            }
        }
        return result;
    }

    private void safeExecuteHandlePropertyOnElement(
            Element element,
            Property property,
            DataWorkerMessage message
    ) throws Exception {
        safeExecuteHandlePropertyOnElement(
                element,
                property,
                message.getWorkspaceId(),
                message.getVisibilitySource(),
                message.getPriority(),
                message.isTraceEnabled(),
                message.getStatus(),
                message.getBeforeActionTimestampOrDefault()
        );
    }

    private void safeExecuteHandlePropertyOnElement(
            Element element,
            Property property,
            String workspaceId,
            String visibilitySource,
            Priority priority,
            boolean traceEnabled,
            ElementOrPropertyStatus status,
            long beforeActionTimestamp
    ) throws Exception {
        String propertyText = getPropertyText(property);

        List<DataWorkerThreadedWrapper> interestedWorkerWrappers = findInterestedWorkers(element, property, status);
        if (interestedWorkerWrappers.size() == 0) {
            LOGGER.debug(
                    "Could not find interested workers for %s %s property %s (%s)",
                    element instanceof Vertex ? "vertex" : "edge",
                    element.getId(),
                    propertyText,
                    status
            );
            return;
        }
        if (LOGGER.isDebugEnabled()) {
            for (DataWorkerThreadedWrapper interestedWorkerWrapper : interestedWorkerWrappers) {
                LOGGER.debug(
                        "interested worker for %s %s property %s: %s (%s)",
                        element instanceof Vertex ? "vertex" : "edge",
                        element.getId(),
                        propertyText,
                        interestedWorkerWrapper.getWorker().getClass().getName(),
                        status
                );
            }
        }

        DataWorkerData workData = new DataWorkerData(
                visibilityTranslator,
                element,
                property,
                workspaceId,
                visibilitySource,
                priority,
                traceEnabled,
                beforeActionTimestamp,
                status
        );

        LOGGER.debug("Begin work on element %s property %s", element.getId(), propertyText);
        if (property != null && property.getValue() instanceof StreamingPropertyValue) {
            StreamingPropertyValue spb = (StreamingPropertyValue) property.getValue();
            safeExecuteStreamingPropertyValue(interestedWorkerWrappers, workData, spb);
        } else {
            safeExecuteNonStreamingProperty(interestedWorkerWrappers, workData);
        }

        lastProcessedPropertyTime.set(System.currentTimeMillis());

        this.graph.flush();

        LOGGER.debug("Completed work on %s", propertyText);
    }

    private String getPropertyText(Property property) {
        return property == null ? "[none]" : (property.getKey() + ":" + property.getName());
    }

    private void safeExecuteNonStreamingProperty(
            List<DataWorkerThreadedWrapper> interestedWorkerWrappers,
            DataWorkerData workData
    ) throws Exception {
        for (DataWorkerThreadedWrapper interestedWorkerWrapper1 : interestedWorkerWrappers) {
            interestedWorkerWrapper1.enqueueWork(null, workData);
        }

        for (DataWorkerThreadedWrapper interestedWorkerWrapper : interestedWorkerWrappers) {
            interestedWorkerWrapper.dequeueResult(true);
        }
    }

    private void safeExecuteStreamingPropertyValue(
            List<DataWorkerThreadedWrapper> interestedWorkerWrappers,
            DataWorkerData workData,
            StreamingPropertyValue streamingPropertyValue
    ) throws Exception {
        String[] workerNames = dataWorkerThreadedWrapperToNames(interestedWorkerWrappers);
        InputStream in = streamingPropertyValue.getInputStream();
        File tempFile = null;
        try {
            boolean requiresLocalFile = isLocalFileRequired(interestedWorkerWrappers);
            if (requiresLocalFile) {
                tempFile = copyToTempFile(in, workData);
                in = new FileInputStream(tempFile);
            }

            TeeInputStream teeInputStream = new TeeInputStream(in, workerNames);
            for (int i = 0; i < interestedWorkerWrappers.size(); i++) {
                interestedWorkerWrappers.get(i).enqueueWork(teeInputStream.getTees()[i], workData);
            }
            teeInputStream.loopUntilTeesAreClosed();
            for (DataWorkerThreadedWrapper interestedWorkerWrapper : interestedWorkerWrappers) {
                interestedWorkerWrapper.dequeueResult(false);
            }
        } finally {
            if (tempFile != null) {
                if (!tempFile.delete()) {
                    LOGGER.warn("Could not delete temp file %s", tempFile.getAbsolutePath());
                }
            }
            in.close();
        }
    }

    private File copyToTempFile(InputStream in, DataWorkerData workData) throws IOException {
        String fileExt = null;
        String fileName = BcSchema.FILE_NAME.getOnlyPropertyValue(workData.getElement());
        if (fileName != null) {
            fileExt = FilenameUtils.getExtension(fileName);
        }
        if (fileExt == null) {
            fileExt = "data";
        }
        File tempFile = File.createTempFile("dataWorkerBolt", fileExt);
        workData.setLocalFile(tempFile);
        try (OutputStream tempFileOut = new FileOutputStream(tempFile)) {
            IOUtils.copy(in, tempFileOut);
        } finally {
            in.close();

        }
        return tempFile;
    }

    private boolean isLocalFileRequired(List<DataWorkerThreadedWrapper> interestedWorkerWrappers) {
        for (DataWorkerThreadedWrapper worker : interestedWorkerWrappers) {
            if (worker.getWorker().isLocalFileRequired()) {
                return true;
            }
        }
        return false;
    }

    private List<DataWorkerThreadedWrapper> findInterestedWorkers(
            Element element,
            Property property,
            ElementOrPropertyStatus status
    ) {
        Set<String> dataWorkerWhiteList = IterableUtils.toSet(BcSchema.DATA_WORKER_WHITE_LIST.getPropertyValues(
                element));
        Set<String> dataWorkerBlackList = IterableUtils.toSet(BcSchema.DATA_WORKER_BLACK_LIST.getPropertyValues(
                element));

        List<DataWorkerThreadedWrapper> interestedWorkers = new ArrayList<>();
        for (DataWorkerThreadedWrapper wrapper : workerWrappers) {
            String dataWorkerName = wrapper.getWorker().getClass().getName();
            if (dataWorkerWhiteList.size() > 0 && !dataWorkerWhiteList.contains(
                    dataWorkerName)) {
                continue;
            }
            if (dataWorkerBlackList.contains(dataWorkerName)) {
                continue;
            }
            DataWorker worker = wrapper.getWorker();
            if (status == ElementOrPropertyStatus.DELETION) {
                addDeletedWorkers(interestedWorkers, worker, wrapper, element, property);
            } else if (status == ElementOrPropertyStatus.HIDDEN) {
                addHiddenWorkers(interestedWorkers, worker, wrapper, element, property);
            } else if (status == ElementOrPropertyStatus.UNHIDDEN) {
                addUnhiddenWorkers(interestedWorkers, worker, wrapper, element, property);
            } else if (worker.isHandled(element, property)) {
                interestedWorkers.add(wrapper);
            }
        }

        return interestedWorkers;
    }

    private void addDeletedWorkers(
            List<DataWorkerThreadedWrapper> interestedWorkers,
            DataWorker worker,
            DataWorkerThreadedWrapper wrapper,
            Element element,
            Property property
    ) {
        if (worker.isDeleteHandled(element, property)) {
            interestedWorkers.add(wrapper);
        }
    }

    private void addHiddenWorkers(
            List<DataWorkerThreadedWrapper> interestedWorkers,
            DataWorker worker,
            DataWorkerThreadedWrapper wrapper,
            Element element,
            Property property
    ) {
        if (worker.isHiddenHandled(element, property)) {
            interestedWorkers.add(wrapper);
        }
    }

    private void addUnhiddenWorkers(
            List<DataWorkerThreadedWrapper> interestedWorkers,
            DataWorker worker,
            DataWorkerThreadedWrapper wrapper,
            Element element,
            Property property
    ) {
        if (worker.isUnhiddenHandled(element, property)) {
            interestedWorkers.add(wrapper);
        }
    }

    private String[] dataWorkerThreadedWrapperToNames(List<DataWorkerThreadedWrapper> interestedWorkerWrappers) {
        String[] names = new String[interestedWorkerWrappers.size()];
        for (int i = 0; i < names.length; i++) {
            names[i] = interestedWorkerWrappers.get(i).getWorker().getClass().getName();
        }
        return names;
    }

    private ImmutableList<Element> getElements(DataWorkerMessage message) {
        ImmutableList.Builder<Element> results = ImmutableList.builder();
        if (message.getGraphVertexId() != null && message.getGraphVertexId().length > 0) {
            results.addAll(getVerticesFromMessage(message));
        }
        if (message.getGraphEdgeId() != null && message.getGraphEdgeId().length > 0) {
            results.addAll(getEdgesFromMessage(message));
        }
        return results.build();
    }

    public void shutdown() {
        for (DataWorkerThreadedWrapper wrapper : this.workerWrappers) {
            wrapper.stop();
        }

        super.stop();
    }

    public UserRepository getUserRepository() {
        return this.userRepository;
    }

    @Inject
    public void setUserRepository(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @Inject
    public void setGraph(Graph graph) {
        this.graph = graph;
    }

    @Inject
    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    @Inject
    public void setVisibilityTranslator(VisibilityTranslator visibilityTranslator) {
        this.visibilityTranslator = visibilityTranslator;
    }


    public void setAuthorizations(Authorizations authorizations) {
        this.authorizations = authorizations;
    }

    public long getLastProcessedTime() {
        return this.lastProcessedPropertyTime.get();
    }

    public User getUser() {
        return this.user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    public boolean isStarted() {
        return this.shouldRun();
    }

    public boolean canHandle(Element element, Property property, ElementOrPropertyStatus status) {
        if (!this.isStarted()) {
            //we are probably on a server and want to submit it to the architecture
            return true;
        }

        for (DataWorker worker : this.getAllDataWorkers()) {
            try {
                if (status == ElementOrPropertyStatus.DELETION && worker.isDeleteHandled(element, property)) {
                    return true;
                } else if (status == ElementOrPropertyStatus.HIDDEN && worker.isHiddenHandled(element, property)) {
                    return true;
                } else if (status == ElementOrPropertyStatus.UNHIDDEN && worker.isUnhiddenHandled(element, property)) {
                    return true;
                } else if (worker.isHandled(element, property)) {
                    return true;
                }
            } catch (Throwable t) {
                LOGGER.warn(
                        "Error checking to see if workers will handle graph property message.  Queueing anyways in case there was just a local error",
                        t
                );
                return true;
            }
        }

        if (property == null) {
            LOGGER.debug(
                    "No interested workers for %s so did not queue it",
                    element.getId()
            );
        } else {
            LOGGER.debug(
                    "No interested workers for %s %s %s so did not queue it",
                    element.getId(),
                    property.getKey(),
                    property.getValue()
            );
        }

        return false;
    }

    public boolean canHandle(Element element, String propertyKey, String propertyName, ElementOrPropertyStatus status) {
        if (!this.isStarted()) {
            //we are probably on a server and want to submit it to the architecture
            return true;
        }

        Property property = element.getProperty(propertyKey, propertyName);
        return canHandle(element, property, status);
    }

    private Collection<DataWorker> getAllDataWorkers() {
        return Lists.newArrayList(this.dataWorkers);
    }

    public static List<DataWorkerRunnerStoppable> startThreaded(int threadCount, User user) {
        List<DataWorkerRunnerStoppable> stoppables = new ArrayList<>();

        LOGGER.info("Starting DataWorkerRunners on %d threads", threadCount);
        for (int i = 0; i < threadCount; i++) {
            DataWorkerRunnerStoppable stoppable = new DataWorkerRunnerStoppable() {
                private DataWorkerRunner dataWorkerRunner = null;

                @Override
                public void run() {
                    try {
                        dataWorkerRunner = InjectHelper.getInstance(DataWorkerRunner.class);
                        dataWorkerRunner.prepare(user);
                        dataWorkerRunner.run();
                    } catch (Exception ex) {
                        LOGGER.error("Failed running DataWorkerRunner", ex);
                    }
                }

                @Override
                public void stop() {
                    try {
                        if (dataWorkerRunner != null) {
                            LOGGER.debug("Stopping DataWorkerRunner");
                            dataWorkerRunner.stop();
                        }
                    } catch (Exception ex) {
                        LOGGER.error("Failed stopping DataWorkerRunner", ex);
                    }
                }

                @Override
                public DataWorkerRunner getDataWorkerRunner() {
                    return dataWorkerRunner;
                }
            };
            stoppables.add(stoppable);
            Thread t = new Thread(stoppable);
            t.setName("dw-runner-" + t.getId());
            t.setDaemon(true);
            LOGGER.debug("Starting DataWorkerRunner thread: %s", t.getName());
            t.start();
        }

        return stoppables;
    }

    public interface DataWorkerRunnerStoppable extends StoppableRunnable  {
        DataWorkerRunner getDataWorkerRunner();
    }
}
