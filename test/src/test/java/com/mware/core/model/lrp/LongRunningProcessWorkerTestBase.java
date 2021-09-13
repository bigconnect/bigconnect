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
package com.mware.core.model.lrp;

import com.google.inject.Injector;
import com.mware.core.config.Configuration;
import com.mware.core.config.ConfigurationLoader;
import com.mware.core.config.HashMapConfigurationLoader;
import com.mware.core.model.graph.GraphRepository;
import com.mware.core.model.longRunningProcess.LongRunningProcessRepository;
import com.mware.core.model.longRunningProcess.LongRunningProcessWorker;
import com.mware.core.model.longRunningProcess.LongRunningWorkerPrepareData;
import com.mware.core.model.role.AuthorizationRepository;
import com.mware.core.model.termMention.TermMentionRepository;
import com.mware.core.model.user.UserRepository;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.security.VisibilityTranslator;
import com.mware.core.status.MetricsManager;
import com.mware.core.user.SystemUser;
import com.mware.core.user.User;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.Authorizations;
import com.mware.ge.Graph;
import com.mware.ge.inmemory.InMemoryGraph;
import org.json.JSONObject;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class LongRunningProcessWorkerTestBase {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(LongRunningProcessWorkerTestBase.class);
    private Graph graph;
    private GraphRepository graphRepository;

    private SystemUser systemUser = new SystemUser();

    @Mock
    private User user;
    @Mock
    private LongRunningProcessRepository longRunningProcessRepository;
    @Mock
    private UserRepository userRepository;
    @Mock
    private AuthorizationRepository authorizationRepository;
    @Mock
    private Injector injector;
    @Mock
    private MetricsManager metricsManager;
    @Mock
    private com.codahale.metrics.Counter mockCounter;
    @Mock
    private com.codahale.metrics.Timer mockTimer;
    @Mock
    private com.codahale.metrics.Meter mockMeter;
    @Mock
    private VisibilityTranslator visibilityTranslator;
    @Mock
    private TermMentionRepository termMentionRepository;
    @Mock
    private WorkQueueRepository workQueueRepository;
    @Mock
    private WebQueueRepository webQueueRepository;
    @Mock
    private Authorizations systemUserAuthorizations;

    protected void before() {
        graph = InMemoryGraph.create();
        when(metricsManager.counter(any())).thenReturn(mockCounter);
        when(metricsManager.timer(any())).thenReturn(mockTimer);
        when(metricsManager.meter(any())).thenReturn(mockMeter);
        when(userRepository.getSystemUser()).thenReturn(systemUser);
        when(authorizationRepository.getGraphAuthorizations(systemUser)).thenReturn(systemUserAuthorizations);
    }

    protected void prepare(LongRunningProcessWorker worker) {
        worker.prepare(getLongRunningWorkerPrepareData());
    }

    protected LongRunningWorkerPrepareData getLongRunningWorkerPrepareData() {
        return new LongRunningWorkerPrepareData(
                getConfig(),
                getUser(),
                getInjector()
        );
    }

    private Injector getInjector() {
        return injector;
    }

    protected Graph getGraph() {
        return graph;
    }

    protected LongRunningProcessRepository getLongRunningProcessRepository() {
        return longRunningProcessRepository;
    }

    protected UserRepository getUserRepository() {
        return userRepository;
    }

    protected AuthorizationRepository getAuthorizationRepository() {
        return authorizationRepository;
    }

    protected User getUser() {
        return user;
    }

    protected Map getConfig() {
        return new HashMap();
    }

    protected MetricsManager getMetricsManager() {
        return metricsManager;
    }

    public GraphRepository getGraphRepository() {
        if (graphRepository == null) {
            ConfigurationLoader hashMapConfigurationLoader = new HashMapConfigurationLoader(getConfig());
            Configuration configuration = new Configuration(hashMapConfigurationLoader, new HashMap<>());

            graphRepository = new GraphRepository(
                    getGraph(),
                    getTermMentionRepository(),
                    getWorkQueueRepository(),
                    getWebQueueRepository(),
                    configuration
            );
        }
        return graphRepository;
    }

    protected void run(LongRunningProcessWorker worker, JSONObject queueItem) {
        if (worker.isHandled(queueItem)) {
            worker.process(queueItem);
        } else {
            LOGGER.warn("Unhandled: %s", queueItem.toString());
        }
    }

    public VisibilityTranslator getVisibilityTranslator() {
        return visibilityTranslator;
    }

    public TermMentionRepository getTermMentionRepository() {
        return termMentionRepository;
    }

    public WorkQueueRepository getWorkQueueRepository() {
        return workQueueRepository;
    }

    public WebQueueRepository getWebQueueRepository() {
        return webQueueRepository;
    }
}
