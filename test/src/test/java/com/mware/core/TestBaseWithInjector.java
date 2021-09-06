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
package com.mware.core;

import com.mware.core.bootstrap.BcBootstrap;
import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.config.Configuration;
import com.mware.core.config.HashMapConfigurationLoader;
import com.mware.core.ingest.video.VideoFrameInfo;
import com.mware.core.lifecycle.LifeSupportService;
import com.mware.core.model.longRunningProcess.LongRunningProcessRepository;
import com.mware.core.model.notification.SystemNotificationService;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.model.termMention.TermMentionRepository;
import com.mware.core.model.user.GraphAuthorizationRepository;
import com.mware.core.model.user.UserRepository;
import com.mware.core.model.workQueue.InMemoryWebQueueRepository;
import com.mware.core.model.workQueue.InMemoryWorkQueueRepository;
import com.mware.core.model.workspace.WorkspaceRepository;
import com.mware.core.orm.inmemory.InMemorySimpleOrmSession;
import com.mware.core.security.BcVisibility;
import com.mware.core.user.SystemUser;
import com.mware.core.user.User;
import com.mware.ge.Authorizations;
import com.mware.ge.Graph;
import com.mware.ge.inmemory.InMemoryGraph;
import com.mware.ge.search.DefaultSearchIndex;
import org.junit.After;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

public abstract class TestBaseWithInjector {
    final Map configMap = new HashMap();
    Configuration configuration;
    Graph graph;

    @Before
    public void before() {
        configMap.put("graph", InMemoryGraph.class.getName());
        configMap.put("graph.search", DefaultSearchIndex.class.getName());
        configMap.put("repository.workQueue", InMemoryWorkQueueRepository.class.getName());
        configMap.put("repository.webQueue", InMemoryWebQueueRepository.class.getName());
        configMap.put("simpleOrmSession", InMemorySimpleOrmSession.class.getName());

        HashMapConfigurationLoader configLoader = new HashMapConfigurationLoader(configMap);
        configuration = configLoader.createConfiguration();
        InjectHelper.inject(this, BcBootstrap.bootstrapModuleMaker(configuration), configuration);

        setupGraphAuthorizations();

        InjectHelper.getInstance(SystemNotificationService.class);

        graph = InjectHelper.getInstance(Graph.class);
    }

    @After
    public void after() {
        InjectHelper.getInstance(LifeSupportService.class).shutdown();
    }

    private void setupGraphAuthorizations() {
        GraphAuthorizationRepository graphAuthorizationRepository = InjectHelper.getInstance(GraphAuthorizationRepository.class);
        graphAuthorizationRepository.addAuthorizationToGraph(
                BcVisibility.SUPER_USER_VISIBILITY_STRING,
                UserRepository.VISIBILITY_STRING,
                TermMentionRepository.VISIBILITY_STRING,
                LongRunningProcessRepository.VISIBILITY_STRING,
                SchemaRepository.VISIBILITY_STRING,
                WorkspaceRepository.VISIBILITY_STRING,
                VideoFrameInfo.VISIBILITY_STRING
        );
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public User getUser() {
        return new SystemUser();
    }

    public Authorizations getAuthorizations() {
        return graph.createAuthorizations();
    }

    public Map getConfigMap() {
        return configMap;
    }
}
