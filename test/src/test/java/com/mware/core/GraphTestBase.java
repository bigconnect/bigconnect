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

import com.mware.core.cache.CacheService;
import com.mware.core.cache.InMemoryCacheService;
import com.mware.core.config.Configuration;
import com.mware.core.config.HashMapConfigurationLoader;
import com.mware.core.exception.BcException;
import com.mware.core.model.file.ClassPathFileSystemRepository;
import com.mware.core.model.file.FileSystemRepository;
import com.mware.core.model.graph.GraphRepository;
import com.mware.core.model.lock.LockRepository;
import com.mware.core.model.lock.SingleJvmLockRepository;
import com.mware.core.model.longRunningProcess.GeLongRunningProcessRepository;
import com.mware.core.model.longRunningProcess.LongRunningProcessRepository;
import com.mware.core.model.notification.InMemorySystemNotificationRepository;
import com.mware.core.model.notification.InMemoryUserNotificationRepository;
import com.mware.core.model.notification.SystemNotificationRepository;
import com.mware.core.model.notification.UserNotificationRepository;
import com.mware.core.model.properties.UserSchema;
import com.mware.core.model.properties.WorkspaceSchema;
import com.mware.core.model.role.AuthorizationRepository;
import com.mware.core.model.role.GeAuthorizationRepository;
import com.mware.core.model.schema.GeSchemaRepository;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.model.termMention.TermMentionRepository;
import com.mware.core.model.user.*;
import com.mware.core.model.workQueue.TestWebQueueRepository;
import com.mware.core.model.workQueue.TestWorkQueueRepository;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.model.workspace.GeWorkspaceRepository;
import com.mware.core.model.workspace.WorkspaceListener;
import com.mware.core.model.workspace.WorkspaceRepository;
import com.mware.core.orm.SimpleOrmSession;
import com.mware.core.orm.inmemory.InMemorySimpleOrmSession;
import com.mware.core.security.AuditService;
import com.mware.core.security.DirectVisibilityTranslator;
import com.mware.core.security.LoggingAuditService;
import com.mware.core.security.VisibilityTranslator;
import com.mware.core.time.MockTimeRepository;
import com.mware.core.time.TimeRepository;
import com.mware.core.user.User;
import com.mware.ge.Authorizations;
import com.mware.ge.Graph;
import com.mware.ge.TextIndexHint;
import com.mware.ge.base.TestGraphFactory;
import com.mware.ge.event.GraphEvent;
import com.mware.ge.event.GraphEventListener;
import com.mware.ge.values.storable.TextValue;
import lombok.SneakyThrows;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;

import java.util.*;

import static com.mware.core.model.workQueue.WorkQueueRepository.DW_DEFAULT_INTERNAL_QUEUE_NAME;
import static com.mware.ge.util.GeAssert.addGraphEvent;
import static com.mware.ge.util.GeAssert.clearGraphEvents;

public abstract class GraphTestBase {
    protected WorkspaceRepository workspaceRepository;
    protected Graph graph;
    protected GraphRepository graphRepository;
    protected Configuration configuration;
    protected UserRepository userRepository;
    protected GraphAuthorizationRepository graphAuthorizationRepository;
    protected LockRepository lockRepository;
    protected VisibilityTranslator visibilityTranslator;
    protected SchemaRepository schemaRepository;
    protected WorkQueueRepository workQueueRepository;
    protected WebQueueRepository webQueueRepository;
    protected AuthorizationRepository authorizationRepository;
    protected TermMentionRepository termMentionRepository;
    protected UserNotificationRepository userNotificationRepository;
    protected SystemNotificationRepository systemNotificationRepository;
    protected UserSessionCounterRepository userSessionCounterRepository;
    protected TimeRepository timeRepository;
    protected PrivilegeRepository privilegeRepository;
    protected FileSystemRepository fileSystemRepository;
    protected LongRunningProcessRepository longRunningProcessRepository;
    protected CacheService cacheService;
    protected Map configurationMap;
    protected AuditService auditService;
    protected User user;
    protected SimpleOrmSession simpleOrmSession;

    @Before
    public void before() throws Exception {
        configurationMap = getConfigurationMap();
        configuration = getConfiguration();
        simpleOrmSession = getSimpleOrmSession();
        graph = getGraph();
        visibilityTranslator = getVisibilityTranslator();
        lockRepository = getLockRepository();
        graphAuthorizationRepository = getGraphAuthorizationRepository();
        cacheService = getCacheService();
        schemaRepository = getSchemaRepository();
        workQueueRepository = getWorkQueueRepository();
        webQueueRepository = getWebQueueRepository();
        termMentionRepository = getTermMentionRepository();
        userSessionCounterRepository = getUserSessionCounterRepository();
        timeRepository = getTimeRepository();
        auditService = getAuditService();
        fileSystemRepository = getFileSystemRepository();
        user = getUser();
        userNotificationRepository = getUserNotificationRepository();
        systemNotificationRepository = getSystemNotificationRepository();
        userRepository = getUserRepository();
        workspaceRepository = getWorkspaceRepository();
        graphRepository = getGraphRepository();
        authorizationRepository = getAuthorizationRepository();
        privilegeRepository = getPrivilegeRepository();
        longRunningProcessRepository = getLongRunningProcessRepository();
    }

    @After
    public void after() throws Exception {
        if (getGraph() != null) {
            getGraph().drop();
            getGraph().shutdown();
            graph = null;
        }
    }

    protected UserRepository getUserRepository() {
        if (userRepository != null) {
            return userRepository;
        }
        userRepository = new GeUserRepository(
                getConfiguration(),
                getSimpleOrmSession(),
                getGraphAuthorizationRepository(),
                getGraph(),
                getSchemaRepository(),
                getUserSessionCounterRepository(),
                getWorkQueueRepository(),
                getWebQueueRepository(),
                getLockRepository(),
                getAuthorizationRepository(),
                getPrivilegeRepository()
        ) {
            @Override
            protected Collection<UserListener> getUserListeners() {
                return GraphTestBase.this.getUserListeners();
            }
        };
        getGraph().defineProperty(UserSchema.USERNAME.getPropertyName()).dataType(TextValue.class).textIndexHint(TextIndexHint.EXACT_MATCH).define();
        getGraph().defineProperty(UserSchema.DISPLAY_NAME.getPropertyName()).dataType(TextValue.class).textIndexHint(TextIndexHint.EXACT_MATCH).define();
        getGraph().defineProperty(UserSchema.CREATE_DATE.getPropertyName()).dataType(TextValue.class).define();
        getGraph().defineProperty(UserSchema.PASSWORD_HASH.getPropertyName()).dataType(TextValue.class).define();
        getGraph().defineProperty(UserSchema.PASSWORD_SALT.getPropertyName()).dataType(TextValue.class).define();
        getGraph().defineProperty(UserSchema.EMAIL_ADDRESS.getPropertyName()).dataType(TextValue.class).define();
        return userRepository;
    }

    protected WorkspaceRepository getWorkspaceRepository() {
        if (workspaceRepository != null) {
            return workspaceRepository;
        }
        workspaceRepository = new GeWorkspaceRepository(
                getGraph(),
                getConfiguration(),
                getUserRepository(),
                getGraphAuthorizationRepository(),
                getLockRepository(),
                getVisibilityTranslator(),
                getTermMentionRepository(),
                getSchemaRepository(),
                getWorkQueueRepository(),
                getWebQueueRepository(),
                getAuthorizationRepository()
        ) {
            @Override
            public Collection<WorkspaceListener> getWorkspaceListeners() {
                return GraphTestBase.this.getWorkspaceListeners();
            }
        };
        getGraph().defineProperty(WorkspaceSchema.TITLE.getPropertyName()).dataType(TextValue.class).textIndexHint(TextIndexHint.ALL).define();
        return workspaceRepository;
    }

    protected Collection<WorkspaceListener> getWorkspaceListeners() {
        return new ArrayList<>();
    }

    protected TermMentionRepository getTermMentionRepository() {
        if (termMentionRepository != null) {
            return termMentionRepository;
        }
        termMentionRepository = new TermMentionRepository(
                getGraph(),
                getGraphAuthorizationRepository()
        );
        return termMentionRepository;
    }

    protected AuthorizationRepository getAuthorizationRepository() {
        if (authorizationRepository != null) {
            return authorizationRepository;
        }
        authorizationRepository = new GeAuthorizationRepository(
                getGraph(),
                getGraphAuthorizationRepository(),
                getConfiguration(),
                getUserNotificationRepository(),
                getWebQueueRepository(),
                getLockRepository()
        ) {
            @Override
            protected UserRepository getUserRepository() {
                return GraphTestBase.this.getUserRepository();
            }
        };
        return authorizationRepository;
    }

    protected UserNotificationRepository getUserNotificationRepository() {
        if (userNotificationRepository != null) {
            return userNotificationRepository;
        }
        userNotificationRepository = new InMemoryUserNotificationRepository(
                getSimpleOrmSession(),
                getWebQueueRepository()
        );
        return userNotificationRepository;
    }

    protected SystemNotificationRepository getSystemNotificationRepository() {
        if (systemNotificationRepository != null) {
            return systemNotificationRepository;
        }
        systemNotificationRepository = new InMemorySystemNotificationRepository(
                getSimpleOrmSession()
        );
        return systemNotificationRepository;
    }

    protected WorkQueueRepository getWorkQueueRepository() {
        if (workQueueRepository != null) {
            return workQueueRepository;
        }
        workQueueRepository = new TestWorkQueueRepository(
                getGraph(),
                getConfiguration()
        );
        return workQueueRepository;
    }

    protected WebQueueRepository getWebQueueRepository() {
        if (webQueueRepository != null) {
            return webQueueRepository;
        }
        webQueueRepository = new TestWebQueueRepository() {
            @Override
            protected UserRepository getUserRepository() {
                return GraphTestBase.this.getUserRepository();
            }

            @Override
            public void pushOntologyChange(String workspaceId, SchemaAction action, Iterable<String> conceptIds, Iterable<String> relationshipIds, Iterable<String> propertyIds) {
            }
        };
        return webQueueRepository;
    }

    protected List<byte[]> getWorkQueueItems(String queueName) {
        WorkQueueRepository workQueueRepository = getWorkQueueRepository();
        if (!(workQueueRepository instanceof TestWorkQueueRepository)) {
            throw new BcException("Can only get work queue items from " + TestWorkQueueRepository.class.getName());
        }
        List<byte[]> items = ((TestWorkQueueRepository) workQueueRepository).getWorkQueue(queueName);
        if (items == null) {
            return new ArrayList<>();
        }
        return items;
    }

    protected void clearWorkQueues() {
        WorkQueueRepository workQueueRepository = getWorkQueueRepository();
        if (!(workQueueRepository instanceof TestWorkQueueRepository)) {
            throw new BcException("Can only clear work queue items from " + TestWorkQueueRepository.class.getName());
        }
        ((TestWorkQueueRepository) workQueueRepository).clearQueue();
    }

    protected List<JSONObject> getBroadcastJsonValues() {
        WorkQueueRepository workQueueRepository = getWorkQueueRepository();
        if (!(workQueueRepository instanceof TestWorkQueueRepository)) {
            throw new BcException("Can only clear work queue items from " + TestWorkQueueRepository.class.getName());
        }
        return ((TestWebQueueRepository) webQueueRepository).getBroadcastJsonValues();
    }

    protected SchemaRepository getSchemaRepository() {
        if (schemaRepository != null) {
            return schemaRepository;
        }
        try {
            schemaRepository = new GeSchemaRepository(
                    getGraph(),
                    getGraphRepository(),
                    getVisibilityTranslator(),
                    getConfiguration(),
                    getGraphAuthorizationRepository(),
                    getCacheService()
            ) {
                @Override
                protected PrivilegeRepository getPrivilegeRepository() {
                    return GraphTestBase.this.getPrivilegeRepository();
                }

                @Override
                protected WorkspaceRepository getWorkspaceRepository() {
                    return GraphTestBase.this.getWorkspaceRepository();
                }
            };
        } catch (Exception ex) {
            throw new BcException("Could not create ontology repository", ex);
        }
        return schemaRepository;
    }

    protected VisibilityTranslator getVisibilityTranslator() {
        if (visibilityTranslator != null) {
            return visibilityTranslator;
        }
        visibilityTranslator = new DirectVisibilityTranslator();
        return visibilityTranslator;
    }

    protected LockRepository getLockRepository() {
        if (lockRepository != null) {
            return lockRepository;
        }
        lockRepository = new SingleJvmLockRepository();
        return lockRepository;
    }

    protected SimpleOrmSession getSimpleOrmSession() {
        if(simpleOrmSession != null) {
            return simpleOrmSession;
        }

        simpleOrmSession = new InMemorySimpleOrmSession();
        return simpleOrmSession;
    }

    protected GraphAuthorizationRepository getGraphAuthorizationRepository() {
        if (graphAuthorizationRepository != null) {
            return graphAuthorizationRepository;
        }
        graphAuthorizationRepository = new InMemoryGraphAuthorizationRepository();
        return graphAuthorizationRepository;
    }

    protected Collection<UserListener> getUserListeners() {
        return new ArrayList<>();
    }

    protected void setPrivileges(User user, Set<String> privileges) {
        ((UserPropertyPrivilegeRepository) getPrivilegeRepository()).setPrivileges(user, privileges, getUserRepository().getSystemUser());
    }

    protected PrivilegeRepository getPrivilegeRepository() {
        if (privilegeRepository != null) {
            return privilegeRepository;
        }
        privilegeRepository = new UserPropertyPrivilegeRepository(
                getSchemaRepository(),
                getConfiguration(),
                getUserNotificationRepository(),
                getWebQueueRepository(),
                getAuthorizationRepository()
        ) {
            @Override
            protected Iterable<PrivilegesProvider> getPrivilegesProviders(Configuration configuration) {
                return GraphTestBase.this.getPrivilegesProviders();
            }

            @Override
            protected UserRepository getUserRepository() {
                return GraphTestBase.this.getUserRepository();
            }
        };
        return privilegeRepository;
    }

    protected Iterable<PrivilegesProvider> getPrivilegesProviders() {
        return new ArrayList<>();
    }

    protected UserSessionCounterRepository getUserSessionCounterRepository() {
        if (userSessionCounterRepository != null) {
            return userSessionCounterRepository;
        }
        userSessionCounterRepository = new InMemoryUserSessionCounterRepository(
                getTimeRepository()
        );
        return userSessionCounterRepository;
    }

    protected TimeRepository getTimeRepository() {
        if (timeRepository != null) {
            return timeRepository;
        }
        timeRepository = new MockTimeRepository();
        return timeRepository;
    }

    protected FileSystemRepository getFileSystemRepository() {
        if (fileSystemRepository != null) {
            return fileSystemRepository;
        }
        fileSystemRepository = new ClassPathFileSystemRepository("");
        return fileSystemRepository;
    }

    protected Configuration getConfiguration() {
        if (configuration != null) {
            return configuration;
        }
        Map config = getConfigurationMap();
        HashMapConfigurationLoader configLoader = new HashMapConfigurationLoader(config);

        configuration = new Configuration(configLoader, config) {
            @Override
            protected SchemaRepository getSchemaRepository() {
                return GraphTestBase.this.getSchemaRepository();
            }

            @Override
            protected PrivilegeRepository getPrivilegeRepository() {
                return GraphTestBase.this.getPrivilegeRepository();
            }
        };
        return configuration;
    }

    @SuppressWarnings("unchecked")
    protected Map getConfigurationMap() {
        if (configurationMap != null) {
            return configurationMap;
        }
        configurationMap = new HashMap();
        configurationMap.put("com.mware.core.model.user.UserPropertyPrivilegeRepository.defaultPrivileges", "");
        return configurationMap;
    }

    protected GraphRepository getGraphRepository() {
        if (graphRepository != null) {
            return graphRepository;
        }
        graphRepository = new GraphRepository(
                getGraph(),
                getVisibilityTranslator(),
                getTermMentionRepository(),
                getWorkQueueRepository(),
                getWebQueueRepository(),
                getConfiguration()
        );
        return graphRepository;
    }

    @SneakyThrows
    protected Graph getGraph() {
        if (graph != null) {
            return graph;
        }
        graph = graphFactory().createGraph();
        clearGraphEvents();
        getGraph().addGraphEventListener(new GraphEventListener() {
            @Override
            public void onGraphEvent(GraphEvent graphEvent) {
                addGraphEvent(graphEvent);
            }
        });
        return graph;
    }

    protected abstract TestGraphFactory graphFactory();

    public LongRunningProcessRepository getLongRunningProcessRepository() {
        if (longRunningProcessRepository != null) {
            return longRunningProcessRepository;
        }
        longRunningProcessRepository = new GeLongRunningProcessRepository(
                getGraphRepository(),
                getGraphAuthorizationRepository(),
                getUserRepository(),
                getWorkQueueRepository(),
                getWebQueueRepository(),
                getGraph(),
                getAuthorizationRepository()
        ) {
            @Override
            public void reportProgress(JSONObject longRunningProcessQueueItem, double progressPercent, String message) {
            }

            @Override
            public void reportProgress(String longRunningProcessGraphVertexId, double progressPercent, String message) {
            }
        };

        return longRunningProcessRepository;
    }

    public CacheService getCacheService() {
        if (cacheService != null) {
            return cacheService;
        }
        cacheService = new InMemoryCacheService();
        return cacheService;
    }

    public AuditService getAuditService() {
        if (auditService != null) {
            return auditService;
        }
        auditService = new LoggingAuditService();
        return auditService;
    }

    public Authorizations getGraphAuthorizations(String... authorizations) {
        return getGraph().createAuthorizations(authorizations);
    }

    public Authorizations getGraphAuthorizations(User user, String... authorizations) {
        return getAuthorizationRepository().getGraphAuthorizations(user, authorizations);
    }

    protected User getUser() {
        if (user == null) {
            user = new InMemoryUser("test", "Test User", "test@example.org", null);
        }
        return user;
    }

    protected List<byte[]> getGraphPropertyQueueItems() {
        return getWorkQueueItems(configuration.get(Configuration.DW_INTERNAL_QUEUE_NAME, DW_DEFAULT_INTERNAL_QUEUE_NAME));
    }
}
