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
package com.mware.core.bootstrap;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.matcher.Matchers;
import com.mware.core.cache.CacheService;
import com.mware.core.cache.InMemoryCacheService;
import com.mware.core.config.Configuration;
import com.mware.core.email.EmailRepository;
import com.mware.core.email.NopEmailRepository;
import com.mware.core.exception.BcException;
import com.mware.core.lifecycle.LifeSupportService;
import com.mware.core.lifecycle.LifecycleListener;
import com.mware.core.lifecycle.LifecycleStatus;
import com.mware.core.model.file.FileSystemRepository;
import com.mware.core.model.file.LocalFileSystemRepository;
import com.mware.core.model.lock.LockRepository;
import com.mware.core.model.lock.SingleJvmLockRepository;
import com.mware.core.model.longRunningProcess.GeLongRunningProcessRepository;
import com.mware.core.model.longRunningProcess.LongRunningProcessRepository;
import com.mware.core.model.regex.GeRegexRepository;
import com.mware.core.model.regex.RegexRepository;
import com.mware.core.model.role.AuthorizationRepository;
import com.mware.core.model.role.GeAuthorizationRepository;
import com.mware.core.model.schema.GeSchemaRepository;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.model.search.GeSearchRepository;
import com.mware.core.model.search.SearchRepository;
import com.mware.core.model.user.*;
import com.mware.core.model.workQueue.InMemoryWebQueueRepository;
import com.mware.core.model.workQueue.InMemoryWorkQueueRepository;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.model.workspace.GeWorkspaceRepository;
import com.mware.core.model.workspace.WorkspaceRepository;
import com.mware.core.orm.SimpleOrmSession;
import com.mware.core.orm.graph.GraphSimpleOrmSession;
import com.mware.core.security.AuditService;
import com.mware.core.security.DirectVisibilityTranslator;
import com.mware.core.security.LoggingAuditService;
import com.mware.core.security.VisibilityTranslator;
import com.mware.core.status.InMemoryStatusRepository;
import com.mware.core.status.JmxMetricsManager;
import com.mware.core.status.MetricsManager;
import com.mware.core.status.StatusRepository;
import com.mware.core.time.TimeRepository;
import com.mware.core.trace.DefaultTraceRepository;
import com.mware.core.trace.TraceRepository;
import com.mware.core.trace.Traced;
import com.mware.core.trace.TracedMethodInterceptor;
import com.mware.core.user.User;
import com.mware.core.util.*;
import com.mware.core.watcher.WatcherGraphListener;
import com.mware.ge.Graph;
import com.mware.ge.GraphFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The BcBootstrap is a Guice Module that configures itself by
 * discovering all available implementations of BootstrapBindingProvider
 * and invoking the addBindings() method.  If any discovered provider
 * cannot be instantiated, configuration of the Bootstrap Module will
 * fail and halt application initialization by throwing a BootstrapException.
 */
public class BcBootstrap extends AbstractModule implements LifecycleListener  {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(BcBootstrap.class);
    private static final String GRAPH_METADATA_GRAPH_VERSION_KEY = "graph.version";
    private static final Integer GRAPH_METADATA_GRAPH_VERSION = 3;

    private static BcBootstrap bcBootstrap;

    public synchronized static BcBootstrap bootstrap(final Configuration configuration) {
        if (bcBootstrap == null) {
            LOGGER.debug("Initializing BcBootstrap with Configuration:\n%s", configuration);
            bcBootstrap = new BcBootstrap(configuration);
        }
        return bcBootstrap;
    }

    /**
     * Get a ModuleMaker that will return the BcBootstrap, initializing it with
     * the provided Configuration if it has not already been created.
     *
     * @param configuration the bigCONNECT configuration
     * @return a ModuleMaker for use with the InjectHelper
     */
    public static InjectHelper.ModuleMaker bootstrapModuleMaker(final Configuration configuration) {
        return new InjectHelper.ModuleMaker() {
            @Override
            public Module createModule() {
                return BcBootstrap.bootstrap(configuration);
            }

            @Override
            public Configuration getConfiguration() {
                return configuration;
            }
        };
    }

    /**
     * The bigCONNECT Configuration.
     */
    private final Configuration configuration;

    /**
     * Create a BcBootstrap with the provided Configuration.
     *
     * @param config the configuration for this bootstrap
     */
    private BcBootstrap(final Configuration config) {
        this.configuration = config;
    }

    @Override
    protected void configure() {
        VersionUtil.printVersion();

        LOGGER.info("Initializing....");

        checkNotNull(configuration, "configuration cannot be null");
        bind(Configuration.class).toInstance(configuration);

        LOGGER.debug("binding %s", JmxMetricsManager.class.getName());
        MetricsManager metricsManager = new JmxMetricsManager();
        bind(MetricsManager.class).toInstance(metricsManager);

        bindInterceptor(Matchers.any(), Matchers.annotatedWith(Traced.class), new TracedMethodInterceptor());

        bind(TraceRepository.class)
                .toProvider(BcBootstrap.getConfigurableProvider(configuration, Configuration.TRACE_REPOSITORY, DefaultTraceRepository.class))
                .in(Scopes.SINGLETON);
        bind(Graph.class)
                .toProvider(getGraphProvider(configuration, Configuration.GRAPH_PROVIDER))
                .in(Scopes.SINGLETON);
        bind(LockRepository.class)
                .toProvider(BcBootstrap.getConfigurableProvider(configuration, Configuration.LOCK_REPOSITORY, SingleJvmLockRepository.class))
                .in(Scopes.SINGLETON);
        bind(WorkQueueRepository.class)
                .toProvider(BcBootstrap.getConfigurableProvider(configuration, Configuration.WORK_QUEUE_REPOSITORY, InMemoryWorkQueueRepository.class))
                .in(Scopes.SINGLETON);
        bind(WebQueueRepository.class)
                .toProvider(BcBootstrap.getConfigurableProvider(configuration, Configuration.WEB_QUEUE_REPOSITORY, InMemoryWebQueueRepository.class))
                .in(Scopes.SINGLETON);
        bind(LongRunningProcessRepository.class)
                .toProvider(BcBootstrap.getConfigurableProvider(configuration, Configuration.LONG_RUNNING_PROCESS_REPOSITORY, GeLongRunningProcessRepository.class))
                .in(Scopes.SINGLETON);
        bind(VisibilityTranslator.class)
                .toProvider(BcBootstrap.getConfigurableProvider(configuration, Configuration.VISIBILITY_TRANSLATOR, DirectVisibilityTranslator.class))
                .in(Scopes.SINGLETON);
        bind(UserRepository.class)
                .toProvider(BcBootstrap.getConfigurableProvider(configuration, Configuration.USER_REPOSITORY, GeUserRepository.class))
                .in(Scopes.SINGLETON);
        bind(UserSessionCounterRepository.class)
                .toProvider(BcBootstrap.getConfigurableProvider(configuration, Configuration.USER_SESSION_COUNTER_REPOSITORY, InMemoryUserSessionCounterRepository.class))
                .in(Scopes.SINGLETON);
        bind(WorkspaceRepository.class)
                .toProvider(BcBootstrap.getConfigurableProvider(configuration, Configuration.WORKSPACE_REPOSITORY, GeWorkspaceRepository.class))
                .in(Scopes.SINGLETON);
        bind(GraphAuthorizationRepository.class)
                .toProvider(BcBootstrap.getConfigurableProvider(configuration, Configuration.GRAPH_AUTHORIZATION_REPOSITORY, InMemoryGraphAuthorizationRepository.class))
                .in(Scopes.SINGLETON);
        bind(SchemaRepository.class)
                .toProvider(BcBootstrap.getConfigurableProvider(configuration, Configuration.ONTOLOGY_REPOSITORY, GeSchemaRepository.class))
                .in(Scopes.SINGLETON);
        bind(SimpleOrmSession.class)
                .toProvider(BcBootstrap.getConfigurableProvider(configuration, Configuration.SIMPLE_ORM_SESSION, GraphSimpleOrmSession.class))
                .in(Scopes.SINGLETON);
        bind(EmailRepository.class)
                .toProvider(BcBootstrap.getConfigurableProvider(configuration, Configuration.EMAIL_REPOSITORY, NopEmailRepository.class))
                .in(Scopes.SINGLETON);
        bind(StatusRepository.class)
                .toProvider(BcBootstrap.getConfigurableProvider(configuration, Configuration.STATUS_REPOSITORY, InMemoryStatusRepository.class))
                .in(Scopes.SINGLETON);
        bind(FileSystemRepository.class)
                .toProvider(BcBootstrap.getConfigurableProvider(configuration, Configuration.FILE_SYSTEM_REPOSITORY, LocalFileSystemRepository.class))
                .in(Scopes.SINGLETON);
        bind(AuthorizationRepository.class)
                .toProvider(BcBootstrap.getConfigurableProvider(configuration, Configuration.AUTHORIZATION_REPOSITORY, GeAuthorizationRepository.class))
                .in(Scopes.SINGLETON);
        bind(PrivilegeRepository.class)
                .toProvider(BcBootstrap.getConfigurableProvider(configuration, Configuration.PRIVILEGE_REPOSITORY, UserPropertyPrivilegeRepository.class))
                .in(Scopes.SINGLETON);
        bind(AuditService.class)
                .toProvider(BcBootstrap.getConfigurableProvider(configuration, Configuration.AUDIT_SERVICE, LoggingAuditService.class))
                .in(Scopes.SINGLETON);
        bind(TimeRepository.class)
                .toInstance(new TimeRepository());
        bind(RegexRepository.class)
                .toProvider(BcBootstrap.getConfigurableProvider(configuration, Configuration.REGEX_REPOSITORY, GeRegexRepository.class))
                .in(Scopes.SINGLETON);
        bind(CacheService.class)
                .toProvider(BcBootstrap.getConfigurableProvider(configuration, Configuration.CACHE_SERVICE, InMemoryCacheService.class))
                .in(Scopes.SINGLETON);
        bind(SearchRepository.class)
                .toProvider(BcBootstrap.getConfigurableProvider(configuration, null, GeSearchRepository.class))
                .in(Scopes.SINGLETON);

        injectProviders();
    }

    private Provider<? extends Graph> getGraphProvider(Configuration configuration, String configurationPrefix) {
        final Map<String, String> configurationSubset = configuration.getSubset(configurationPrefix);
        return (Provider<Graph>) () -> {
            Graph g;
            try {
                LOGGER.debug("creating graph");
                g = new GraphFactory().createGraph(configurationSubset);
            } catch (Exception e) {
                throw new RuntimeException("Could not create graph", e);
            }

            checkGraphVersion(g);
            if (configuration.getBoolean("watcher.enabled", false)) {
                g.addGraphEventListener(new WatcherGraphListener());
            }

            getLifeSupportService().addLifecycleListener((instance, from, to) -> {
                if (to == LifecycleStatus.SHUTDOWN) {
                    g.shutdown();
                }
            });

            getLifeSupportService().addLifecycleListener(this);
            getLifeSupportService().start();

            LOGGER.info("Initialization finished.");

            return g;
        };
    }

    private LifeSupportService getLifeSupportService() {
        return InjectHelper.getInstance(LifeSupportService.class);
    }

    public void checkGraphVersion(Graph g) {
        Object graphVersionObj = g.getMetadata(GRAPH_METADATA_GRAPH_VERSION_KEY);
        if (graphVersionObj == null) {
            g.setMetadata(GRAPH_METADATA_GRAPH_VERSION_KEY, GRAPH_METADATA_GRAPH_VERSION);
        } else if (graphVersionObj instanceof Integer) {
            Integer graphVersion = (Integer) graphVersionObj;
            if (!GRAPH_METADATA_GRAPH_VERSION.equals(graphVersion)) {
                throw new BcException("Invalid " + GRAPH_METADATA_GRAPH_VERSION_KEY + " expected " + GRAPH_METADATA_GRAPH_VERSION + " found " + graphVersion);
            }
        } else {
            throw new BcException("Invalid " + GRAPH_METADATA_GRAPH_VERSION_KEY + " expected Integer found " + graphVersionObj.getClass().getName());
        }
    }

    private void injectProviders() {
        LOGGER.debug("Running %s", BootstrapBindingProvider.class.getName());
        Iterable<BootstrapBindingProvider> bindingProviders = ServiceLoaderUtil.loadWithoutInjecting(BootstrapBindingProvider.class, configuration);
        for (BootstrapBindingProvider provider : bindingProviders) {
            LOGGER.debug("Configuring bindings from BootstrapBindingProvider: %s", provider.getClass().getName());
            provider.addBindings(this.binder(), configuration);
        }
    }

    public static void shutdown() {
        bcBootstrap = null;
    }

    public static <T> Provider<? extends T> getConfigurableProvider(final Configuration config, final String key, final String defaultClass) {
        return getConfigurableProvider(config, key, ClassUtil.<T>forName(defaultClass));
    }

    public static <T> Provider<? extends T> getConfigurableProvider(final Configuration config, final String key, final Class<T> defaultClass) {
        Class<? extends T> configuredClass = config.getClass(key, defaultClass);
        return configuredClass != null ? new ConfigurableProvider<>(configuredClass, config, key, null) : new NullProvider<>();
    }

    @Override
    public void lifeStatusChanged(Object instance, LifecycleStatus from, LifecycleStatus to) {
        if (LifecycleStatus.SHUTDOWN == to) {
            LOGGER.info("Shutdown: InjectHelper");
            InjectHelper.shutdown();

            LOGGER.info("Shutdown: BcBootstrap");
            BcBootstrap.shutdown();
        }
    }

    private static class NullProvider<T> implements Provider<T> {
        @Override
        public T get() {
            return null;
        }
    }

    private static class ConfigurableProvider<T> implements Provider<T> {
        private final Class<? extends T> clazz;
        private final Method initMethod;
        private final Object[] initMethodArgs;
        private final Configuration config;
        private final String keyPrefix;

        public ConfigurableProvider(final Class<? extends T> clazz, final Configuration config, String keyPrefix, final User user) {
            this.config = config;
            this.keyPrefix = keyPrefix;
            Object[] initArgs = null;
            Method init = findInit(clazz, Configuration.class, User.class);
            if (init != null) {
                initArgs = new Object[]{config, user};
            } else {
                init = findInit(clazz, Map.class, User.class);
                if (init != null) {
                    initArgs = new Object[]{config.toMap(), user};
                } else {
                    init = findInit(clazz, Configuration.class);
                    if (init != null) {
                        initArgs = new Object[]{config};
                    } else {
                        init = findInit(clazz, Map.class);
                        if (init != null) {
                            initArgs = new Object[]{config.toMap()};
                        }
                    }
                }
            }
            this.clazz = clazz;
            this.initMethod = init;
            this.initMethodArgs = initArgs;
        }

        private Method findInit(Class<? extends T> target, Class<?>... paramTypes) {
            try {
                return target.getMethod("init", paramTypes);
            } catch (NoSuchMethodException ex) {
                return null;
            } catch (SecurityException ex) {
                List<String> paramNames = new ArrayList<>();
                for (Class<?> pc : paramTypes) {
                    paramNames.add(pc.getSimpleName());
                }
                throw new BcException(String.format("Error accessing init(%s) method in %s.", paramNames, clazz.getName()), ex);
            }
        }

        @Override
        public T get() {
            Throwable error;
            try {
                LOGGER.debug("creating %s", this.clazz.getName());
                T impl;
                if (InjectHelper.getInjector() != null) {
                    impl = InjectHelper.getInstance(this.clazz);
                } else {
                    Constructor<? extends T> constructor = this.clazz.getConstructor();
                    impl = constructor.newInstance();
                }
                if (initMethod != null) {
                    initMethod.invoke(impl, initMethodArgs);
                }
                config.setConfigurables(impl, this.keyPrefix);
                return impl;
            } catch (IllegalAccessException iae) {
                LOGGER.error("Unable to access default constructor for %s", clazz.getName(), iae);
                error = iae;
            } catch (IllegalArgumentException iae) {
                LOGGER.error("Unable to initialize instance of %s.", clazz.getName(), iae);
                error = iae;
            } catch (InvocationTargetException ite) {
                LOGGER.error("Error initializing instance of %s.", clazz.getName(), ite);
                error = ite;
            } catch (NoSuchMethodException e) {
                LOGGER.error("Could not find constructor for %s.", clazz.getName(), e);
                error = e;
            } catch (InstantiationException e) {
                LOGGER.error("Could not create %s.", clazz.getName(), e);
                error = e;
            }
            throw new BcException(String.format("Unable to initialize instance of %s", clazz.getName()), error);
        }
    }
}
