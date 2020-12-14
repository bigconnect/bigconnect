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
package com.mware.core.cmdline;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.inject.Inject;
import com.mware.core.bootstrap.BcBootstrap;
import com.mware.core.lifecycle.LifeSupportService;
import com.mware.core.trace.TraceRepository;
import com.mware.core.orm.SimpleOrmSession;
import com.mware.ge.Authorizations;
import com.mware.ge.Graph;
import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.config.Configuration;
import com.mware.core.config.ConfigurationLoader;
import com.mware.core.model.lock.LockRepository;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.model.role.AuthorizationRepository;
import com.mware.core.model.user.PrivilegeRepository;
import com.mware.core.model.user.UserRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.security.VisibilityTranslator;
import com.mware.core.user.User;
import com.mware.core.util.VersionUtil;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;

public abstract class CommandLineTool {
    public static final String THREAD_NAME = "BigConnect CLI";
    protected BcLogger LOGGER;
    public static final boolean DEFAULT_INIT_FRAMEWORK = true;
    private Configuration configuration;
    private boolean willExit = false;
    private UserRepository userRepository;
    private AuthorizationRepository authorizationRepository;
    private Authorizations authorizations;
    private PrivilegeRepository privilegeRepository;
    private LockRepository lockRepository;
    private User user;
    private Graph graph;
    private WorkQueueRepository workQueueRepository;
    private SchemaRepository schemaRepository;
    private VisibilityTranslator visibilityTranslator;
    private SimpleOrmSession simpleOrmSession;
    private LifeSupportService lifeSupportService;
    private JCommander jCommander;
    private boolean frameworkInitialized;

    @Parameter(names = {"--help", "-h"}, description = "Print help", help = true)
    private boolean help;

    @Parameter(names = {"--version"}, description = "Print version")
    private boolean version;

    public int run(String[] args) throws Exception {
        return run(args, DEFAULT_INIT_FRAMEWORK);
    }

    public int run(String[] args, boolean initFramework) throws Exception {
        try {
            LOGGER = BcLoggerFactory.getLogger(CommandLineTool.class, "cli");
            final Thread mainThread = Thread.currentThread();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    willExit = true;
                    try {
                        mainThread.join(1000);
                    } catch (InterruptedException e) {
                        // nothing useful to do here
                    }
                }
            });

            try {
                jCommander = parseArguments(args);
                if (jCommander == null) {
                    return -1;
                }
                if (help) {
                    printHelp(jCommander);
                    return -1;
                }
                if (version) {
                    VersionUtil.printVersion();
                    return -1;
                }
            } catch (ParameterException ex) {
                System.err.println(ex.getMessage());
                return -1;
            }

            if (initFramework) {
                initializeFramework();
            }

            int result = -1;
            result = run();
            LOGGER.debug("command result: %d", result);
            return result;
        } finally {
            if (frameworkInitialized) {
                shutdown();
            }
        }
    }

    protected void initializeFramework() {
        InjectHelper.inject(this, BcBootstrap.bootstrapModuleMaker(getConfiguration()), getConfiguration());
        if(configuration.getBoolean(Configuration.TRACE_ENABLED, Configuration.DEFAULT_TRACE_ENABLED)) {
            TraceRepository traceRepository = InjectHelper.getInstance(TraceRepository.class);
            traceRepository.enable();
        }
        InjectHelper.getInstance(SchemaRepository.class); // verify we are up
        frameworkInitialized = true;
    }

    protected void printHelp(JCommander j) {
        j.usage();
    }

    protected JCommander parseArguments(String[] args) {
        return new JCommander(this, args);
    }

    protected void shutdown() {
        getLifeSupportService().shutdown();
    }

    protected abstract int run() throws Exception;

    protected Configuration getConfiguration() {
        if (configuration == null) {
            configuration = ConfigurationLoader.load();
        }
        return configuration;
    }

    protected User getUser() {
        if (this.user == null) {
            this.user = userRepository.getSystemUser();
        }
        return this.user;
    }

    protected Authorizations getAuthorizations() {
        if (this.authorizations == null) {
            this.authorizations = getAuthorizationRepository().getGraphAuthorizations(getUser());
        }
        return this.authorizations;
    }

    protected boolean willExit() {
        return willExit;
    }

    @Inject
    public void setLockRepository(LockRepository lockRepository) {
        this.lockRepository = lockRepository;
    }

    @Inject
    public final void setUserRepository(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @Inject
    public void setAuthorizationRepository(AuthorizationRepository authorizationRepository) {
        this.authorizationRepository = authorizationRepository;
    }

    @Inject
    public void setPrivilegeRepository(PrivilegeRepository privilegeRepository) {
        this.privilegeRepository = privilegeRepository;
    }

    @Inject
    public final void setGraph(Graph graph) {
        this.graph = graph;
    }

    @Inject
    public final void setWorkQueueRepository(WorkQueueRepository workQueueRepository) {
        this.workQueueRepository = workQueueRepository;
    }

    @Inject
    public final void setSchemaRepository(SchemaRepository schemaRepository) {
        this.schemaRepository = schemaRepository;
    }

    @Inject
    public final void setSimpleOrmSession(SimpleOrmSession simpleOrmSession) {
        this.simpleOrmSession = simpleOrmSession;
    }

    @Inject
    public void setLifeSupportService(LifeSupportService lifeSupportService) {
        this.lifeSupportService = lifeSupportService;
    }

    public SimpleOrmSession getSimpleOrmSession() {
        return simpleOrmSession;
    }

    public Graph getGraph() {
        return graph;
    }

    public WorkQueueRepository getWorkQueueRepository() {
        return workQueueRepository;
    }

    public UserRepository getUserRepository() {
        return userRepository;
    }

    public AuthorizationRepository getAuthorizationRepository() {
        return authorizationRepository;
    }

    public PrivilegeRepository getPrivilegeRepository() {
        return privilegeRepository;
    }

    public SchemaRepository getSchemaRepository() {
        return schemaRepository;
    }

    public VisibilityTranslator getVisibilityTranslator() {
        return visibilityTranslator;
    }

    public LifeSupportService getLifeSupportService() {
        return lifeSupportService;
    }

    @Inject
    public void setVisibilityTranslator(VisibilityTranslator visibilityTranslator) {
        this.visibilityTranslator = visibilityTranslator;
    }

    public static void main(CommandLineTool commandLineTool, String[] args, boolean initFramework) throws Exception {
        int res = commandLineTool.run(args, initFramework);
        if (res != 0) {
            System.exit(res);
        }
    }

    public static void main(CommandLineTool commandLineTool, String[] args) throws Exception {
        Thread.currentThread().setName(THREAD_NAME);
        main(commandLineTool, args, DEFAULT_INIT_FRAMEWORK);
    }
}
