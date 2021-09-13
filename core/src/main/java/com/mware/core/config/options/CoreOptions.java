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
package com.mware.core.config.options;

import com.mware.core.config.ConfigOption;
import com.mware.core.config.OptionHolder;
import com.mware.core.email.EmailRepository;
import com.mware.core.email.NopEmailRepository;
import com.mware.core.model.file.FileSystemRepository;
import com.mware.core.model.file.LocalFileSystemRepository;
import com.mware.core.model.lock.LockRepository;
import com.mware.core.model.lock.SingleJvmLockRepository;
import com.mware.core.model.user.GraphAuthorizationRepository;
import com.mware.core.model.user.InMemoryGraphAuthorizationRepository;
import com.mware.core.model.user.InMemoryUserSessionCounterRepository;
import com.mware.core.model.workQueue.InMemoryWebQueueRepository;
import com.mware.core.model.workQueue.InMemoryWorkQueueRepository;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.orm.SimpleOrmSession;
import com.mware.core.orm.graph.GraphSimpleOrmSession;
import com.mware.core.security.AuditService;
import com.mware.core.security.NopAuditService;
import com.mware.core.trace.DefaultTraceRepository;
import com.mware.core.trace.TraceRepository;
import com.mware.ge.metric.DropWizardMetricRegistry;

import static com.mware.core.config.OptionChecker.disallowEmpty;
import static com.mware.core.config.OptionChecker.positiveInt;

public class CoreOptions extends OptionHolder {
    public static final ConfigOption<Class<? extends LockRepository>> LOCK_REPOSITORY = new ConfigOption(
            "repository.lock",
            "Implementation of LockRepository",
            disallowEmpty(),
            Class.class,
            SingleJvmLockRepository.class
    );

    public static final ConfigOption<Class<? extends TraceRepository>> TRACE_REPOSITORY = new ConfigOption(
            "repository.trace",
            "Implementation of TraceRepository",
            disallowEmpty(),
            Class.class,
            DefaultTraceRepository.class
    );

    public static final ConfigOption<Class<? extends WorkQueueRepository>> WORK_QUEUE_REPOSITORY = new ConfigOption(
            "repository.workQueue",
            "Implementation of WorkQueueRepository",
            disallowEmpty(),
            Class.class,
            InMemoryWorkQueueRepository.class
    );

    public static final ConfigOption<Class<? extends WebQueueRepository>> WEB_QUEUE_REPOSITORY = new ConfigOption(
            "repository.webQueue",
            "Implementation of WebQueueRepository",
            disallowEmpty(),
            Class.class,
            InMemoryWebQueueRepository.class
    );

    public static final ConfigOption<Class<? extends InMemoryUserSessionCounterRepository>> USER_SESSION_COUNTER_REPOSITORY = new ConfigOption(
            "repository.userSessionCounter",
            "Implementation of UserSessionCounterRepository",
            disallowEmpty(),
            Class.class,
            InMemoryUserSessionCounterRepository.class
    );

    public static final ConfigOption<Class<? extends GraphAuthorizationRepository>> GRAPH_AUTHORIZATION_REPOSITORY = new ConfigOption(
            "repository.graphAuthorization",
            "Implementation of GraphAuthorizationRepository",
            disallowEmpty(),
            Class.class,
            InMemoryGraphAuthorizationRepository.class
    );

    public static final ConfigOption<Class<? extends SimpleOrmSession>> SIMPLE_ORM_SESSION = new ConfigOption(
            "simpleOrmSession",
            "Implementation of SimpleOrmSession",
            disallowEmpty(),
            Class.class,
            GraphSimpleOrmSession.class
    );

    public static final ConfigOption<Class<? extends EmailRepository>> EMAIL_REPOSITORY = new ConfigOption(
            "repository.email",
            "Implementation of EmailRepository",
            disallowEmpty(),
            Class.class,
            NopEmailRepository.class
    );

    public static final ConfigOption<Class<? extends FileSystemRepository>> FILE_SYSTEM_REPOSITORY = new ConfigOption(
            "repository.fileSystem",
            "Implementation of FileSystemRepository",
            disallowEmpty(),
            Class.class,
            LocalFileSystemRepository.class
    );

    public static final ConfigOption<Class<? extends AuditService>> AUDIT_SERVICE = new ConfigOption(
            "service.audit",
            "Implementation of AuditService",
            disallowEmpty(),
            Class.class,
            NopAuditService.class
    );

    public static final ConfigOption<Boolean> WATCHER_ENABLED = new ConfigOption<>(
            "watcher.enabled",
            "Enable property/relationship watches",
            disallowEmpty(),
            Boolean.class,
            false
    );

    public static final ConfigOption<Boolean> STATUS_ENABLED = new ConfigOption<>(
            "status.enabled",
            "Enable status server for workers",
            disallowEmpty(),
            Boolean.class,
            true
    );

    public static final ConfigOption<String> STATUS_PORT_RANGE = new ConfigOption<>(
            "status.portRange",
            "Range of ports where the status server should listen",
            disallowEmpty(),
            String.class,
            "40000-41000"
    );

    public static final ConfigOption<Boolean> TRACE_ENABLED = new ConfigOption<>(
            "trace.enabled",
            "Enable method performance tracing",
            disallowEmpty(),
            Boolean.class,
            false
    );

    public static final ConfigOption<String> METRICS_REGISTRY = new ConfigOption<>(
            "metricsRegistry",
            "Implementation for MetricsRegistry",
            disallowEmpty(),
            String.class,
            DropWizardMetricRegistry.class.getName()
    );

    public static final ConfigOption<String> AUTH_TOKEN_PASSWORD = new ConfigOption<>(
            "auth.token.password",
            "",
            disallowEmpty(),
            String.class,
            "4X5rWTCDKbbFoUy7TrxoaKTKQkBgnUB8d45jvABwHgo"
    );

    public static final ConfigOption<String> AUTH_TOKEN_SALT = new ConfigOption<>(
            "auth.token.salt",
            "",
            disallowEmpty(),
            String.class,
            "jNQheYMYfNY8sLc61LuGEg"
    );

    public static final ConfigOption<Integer> AUTH_TOKEN_EXPIRATION_TOLERANCE_IN_SECS = new ConfigOption<>(
            "auth.token.expiration_tolerance_seconds",
            "",
            positiveInt(),
            Integer.class,
            60
    );

    public static final ConfigOption<Integer> DEFAULT_SEARCH_RESULT_COUNT = new ConfigOption<>(
            "search.defaultSearchCount",
            "",
            positiveInt(),
            Integer.class,
            100
    );

    private CoreOptions() {
        super();
    }

    private static volatile CoreOptions instance;

    public static synchronized CoreOptions instance() {
        if (instance == null) {
            instance = new CoreOptions();
            // Should initialize all static members first, then register.
            instance.registerOptions();
        }
        return instance;
    }
}
