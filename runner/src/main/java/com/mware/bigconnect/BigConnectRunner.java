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
package com.mware.bigconnect;

import com.mware.bolt.BoltServer;
import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.cmdline.CommandLineTool;
import com.mware.core.ingest.video.VideoFrameInfo;
import com.mware.core.model.graph.GraphRepository;
import com.mware.core.model.longRunningProcess.LongRunningProcessRepository;
import com.mware.core.model.notification.SystemNotificationService;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.model.termMention.TermMentionRepository;
import com.mware.core.model.user.GraphAuthorizationRepository;
import com.mware.core.model.user.UserRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.model.workspace.WorkspaceRepository;
import com.mware.core.process.DataWorkerRunnerProcess;
import com.mware.core.process.ExternalResourceRunnerProcess;
import com.mware.core.process.LongRunningProcessRunnerProcess;
import com.mware.core.process.SystemNotificationProcess;
import com.mware.core.security.BcVisibility;
import com.mware.core.util.VersionUtil;
import com.mware.ge.metric.DropWizardMetricRegistry;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class BigConnectRunner extends CommandLineTool {
    private ExecutorService executorService;

    @Override
    protected int run() throws Exception {
        executorService = Executors.newSingleThreadExecutor((runnable) -> {
            Thread t = new Thread(runnable);
            t.setName("bc-runner-" + t.getId());
            t.setDaemon(true);
            return t;
        });

        verifyGraphVersion();
        setupGraphAuthorizations();

        InjectHelper.getInstance(WorkQueueRepository.class);
        InjectHelper.getInstance(DataWorkerRunnerProcess.class);
        InjectHelper.getInstance(ExternalResourceRunnerProcess.class);
        InjectHelper.getInstance(LongRunningProcessRunnerProcess.class);
        InjectHelper.getInstance(SystemNotificationProcess.class);
        InjectHelper.getInstance(BoltServer.class);
        InjectHelper.getInstance(SystemNotificationService.class);
        InjectHelper.getInstance(WebServer.class);

        VersionUtil.printVersion();

        // start metric registry
        new DropWizardMetricRegistry();

        executorService.awaitTermination(10000, TimeUnit.DAYS);
        return 0;
    }

    private void verifyGraphVersion() {
        GraphRepository graphRepository = InjectHelper.getInstance(GraphRepository.class);
        graphRepository.verifyVersion();
    }

    private void setupGraphAuthorizations() {
        LOGGER.debug("setupGraphAuthorizations");
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

    @Override
    protected void shutdown() {
        executorService.shutdownNow();
        super.shutdown();
    }

    public static void main(String[] args) throws Exception {
        CommandLineTool.main(new BigConnectRunner(), args);
    }
}

