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
package com.mware.core.model.yarn;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Lists;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public abstract class ClientBase {
    @SuppressWarnings("OctalInteger")
    public static final short FILE_PERMISSIONS = (short) 0710;

    private static BcLogger LOGGER;

    @Parameter(names = {"-memory", "-mem"}, description = "Memory for each process in MB.")
    private int memory = 512;

    @Parameter(names = {"-cores"}, description = "Number of virtual cores each process uses.")
    private int virtualCores = 1;

    @Parameter(names = {"-instances", "-i"}, description = "Number of instances to start.")
    private int instances = 1;

    @Parameter(names = {"-jar"}, description = "Path to jar.", required = true)
    private String jar = null;

    @Parameter(names = {"-queue"}, description = "Yarn queue to run the job on")
    private String queue = "default";

    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    @DynamicParameter(names = {"-env"}, description = "Environment variable override. (e.g.: -envPATH=/foo:/bar -envLD_LIBRARY_PATH=/baz)")
    private Map<String, String> environmentVariableOverrides = new HashMap<>();

    public static void printEnv() {
        initLogger();
        LOGGER.info("Environment:");
        LinkedList<Map.Entry<String, String>> environmentVariables = Lists.newLinkedList(System.getenv().entrySet());
        environmentVariables.sort((o1, o2) -> o1.getKey().compareTo(o2.getKey()));
        for (Map.Entry<String, String> e : environmentVariables) {
            LOGGER.info("  %s=%s", e.getKey(), e.getValue());
        }
    }

    public static void printSystemProperties() {
        initLogger();
        LOGGER.info("System Properties:");
        List<String> environmentVariables = Lists.newLinkedList(System.getProperties().stringPropertyNames());

        Collections.sort(environmentVariables);

        for (String e : environmentVariables) {
            LOGGER.info("  %s=%s", e, System.getProperty(e));
        }
    }

    protected int run(String[] args) throws Exception {
        initLogger();
        new JCommander(this, args);

        printEnv();
        printSystemProperties();

        final String myClasspath = System.getProperty("java.class.path");
        final String localResourceJarFileName = getAppName() + ".jar";

        final File jarPath = new File(jar);
        if (!jarPath.isFile()) {
            throw new Exception("YARN app must be packaged as a jar file (found path: " + jarPath + ").");
        }
        LOGGER.info("Using jar path: " + jarPath);

        final String classPathEnv = myClasspath + ":./" + localResourceJarFileName;
        LOGGER.info("Classpath: " + classPathEnv);

        final YarnConfiguration conf = new YarnConfiguration();
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        final FileSystem fs = FileSystem.get(conf);
        final Path remotePath = new Path(fs.getHomeDirectory(), getAppName());

        final YarnClient yarnClient = createYarnClient(conf);
        final YarnClientApplication app = yarnClient.createApplication();
        final ContainerLaunchContext amContainer = createContainerLaunchContextRecord(classPathEnv, remotePath);
        final Resource capability = createResourceRecord();
        final ApplicationSubmissionContext appContext = createApplicationSubmissionContext(app, amContainer, capability);
        final ApplicationId appId = appContext.getApplicationId();
        amContainer.setLocalResources(createLocalResources(fs, remotePath, localResourceJarFileName, jarPath));
        amContainer.setEnvironment(createEnvironment(classPathEnv));

        LOGGER.info("Submitting application " + appId);
        yarnClient.submitApplication(appContext);

        waitForApplication(yarnClient, appId, 30, TimeUnit.SECONDS);

        return 0;
    }

    protected abstract String getAppName();

    private void waitForApplication(YarnClient yarnClient, ApplicationId appId, int time, TimeUnit timeUnit) throws YarnException, IOException, InterruptedException {
        Date startTime = new Date();
        Date endTime = new Date(startTime.getTime() + timeUnit.toMillis(time));
        ApplicationReport appReport = yarnClient.getApplicationReport(appId);
        YarnApplicationState appState = appReport.getYarnApplicationState();
        while (appState != YarnApplicationState.FINISHED &&
                appState != YarnApplicationState.KILLED &&
                appState != YarnApplicationState.FAILED &&
                appState != YarnApplicationState.RUNNING) {
            if (System.currentTimeMillis() > endTime.getTime()) {
                break;
            }
            Thread.sleep(100);
            appReport = yarnClient.getApplicationReport(appId);
            appState = appReport.getYarnApplicationState();
        }

        LOGGER.info("Application " + appId + " state " + appState);
    }

    private Map<String, String> createEnvironment(String classPathEnv) {
        Map<String, String> appMasterEnv = new HashMap<>();
        appMasterEnv.putAll(System.getenv());
        appMasterEnv.put(ApplicationConstants.Environment.CLASSPATH.name(), classPathEnv);
        appMasterEnv.putAll(environmentVariableOverrides);
        return appMasterEnv;
    }

    private Map<String, LocalResource> createLocalResources(FileSystem fs, Path remotePath, String localResourceJarFileName, File jarPath) throws IOException {
        Map<String, LocalResource> localResources = new HashMap<>();
        addToLocalResources(fs, remotePath, jarPath.getPath(), localResourceJarFileName, localResources, null);
        return localResources;
    }

    private YarnClient createYarnClient(YarnConfiguration conf) {
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
        return yarnClient;
    }

    private Resource createResourceRecord() {
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(memory);
        capability.setVirtualCores(virtualCores);
        return capability;
    }

    private ContainerLaunchContext createContainerLaunchContextRecord(String classPathEnv, Path remotePath) {
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
        String command = createCommand(classPathEnv, remotePath);
        LOGGER.info("Running: " + command);
        amContainer.setCommands(Collections.singletonList(command));
        return amContainer;
    }

    protected String createCommand(String classPathEnv, Path remotePath) {
        return "${JAVA_HOME}/bin/java"
                + " -Xmx" + memory + "M"
                + " -Djava.net.preferIPv4Stack=true"
                + " -cp " + classPathEnv
                + " " + getApplicationMasterClass().getName()
                + " -memory " + memory
                + " -cores " + virtualCores
                + " -instances " + instances
                + " -appname " + getAppName()
                + " -remotepath " + remotePath
                + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
                + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";
    }

    protected abstract Class getApplicationMasterClass();

    private ApplicationSubmissionContext createApplicationSubmissionContext(YarnClientApplication app, ContainerLaunchContext amContainer, Resource capability) {
        final ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        appContext.setApplicationName(getAppName());
        appContext.setAMContainerSpec(amContainer);
        appContext.setResource(capability);
        appContext.setQueue(queue);
        return appContext;
    }

    private void addToLocalResources(FileSystem fs, Path remotePath, String fileSrcPath, String fileDstPath, Map<String, LocalResource> localResources, String resources) throws IOException {
        Path dst = new Path(remotePath, fileDstPath);
        if (fileSrcPath == null) {
            FSDataOutputStream out = null;
            try {
                out = FileSystem.create(fs, dst, new FsPermission(FILE_PERMISSIONS));
                out.writeUTF(resources);
            } finally {
                IOUtils.closeQuietly(out);
            }
        } else {
            fs.copyFromLocalFile(new Path(fileSrcPath), dst);
        }
        FileStatus scFileStatus = fs.getFileStatus(dst);
        LocalResource localResource = LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(dst.toUri()), LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, scFileStatus.getLen(), scFileStatus.getModificationTime());
        localResources.put(fileDstPath, localResource);
    }

    private static void initLogger() {
        if (LOGGER == null) {
            LOGGER = BcLoggerFactory.getLogger(ClientBase.class);
        }
    }

    public int getMemory() {
        return memory;
    }

    public int getVirtualCores() {
        return virtualCores;
    }

    public int getInstances() {
        return instances;
    }
}
