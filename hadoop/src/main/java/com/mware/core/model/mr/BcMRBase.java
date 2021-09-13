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
package com.mware.core.model.mr;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.mware.core.bootstrap.BcBootstrap;
import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.config.BcHadoopConfiguration;
import com.mware.core.config.ConfigurationLoader;
import com.mware.core.config.options.CoreOptions;
import com.mware.core.exception.BcException;
import com.mware.core.trace.TraceRepository;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.core.util.VersionUtil;
import com.mware.ge.accumulo.AccumuloGraphConfiguration;
import com.mware.ge.accumulo.mapreduce.AccumuloElementOutputFormat;
import com.mware.ge.accumulo.mapreduce.ElementMapper;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public abstract class BcMRBase extends Configured implements Tool {
    private static BcLogger LOGGER;
    public static final String CONFIG_SOURCE_FILE_NAME = "sourceFileName";
    public static final int PERIODIC_COUNTER_OUTPUT_PERIOD = 30 * 1000;
    private String instanceName;
    private String zooKeepers;
    private String principal;
    private AuthenticationToken authorizationToken;
    private boolean local;
    private Timer periodicCounterOutputTimer;
    private com.mware.core.config.Configuration bcConfig;
    private AccumuloGraphConfiguration accumuloGraphConfiguration;

    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    @DynamicParameter(names = {"-job"}, description = "Set a job property. (e.g.: -job mapreduce.map.memory.mb=1024)")
    private Map<String, String> jobProperties = new HashMap<>();

    @Parameter(names = {"--help", "-h"}, description = "Print help", help = true)
    private boolean help;

    @Parameter(names = {"--version"}, description = "Print version")
    private boolean version;

    @Override
    public int run(String[] args) throws Exception {
        LOGGER = BcLoggerFactory.getLogger(BcMRBase.class);

        bcConfig = ConfigurationLoader.load();
        JobConf conf = getConfiguration(args, bcConfig);
        if (conf == null) {
            return -1;
        }
        accumuloGraphConfiguration = new AccumuloGraphConfiguration(conf);
        InjectHelper.inject(this, BcBootstrap.bootstrapModuleMaker(bcConfig), bcConfig);
        if(bcConfig.get(CoreOptions.TRACE_ENABLED)) {
            TraceRepository traceRepository = InjectHelper.getInstance(TraceRepository.class);
            traceRepository.enable();
        }

        Job job = Job.getInstance(conf, getJobName());

        instanceName = accumuloGraphConfiguration.getAccumuloInstanceName();
        zooKeepers = accumuloGraphConfiguration.getZookeeperServers();
        principal = accumuloGraphConfiguration.getAccumuloUsername();
        authorizationToken = accumuloGraphConfiguration.getAuthenticationToken();
        AccumuloElementOutputFormat.setOutputInfo(job, instanceName, zooKeepers, principal, authorizationToken);

        boolean periodicCounterOutput = conf.getBoolean("bc.periodic.counter.output.enabled", false);

        if (job.getConfiguration().get("mapreduce.framework.name").equals("local")) {
            LOGGER.warn("!!!!!! Running in local mode !!!!!!");
            local = true;
            periodicCounterOutput = true;
        }

        setupJob(job);

        if (periodicCounterOutput) {
            startPeriodicCounterOutputThread(job);
        }

        LOGGER.info("Starting job");
        long startTime = System.currentTimeMillis();
        int result = job.waitForCompletion(true) ? 0 : 1;
        long endTime = System.currentTimeMillis();
        LOGGER.info("Job complete");

        if (job.getStatus().getState() != JobStatus.State.SUCCEEDED) {
            LOGGER.warn("Unexpected job state: %s", job.getStatus().getState());
        }

        if (periodicCounterOutputTimer != null) {
            periodicCounterOutputTimer.cancel();
        }

        printCounters(job);
        LOGGER.info("Time: %,.2f minutes", ((double) (endTime - startTime) / 1000.0 / 60.0));
        LOGGER.info("Return code: " + result);

        return result;
    }

    public boolean isLocal() {
        return local;
    }

    protected void printCounters(Job job) {
        try {
            if (job.getJobState() != JobStatus.State.RUNNING) {
                return;
            }
        } catch (IllegalStateException e) {
            if (e.getMessage().contains("Job in state DEFINE instead of RUNNING")) {
                return;
            }
            throw new BcException("Could not get job state", e);
        } catch (Exception e) {
            throw new BcException("Could not get job state", e);
        }
        try {
            LOGGER.info("Counters");
            for (String groupName : job.getCounters().getGroupNames()) {
                CounterGroup groupCounters = job.getCounters().getGroup(groupName);
                LOGGER.info(groupCounters.getDisplayName());
                for (Counter counter : groupCounters) {
                    LOGGER.info("  " + counter.getDisplayName() + ": " + counter.getValue());
                }
            }
        } catch (IOException ex) {
            LOGGER.error("Could not print counters", ex);
        }
    }

    protected String getJobName() {
        return this.getClass().getSimpleName();
    }

    protected abstract void setupJob(Job job) throws Exception;

    protected JobConf getConfiguration(String[] args, com.mware.core.config.Configuration bcConfig) {
        Configuration hadoopConfig = BcHadoopConfiguration.getHadoopConfiguration(bcConfig, getConf());
        hadoopConfig.set(ElementMapper.GRAPH_CONFIG_PREFIX, "graph.");
        JobConf result = new JobConf(hadoopConfig, this.getClass());
        JCommander j = new JCommander(this, args);
        j.setProgramName("hadoop jar <jar>");
        if (help) {
            j.usage();
            return null;
        }
        if (version) {
            VersionUtil.printVersion();
            return null;
        }
        processArgs(result, args);
        for (Map.Entry<String, String> jobProperty : jobProperties.entrySet()) {
            result.set(jobProperty.getKey(), jobProperty.getValue());
            LOGGER.info("setting config: %s = %s", jobProperty.getKey(), jobProperty.getValue());
        }
        setConf(result);
        LOGGER.info("Using config:\n" + result);
        return result;
    }

    protected abstract void processArgs(JobConf conf, String[] args);

    public String getInstanceName() {
        return instanceName;
    }

    public String getZooKeepers() {
        return zooKeepers;
    }

    public String getPrincipal() {
        return principal;
    }

    public com.mware.core.config.Configuration getBcConfig() {
        return bcConfig;
    }

    public AccumuloGraphConfiguration getAccumuloGraphConfiguration() {
        return accumuloGraphConfiguration;
    }

    public AuthenticationToken getAuthorizationToken() {
        return authorizationToken;
    }

    private void startPeriodicCounterOutputThread(final Job job) {
        periodicCounterOutputTimer = new Timer("periodicCounterOutput", true);
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                printCounters(job);
            }
        };
        periodicCounterOutputTimer.scheduleAtFixedRate(task, PERIODIC_COUNTER_OUTPUT_PERIOD, PERIODIC_COUNTER_OUTPUT_PERIOD);
    }
}
