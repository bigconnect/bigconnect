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
package com.mware.core.model.workQueue;

import com.google.inject.Inject;
import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.config.Configuration;
import com.mware.core.exception.BcException;
import com.mware.core.ingest.WorkerSpout;
import com.mware.core.status.model.QueueStatus;
import com.mware.core.status.model.Status;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.Graph;
import com.rabbitmq.client.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.time.Duration;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RabbitMQWorkQueueRepository extends WorkQueueRepository {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(RabbitMQWorkQueueRepository.class);

    private Connection connection;
    private Channel channel;
    private Integer deliveryMode;
    private Address[] rabbitMqAddresses;
    private Set<String> declaredQueues = new HashSet<>();

    @Inject
    public RabbitMQWorkQueueRepository(
            Graph graph,
            Configuration configuration
    ) {
        super(graph, configuration);
    }

    @Override
    public void start() throws Throwable {
        this.connection = RabbitMQUtils.openConnection(getConfiguration());
        this.channel = RabbitMQUtils.openChannel(this.connection);
        this.deliveryMode = getConfiguration().getInt(RabbitMQUtils.RABBITMQ_DELIVERY_MODE, MessageProperties.PERSISTENT_BASIC.getDeliveryMode());
        this.rabbitMqAddresses = RabbitMQUtils.getAddresses(getConfiguration());
    }

    @Override
    public void pushOnQueue(String queueName, byte[] data, Priority priority) {
        try {
            ensureQueue(queueName);
            AMQP.BasicProperties.Builder propsBuilder = new AMQP.BasicProperties.Builder();
            if (deliveryMode != null) {
                propsBuilder.deliveryMode(deliveryMode);
            }
            LOGGER.debug("enqueuing message to queue [%s]: %s", queueName, new String(data));
            propsBuilder.priority(toRabbitMQPriority(priority));
            channel.basicPublish("", queueName, propsBuilder.build(), data);
        } catch (Exception ex) {
            throw new BcException("Could not push on queue", ex);
        }
    }

    private Integer toRabbitMQPriority(Priority priority) {
        switch (priority) {
            case HIGH:
                return 2;
            case NORMAL:
                return 1;
            case LOW:
                return 0;
            default:
                return 0;
        }
    }

    private void ensureQueue(String queueName) throws IOException {
        if (!declaredQueues.contains(queueName)) {
            createQueue(channel, queueName);
            declaredQueues.add(queueName);
        }
    }

    public static void createQueue(Channel channel, String queueName) throws IOException {
        Map<String, Object> args = new HashMap<>();
        args.put("x-max-priority", 3);
        args.put("x-message-ttl",  12 * 3600 * 1000); // 12h
        channel.queueDeclare(queueName, true, false, false, args);
    }

    @Override
    public void flush() {
    }

    public void shutdown() {
        try {
            LOGGER.debug("Closing RabbitMQ channel");
            this.channel.close();
        } catch (Throwable e) {
            LOGGER.error("Could not close RabbitMQ channel", e);
        }
        try {
            LOGGER.debug("Closing RabbitMQ connection");
            this.connection.close();
        } catch (Throwable e) {
            LOGGER.error("Could not close RabbitMQ connection", e);
        }
    }

    @Override
    public Map<String, Status> getQueuesStatus() {
        try {
            Map<String, Status> results = new HashMap<>();
            URL url = new URL(String.format("http://%s:15672/api/queues", rabbitMqAddresses[0].getHost()));
            URLConnection conn = url.openConnection();
            String basicAuth = Base64.encodeBase64String("guest:guest".getBytes());
            conn.setRequestProperty("Authorization", "Basic " + basicAuth);
            try (InputStream in = conn.getInputStream()) {
                JSONArray queuesJson = new JSONArray(IOUtils.toString(in));
                for (int i = 0; i < queuesJson.length(); i++) {
                    JSONObject queueJson = queuesJson.getJSONObject(i);
                    String name = queueJson.getString("name");
                    int messages = queueJson.getInt("messages");
                    results.put(name, new QueueStatus(messages));
                }
            }
            return results;
        } catch (Exception e) {
            throw new BcException("Could not connect to RabbitMQ", e);
        }
    }

    @Override
    protected void deleteQueue(String queueName) {
        try {
            channel.queueDelete(queueName);
        } catch (IOException e) {
            throw new BcException("Could not delete queue: " + queueName, e);
        }
    }

    @Override
    public WorkerSpout createWorkerSpout(String queueName) {
        return InjectHelper.inject(new RabbitMQWorkQueueSpout(queueName));
    }
}

