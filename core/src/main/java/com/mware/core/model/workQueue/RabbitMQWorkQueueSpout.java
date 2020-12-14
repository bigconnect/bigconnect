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
import com.mware.core.config.Configuration;
import com.mware.core.exception.BcException;
import com.mware.core.ingest.WorkerSpout;
import com.mware.core.ingest.WorkerTuple;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitMQWorkQueueSpout extends WorkerSpout {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(RabbitMQWorkQueueSpout.class);
    public static final int DEFAULT_RABBITMQ_PREFETCH_COUNT = 10;
    private final String queueName;
    private Channel channel;
    private QueueingConsumer consumer;
    private Connection connection;
    private Configuration configuration;

    public RabbitMQWorkQueueSpout(String queueName) {
        this.queueName = queueName;
    }

    @Override
    public void open() {
        try {
            this.connection = RabbitMQUtils.openConnection(configuration);
            this.channel = RabbitMQUtils.openChannel(this.connection);
            RabbitMQWorkQueueRepository.createQueue(channel, queueName);
            this.consumer = new QueueingConsumer(channel);
            Integer prefetchCount = configuration.getInt(Configuration.RABBITMQ_PREFETCH_COUNT, DEFAULT_RABBITMQ_PREFETCH_COUNT);
            this.channel.basicQos(prefetchCount, false);
            this.channel.basicConsume(this.queueName, false, consumer);
        } catch (IOException ex) {
            throw new BcException("Could not startup RabbitMQ", ex);
        }
    }

    @Override
    public void close() {
        super.close();
        try {
            LOGGER.debug("Closing RabbitMQ channel");
            this.channel.close();
            LOGGER.debug("Closing RabbitMQ connection");
            this.connection.close();
        } catch (IOException | TimeoutException ex) {
            LOGGER.error("Could not close RabbitMQ connection and channel", ex);
        }
    }

    @Override
    public WorkerTuple nextTuple() throws InterruptedException {
        QueueingConsumer.Delivery delivery = this.consumer.nextDelivery(100);
        if (delivery == null) {
            return null;
        }
        return new WorkerTuple(delivery.getEnvelope().getDeliveryTag(), delivery.getBody());
    }

    @Override
    public void ack(WorkerTuple workerTuple) {
        long deliveryTag = (Long) workerTuple.getMessageId();
        try {
            this.channel.basicAck(deliveryTag, false);
        } catch (IOException ex) {
            LOGGER.error("Could not ack: %d", deliveryTag, ex);
        }
    }

    @Override
    public void fail(WorkerTuple workerTuple) {
        long deliveryTag = (Long) workerTuple.getMessageId();
        try {
            this.channel.basicNack(deliveryTag, false, false);
        } catch (IOException ex) {
            LOGGER.error("Could not ack: %d", deliveryTag, ex);
        }
    }

    protected QueueingConsumer getConsumer() {
        return this.consumer;
    }

    @Inject
    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }
}
