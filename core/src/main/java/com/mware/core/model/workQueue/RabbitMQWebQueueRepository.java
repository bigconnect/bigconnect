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
import com.mware.core.lifecycle.LifeSupportService;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.Graph;
import com.rabbitmq.client.*;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeoutException;

public class RabbitMQWebQueueRepository extends WebQueueRepository {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(RabbitMQWebQueueRepository.class);
    public static final String CONFIG_BROADCAST_EXCHANGE_NAME = "rabbitmq.broadcastExchangeName";
    public static final String CONFIG_BROADCAST_EXCHANGE_NAME_DEFAULT = "exBroadcast";

    private final Connection connection;
    private final Channel channel;
    private final Integer deliveryMode;
    private final Address[] rabbitMqAddresses;
    private Set<String> declaredQueues = new HashSet<>();

    @Inject
    public RabbitMQWebQueueRepository(
            Graph graph,
            Configuration configuration,
            LifeSupportService lifeSupportService
    )
            throws IOException {
        this.connection = RabbitMQUtils.openConnection(configuration);
        this.channel = RabbitMQUtils.openChannel(this.connection);
        this.channel.exchangeDeclare(getExchangeName(), "fanout");
        this.deliveryMode = configuration.getInt(RabbitMQUtils.RABBITMQ_DELIVERY_MODE, MessageProperties.PERSISTENT_BASIC.getDeliveryMode());
        this.rabbitMqAddresses = RabbitMQUtils.getAddresses(configuration);
        lifeSupportService.add(this);
    }

    @Override
    public void broadcastJson(JSONObject json) {
        try {
            LOGGER.debug("publishing message to broadcast exchange [%s]: %s", getExchangeName(), json.toString());
            channel.basicPublish(getExchangeName(), "", null, json.toString().getBytes());
        } catch (IOException ex) {
            throw new BcException("Could not broadcast json", ex);
        }
    }

    @Override
    public void subscribeToBroadcastMessages(final BroadcastConsumer broadcastConsumer) {
        try {
            String queueName = this.channel.queueDeclare().getQueue();
            this.channel.queueBind(queueName, getExchangeName(), "");

            final QueueingConsumer callback = new QueueingConsumer(this.channel);
            this.channel.basicConsume(queueName, true, callback);

            final Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        //noinspection InfiniteLoopStatement
                        while (true) {
                            QueueingConsumer.Delivery delivery = callback.nextDelivery();
                            try {
                                JSONObject json = new JSONObject(new String(delivery.getBody()));
                                LOGGER.debug("received message from broadcast exchange [%s]: %s", getExchangeName(), json.toString());
                                broadcastConsumer.broadcastReceived(json);
                            } catch (Throwable ex) {
                                LOGGER.error("problem in broadcast thread", ex);
                            }
                        }
                    } catch (InterruptedException e) {
                        throw new BcException("broadcast listener has died", e);
                    }
                }
            });
            t.setName("rabbitmq-subscribe-" + broadcastConsumer.getClass().getName());
            t.setDaemon(true);
            t.start();
        } catch (IOException e) {
            throw new BcException("Could not subscribe to broadcasts", e);
        }
    }

    @Override
    public void unsubscribeFromBroadcastMessages(BroadcastConsumer broadcastConsumer) {
        try {
            this.channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void shutdown() {

    }

    private String getExchangeName() {
        return CONFIG_BROADCAST_EXCHANGE_NAME_DEFAULT;
    }
}
