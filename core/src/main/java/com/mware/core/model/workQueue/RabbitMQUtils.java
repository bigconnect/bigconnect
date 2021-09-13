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

import com.beust.jcommander.internal.Lists;
import com.mware.core.config.Configuration;
import com.mware.core.config.options.RabbitMqOptions;
import com.mware.core.exception.BcException;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.rabbitmq.client.*;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkNotNull;

public class RabbitMQUtils {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(RabbitMQUtils.class);

    public static Connection openConnection(Address[] addresses, Configuration configuration) throws IOException {
        if (addresses.length == 0) {
            throw new BcException("Could not configure RabbitMQ. No addresses specified. expecting configuration parameter 'rabbitmq.addr.0.host'.");
        }

        String username = configuration.get(RabbitMqOptions.RABBITMQ_USERNAME);
        String password = configuration.get(RabbitMqOptions.RABBITMQ_PASSWORD);

        if (StringUtils.isEmpty(username) || StringUtils.isEmpty(password)) {
            throw new BcException("Could not configure RabbitMQ. Username or password is null. Expecting configuration parameters 'rabbitmq.username' and 'rabbitmq.password'.");
        }

        try {
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setAutomaticRecoveryEnabled(true);
            connectionFactory.setUsername(username);
            connectionFactory.setPassword(password);
            final Connection connection = connectionFactory.newConnection(addresses);
            ((Recoverable) connection).addRecoveryListener(new RecoveryListener() {
                @Override
                public void handleRecovery(Recoverable recoverable) {
                    Connection recoveredConnection = (Connection) recoverable;
                    LOGGER.warn("recovered RabbitMQ connection to %s:%d", recoveredConnection.getAddress(), recoveredConnection.getPort());
                }

                @Override
                public void handleRecoveryStarted(Recoverable recoverable) {

                }
            });
            return connection;
        } catch (TimeoutException e) {
            throw new IOException(e);
        }
    }

    public static Connection openConnection(Configuration configuration) throws IOException {
        Address[] addresses = getAddresses(configuration);
        return openConnection(addresses, configuration);
    }

    public static Channel openChannel(Connection connection) {
        try {
            return connection.createChannel();
        } catch (IOException ex) {
            throw new BcException("Could not open channel to RabbitMQ", ex);
        }
    }

    public static Address[] getAddresses(Configuration configuration) {
        List<Address> addresses = new ArrayList<>();
        for (String key : configuration.getKeys(RabbitMqOptions.RABBITMQ_ADDR_PREFIX)) {
            if (key.endsWith(".host")) {
                String host = configuration.get(key, null);
                checkNotNull(host, "Configuration " + key + " is required");
                String port = configuration.get(key.replace(".host", ".port"), RabbitMqOptions.DEFAULT_PORT);
                addresses.add(new Address(host, Integer.parseInt(port)));
            }
        }
        return addresses.toArray(new Address[0]);
    }

    private static Address[] createAddresses(String[] addresses) {
        List<Address> addressList = Lists.newArrayList();

        for (String address : addresses) {
            String[] addressParts = address.split(":");

            if (addressParts.length == 1) {
                addressList.add(new Address(address));
            } else if (addressParts.length == 2) {
                addressList.add(new Address(addressParts[0], Integer.parseInt(addressParts[1])));
            } else {
                throw new IllegalArgumentException(String.format("malformed rabbitmq address: %s", address));
            }
        }

        return addressList.toArray(new Address[0]);
    }
}

