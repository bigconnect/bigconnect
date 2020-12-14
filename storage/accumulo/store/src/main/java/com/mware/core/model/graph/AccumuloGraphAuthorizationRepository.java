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
package com.mware.core.model.graph;

import com.google.common.base.Joiner;
import com.google.inject.Inject;
import com.mware.core.exception.BcException;
import com.mware.core.model.lock.LockRepository;
import com.mware.core.model.user.GraphAuthorizationRepository;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import org.apache.accumulo.core.security.Authorizations;
import com.mware.ge.Graph;
import com.mware.ge.accumulo.AccumuloGraph;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AccumuloGraphAuthorizationRepository implements GraphAuthorizationRepository {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(AccumuloGraphAuthorizationRepository.class);

    public static final String LOCK_NAME = AccumuloGraphAuthorizationRepository.class.getName();
    private Graph graph;
    private LockRepository lockRepository;

    public void addAuthorizationToGraph(final String... auths) {
        LOGGER.debug("adding authorizations [%s]", Joiner.on(", ").join(auths));
        lockRepository.lock(LOCK_NAME, () -> {
            LOGGER.debug("got lock to add authorizations [%s]", Joiner.on(", ").join(auths));
            if (graph instanceof AccumuloGraph) {
                Charset charset = StandardCharsets.UTF_8;
                for (String auth : auths) {
                    LOGGER.debug("adding authorization [%s]", auth);
                    try {
                        AccumuloGraph accumuloGraph = (AccumuloGraph) graph;
                        String principal = accumuloGraph.getConnector().whoami();
                        Authorizations currentAuthorizations = accumuloGraph.getConnector().securityOperations().getUserAuthorizations(principal);
                        if (currentAuthorizations.contains(auth)) {
                            continue;
                        }
                        List<byte[]> newAuthorizationsArray = new ArrayList<>();
                        for (byte[] currentAuth : currentAuthorizations) {
                            newAuthorizationsArray.add(currentAuth);
                        }

                        newAuthorizationsArray.add(auth.getBytes(charset));
                        Authorizations newAuthorizations = new Authorizations(newAuthorizationsArray);
                        accumuloGraph.getConnector().securityOperations().changeUserAuthorizations(principal, newAuthorizations);
                    } catch (Exception ex) {
                        throw new BcException("Could not update authorizations in accumulo", ex);
                    }
                }
            } else {
                throw new BcException("graph type not supported to add authorizations.");
            }
        });
    }

    public void removeAuthorizationFromGraph(final String auth) {
        LOGGER.info("removing authorization %s", auth);
        lockRepository.lock(LOCK_NAME, () -> {
            LOGGER.debug("got lock removing authorization to graph user %s", auth);
            if (graph instanceof AccumuloGraph) {
                try {
                    AccumuloGraph accumuloGraph = (AccumuloGraph) graph;
                    String principal = accumuloGraph.getConnector().whoami();
                    Authorizations currentAuthorizations = accumuloGraph.getConnector().securityOperations().getUserAuthorizations(principal);
                    if (!currentAuthorizations.toString().contains(auth)) {
                        return;
                    }
                    byte[] authBytes = auth.getBytes("UTF8");
                    List<byte[]> newAuthorizationsArray = new ArrayList<>();
                    for (byte[] currentAuth : currentAuthorizations) {
                        if (Arrays.equals(currentAuth, authBytes)) {
                            continue;
                        }
                        newAuthorizationsArray.add(currentAuth);
                    }
                    Authorizations newAuthorizations = new Authorizations(newAuthorizationsArray);
                    accumuloGraph.getConnector().securityOperations().changeUserAuthorizations(principal, newAuthorizations);
                } catch (Exception ex) {
                    throw new RuntimeException("Could not update authorizations in accumulo", ex);
                }
            } else {
                throw new RuntimeException("graph type not supported to add authorizations.");
            }
        });
    }

    @Override
    public List<String> getGraphAuthorizations() {
        if (graph instanceof AccumuloGraph) {
            try {
                AccumuloGraph accumuloGraph = (AccumuloGraph) graph;
                String principal = accumuloGraph.getConnector().whoami();
                Authorizations currentAuthorizations = accumuloGraph.getConnector().securityOperations().getUserAuthorizations(principal);
                ArrayList<String> auths = new ArrayList<>();
                for (byte[] currentAuth : currentAuthorizations) {
                    auths.add(new String(currentAuth));
                }
                return auths;
            } catch (Exception ex) {
                throw new RuntimeException("Could not get authorizations from accumulo", ex);
            }
        } else {
            throw new RuntimeException("graph type not supported to add authorizations.");
        }
    }

    @Inject
    public void setGraph(Graph graph) {
        this.graph = graph;
    }

    @Inject
    public void setLockRepository(LockRepository lockRepository) {
        this.lockRepository = lockRepository;
    }
}
