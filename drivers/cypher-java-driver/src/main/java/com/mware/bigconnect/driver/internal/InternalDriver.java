/*
 * Copyright (c) 2013-2020 "BigConnect,"
 * MWARE SOLUTIONS SRL
 *
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.mware.bigconnect.driver.internal;

import com.mware.bigconnect.driver.*;
import com.mware.bigconnect.driver.async.AsyncSession;
import com.mware.bigconnect.driver.internal.async.InternalAsyncSession;
import com.mware.bigconnect.driver.internal.async.NetworkSession;
import com.mware.bigconnect.driver.internal.metrics.MetricsProvider;
import com.mware.bigconnect.driver.internal.reactive.InternalRxSession;
import com.mware.bigconnect.driver.internal.security.SecurityPlan;
import com.mware.bigconnect.driver.internal.types.InternalTypeSystem;
import com.mware.bigconnect.driver.internal.util.Futures;
import com.mware.bigconnect.driver.reactive.RxSession;
import com.mware.bigconnect.driver.types.TypeSystem;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mware.bigconnect.driver.internal.util.Futures.completedWithNull;

public class InternalDriver implements Driver
{
    private final SecurityPlan securityPlan;
    private final SessionFactory sessionFactory;
    private final Logger log;

    private AtomicBoolean closed = new AtomicBoolean( false );
    private final MetricsProvider metricsProvider;

    InternalDriver( SecurityPlan securityPlan, SessionFactory sessionFactory, MetricsProvider metricsProvider, Logging logging )
    {
        this.securityPlan = securityPlan;
        this.sessionFactory = sessionFactory;
        this.metricsProvider = metricsProvider;
        this.log = logging.getLog( Driver.class.getSimpleName() );
    }

    @Override
    public Session session()
    {
        return new InternalSession( newSession( SessionConfig.defaultConfig() )  );
    }

    @Override
    public Session session( SessionConfig sessionConfig )
    {
        return new InternalSession( newSession( sessionConfig ) );
    }

    @Override
    public RxSession rxSession()
    {
        return new InternalRxSession( newSession( SessionConfig.defaultConfig() ) );
    }

    @Override
    public RxSession rxSession( SessionConfig sessionConfig )
    {
        return new InternalRxSession( newSession( sessionConfig ) );
    }

    @Override
    public AsyncSession asyncSession()
    {
        return new InternalAsyncSession( newSession( SessionConfig.defaultConfig() ) );
    }

    @Override
    public AsyncSession asyncSession( SessionConfig sessionConfig )
    {
        return new InternalAsyncSession( newSession( sessionConfig ) );
    }

    @Override
    public Metrics metrics()
    {
        return metricsProvider.metrics();
    }

    @Override
    public boolean isMetricsEnabled()
    {
        return metricsProvider.isMetricsEnabled();
    }

    @Override
    public boolean isEncrypted()
    {
        assertOpen();
        return securityPlan.requiresEncryption();
    }

    @Override
    public void close()
    {
        Futures.blockingGet( closeAsync() );
    }

    @Override
    public CompletionStage<Void> closeAsync()
    {
        if ( closed.compareAndSet( false, true ) )
        {
            log.info( "Closing driver instance %s", hashCode() );
            return sessionFactory.close();
        }
        return completedWithNull();
    }

    @Override
    public final TypeSystem defaultTypeSystem()
    {
        return InternalTypeSystem.TYPE_SYSTEM;
    }

    @Override
    public CompletionStage<Void> verifyConnectivityAsync()
    {
        return sessionFactory.verifyConnectivity();
    }

    @Override
    public void verifyConnectivity()
    {
        Futures.blockingGet( verifyConnectivityAsync() );
    }

    /**
     * Get the underlying session factory.
     * <p>
     * <b>This method is only for testing</b>
     *
     * @return the session factory used by this driver.
     */
    public SessionFactory getSessionFactory()
    {
        return sessionFactory;
    }

    private static RuntimeException driverCloseException()
    {
        return new IllegalStateException( "This driver instance has already been closed" );
    }

    public NetworkSession newSession( SessionConfig parameters )
    {
        assertOpen();
        NetworkSession session = sessionFactory.newInstance( parameters );
        if ( closed.get() )
        {
            // session does not immediately acquire connection, it is fine to just throw
            throw driverCloseException();
        }
        return session;
    }

    private void assertOpen()
    {
        if ( closed.get() )
        {
            throw driverCloseException();
        }
    }
}
