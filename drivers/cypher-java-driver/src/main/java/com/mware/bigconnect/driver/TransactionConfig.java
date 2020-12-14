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
package com.mware.bigconnect.driver;

import com.mware.bigconnect.driver.async.AsyncSession;
import com.mware.bigconnect.driver.async.AsyncTransactionWork;
import com.mware.bigconnect.driver.internal.util.Extract;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static com.mware.bigconnect.driver.internal.util.Preconditions.checkArgument;

/**
 * Configuration object containing settings for explicit and auto-commit transactions.
 * Instances are immutable and can be reused for multiple transactions.
 * <p>
 * Configuration is supported for:
 * <ul>
 * <li>queries executed in auto-commit transactions - using various overloads of {@link Session#run(String, TransactionConfig)} and
 * {@link AsyncSession#runAsync(String, TransactionConfig)}</li>
 * <li>transactions started by transaction functions - using {@link Session#readTransaction(TransactionWork, TransactionConfig)},
 * {@link Session#writeTransaction(TransactionWork, TransactionConfig)}, {@link AsyncSession#readTransactionAsync(AsyncTransactionWork, TransactionConfig)} and
 * {@link AsyncSession#writeTransactionAsync(AsyncTransactionWork, TransactionConfig)}</li>
 * <li>explicit transactions - using {@link Session#beginTransaction(TransactionConfig)} and {@link AsyncSession#beginTransactionAsync(TransactionConfig)}</li>
 * </ul>
 * <p>
 * Creation of configuration objects can be done using the builder API:
 * <pre>
 * {@code
 * Map<String, Object> metadata = new HashMap<>();
 * metadata.put("type", "update user");
 * metadata.put("application", "my application");
 *
 * TransactionConfig config = TransactionConfig.builder()
 *                 .withTimeout(Duration.ofSeconds(4))
 *                 .withMetadata(metadata)
 *                 .build();
 * }
 * </pre>
 *
 * @see Session
 */
public class TransactionConfig
{
    private static final TransactionConfig EMPTY = builder().build();

    private final Duration timeout;
    private final Map<String,Value> metadata;

    private TransactionConfig( Builder builder )
    {
        this.timeout = builder.timeout;
        this.metadata = unmodifiableMap( builder.metadata );
    }

    /**
     * Get a configuration object that does not have any values configures.
     *
     * @return an empty configuration object.
     */
    public static TransactionConfig empty()
    {
        return EMPTY;
    }

    /**
     * Create new {@link Builder} used to construct a configuration object.
     *
     * @return new builder.
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Get the configured transaction timeout.
     *
     * @return timeout or {@code null} when it is not configured.
     */
    public Duration timeout()
    {
        return timeout;
    }

    /**
     * Get the configured transaction metadata.
     *
     * @return metadata or empty map when it is not configured.
     */
    public Map<String,Value> metadata()
    {
        return metadata;
    }

    /**
     * Check if this configuration object contains any values.
     *
     * @return {@code true} when no values are configured, {@code false otherwise}.
     */
    public boolean isEmpty()
    {
        return timeout == null && (metadata == null || metadata.isEmpty());
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        TransactionConfig that = (TransactionConfig) o;
        return Objects.equals( timeout, that.timeout ) &&
               Objects.equals( metadata, that.metadata );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( timeout, metadata );
    }

    @Override
    public String toString()
    {
        return "TransactionConfig{" +
               "timeout=" + timeout +
               ", metadata=" + metadata +
               '}';
    }

    /**
     * Builder used to construct {@link TransactionConfig transaction configuration} objects.
     */
    public static class Builder
    {
        private Duration timeout;
        private Map<String,Value> metadata = emptyMap();

        private Builder()
        {
        }

        /**
         * Set the transaction timeout. Transactions that execute longer than the configured timeout will be terminated by the database.
         * <p>
         * This functionality allows to limit query/transaction execution time. Specified timeout overrides the default timeout configured in the database
         * using {@code dbms.transaction.timeout} setting.
         * <p>
         * Provided value should not be {@code null} and should not represent a duration of zero or negative duration.
         *
         * @param timeout the timeout.
         * @return this builder.
         */
        public Builder withTimeout( Duration timeout )
        {
            requireNonNull( timeout, "Transaction timeout should not be null" );
            checkArgument( !timeout.isZero(), "Transaction timeout should not be zero" );
            checkArgument( !timeout.isNegative(), "Transaction timeout should not be negative" );

            this.timeout = timeout;
            return this;
        }

        /**
         * Set the transaction metadata. Specified metadata will be attached to the executing transaction and visible in the output of
         * {@code dbms.listQueries} and {@code dbms.listTransactions} procedures. It will also get logged to the {@code query.log}.
         * <p>
         * This functionality makes it easier to tag transactions and is equivalent to {@code dbms.setTXMetaData} procedure.
         * <p>
         * Provided value should not be {@code null}.
         *
         * @param metadata the metadata.
         * @return this builder.
         */
        public Builder withMetadata( Map<String, Object> metadata )
        {
            requireNonNull( metadata, "Transaction metadata should not be null" );

            this.metadata = Extract.mapOfValues( metadata );
            return this;
        }

        /**
         * Build the transaction configuration object using the specified settings.
         *
         * @return new transaction configuration object.
         */
        public TransactionConfig build()
        {
            return new TransactionConfig( this );
        }
    }
}
