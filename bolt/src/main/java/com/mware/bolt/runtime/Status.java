/*
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
package com.mware.bolt.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import static com.mware.bolt.runtime.Status.Classification.ClientError;
import static com.mware.bolt.runtime.Status.Classification.TransientError;
import static java.lang.String.format;

public interface Status {
    enum Transaction implements Status {
        InvalidBookmark(ClientError,
                "Supplied bookmark cannot be interpreted. You should only supply a bookmark previously that was " +
                        "previously generated by BigConnect. Maybe you have generated your own bookmark, " +
                        "or modified a bookmark since it was generated by BigConnect."),
        Terminated(TransientError,
                "Explicitly terminated by the user."),
        Interrupted(TransientError,
                "Interrupted while waiting.");

        private final Code code;

        @Override
        public Code code() {
            return code;
        }

        Transaction(Classification classification, String description) {
            this.code = new Code(classification, this, description);
        }
    }

    enum Security implements Status {
        Unauthorized(ClientError, "The client is unauthorized due to authentication failure.");

        private final Code code;

        @Override
        public Code code() {
            return code;
        }

        Security(Classification classification, String description) {
            this.code = new Code(classification, this, description);
        }
    }

    enum Network implements Status {
        // transient
        CommunicationError(TransientError, "An unknown network failure occurred, a retry may resolve the issue.");
        private final Code code;

        @Override
        public Code code() {
            return code;
        }

        Network(Classification classification, String description) {
            this.code = new Code(classification, this, description);
        }
    }

    enum Statement implements Status {
        SyntaxError(ClientError,
                "The statement contains invalid or unsupported syntax."),
        TypeError(ClientError,
                "The statement is attempting to perform operations on values with types that are not supported by " +
                        "the operation.");

        private final Code code;

        @Override
        public Code code() {
            return code;
        }

        Statement(Classification classification, String description) {
            this.code = new Code(classification, this, description);
        }
    }

    enum General implements Status {
        OutOfMemoryError(TransientError,
                "There is not enough memory to perform the current task. Please try increasing " +
                        "the max heap size in the java configuration by using '-Xmx' command line flag, and then restart BigConnect."),

        StackOverFlowError(TransientError,
                "There is not enough stack size to perform the current task. This is generally considered to be a " +
                        "system error, so please contact BigConnect support. You could try increasing the stack size: " +
                        "for example to set the stack size to 2M, just add -Xss2M as command line flag."),

        UnknownError(Classification.DatabaseError,
                "An unknown error occurred.");

        private final Code code;

        @Override
        public Code code() {
            return code;
        }

        General(Classification classification, String description) {
            this.code = new Code(classification, this, description);
        }
    }

    enum Request implements Status {
        Invalid(ClientError,
                "The client provided an invalid request."),
        InvalidFormat(Classification.ClientError,
                "The client provided a request that was missing required fields, or had values that are not allowed."),
        InvalidUsage(ClientError,
                "The client made a request but did not consume outgoing buffers in a timely fashion."),
        NoThreadsAvailable(TransientError,  // TODO: see above
                "There are no available threads to serve this request at the moment. You can retry at a later time " +
                        "or consider increasing max thread pool size for bolt connector(s).");

        private final Code code;

        @Override
        public Code code() {
            return code;
        }

        Request(Classification classification, String description) {
            this.code = new Code(classification, this, description);
        }
    }

    Code code();

    class Code {
        public static Collection<Status> all() {
            Collection<Status> result = new ArrayList<>();
            for (Class<?> child : Status.class.getDeclaredClasses()) {
                if (child.isEnum() && Status.class.isAssignableFrom(child)) {
                    @SuppressWarnings("unchecked")
                    Class<? extends Status> statusType = (Class<? extends Status>) child;
                    Collections.addAll(result, statusType.getEnumConstants());
                }
            }
            return result;
        }

        private final Classification classification;
        private final String description;
        private final String category;
        private final String title;

        <C extends Enum<C> & Status> Code(Classification classification, C categoryAndTitle, String description) {
            this.classification = classification;
            this.category = categoryAndTitle.getDeclaringClass().getSimpleName();
            this.title = categoryAndTitle.name();

            this.description = description;
        }

        @Override
        public String toString() {
            return "Status.Code[" + serialize() + "]";
        }

        /**
         * The portable, serialized status code. This will always be in the format:
         *
         * <pre>
         * Neo.[Classification].[Category].[Title]
         * </pre>
         */
        public final String serialize() {
            return format("Cypher.%s.%s.%s", classification, category, title);
        }

        public final String description() {
            return description;
        }

        public Classification classification() {
            return classification;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Code code = (Code) o;

            return category.equals(code.category) && classification == code.classification &&
                    title.equals(code.title);
        }

        @Override
        public int hashCode() {
            int result = classification.hashCode();
            result = 31 * result + category.hashCode();
            result = 31 * result + title.hashCode();
            return result;
        }
    }

    enum Classification {
        /**
         * The Client sent a bad request - changing the request might yield a successful outcome.
         */
        ClientError(TransactionEffect.ROLLBACK,
                "The Client sent a bad request - changing the request might yield a successful outcome."),
        /**
         * There are notifications about the request sent by the client.
         */
        ClientNotification(TransactionEffect.NONE,
                "There are notifications about the request sent by the client."),

        /**
         * The database cannot service the request right now, retrying later might yield a successful outcome.
         */
        TransientError(TransactionEffect.ROLLBACK,
                "The database cannot service the request right now, retrying later might yield a successful outcome. "),

        // Implementation note: These are a sharp tool, database error signals
        // that something is *seriously* wrong, and will prompt the user to send
        // an error report back to us. Only use this if the code path you are
        // at would truly indicate the database is in a broken or bug-induced state.
        /**
         * The database failed to service the request.
         */
        DatabaseError(TransactionEffect.ROLLBACK,
                "The database failed to service the request. ");

        private enum TransactionEffect {
            ROLLBACK, NONE,
        }

        private final boolean rollbackTransaction;
        private final String description;

        Classification(TransactionEffect transactionEffect, String description) {
            this.description = description;
            this.rollbackTransaction = transactionEffect == TransactionEffect.ROLLBACK;
        }

        public boolean rollbackTransaction() {
            return rollbackTransaction;
        }

        public String description() {
            return description;
        }
    }

    interface HasStatus {
        Status status();
    }
}
