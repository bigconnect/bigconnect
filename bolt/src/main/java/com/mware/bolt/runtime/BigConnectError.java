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

import java.util.UUID;

/**
 * An error object, represents something having gone wrong that is to be signaled to the user. This is, by design, not
 * using the java exception system.
 */
public class BigConnectError {
    private final Status status;
    private final String message;
    private final Throwable cause;
    private final UUID reference;
    private final boolean fatal;

    private BigConnectError(Status status, String message, Throwable cause, boolean fatal) {
        this.status = status;
        this.message = message;
        this.cause = cause;
        this.fatal = fatal;
        this.reference = UUID.randomUUID();
    }

    private BigConnectError(Status status, String message, boolean fatal) {
        this(status, message, null, fatal);
    }

    private BigConnectError(Status status, Throwable cause, boolean fatal) {
        this(status, status.code().description(), cause, fatal);
    }

    public Status status() {
        return status;
    }

    public String message() {
        return message;
    }

    public Throwable cause() {
        return cause;
    }

    public UUID reference() {
        return reference;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BigConnectError that = (BigConnectError) o;

        return (status != null ? status.equals(that.status) : that.status == null) &&
                !(message != null ? !message.equals(that.message) : that.message != null);

    }

    @Override
    public int hashCode() {
        int result = status != null ? status.hashCode() : 0;
        result = 31 * result + (message != null ? message.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "BigConnectError{" +
                "status=" + status +
                ", message='" + message + '\'' +
                ", cause=" + cause +
                ", reference=" + reference +
                '}';
    }

    public static Status codeFromString(String codeStr) {
        String[] parts = codeStr.split("\\.");
        if (parts.length != 4) {
            return Status.General.UnknownError;
        }

        String category = parts[2];
        String error = parts[3];

        // Note: the input string may contain arbitrary input data, using reflection would open network attack vector
        switch (category) {
            case "General":
                return Status.General.valueOf(error);
            case "Statement":
                return Status.Statement.valueOf(error);
            case "Request":
                return Status.Request.valueOf(error);
            case "Network":
                return Status.Network.valueOf(error);
            case "Security":
                return Status.Security.valueOf(error);
            default:
                return Status.General.UnknownError;
        }
    }

    private static BigConnectError fromThrowable(Throwable any, boolean isFatal) {
        for (Throwable cause = any; cause != null; cause = cause.getCause()) {
            if (cause instanceof Status.HasStatus) {
                return new BigConnectError(((Status.HasStatus) cause).status(), any.getMessage(), any, isFatal);
            }
            if (cause instanceof OutOfMemoryError) {
                return new BigConnectError(Status.General.OutOfMemoryError, cause, isFatal);
            }
            if (cause instanceof StackOverflowError) {
                return new BigConnectError(Status.General.StackOverFlowError, cause, isFatal);
            }
        }

        // In this case, an error has "slipped out", and we don't have a good way to handle it. This indicates
        // a buggy code path, and we need to try to convince whoever ends up here to tell us about it.

        return new BigConnectError(Status.General.UnknownError, any != null ? any.getMessage() : null, any, isFatal);
    }

    public static BigConnectError from(Status status, String message) {
        return new BigConnectError(status, message, false);
    }

    public static BigConnectError from(Throwable any) {
        return fromThrowable(any, false);
    }

    public static BigConnectError fatalFrom(Throwable any) {
        return fromThrowable(any, true);
    }

    public static BigConnectError fatalFrom(Status status, String message) {
        return new BigConnectError(status, message, true);
    }

    public boolean isFatal() {
        return fatal;
    }
}
