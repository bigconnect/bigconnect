/*
 * Copyright 2021 BigConnect Authors
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.bigconnect.biggraph;

import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;

import java.io.InterruptedIOException;

public class BigGraphException extends RuntimeException {

    private static final long serialVersionUID = -8711375282196157058L;

    public BigGraphException(String message) {
        super(message);
    }

    public BigGraphException(String message, Throwable cause) {
        super(message, cause);
    }

    public BigGraphException(String message, Object... args) {
        super(String.format(message, args));
    }

    public BigGraphException(String message, Throwable cause, Object... args) {
        super(String.format(message, args), cause);
    }

    public Throwable rootCause() {
        return rootCause(this);
    }

    public static Throwable rootCause(Throwable e) {
        Throwable cause = e;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    public static boolean isInterrupted(Throwable e) {
        Throwable rootCause = BigGraphException.rootCause(e);
        return rootCause instanceof InterruptedException ||
               rootCause instanceof TraversalInterruptedException ||
               rootCause instanceof InterruptedIOException;
    }
}
