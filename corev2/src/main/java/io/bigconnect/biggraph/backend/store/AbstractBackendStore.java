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

package io.bigconnect.biggraph.backend.store;

import io.bigconnect.biggraph.exception.ConnectionException;
import io.bigconnect.biggraph.type.BigType;

public abstract class AbstractBackendStore<Session extends io.bigconnect.biggraph.backend.store.BackendSession>
                implements BackendStore {

    private final io.bigconnect.biggraph.backend.store.MetaDispatcher<Session> dispatcher;

    public AbstractBackendStore() {
        this.dispatcher = new io.bigconnect.biggraph.backend.store.MetaDispatcher<>();
    }

    protected io.bigconnect.biggraph.backend.store.MetaDispatcher<Session> metaDispatcher() {
        return this.dispatcher;
    }

    public void registerMetaHandler(String name, MetaHandler<Session> handler) {
        this.dispatcher.registerMetaHandler(name, handler);
    }

    // Get metadata by key
    @Override
    public <R> R metadata(BigType type, String meta, Object[] args) {
        Session session = this.session(type);
        io.bigconnect.biggraph.backend.store.MetaDispatcher<Session> dispatcher = null;
        if (type == null) {
            dispatcher = this.metaDispatcher();
        } else {
            io.bigconnect.biggraph.backend.store.BackendTable<Session, ?> table = this.table(type);
            dispatcher = table.metaDispatcher();
        }
        return dispatcher.dispatchMetaHandler(session, meta, args);
    }

    protected void checkOpened() throws ConnectionException {
        if (!this.opened()) {
            throw new ConnectionException(
                      "The '%s' store of %s has not been opened",
                      this.database(), this.provider().type());
        }
    }

    @Override
    public String toString() {
        return String.format("%s/%s", this.database(), this.store());
    }

    protected abstract io.bigconnect.biggraph.backend.store.BackendTable<Session, ?> table(BigType type);

    // NOTE: Need to support passing null
    protected abstract Session session(BigType type);
}
