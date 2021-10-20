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

package io.bigconnect.biggraph.backend.query;

import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.structure.BigElement;
import io.bigconnect.biggraph.type.BigType;
import io.bigconnect.biggraph.util.E;
import io.bigconnect.biggraph.util.InsertionOrderUtil;
import com.google.common.collect.ImmutableSet;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public class IdQuery extends Query {

    private static final Set<Id> EMPTY_IDS = ImmutableSet.of();

    // The id(s) will be concated with `or`
    private Set<Id> ids = EMPTY_IDS;
    private boolean mustSortByInput = true;

    public IdQuery(BigType resultType) {
        super(resultType);
    }

    public IdQuery(BigType resultType, Query originQuery) {
        super(resultType, originQuery);
    }

    public IdQuery(BigType resultType, Set<Id> ids) {
        this(resultType);
        this.query(ids);
    }

    public IdQuery(BigType resultType, Id id) {
        this(resultType);
        this.query(id);
    }

    public IdQuery(Query originQuery, Id id) {
        this(originQuery.resultType(), originQuery);
        this.query(id);
    }

    public IdQuery(Query originQuery, Set<Id> ids) {
        this(originQuery.resultType(), originQuery);
        this.query(ids);
    }

    public boolean mustSortByInput() {
        return this.mustSortByInput;
    }

    public void mustSortByInput(boolean mustSortedByInput) {
        this.mustSortByInput = mustSortedByInput;
    }

    @Override
    public Set<Id> ids() {
        return Collections.unmodifiableSet(this.ids);
    }

    public void resetIds() {
        this.ids = EMPTY_IDS;
    }

    public IdQuery query(Id id) {
        E.checkArgumentNotNull(id, "Query id can't be null");
        if (this.ids == EMPTY_IDS) {
            this.ids = new LinkedHashSet<>();
        }
        this.ids.add(id);
        this.checkCapacity(this.ids.size());
        return this;
    }

    public IdQuery query(Set<Id> ids) {
        for (Id id : ids) {
            this.query(id);
        }
        return this;
    }

    @Override
    public boolean test(BigElement element) {
        return this.ids.contains(element.id());
    }

    @Override
    public IdQuery copy() {
        IdQuery query = (IdQuery) super.copy();
        query.ids = this.ids == EMPTY_IDS ? EMPTY_IDS :
                    InsertionOrderUtil.newSet(this.ids);
        return query;
    }
}
