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

package io.bigconnect.biggraph.traversal.algorithm.records;

import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.traversal.algorithm.BigTraverser.PathSet;
import io.bigconnect.biggraph.traversal.algorithm.records.record.IntIterator;
import io.bigconnect.biggraph.traversal.algorithm.records.record.Record;
import io.bigconnect.biggraph.traversal.algorithm.records.record.RecordType;
import io.bigconnect.biggraph.type.define.CollectionType;
import io.bigconnect.biggraph.util.collection.CollectionFactory;

import java.util.List;
import java.util.Stack;

import static io.bigconnect.biggraph.backend.query.Query.NO_LIMIT;

public class KoutRecords extends SingleWayMultiPathsRecords {

    public KoutRecords(RecordType type, boolean concurrent,
                       Id source, boolean nearest) {
        super(type, concurrent, source, nearest);
    }

    @Override
    public int size() {
        return this.currentRecord().size();
    }

    public List<Id> ids(long limit) {
        List<Id> ids = CollectionFactory.newList(CollectionType.EC);
        IntIterator iterator = this.records().peek().keys();
        while ((limit == NO_LIMIT || limit-- > 0L) && iterator.hasNext()) {
            ids.add(this.id(iterator.next()));
        }
        return ids;
    }

    public PathSet paths(long limit) {
        PathSet paths = new PathSet();
        Stack<Record> records = this.records();
        IntIterator iterator = records.peek().keys();
        while ((limit == NO_LIMIT || limit-- > 0L) && iterator.hasNext()) {
            paths.add(this.getPath(records.size() - 1, iterator.next()));
        }
        return paths;
    }
}
