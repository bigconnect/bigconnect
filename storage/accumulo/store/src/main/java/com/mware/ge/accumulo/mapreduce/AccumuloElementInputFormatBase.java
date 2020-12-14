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
package com.mware.ge.accumulo.mapreduce;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.mware.ge.*;
import com.mware.ge.accumulo.AccumuloGraph;
import com.mware.ge.accumulo.iterator.model.ElementData;
import com.mware.ge.accumulo.iterator.model.IteratorFetchHints;
import com.mware.ge.accumulo.iterator.util.ByteSequenceUtils;
import com.mware.ge.store.util.LazyMutableProperty;
import com.mware.ge.util.MapUtils;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloRowInputFormat;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public abstract class AccumuloElementInputFormatBase<TValue extends Element> extends InputFormat<Text, TValue> {
    private final AccumuloRowInputFormat accumuloInputFormat;

    public AccumuloElementInputFormatBase() {
        accumuloInputFormat = new AccumuloRowInputFormat();
    }

    public static void setInputInfo(Job job, String instanceName, String zooKeepers, String principal, AuthenticationToken token, String[] authorizations, String tableName) throws AccumuloSecurityException {
        AccumuloRowInputFormat.setInputTableName(job, tableName);
        AccumuloRowInputFormat.setConnectorInfo(job, principal, token);
        ClientConfiguration clientConfig = new ClientConfiguration()
                .withInstance(instanceName)
                .withZkHosts(zooKeepers);
        AccumuloRowInputFormat.setZooKeeperInstance(job, clientConfig);
        AccumuloRowInputFormat.setScanAuthorizations(job, new org.apache.accumulo.core.security.Authorizations(authorizations));
        job.getConfiguration().setStrings(GeMRUtils.CONFIG_AUTHORIZATIONS, authorizations);
    }

    public static void setFetchHints(Job job, ElementType elementType, FetchHints fetchHints) {
        Iterable<Text> columnFamiliesToFetch = AccumuloGraph.getColumnFamiliesToFetch(elementType, fetchHints);
        Collection<Pair<Text, Text>> columnFamilyColumnQualifierPairs = new ArrayList<>();
        for (Text columnFamilyToFetch : columnFamiliesToFetch) {
            columnFamilyColumnQualifierPairs.add(new Pair<>(columnFamilyToFetch, null));
        }
        AccumuloInputFormat.fetchColumns(job, columnFamilyColumnQualifierPairs);
    }

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
        return accumuloInputFormat.getSplits(jobContext);
    }

    @Override
    public RecordReader<Text, TValue> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        final RecordReader<Text, PeekingIterator<Map.Entry<Key, Value>>> reader = accumuloInputFormat.createRecordReader(inputSplit, taskAttemptContext);
        return new RecordReader<Text, TValue>() {
            public AccumuloGraph graph;
            public Authorizations authorizations;

            @Override
            public void initialize(InputSplit inputSplit, TaskAttemptContext ctx) throws IOException, InterruptedException {
                reader.initialize(inputSplit, ctx);

                Map configurationMap = GeMRUtils.toMap(ctx.getConfiguration());
                this.graph = (AccumuloGraph) new GraphFactory().createGraph(MapUtils.getAllWithPrefix(configurationMap, "graph"));
                this.authorizations = new Authorizations(ctx.getConfiguration().getStrings(GeMRUtils.CONFIG_AUTHORIZATIONS));
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                return reader.nextKeyValue();
            }

            @Override
            public Text getCurrentKey() throws IOException, InterruptedException {
                return reader.getCurrentKey();
            }

            @Override
            public TValue getCurrentValue() throws IOException, InterruptedException {
                PeekingIterator<Map.Entry<Key, Value>> row = reader.getCurrentValue();
                return createElementFromRow(graph, row, authorizations);
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                return reader.getProgress();
            }

            @Override
            public void close() throws IOException {
                reader.close();
            }
        };
    }

    protected abstract TValue createElementFromRow(AccumuloGraph graph, PeekingIterator<Map.Entry<Key, Value>> row, Authorizations authorizations);

    protected static Iterable<Property> makePropertiesFromElementData(final AccumuloGraph graph, ElementData elementData, IteratorFetchHints fetchHints) {
        return Iterables.transform(elementData.getProperties(fetchHints), new Function<com.mware.ge.accumulo.iterator.model.Property, Property>() {
            @Nullable
            @Override
            public Property apply(@Nullable com.mware.ge.accumulo.iterator.model.Property property) {
                return makePropertyFromIteratorProperty(graph, property);
            }
        });
    }

    private static Property makePropertyFromIteratorProperty(AccumuloGraph graph, com.mware.ge.accumulo.iterator.model.Property property) {
        Set<Visibility> hiddenVisibilities = null;
        if (property.hiddenVisibilities != null) {
            hiddenVisibilities = Sets.newHashSet(Iterables.transform(property.hiddenVisibilities, new Function<ByteSequence, Visibility>() {
                @Nullable
                @Override
                public Visibility apply(ByteSequence visibilityText) {
                    return AccumuloGraph.accumuloVisibilityToVisibility(AccumuloGraph.visibilityToAccumuloVisibility(ByteSequenceUtils.toString(visibilityText)));
                }
            }));
        }
        Visibility visibility = AccumuloGraph.accumuloVisibilityToVisibility(
                AccumuloGraph.visibilityToAccumuloVisibility(property.visibility)
        );
        return new LazyMutableProperty(
                graph,
                graph.getGeSerializer(),
                graph.getNameSubstitutionStrategy().inflate(new String(property.key.toArray())),
                graph.getNameSubstitutionStrategy().inflate(new String(property.name.toArray())),
                property.value,
                null,
                hiddenVisibilities,
                visibility,
                property.timestamp,
                FetchHints.ALL_INCLUDING_HIDDEN
        );
    }
}
