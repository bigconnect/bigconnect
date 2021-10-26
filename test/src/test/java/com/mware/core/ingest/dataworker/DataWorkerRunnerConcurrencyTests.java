package com.mware.core.ingest.dataworker;

import com.github.freva.asciitable.AsciiTable;
import com.github.freva.asciitable.Column;
import com.google.common.collect.Lists;
import com.mware.core.TestBaseWithInjector;
import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.process.DataWorkerRunnerProcess;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.*;
import com.mware.ge.inmemory.InMemoryExtendedDataRow;
import com.mware.ge.values.storable.TextValue;
import com.mware.ge.values.storable.Value;
import com.mware.ge.values.storable.Values;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DataWorkerRunnerConcurrencyTests extends TestBaseWithInjector {
    private static BcLogger LOGGER = BcLoggerFactory.getLogger(DataWorkerRunnerConcurrencyTests.class);
    private static final String VERTEX_ID = "vertexID";
    private static final String PROP_NAME = "propName";
    private static final String PROP_KEY = "propKey";
    private static final TextValue PROP_VALUE = Values.stringValue("propValue");

    Authorizations AUTHS;
    WorkQueueRepository workQueueRepository;
    DataWorkerRunnerProcess dataWorkerRunnerProcess;

    @SneakyThrows
    @Before
    public void before() {
        getConfigMap().put("com.mware.core.process.DataWorkerRunnerProcess.threadCount", "8");
        super.before();
        AUTHS = graph.createAuthorizations("A");
        workQueueRepository = InjectHelper.getInstance(WorkQueueRepository.class);

        dataWorkerRunnerProcess = InjectHelper.getInstance(DataWorkerRunnerProcess.class);

        while (!workQueueRepository.hasDataWorkerRunner()) {
            LOGGER.info("Waiting for DataWorkers to start....");
            Thread.sleep(500L);
        }
    }

    @Test
    public void testMultithreading() throws InterruptedException {
        int numElements = 500;

        ExecutorService executorService = Executors.newFixedThreadPool(10);

        for (int i = 0; i < numElements; i++) {
            final int idx = i;
            executorService.submit(() -> {
                Vertex v = createVertex(VERTEX_ID + "_" + idx, createNumProperties(1));
                workQueueRepository.pushOnDwQueue(
                        v,
                        null,
                        null,
                        null,
                        null,
                        Priority.HIGH,
                        ElementOrPropertyStatus.UPDATE,
                        null
                );
            });
        }

        waitForQueueEmpty();
        dataWorkerRunnerProcess.shutdown();

        dataWorkerRunnerProcess.getDataWorkerRunners().forEach(r -> {
            try {
                r.getProcessThread().join(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        List<Vertex> data = new ArrayList<>();
        for (int i = 0; i < numElements; i++) {
            Vertex v = graph.getVertex(VERTEX_ID + "_" + i, AUTHS);
            data.add(v);
        }

        String content = AsciiTable.getTable(data, Arrays.asList(
                new Column().header("id").with(ElementId::getId),
                new Column().header("p1").with(row ->  String.valueOf(row.getPropertyValue("p1"))),
                new Column().header("p2").with(row -> String.valueOf(row.getPropertyValue("p2"))),
                new Column().header("p3").with(row -> String.valueOf(row.getPropertyValue("p3")))
        ));

        System.out.println(content);

        for (int i = 0; i < numElements; i++) {
            Vertex v = graph.getVertex(VERTEX_ID + "_" + i, AUTHS);
            Assert.assertEquals(Values.stringValue("v1"), v.getPropertyValue("p1"));
            Assert.assertEquals(Values.stringValue("v2"), v.getPropertyValue("p2"));
            Assert.assertEquals(Values.stringValue("v3"), v.getPropertyValue("p3"));
        }
    }

    private Vertex createVertex(String vertexId, Property... properties) {
        VertexBuilder v = graph.prepareVertex(vertexId, Visibility.EMPTY, SchemaConstants.CONCEPT_TYPE_THING);
        for (Property property : properties) {
            v.addPropertyValue(property.getKey(), property.getName(), property.getValue(), property.getVisibility());
        }
        return v.save(AUTHS);
    }

    private Property[] createNumProperties(int num) {
        List<Property> props = Lists.newArrayList();

        for (long i = 0; i < num; i++) {
            props.add(createProperty(PROP_NAME + i, PROP_KEY + i, Values.stringValue(PROP_VALUE.stringValue() + i)));
        }

        return props.toArray(new Property[0]);
    }

    private Property createProperty(String name, String key, Value value) {
        return new InMemoryExtendedDataRow.InMemoryProperty(name, key, value, FetchHints.ALL, 0L, Visibility.EMPTY);
    }

    @SneakyThrows
    private void waitForQueueEmpty() {
        Thread.sleep(1000);
        boolean hasMessages = true;
        while (hasMessages) {
            hasMessages = workQueueRepository.getDwQueueSize() > 0;
            Thread.sleep(1000);
        }
    }
}
