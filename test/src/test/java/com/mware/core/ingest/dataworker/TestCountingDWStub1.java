package com.mware.core.ingest.dataworker;

import com.google.common.collect.Sets;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.Element;
import com.mware.ge.Property;
import com.mware.ge.Visibility;
import com.mware.ge.values.storable.Values;

import java.io.InputStream;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class TestCountingDWStub1 extends DataWorker {
    private static BcLogger LOGGER = BcLoggerFactory.getLogger(TestCountingDWStub1.class);

    public static AtomicLong isHandledCount = new AtomicLong(0);
    public static AtomicLong isExecutingCount = new AtomicLong(0);
    public static Set<Property> workedOnProperties = Sets.newHashSet();

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);
    }

    @Override
    public void execute(InputStream in, DataWorkerData data) throws Exception {
        isExecutingCount.incrementAndGet();

        Element element = data.getElement().prepareMutation()
                .setProperty("p1", Values.stringValue("v1 - " + Thread.currentThread().getName()), Visibility.EMPTY)
                .save(getAuthorizations());
        getGraph().flush();

        getWorkQueueRepository().pushOnDwQueue(
                element,
                "",
                "p1",
                null,
                null,
                Priority.HIGH,
                ElementOrPropertyStatus.UPDATE,
                null
        );
    }

    @Override
    public boolean isHandled(Element element, Property property) {
        return element.getPropertyValue("p1") == null;
    }
}
