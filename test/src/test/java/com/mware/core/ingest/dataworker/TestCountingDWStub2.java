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

public class TestCountingDWStub2 extends DataWorker {
    private static BcLogger LOGGER = BcLoggerFactory.getLogger(TestCountingDWStub2.class);

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
                .setProperty("p2", Values.stringValue("v2 - " + Thread.currentThread().getName()), Visibility.EMPTY)
                .save(getAuthorizations());
        getGraph().flush();

        getWorkQueueRepository().pushOnDwQueue(
                element,
                "",
                "p2",
                null,
                null,
                Priority.HIGH,
                ElementOrPropertyStatus.UPDATE,
                null
        );
    }

    @Override
    public boolean isHandled(Element element, Property property) {
       if (property != null && "p1".equals(property.getName())) {
           return element.getPropertyValue("p2") == null;
       }
       return false;
    }
}
