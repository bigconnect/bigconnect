package com.mware.ge.hbase;

import com.mware.ge.Graph;
import com.mware.ge.base.TestGraphFactory;

import java.util.Collections;

public class HBaseGraphFactory implements TestGraphFactory {
    @Override
    public Graph createGraph() throws Exception {
        return HBaseGraph.create(new HBaseGraphConfiguration(Collections.emptyMap()));
    }
}
