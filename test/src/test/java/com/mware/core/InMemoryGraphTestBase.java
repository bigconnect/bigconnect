package com.mware.core;

import com.mware.ge.base.TestGraphFactory;
import com.mware.ge.inmemory.InMemoryGraphFactory;

public class InMemoryGraphTestBase extends GraphTestBase {
    @Override
    protected TestGraphFactory graphFactory() {
        return new InMemoryGraphFactory();
    }
}
