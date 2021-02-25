package com.mware.bigconnect;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.mware.core.bootstrap.BootstrapBindingProvider;
import com.mware.core.config.Configuration;
import com.mware.ge.cypher.connection.DefaultNetworkConnectionTracker;
import com.mware.ge.cypher.connection.NetworkConnectionTracker;

public class ApplicationBindingProvider implements BootstrapBindingProvider {
    @Override
    public void addBindings(Binder binder, Configuration configuration) {
        binder.bind(NetworkConnectionTracker.class)
                .toProvider(DefaultNetworkConnectionTracker::new)
                .in(Scopes.SINGLETON);
    }
}
