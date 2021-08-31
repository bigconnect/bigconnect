package com.mware.core.model.plugin;

import com.google.inject.Inject;
import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.model.user.UserRepository;
import com.mware.core.orm.SimpleOrmContext;
import com.mware.core.orm.SimpleOrmSession;
import com.mware.core.user.User;
import com.mware.ge.Visibility;

public class PluginStateRepository {
    private final SimpleOrmSession simpleOrmSession;
    private UserRepository userRepository;

    @Inject
    public PluginStateRepository(
            final SimpleOrmSession simpleOrmSession
    ) {
        this.simpleOrmSession = simpleOrmSession;
    }

    private void save(PluginState pluginState, User authUser) {
        simpleOrmSession.save(pluginState, Visibility.EMPTY.getVisibilityString(), getUserRepository().getSimpleOrmContext(authUser));
    }

    private Iterable<PluginState> findAll(User user) {
        SimpleOrmContext ctx = getUserRepository().getSimpleOrmContext(user);
        return simpleOrmSession.findAll(PluginState.class, ctx);
    }

    private UserRepository getUserRepository() {
        if (userRepository == null) {
            userRepository = InjectHelper.getInstance(UserRepository.class);
        }

        return userRepository;
    }
}
