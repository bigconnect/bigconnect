package com.mware.core.model.plugin;

import com.google.inject.Inject;
import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.model.user.UserRepository;
import com.mware.core.orm.SimpleOrmContext;
import com.mware.core.orm.SimpleOrmSession;
import com.mware.core.user.SystemUser;
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

    public void save(PluginState pluginState, User authUser) {
        simpleOrmSession.save(
                pluginState,
                Visibility.EMPTY.getVisibilityString(),
                getUserRepository().getSimpleOrmContext(authUser)
        );
    }

    public PluginState findOne(String clazz, User user) {
        SimpleOrmContext ctx = getUserRepository().getSimpleOrmContext(user);
        return simpleOrmSession.findById(PluginState.class, clazz, ctx);
    }

    public void registerPlugin(String clazz, Boolean systemPlugin, User user) {
        PluginState existing = findOne(clazz, user);
        if (existing == null) {
            save(new PluginState(clazz, true, systemPlugin), user);
        } else {
            // maybe the plugin became a system plugin
            existing.setSystemPlugin(systemPlugin);
            save(existing, user);
        }
    }

    public boolean isEnabled(String clazz) {
        PluginState state = findOne(clazz, new SystemUser());
        if (state == null || state.getSystemPlugin())
            return true;
        else
            return state.getEnabled();
    }

    public Iterable<PluginState> findAll(User user) {
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
