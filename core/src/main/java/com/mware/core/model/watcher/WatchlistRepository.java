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
package com.mware.core.model.watcher;

import com.google.inject.Inject;
import com.mware.core.orm.SimpleOrmContext;
import com.mware.core.orm.SimpleOrmSession;
import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.model.user.UserRepository;
import com.mware.core.user.User;

import java.util.stream.Stream;

import static com.mware.core.util.StreamUtil.stream;

public class WatchlistRepository {
    private static final String VISIBILITY_STRING = "";

    private final SimpleOrmSession simpleOrmSession;
    private UserRepository userRepository;

    @Inject
    public WatchlistRepository(
            final SimpleOrmSession simpleOrmSession
    ) {
        this.simpleOrmSession = simpleOrmSession;
    }

    public void createWatch(String userId, String elementId, String propertyName, User authUser, String elementTitle) {
        Watch watch = new Watch(userId, elementId, propertyName, elementTitle);
        saveWatch(watch, authUser);
    }

    public void deleteWatch(String id, User authUser) {
        getSimpleOrmSession().delete(Watch.class, id, getUserRepository().getSimpleOrmContext(authUser));
    }

    private void saveWatch(Watch watch, User authUser) {
        getSimpleOrmSession().save(watch, VISIBILITY_STRING, getUserRepository().getSimpleOrmContext(authUser));
    }

    public Stream<Watch> getUserWatches(User user) {
        return findAll(user)
                .filter(watch ->
                        user.getUserId().equals(watch.getUserId())
                );
    }

    public Stream<Watch> getElementWatches(String elementId) {
        return findAll(getUserRepository().getSystemUser())
                .filter(watch ->
                        watch.getElementId().equals(elementId)
                );
    }

    private Stream<Watch> findAll(User user) {
        SimpleOrmContext ctx = getUserRepository().getSimpleOrmContext(user);
        return stream(getSimpleOrmSession().findAll(Watch.class, ctx));
    }

    public SimpleOrmSession getSimpleOrmSession() {
        return simpleOrmSession;
    }

    private UserRepository getUserRepository() {
        if (userRepository == null) {
            userRepository = InjectHelper.getInstance(UserRepository.class);
        }

        return userRepository;
    }
}
