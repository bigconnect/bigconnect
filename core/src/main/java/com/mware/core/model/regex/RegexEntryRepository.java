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
package com.mware.core.model.regex;

import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.mware.core.model.user.UserRepository;
import com.mware.core.orm.Entity;
import com.mware.core.orm.SimpleOrmContext;
import com.mware.core.orm.SimpleOrmSession;
import com.mware.core.user.User;


public class RegexEntryRepository {
    private static final String VISIBILITY_STRING = "";
    private final SimpleOrmSession simpleOrmSession;
    private final UserRepository userRepository;

    @Inject
    public RegexEntryRepository(SimpleOrmSession simpleOrmSession, UserRepository userRepository) {
        this.simpleOrmSession = simpleOrmSession;
        this.userRepository = userRepository;
    }

    public Iterable<RegexEntry> findAll(User user) {
        return this.simpleOrmSession.findAll(RegexEntry.class, userRepository.getSimpleOrmContext(user));
    }

    public Iterable<RegexEntry> findByIdStartsWith(SimpleOrmContext simpleOrmContext, String prefix) {
        return this.simpleOrmSession.findByIdStartsWith(RegexEntry.class, prefix, simpleOrmContext);
    }

    public Iterable<RegexEntry> findById(String id, User user) {
        return this.simpleOrmSession.findByIdStartsWith(RegexEntry.class, id, userRepository.getSimpleOrmContext(user));
    }

    public Iterable<RegexEntry> findByName(final String name, User user) {
        Iterable<RegexEntry> rows = findAll(user);
        return Iterables.filter(rows, regexEntry -> regexEntry.getName().equals(name));
    }

    public void delete(String id, User user) {
        this.simpleOrmSession.delete(RegexEntry.class, id, userRepository.getSimpleOrmContext(user));
    }

    public void deleteAll(User user) {
        this.simpleOrmSession.deleteTable(this.simpleOrmSession.getTablePrefix() + RegexEntry.class.getAnnotationsByType(Entity.class)[0].tableName(), userRepository.getSimpleOrmContext(user));
    }


    public RegexEntry createNew(String name, String pattern, String concept) {
        return new RegexEntry(
                name,
                pattern,
                concept
        );
    }

    public RegexEntry saveNew(String name, String pattern, String concept, User user) {
        RegexEntry entry = createNew(name, pattern, concept);
        this.simpleOrmSession.save(entry, VISIBILITY_STRING, userRepository.getSimpleOrmContext(user));
        return entry;
    }

    public void updateRegexEntry(RegexEntry entry, User user) {
        this.simpleOrmSession.save(entry, VISIBILITY_STRING, userRepository.getSimpleOrmContext(user));
    }
}
