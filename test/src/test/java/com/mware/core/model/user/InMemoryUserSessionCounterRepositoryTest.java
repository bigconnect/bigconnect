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
package com.mware.core.model.user;

import com.mware.core.time.MockTimeRepository;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;

import static junit.framework.TestCase.assertEquals;

public class InMemoryUserSessionCounterRepositoryTest {
    public static final String USER1_ID = "user1";
    private MockTimeRepository timeRepository = new MockTimeRepository();
    private InMemoryUserSessionCounterRepository sessionCounterRepository;

    @Before
    public void setUp() {
        timeRepository.setNow(new Date());
        sessionCounterRepository = new InMemoryUserSessionCounterRepository(timeRepository);
    }

    @Test
    public void testDeleteSessionsNoCurrentSessions() {
        assertEquals(0, sessionCounterRepository.getSessionCount(USER1_ID));
        sessionCounterRepository.deleteSessions(USER1_ID);
        assertEquals(0, sessionCounterRepository.getSessionCount(USER1_ID));
    }

    @Test
    public void testDeleteSessionsWithCurrentSessions() {
        assertEquals(0, sessionCounterRepository.getSessionCount(USER1_ID));
        assertEquals(1, sessionCounterRepository.updateSession(USER1_ID, "session1", false));
        assertEquals(1, sessionCounterRepository.getSessionCount(USER1_ID));
        sessionCounterRepository.deleteSessions(USER1_ID);
        assertEquals(0, sessionCounterRepository.getSessionCount(USER1_ID));
    }

    @Test
    public void testUpdateSession() {
        assertEquals(0, sessionCounterRepository.getSessionCount(USER1_ID));
        assertEquals(1, sessionCounterRepository.updateSession(USER1_ID, "session1", false));
        assertEquals(2, sessionCounterRepository.updateSession(USER1_ID, "session2", false));
        assertEquals(2, sessionCounterRepository.updateSession(USER1_ID, "session2", false));
        assertEquals(2, sessionCounterRepository.getSessionCount(USER1_ID));
    }

    @Test
    public void testDeleteSession() {
        assertEquals(0, sessionCounterRepository.getSessionCount(USER1_ID));
        assertEquals(1, sessionCounterRepository.updateSession(USER1_ID, "session1", false));
        assertEquals(2, sessionCounterRepository.updateSession(USER1_ID, "session2", false));
        assertEquals(1, sessionCounterRepository.deleteSession(USER1_ID, "session2"));
        assertEquals(0, sessionCounterRepository.deleteSession(USER1_ID, "session1"));
        assertEquals(0, sessionCounterRepository.getSessionCount(USER1_ID));
    }

    @Test
    public void testExpire() {
        Date t1 = new Date(System.currentTimeMillis());
        Date t2 = new Date(System.currentTimeMillis() + 5000);
        Date t3 = new Date(System.currentTimeMillis() + InMemoryUserSessionCounterRepository.UNSEEN_SESSION_DURATION + 100);

        assertEquals(0, sessionCounterRepository.getSessionCount(USER1_ID));
        timeRepository.setNow(t1);
        assertEquals(1, sessionCounterRepository.updateSession(USER1_ID, "session1", true));
        assertEquals(1, sessionCounterRepository.getSessionCount(USER1_ID));
        timeRepository.setNow(t2);
        assertEquals(1, sessionCounterRepository.getSessionCount(USER1_ID));
        timeRepository.setNow(t3);
        assertEquals(0, sessionCounterRepository.getSessionCount(USER1_ID));
    }
}