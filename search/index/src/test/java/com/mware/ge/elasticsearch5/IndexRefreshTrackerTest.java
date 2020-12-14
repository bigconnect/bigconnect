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
package com.mware.ge.elasticsearch5;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import com.mware.ge.metric.NullMetricRegistry;
import org.elasticsearch.client.Client;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class IndexRefreshTrackerTest {
    private IndexRefreshTracker indexRefreshTracker;
    private long time;
    private Set<String> lastIndexNamesNeedingRefresh;

    @Before
    public void before() {
        this.indexRefreshTracker = new IndexRefreshTracker(new NullMetricRegistry()) {
            @Override
            protected long getTime() {
                return time;
            }

            @Override
            protected void refresh(Client client, Set<String> indexNamesNeedingRefresh) {
                lastIndexNamesNeedingRefresh = indexNamesNeedingRefresh;
            }
        };
    }

    @Test
    public void testRefreshListOfIndexNames_noChanges() {
        indexRefreshTracker.refresh(null, "a", "b");
        assertNull(lastIndexNamesNeedingRefresh);
    }

    @Test
    public void testRefreshListOfIndexNames_singleChange() {
        time = 0;
        indexRefreshTracker.pushChange("a");

        time = 2;
        indexRefreshTracker.refresh(null, "a", "b");
        assertLastIndexNamesNeedingRefresh(Sets.newHashSet("a"));
        assertLastIndexNamesNeedingRefresh(Sets.newHashSet());
    }

    @Test
    public void testRefreshListOfIndexNames_multipleChanges() {
        time = 1;
        indexRefreshTracker.pushChange("a");
        indexRefreshTracker.pushChange("b");

        time = 2;
        indexRefreshTracker.refresh(null, "a", "b");
        assertLastIndexNamesNeedingRefresh(Sets.newHashSet("a", "b"));
        assertLastIndexNamesNeedingRefresh(Sets.newHashSet());
    }

    @Test
    public void testRefreshListOfIndexNames_time() {
        time = 2;
        indexRefreshTracker.pushChange("a");
        indexRefreshTracker.pushChange("b");

        time = 1;
        indexRefreshTracker.refresh(null, "a", "b");
        assertLastIndexNamesNeedingRefresh(Sets.newHashSet());

        time = 3;
        indexRefreshTracker.refresh(null, "a", "b");
        assertLastIndexNamesNeedingRefresh(Sets.newHashSet("a", "b"));
        assertLastIndexNamesNeedingRefresh(Sets.newHashSet());
    }

    private void assertLastIndexNamesNeedingRefresh(Set<String> expected) {
        Set<String> found = lastIndexNamesNeedingRefresh;
        if (found == null) {
            found = new HashSet<>();
        }
        lastIndexNamesNeedingRefresh = null;
        ArrayList<String> expectedList = new ArrayList<>(expected);
        Collections.sort(expectedList);
        ArrayList<String> foundList = new ArrayList<>(found);
        Collections.sort(foundList);

        assertEquals(
                Joiner.on(", ").join(expectedList),
                Joiner.on(", ").join(foundList)
        );
    }
}
