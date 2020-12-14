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
package com.mware.ge.mutation;

import com.mware.ge.Authorizations;
import com.mware.ge.Element;
import com.mware.ge.FetchHints;
import com.mware.ge.Visibility;
import com.mware.ge.property.MutablePropertyImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static com.mware.ge.values.storable.Values.stringValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ExistingElementMutationImplTest {
    private TestExistingElementMutationImpl mutation;

    @Mock
    private Element element;

    @Before
    public void before() {
        when(element.getFetchHints()).thenReturn(FetchHints.ALL);
        mutation = new TestExistingElementMutationImpl<>(element);
    }

    @Test
    public void testEmptyMutationHasChanges() {
        assertFalse("should not have changes", mutation.hasChanges());
    }

    @Test
    public void testHasChangesProperties() {
        mutation.addPropertyValue("key1", "name1", stringValue("Hello"), new Visibility(""));
        assertTrue("should have changes", mutation.hasChanges());
    }

    @Test
    public void testHasChangesDeleteProperty() {
        mutation.deleteProperty(new MutablePropertyImpl(
                "key1",
                "name1",
                stringValue("value"),
                null,
                null,
                null,
                new Visibility(""),
                FetchHints.ALL
        ));
        assertTrue("should have changes", mutation.hasChanges());
    }

    @Test
    public void testHasChangesSoftDeleteProperty() {
        mutation.softDeleteProperty(new MutablePropertyImpl(
                "key1",
                "name1",
                stringValue("value"),
                null,
                null,
                null,
                new Visibility(""),
                FetchHints.ALL
        ));
        assertTrue("should have changes", mutation.hasChanges());
    }

    @Test
    public void testHasChangesNewElementVisibility() {
        mutation.alterElementVisibility(new Visibility(""));
        assertTrue("should have changes", mutation.hasChanges());
    }

    @Test
    public void testHasChangesAlterPropertyVisibility() {
        mutation.alterPropertyVisibility("key1", "name1", new Visibility(""));
        assertTrue("should have changes", mutation.hasChanges());
    }

    @Test
    public void testHasChangesSetPropertyMetadata() {
        mutation.setPropertyMetadata("key1", "name1", stringValue("value"), new Visibility(""));
        assertTrue("should have changes", mutation.hasChanges());
    }

    private static class TestExistingElementMutationImpl<T extends Element> extends ExistingElementMutationImpl<T> {
        public TestExistingElementMutationImpl(T element) {
            super(element);
        }

        @Override
        public T save(Authorizations authorizations) {
            return null;
        }
    }
}
