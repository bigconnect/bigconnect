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
package com.mware.core.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SourceInfoSnippetSanitizerTest {

    @Test
    public void sanitizeIrregularSnippetEscaped() {
        assertEquals("test&lt;ing",
                SourceInfoSnippetSanitizer.sanitizeSnippet("test<ing"));
    }

    @Test
    public void sanitizeRegularSnippetEscaped() {
        assertEquals("testing &amp; <span class=\"selection\">a</span>more text &amp; bold",
                SourceInfoSnippetSanitizer.sanitizeSnippet("testing & <span class=\"selection\">a</span>more text & bold"));
    }

    @Test
    public void sanitizeXssSnippetEscaped() {
        assertEquals("&lt;script&gt;xss&lt;/script&gt;",
                SourceInfoSnippetSanitizer.sanitizeSnippet("<script>xss</script>"));
    }

    @Test
    public void sanitizeRegularXssSnippetEscaped() {
        assertEquals("&lt;script&gt;xss&lt;/script&gt;<span class=\"selection\">&lt;script&gt;xss&lt;/script&gt;</span>&lt;script&gt;xss&lt;/script&gt;",
                SourceInfoSnippetSanitizer.sanitizeSnippet("<script>xss</script><span class=\"selection\"><script>xss</script></span><script>xss</script>"));
    }

    @Test
    public void sanitizeSpanAttributes() {
        assertEquals("prefix&lt;span onClick=&quot;javascript:void&quot; class=&quot;selection&quot;&gt;normal&lt;/span&gt;post",
                SourceInfoSnippetSanitizer.sanitizeSnippet("prefix<span onClick=\"javascript:void\" class=\"selection\">normal</span>post"));
    }

    @Test
    public void sanitizeSelectionWithUnicode() {
        assertEquals("😎<span class=\"selection\">😎</span>😎",
                SourceInfoSnippetSanitizer.sanitizeSnippet("😎<span class=\"selection\">😎</span>😎"));
    }

    @Test
    public void sanitizeNoPrefix() {
        assertEquals("<span class=\"selection\">sel</span>post",
                SourceInfoSnippetSanitizer.sanitizeSnippet("<span class=\"selection\">sel</span>post"));
    }

    @Test
    public void sanitizeNoSuffix() {
        assertEquals("pre<span class=\"selection\">sel</span>",
                SourceInfoSnippetSanitizer.sanitizeSnippet("pre<span class=\"selection\">sel</span>"));
    }

    @Test
    public void sanitizeOnlySelection() {
        assertEquals("<span class=\"selection\">sel</span>",
                SourceInfoSnippetSanitizer.sanitizeSnippet("<span class=\"selection\">sel</span>"));
    }


    @Test
    public void sanitizeMultipleSelection() {
        assertEquals("<span class=\"selection\">sel</span>&lt;span class=&quot;selection&quot;&gt;sel&lt;/span&gt;",
                SourceInfoSnippetSanitizer.sanitizeSnippet("<span class=\"selection\">sel</span><span class=\"selection\">sel</span>"));
    }

    @Test
    public void sanitizeHandlesNull() {
        assertEquals(null,
                SourceInfoSnippetSanitizer.sanitizeSnippet(null));
    }
}
