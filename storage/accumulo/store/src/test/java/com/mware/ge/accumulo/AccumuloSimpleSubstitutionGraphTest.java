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
package com.mware.ge.accumulo;

import com.google.common.base.Joiner;
import org.junit.ClassRule;
import com.mware.ge.id.SimpleNameSubstitutionStrategy;

import java.util.HashMap;

import static com.mware.ge.id.SimpleSubstitutionUtils.*;

public class AccumuloSimpleSubstitutionGraphTest extends AccumuloBaseTests {

    @ClassRule
    public static final AccumuloResource accumuloResource = new AccumuloResource(new HashMap<String, String>() {{
        put(AccumuloGraphConfiguration.NAME_SUBSTITUTION_STRATEGY_PROP_PREFIX, SimpleNameSubstitutionStrategy.class.getName());
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "0", KEY_IDENTIFIER}), "k1");
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "0", VALUE_IDENTIFIER}), "k");
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "1", KEY_IDENTIFIER}), "author");
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "1", VALUE_IDENTIFIER}), "a");
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "2", KEY_IDENTIFIER}), "label");
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "2", VALUE_IDENTIFIER}), "l");
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "3", KEY_IDENTIFIER}), LABEL_LABEL1);
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "3", VALUE_IDENTIFIER}), "l1");
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "4", KEY_IDENTIFIER}), LABEL_LABEL2);
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "4", VALUE_IDENTIFIER}), "l2");
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "5", KEY_IDENTIFIER}), LABEL_LABEL3);
        put(Joiner.on('.').join(new String[]{SUBSTITUTION_MAP_PREFIX, "5", VALUE_IDENTIFIER}), "l3");
    }});

//    @Override
//    protected String substitutionDeflate(String str) {
//        if (str.equals("author")) {
//            return SimpleNameSubstitutionStrategy.SUBS_DELIM + "a" + SimpleNameSubstitutionStrategy.SUBS_DELIM;
//        }
//        return str;
//    }
}
