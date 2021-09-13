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
package com.mware.core.config;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class ConfigurationTest {
    private static Configuration configuration;

    @BeforeClass
    public static void setUp() {
        Map<String, String> map = new HashMap<String, String>();
        map.put("rabbitmq.addr.0.host", "${ip.address.prefix}.101");
        map.put("rabbitmq.addr.2.host", "${ip.address.prefix}.103");
        map.put("rabbitmq.addr.1.host", "${ip.address.prefix}.102");
        map.put("foo", "A");
        map.put("bar", "B");
        map.put("bar.baz", "C");
        map.put("ip.address.prefix", "10.0.1");
        map.put("baz.a", "a");
        map.put("baz.b", "${baz.a}");
        map.put("baz.c", "${baz.b}");
        map.put("baz.d", "${baz.c}");
        map.put("baz.e", "${baz.d}");
        map.put("baz.f", "${baz.e}");
        map.put("baz.g", "${baz.f}");

        ConfigurationLoader configurationLoader = new HashMapConfigurationLoader(map);
        configuration = configurationLoader.createConfiguration();
    }

    @Test
    public void testRecursiveResolution() {
        assertEquals("a", configuration.get("baz.b", null));
        assertEquals("a", configuration.get("baz.c", null));
        assertEquals("a", configuration.get("baz.d", null));
        assertEquals("a", configuration.get("baz.e", null));
        assertEquals("a", configuration.get("baz.f", null));
        assertEquals("a", configuration.get("baz.g", null));
    }

    @Test
    public void testGetSubset() {
        Map<String, Object> subset = configuration.getSubset("rabbitmq.addr");
        assertEquals(3, subset.size());
        assertTrue(subset.keySet().contains("0.host"));
        assertTrue(subset.keySet().contains("1.host"));
        assertTrue(subset.keySet().contains("2.host"));
        assertEquals("10.0.1.101", subset.get("0.host"));
        assertEquals("10.0.1.102", subset.get("1.host"));
        assertEquals("10.0.1.103", subset.get("2.host"));
    }

    @Test
    public void testGetKeysWithPrefix() {
        Set<String> addrKeys = (Set<String>) configuration.getKeys("rabbitmq.addr.");
        assertEquals(3, addrKeys.size());
        assertTrue(addrKeys.contains("rabbitmq.addr.0.host"));
        assertTrue(addrKeys.contains("rabbitmq.addr.1.host"));
        assertTrue(addrKeys.contains("rabbitmq.addr.2.host"));

        Set<String> barKeys = (Set<String>) configuration.getKeys("bar");
        assertEquals(2, barKeys.size());
        assertTrue(barKeys.contains("bar"));
        assertTrue(barKeys.contains("bar.baz"));

        Set<String> barDotKeys = (Set<String>) configuration.getKeys("bar.");
        assertEquals(1, barDotKeys.size());
        assertTrue(barDotKeys.contains("bar.baz"));
    }

    @Test
    public void testGet() {
        String hit = configuration.get("foo", null);
        assertEquals("A", hit);

        String miss = configuration.get("no.such.key", null);
        assertEquals(null, miss);
    }

    @Test
    public void testSetConfigurables() {
        SetConfigurablesTestClass obj = new SetConfigurablesTestClass();
        configuration.setConfigurables(obj, new HashMap<String, Object>() {{
            put("propWithDifferentNameDifferent", "propWithDifferentNameDifferentValue");
            put("propWithSetter", "propWithSetterValue");
            put("map.0.a", "0a");
            put("map.0.b", "0b");
            put("map.1.a", "1a");
            put("map.1.b", "1b");
        }});

        assertEquals("propWithCodeDefaultValue", obj.getPropWithCodeDefault());
        assertEquals("propWithAnnotationDefaultValue", obj.getPropWithAnnotationDefault());
        assertEquals("propWithDifferentNameDifferentValue", obj.getPropWithDifferentName());
        assertEquals("propWithSetterValue", obj.getPropWithSetter());
        assertEquals("0a", obj.map.get("0").get("a"));
        assertEquals("0b", obj.map.get("0").get("b"));
        assertEquals("1a", obj.map.get("1").get("a"));
        assertEquals("1b", obj.map.get("1").get("b"));
    }

    public static class SetConfigurablesTestClass {
        @Configurable
        private String propWithCodeDefault = "propWithCodeDefaultValue";

        @Configurable(defaultValue = "propWithAnnotationDefaultValue")
        private String propWithAnnotationDefault;

        @Configurable(name = "propWithDifferentNameDifferent")
        private String propWithDifferentName;

        private String propWithSetter;

        private Map<String, Map<String, String>> map;

        public String getPropWithSetter() {
            return propWithSetter;
        }

        @Configurable
        public void setPropWithSetter(String propWithSetter) {
            this.propWithSetter = propWithSetter;
        }

        public String getPropWithCodeDefault() {
            return propWithCodeDefault;
        }

        public String getPropWithAnnotationDefault() {
            return propWithAnnotationDefault;
        }

        public String getPropWithDifferentName() {
            return propWithDifferentName;
        }

        @Configurable
        public void setMap(Map<String, Map<String, String>> map) {
            this.map = map;
        }
    }
}

