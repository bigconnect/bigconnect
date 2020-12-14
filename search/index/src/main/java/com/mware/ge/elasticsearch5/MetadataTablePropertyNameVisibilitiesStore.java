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

import com.google.common.hash.Hashing;
import com.mware.ge.Authorizations;
import com.mware.ge.Graph;
import com.mware.ge.GraphMetadataEntry;
import com.mware.ge.Visibility;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import org.junit.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MetadataTablePropertyNameVisibilitiesStore extends PropertyNameVisibilitiesStore {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(MetadataTablePropertyNameVisibilitiesStore.class);
    public static final String PROPERTY_NAME_VISIBILITY_TO_HASH_PREFIX = "propertyNameVisibility.";
    public static final String HASH_TO_VISIBILITY = "visibilityHash.";
    private static final Charset UTF8 = StandardCharsets.UTF_8;
    private Map<String, Visibility> visibilityCache = new ConcurrentHashMap<>();

    @Test
    public Collection<String> getHashesWithAuthorization(Graph graph, String authorization, Authorizations authorizations) {
        List<String> hashes = new ArrayList<>();
        for (GraphMetadataEntry metadata : graph.getMetadataWithPrefix(HASH_TO_VISIBILITY)) {
            Visibility visibility = getVisibility((String) metadata.getValue());
            if (authorizations.canRead(visibility) && visibility.hasAuthorization(authorization)) {
                String hash = metadata.getKey().substring(HASH_TO_VISIBILITY.length());
                hashes.add(hash);
            }
        }
        return hashes;
    }

    @Override
    public Collection<String> getHashes(Graph graph, Authorizations authorizations) {
        List<String> hashes = new ArrayList<>();
        for (GraphMetadataEntry metadata : graph.getMetadataWithPrefix(HASH_TO_VISIBILITY)) {
            Visibility visibility = getVisibility((String) metadata.getValue());
            if (authorizations.canRead(visibility)) {
                String hash = metadata.getKey().substring(HASH_TO_VISIBILITY.length());
                hashes.add(hash);
            }
        }
        return hashes;
    }

    @Override
    public Collection<String> getHashes(Graph graph, String propertyName, Authorizations authorizations) {
        List<String> results = new ArrayList<>();
        String prefix = getPropertyNameVisibilityToHashPrefix(propertyName);
        for (GraphMetadataEntry metadata : graph.getMetadataWithPrefix(prefix)) {
            String visibilityString = metadata.getKey().substring(prefix.length());
            Visibility visibility = getVisibility(visibilityString);
            if (authorizations.canRead(visibility)) {
                String hash = (String) metadata.getValue();
                results.add(hash);
            }
        }
        return results;
    }

    private Visibility getVisibility(String visibilityString) {
        return visibilityCache.computeIfAbsent(visibilityString, Visibility::new);
    }

    @Override
    public String getHash(Graph graph, String propertyName, Visibility visibility) {
        String visibilityString = visibility.getVisibilityString();
        String propertyNameVisibilityToHashKey = getMetadataKey(propertyName, visibilityString);
        String hash = (String) graph.getMetadata(propertyNameVisibilityToHashKey);
        if (hash != null) {
            saveHashToVisibility(graph, hash, visibilityString);
            return hash;
        }

        hash = Hashing.murmur3_128().hashString(visibilityString, UTF8).toString();
        graph.setMetadata(propertyNameVisibilityToHashKey, hash);
        saveHashToVisibility(graph, hash, visibilityString);
        return hash;
    }

    private void saveHashToVisibility(Graph graph, String hash, String visibilityString) {
        String hashToVisibilityKey = getHashToVisibilityKey(hash);
        String foundVisibilityString = (String) graph.getMetadata(hashToVisibilityKey);
        if (foundVisibilityString == null) {
            graph.setMetadata(hashToVisibilityKey, visibilityString);
        }
    }

    @Override
    public Visibility getVisibilityFromHash(Graph graph, String visibilityHash) {
        String metadataKey = getHashToVisibilityKey(visibilityHash);
        String visibilityString = (String) graph.getMetadata(metadataKey);
        if (visibilityString == null) {
            graph.reloadMetadata();
            visibilityString = (String) graph.getMetadata(metadataKey);
            if (visibilityString == null) {
                LOGGER.warn("Could not find visibility matching the hash \"%s\" in the metadata table with key \"%s\".", visibilityHash, metadataKey);
                return null;
            }
        }
        return new Visibility(visibilityString);
    }

    private String getHashToVisibilityKey(String visibilityHash) {
        return HASH_TO_VISIBILITY + visibilityHash;
    }

    private String getPropertyNameVisibilityToHashPrefix(String propertyName) {
        return PROPERTY_NAME_VISIBILITY_TO_HASH_PREFIX + propertyName + ".";
    }

    private String getMetadataKey(String propertyName, String visibilityString) {
        return getPropertyNameVisibilityToHashPrefix(propertyName) + visibilityString;
    }
}
