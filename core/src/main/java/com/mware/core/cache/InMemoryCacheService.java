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
package com.mware.core.cache;

import com.google.inject.Provider;
import com.google.inject.Singleton;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Singleton
public class InMemoryCacheService implements CacheService {
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Map<String, Cache<?, ?>> caches = new HashMap<>();

    @Override
    public <T> T put(String cacheName, String key, T t, CacheOptions cacheOptions) {
        return write(() -> {
            Cache<String, T> cache = getOrCreateCache(cacheName, cacheOptions);
            cache.put(key, t);
            return t;
        });
    }

    @Override
    public <T> T getIfPresent(String cacheName, String key) {
        return read(() -> {
            Cache<String, T> cache = getCache(cacheName);
            if (cache == null) {
                return null;
            }
            return cache.get(key);
        });
    }

    @Override
    public void invalidate(String cacheName) {
        write(() -> {
            Cache<String, ?> cache = getCache(cacheName);
            if (cache == null) {
                return null;
            }
            cache.clear();
            return null;
        });
    }

    @Override
    public void invalidate(String cacheName, String key) {
        write(() -> {
            Cache<String, ?> cache = getCache(cacheName);
            if (cache == null) {
                return null;
            }
            cache.remove(key);
            return null;
        });
    }

    private <T> Cache<String, T> getCache(String cacheName) {
        //noinspection unchecked
        return (Cache<String, T>) caches.get(cacheName);
    }

    private <T> Cache<String, T> getOrCreateCache(String cacheName, CacheOptions cacheOptions) {
        Cache<String, T> cache = getCache(cacheName);
        if (cache != null) {
            return cache;
        }
        //noinspection unchecked
        Cache2kBuilder<String, T> builder = (Cache2kBuilder<String, T>) Cache2kBuilder.of(String.class, Object.class);
        if (cacheOptions.getExpireAfterWrite() != null) {
            builder.expireAfterWrite(cacheOptions.getExpireAfterWrite(), TimeUnit.SECONDS);
        } else {
            builder.eternal(true);
        }

        if (cacheOptions.getMaximumSize() != null) {
            builder.entryCapacity(cacheOptions.getMaximumSize());
        }
        cache = builder.build();
        caches.put(cacheName, cache);
        return cache;
    }

    private <T> T write(Provider<T> provider) {
        readWriteLock.writeLock().lock();
        try {
            return provider.get();
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    private <T> T read(Provider<T> provider) {
        readWriteLock.readLock().lock();
        try {
            return provider.get();
        } finally {
            readWriteLock.readLock().unlock();
        }
    }
}
