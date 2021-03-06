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
package com.mware.core.lifecycle;


import com.google.inject.Singleton;
import com.mware.core.util.ExceptionUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

/**
 * Support class for handling collections of Lifecycle instances. Manages the transitions from one state to another.
 * <p>
 * To use this, first add instances to it that implement the Lifecycle interface. When lifecycle methods on this
 * class are called it will try to invoke the same methods on the registered instances.
 * <p>
 * Components that internally owns other components that has a lifecycle can use this to control them as well.
 */
@Singleton
public class LifeSupportService implements Lifecycle {
    private volatile List<LifecycleInstance> instances = new ArrayList<>();
    private volatile LifecycleStatus status = LifecycleStatus.NONE;
    private final List<LifecycleListener> listeners = new ArrayList<>();
    private LifecycleInstance last;

    public LifeSupportService() {
    }

    /**
     * Initialize all registered instances, transitioning from status NONE to STOPPED.
     * <p>
     * If transition fails, then it goes to STOPPED and then SHUTDOWN, so it cannot be restarted again.
     */
    @Override
    public synchronized void init()
            throws LifecycleException {
        if (status == LifecycleStatus.NONE) {
            status = changedStatus(this, status, LifecycleStatus.INITIALIZING);
            for (LifecycleInstance instance : instances) {
                try {
                    instance.init();
                } catch (LifecycleException e) {
                    status = changedStatus(this, status, LifecycleStatus.STOPPED);

                    try {
                        shutdown();
                    } catch (LifecycleException shutdownErr) {
                        e.addSuppressed(shutdownErr);
                    }

                    throw e;
                }
            }
            status = changedStatus(this, status, LifecycleStatus.STOPPED);
        }
    }

    /**
     * Start all registered instances, transitioning from STOPPED to STARTED.
     * <p>
     * If it was previously not initialized, it will be initialized first.
     * <p>
     * If any instance fails to start, the already started instances will be stopped, so
     * that the overall status is STOPPED.
     *
     * @throws LifecycleException
     */
    @Override
    public synchronized void start()
            throws LifecycleException {
        init();

        if (status == LifecycleStatus.STOPPED) {
            status = changedStatus(this, status, LifecycleStatus.STARTING);
            for (LifecycleInstance instance : instances) {
                try {
                    instance.start();
                } catch (LifecycleException e) {
                    // TODO perhaps reconsider chaining of exceptions coming from LifeSupports?
                    status = changedStatus(this, status, LifecycleStatus.STARTED);
                    try {
                        stop();

                    } catch (LifecycleException stopErr) {
                        e.addSuppressed(stopErr);
                    }

                    throw e;
                }
            }
            status = changedStatus(this, status, LifecycleStatus.STARTED);
        }
    }

    /**
     * Stop all registered instances, transitioning from STARTED to STOPPED.
     * <p>
     * If any instance fails to stop, the rest of the instances will still be stopped,
     * so that the overall status is STOPPED.
     */
    @Override
    public synchronized void stop()
            throws LifecycleException {
        if (status == LifecycleStatus.STARTED) {
            status = changedStatus(this, status, LifecycleStatus.STOPPING);
            LifecycleException ex = stopInstances(instances);
            status = changedStatus(this, status, LifecycleStatus.STOPPED);

            if (ex != null) {
                throw ex;
            }
        }
    }

    /**
     * Shutdown all registered instances, transitioning from either STARTED or STOPPED to SHUTDOWN.
     * <p>
     * If any instance fails to shutdown, the rest of the instances will still be shut down,
     * so that the overall status is SHUTDOWN.
     */
    @Override
    public synchronized void shutdown()
            throws LifecycleException {
        LifecycleException ex = null;
        try {
            stop();
        } catch (LifecycleException e) {
            ex = e;
        }

        if (status == LifecycleStatus.STOPPED) {
            status = changedStatus(this, status, LifecycleStatus.SHUTTING_DOWN);
            for (int i = instances.size() - 1; i >= 0; i--) {
                LifecycleInstance lifecycleInstance = instances.get(i);
                try {
                    lifecycleInstance.shutdown();
                } catch (LifecycleException e) {
                    ex = ExceptionUtil.chain(ex, e);
                }
            }

            status = changedStatus(this, status, LifecycleStatus.SHUTDOWN);

            if (ex != null) {
                throw ex;
            }
        }
    }

    /**
     * Add a new Lifecycle instance. It will immediately be transitioned
     * to the state of this LifeSupport.
     *
     * @param instance the Lifecycle instance to add
     * @param <T>      type of the instance
     * @return the instance itself
     * @throws LifecycleException if the instance could not be transitioned properly
     */
    public synchronized <T extends Lifecycle> T add(T instance)
            throws LifecycleException {
        addNewComponent(instance);
        return instance;
    }

    public synchronized <T extends Lifecycle> T setLast(T instance) {
        if (last != null) {
            throw new IllegalStateException(
                    format("Lifecycle supports only one last component. Already defined component: %s, new component: %s", last, instance));
        }
        last = addNewComponent(instance);
        return instance;
    }

    private <T extends Lifecycle> LifecycleInstance addNewComponent(T instance) {
        Objects.requireNonNull(instance);
        validateNotAlreadyPartOfLifecycle(instance);
        LifecycleInstance newInstance = new LifecycleInstance(instance);
        List<LifecycleInstance> tmp = new ArrayList<>(instances);
        int position = last != null ? tmp.size() - 1 : tmp.size();
        tmp.add(position, newInstance);
        instances = tmp;
        bringToState(newInstance);
        return newInstance;
    }

    private void validateNotAlreadyPartOfLifecycle(Lifecycle instance) {
        for (LifecycleInstance candidate : instances) {
            if (candidate.instance == instance) {
                throw new IllegalStateException(instance + " already added", candidate.addedWhere);
            }
        }
    }

    private LifecycleException stopInstances(List<LifecycleInstance> instances) {
        LifecycleException ex = null;
        for (int i = instances.size() - 1; i >= 0; i--) {
            LifecycleInstance lifecycleInstance = instances.get(i);
            try {
                lifecycleInstance.stop();
            } catch (LifecycleException e) {
                ex = ExceptionUtil.chain(ex, e);
            }
        }
        return ex;
    }

    public synchronized boolean remove(Lifecycle instance) {
        for (int i = 0; i < instances.size(); i++) {
            if (instances.get(i).isInstance(instance)) {
                List<LifecycleInstance> tmp = new ArrayList<>(instances);
                LifecycleInstance lifecycleInstance = tmp.remove(i);
                lifecycleInstance.shutdown();
                instances = tmp;
                return true;
            }
        }
        return false;
    }

    public List<Lifecycle> getLifecycleInstances() {
        return instances.stream().map(l -> l.instance).collect(toList());
    }

    /**
     * Shutdown and throw away all the current instances. After
     * this you can add new instances. This method does not change
     * the status of the LifeSupport (i.e. if it was started it will remain started)
     */
    public synchronized void clear() {
        for (LifecycleInstance instance : instances) {
            instance.shutdown();
        }
        instances = new ArrayList<>();
    }

    public LifecycleStatus getStatus() {
        return status;
    }

    public synchronized void addLifecycleListener(LifecycleListener listener) {
        listeners.add(listener);
    }

    private void bringToState(LifecycleInstance instance)
            throws LifecycleException {
        switch (status) {
            case STARTED:
                instance.start();
                break;
            case STOPPED:
                instance.init();
                break;
            default:
                break;
        }
    }

    private LifecycleStatus changedStatus(Lifecycle instance,
                                          LifecycleStatus oldStatus,
                                          LifecycleStatus newStatus
    ) {
        for (LifecycleListener listener : listeners) {
            listener.lifeStatusChanged(instance, oldStatus, newStatus);
        }

        return newStatus;
    }

    public boolean isRunning() {
        return status == LifecycleStatus.STARTED;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        toString(0, sb);
        return sb.toString();
    }

    private void toString(int indent, StringBuilder sb) {
        for (int i = 0; i < indent; i++) {
            sb.append(' ');
        }
        sb.append("Lifecycle status:" + status.name()).append('\n');
        for (LifecycleInstance instance : instances) {
            if (instance.instance instanceof LifeSupportService) {
                ((LifeSupportService) instance.instance).toString(indent + 3, sb);
            } else {
                for (int i = 0; i < indent + 3; i++) {
                    sb.append(' ');
                }
                sb.append(instance.toString()).append('\n');

            }

        }

    }

    private class LifecycleInstance
            implements Lifecycle {
        Lifecycle instance;
        LifecycleStatus currentStatus = LifecycleStatus.NONE;
        Exception addedWhere;

        private LifecycleInstance(Lifecycle instance) {
            this.instance = instance;
            assert trackInstantiationStackTrace();
        }

        private boolean trackInstantiationStackTrace() {
            addedWhere = new Exception();
            return true;
        }

        @Override
        public void init()
                throws LifecycleException {
            if (currentStatus == LifecycleStatus.NONE) {
                currentStatus = changedStatus(instance, currentStatus, LifecycleStatus.INITIALIZING);
                try {
                    instance.init();
                    currentStatus = changedStatus(instance, currentStatus, LifecycleStatus.STOPPED);
                } catch (Throwable e) {
                    currentStatus = changedStatus(instance, currentStatus, LifecycleStatus.NONE);
                    try {
                        instance.shutdown();
                    } catch (Throwable se) {
                        LifecycleException lifecycleException = new LifecycleException("Exception during graceful " +
                                "attempt to shutdown partially initialized component. Please use non suppressed" +
                                " exception to see original component failure.", se);
                        e.addSuppressed(lifecycleException);
                    }
                    if (e instanceof LifecycleException) {
                        throw (LifecycleException) e;
                    }
                    throw new LifecycleException(instance, LifecycleStatus.NONE, LifecycleStatus.STOPPED, e);
                }
            }
        }

        @Override
        public void start()
                throws LifecycleException {
            if (currentStatus == LifecycleStatus.NONE) {
                init();
            }
            if (currentStatus == LifecycleStatus.STOPPED) {
                currentStatus = changedStatus(instance, currentStatus, LifecycleStatus.STARTING);
                try {
                    instance.start();
                    currentStatus = changedStatus(instance, currentStatus, LifecycleStatus.STARTED);
                } catch (Throwable e) {
                    currentStatus = changedStatus(instance, currentStatus, LifecycleStatus.STOPPED);
                    try {
                        instance.stop();
                    } catch (Throwable se) {
                        LifecycleException lifecycleException = new LifecycleException("Exception during graceful " +
                                "attempt to stop partially started component. Please use non suppressed" +
                                " exception to see original component failure.", se);
                        e.addSuppressed(lifecycleException);
                    }
                    if (e instanceof LifecycleException) {
                        throw (LifecycleException) e;
                    }
                    throw new LifecycleException(instance, LifecycleStatus.STOPPED, LifecycleStatus.STARTED, e);
                }
            }
        }

        @Override
        public void stop()
                throws LifecycleException {
            if (currentStatus == LifecycleStatus.STARTED) {
                currentStatus = changedStatus(instance, currentStatus, LifecycleStatus.STOPPING);
                try {
                    instance.stop();
                } catch (LifecycleException e) {
                    throw e;
                } catch (Throwable e) {
                    throw new LifecycleException(instance, LifecycleStatus.STARTED, LifecycleStatus.STOPPED, e);
                } finally {
                    currentStatus = changedStatus(instance, currentStatus, LifecycleStatus.STOPPED);
                }
            }
        }

        @Override
        public void shutdown()
                throws LifecycleException {
            if (currentStatus == LifecycleStatus.STARTED) {
                stop();
            }

            if (currentStatus == LifecycleStatus.STOPPED) {
                currentStatus = changedStatus(instance, currentStatus, LifecycleStatus.SHUTTING_DOWN);
                try {
                    instance.shutdown();
                } catch (LifecycleException e) {
                    throw e;
                } catch (Throwable e) {
                    throw new LifecycleException(instance, LifecycleStatus.STOPPED, LifecycleStatus.SHUTTING_DOWN, e);
                } finally {
                    currentStatus = changedStatus(instance, currentStatus, LifecycleStatus.SHUTDOWN);
                }
            }
        }

        @Override
        public String toString() {
            return instance.toString() + ": " + currentStatus.name();
        }

        public boolean isInstance(Lifecycle instance) {
            return this.instance == instance;
        }
    }
}
