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

import java.util.ArrayList;
import java.util.List;

public class AutoDependencyTreeRunner {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(AutoDependencyTreeRunner.class);
    private List<DependencyNode> dependencyNodes = new ArrayList<DependencyNode>();

    public void add(Runnable... newRunnables) {
        for (int i = 0; i < newRunnables.length; i++) {
            if (i == 0) {
                addDependencyNode(newRunnables[i], null);
            } else {
                addDependencyNode(newRunnables[i], newRunnables[i - 1]);
            }
        }
    }

    private void addDependencyNode(Runnable runnable, Runnable dependency) {
        DependencyNode dependencyNode = findOrAddDependencyNode(runnable);
        if (dependency != null) {
            dependencyNode.addDependency(findOrAddDependencyNode(dependency));
        }
    }

    private DependencyNode findOrAddDependencyNode(Runnable runnable) {
        DependencyNode dependencyNode = findDependencyNode(runnable);
        if (dependencyNode == null) {
            dependencyNode = new DependencyNode(runnable);
            dependencyNodes.add(dependencyNode);
        }
        return dependencyNode;
    }

    private DependencyNode findDependencyNode(Runnable runnable) {
        for (DependencyNode dependencyNode : dependencyNodes) {
            if (dependencyNode.equals(runnable)) {
                return dependencyNode;
            }
        }
        return null;
    }

    public void dryRun() {
        run(true);
    }

    public void run() {
        run(false);
    }

    private void run(boolean dryRun) {
        List<DependencyNode> ranNodes = new ArrayList<DependencyNode>();
        for (DependencyNode dependencyNode : dependencyNodes) {
            run(dependencyNode, ranNodes, dryRun);
        }
    }

    private void run(DependencyNode dependencyNode, List<DependencyNode> ranNodes, boolean dryRun) {
        for (DependencyNode dependent : dependencyNode.getDependents()) {
            if (ranNodes.contains(dependent)) {
                continue;
            }

            run(dependent, ranNodes, dryRun);
            ranNodes.add(dependent);
        }
        if (!ranNodes.contains(dependencyNode)) {
            LOGGER.debug("Running " + dependencyNode);
            if (!dryRun) {
                dependencyNode.getRunnable().run();
            }
            ranNodes.add(dependencyNode);
        }
    }

    private static class DependencyNode {
        private final Runnable runnable;
        private final List<DependencyNode> dependents = new ArrayList<DependencyNode>();

        public DependencyNode(Runnable runnable) {
            this.runnable = runnable;
        }

        public void addDependency(DependencyNode dependentNode) {
            dependents.add(dependentNode);
        }

        public List<DependencyNode> getDependents() {
            return dependents;
        }

        public Runnable getRunnable() {
            return runnable;
        }

        @Override
        public String toString() {
            return getRunnable().toString();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Runnable) {
                return obj == this.runnable;
            } else if (obj instanceof DependencyNode) {
                return ((DependencyNode) obj).getRunnable() == this.runnable;
            } else {
                throw new RuntimeException("Not supported");
            }
        }
    }
}
