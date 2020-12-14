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
package com.mware.core.cmdline;

import com.beust.jcommander.Parameters;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.mware.core.exception.BcException;
import com.mware.core.util.ServiceLoaderUtil;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


import static com.mware.ge.util.IterableUtils.toList;

public class Cli extends CommandLineTool {
    @Override
    public int run(String[] args, boolean initFramework) throws Exception {
        List<Class<? extends CommandLineTool>> commandLineToolClasses = toList(findCommandLineToolClasses());

        if (args.length == 0) {
            printHelp(commandLineToolClasses, "Require tools classname");
            return -1;
        }

        String className = args[0];
        String[] remainingOfArgs = Arrays.copyOfRange(args, 1, args.length);

        Class<? extends CommandLineTool> clazz = findToolClass(commandLineToolClasses, className);
        if (clazz == null) {
            return -1;
        }
        Method mainMethod = clazz.getMethod("main", String[].class);
        mainMethod.invoke(null, new Object[]{remainingOfArgs});
        return 0;
    }

    private void printHelp(List<Class<? extends CommandLineTool>> commandLineToolClasses, String message) {
        System.err.println(message);

        Collections.sort(commandLineToolClasses, new Comparator<Class<? extends CommandLineTool>>() {
            @Override
            public int compare(Class<? extends CommandLineTool> o1, Class<? extends CommandLineTool> o2) {
                return o1.getSimpleName().compareTo(o2.getSimpleName());
            }
        });

        int maxLength = Ordering.natural().max(Iterables.transform(commandLineToolClasses, new Function<Class<? extends CommandLineTool>, Integer>() {
            @Override
            public Integer apply(Class<? extends CommandLineTool> input) {
                return input.getSimpleName().length();
            }
        }));

        for (Class<? extends CommandLineTool> commandLineToolClass : commandLineToolClasses) {
            String description = getDescription(commandLineToolClass);
            System.err.println(String.format("  %-" + maxLength + "s  %s", commandLineToolClass.getSimpleName(), description));
        }
    }

    private String getDescription(Class<? extends CommandLineTool> commandLineToolClass) {
        Parameters parameters = commandLineToolClass.getAnnotation(Parameters.class);
        if (parameters == null) {
            return "";
        }
        return parameters.commandDescription();
    }

    @Override
    protected int run() throws Exception {
        throw new BcException("This run should not be called.");
    }

    private Iterable<Class<? extends CommandLineTool>> findCommandLineToolClasses() {
        return ServiceLoaderUtil.loadClasses(CommandLineTool.class, getConfiguration());
    }

    private Class<? extends CommandLineTool> findToolClass(List<Class<? extends CommandLineTool>> commandLineToolClasses, String classname) {
        for (Class<? extends CommandLineTool> commandLineToolClass : commandLineToolClasses) {
            if (commandLineToolClass.getName().equalsIgnoreCase(classname) || commandLineToolClass.getSimpleName().equalsIgnoreCase(classname)) {
                return commandLineToolClass;
            }
        }
        try {
            return Class.forName(classname).asSubclass(CommandLineTool.class);
        } catch (ClassNotFoundException e) {
            printHelp(commandLineToolClasses, "Could not find command line tool: " + classname);
            return null;
        }
    }

    public static void main(String[] args) throws Exception {
        CommandLineTool.main(new Cli(), args, false);
    }
}
