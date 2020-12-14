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
package com.mware.core.ingest.dataworker;

import com.mware.core.model.schema.SchemaRepository;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.mware.core.model.schema.SchemaRepository.PUBLIC;

public class VerifyResults {
    private List<Failure> failures = new ArrayList<>();

    public boolean verifyRequiredPropertyIntent(SchemaRepository schemaRepository, String intentName) {
        String propertyName = schemaRepository.getPropertyNameByIntent(intentName, PUBLIC);
        if (propertyName == null || propertyName.length() == 0) {
            addFailure(new RequiredPropertyIntentFailure(intentName));
            return false;
        }
        return true;
    }

    public boolean verifyRequiredExecutable(String executableName) {
        try {
            final ProcessBuilder procBuilder = new ProcessBuilder("which", executableName);
            Process proc = procBuilder.start();
            if (proc.waitFor() == 0) {
                return true;
            }
        } catch (Exception ex) {
            String path = System.getenv("PATH");
            String[] pathParts = path.split(File.pathSeparator);
            for (String pathPart : pathParts) {
                if (new File(pathPart, executableName).exists()) {
                    return true;
                }
            }
        }

        addFailure(new RequiredExecutableFailure(executableName));
        return false;
    }

    public void addFailure(Failure failure) {
        failures.add(failure);
    }

    public Collection<Failure> getFailures() {
        return failures;
    }

    public String toString() {
        return String.format("VerifyResults: %d failures", getFailures().size());
    }

    public static abstract class Failure {
        public abstract String getMessage();

        public String toString() {
            return getMessage();
        }
    }

    public static class RequiredPropertyIntentFailure extends Failure {
        private final String intentName;

        public RequiredPropertyIntentFailure(String intentName) {
            this.intentName = intentName;
        }

        public String getIntentName() {
            return intentName;
        }

        @Override
        public String getMessage() {
            return String.format("Missing required property intent: %s", getIntentName());
        }
    }

    private class RequiredExecutableFailure extends Failure {
        private final String executableName;

        public RequiredExecutableFailure(String executableName) {
            this.executableName = executableName;
        }

        public String getExecutableName() {
            return executableName;
        }

        @Override
        public String getMessage() {
            return String.format("Missing required executable: %s", getExecutableName());
        }
    }

    public static class GenericFailure extends Failure {
        private final String message;

        public GenericFailure(String message) {
            this.message = message;
        }

        @Override
        public String getMessage() {
            return message;
        }
    }
}
