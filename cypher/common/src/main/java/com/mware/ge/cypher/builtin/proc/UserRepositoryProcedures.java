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
package com.mware.ge.cypher.builtin.proc;

import com.mware.core.model.user.UserRepository;
import com.mware.core.user.User;
import com.mware.ge.collection.RawIterator;
import com.mware.ge.cypher.exception.ProcedureException;
import com.mware.ge.cypher.procedure.impl.Neo4jTypes;
import com.mware.ge.io.ResourceTracker;
import com.mware.ge.cypher.procedure.impl.CallableProcedure;
import com.mware.ge.cypher.procedure.impl.Context;
import com.mware.ge.cypher.procedure.Mode;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import static com.mware.ge.cypher.procedure.impl.ProcedureSignature.procedureName;
import static com.mware.ge.cypher.procedure.impl.ProcedureSignature.procedureSignature;

public interface UserRepositoryProcedures {
    class GetUserProcedure extends CallableProcedure.BasicProcedure {
        private final UserRepository userRepository;

        public GetUserProcedure(UserRepository userRepository) {
            super(procedureSignature(procedureName("user","getUser"))
                    .in("username", Neo4jTypes.NTString)
                    .out("id", Neo4jTypes.NTString)
                    .out("displayName", Neo4jTypes.NTString)
                    .out("emailAddress", Neo4jTypes.NTString)
                    .out("createDate", Neo4jTypes.NTDateTime)
                    .out("currentWorkspaceId", Neo4jTypes.NTString)
                    .out("preferences", Neo4jTypes.NTString)
                    .out("properties", Neo4jTypes.NTMap)
                    .mode(Mode.READ)
                    .description("Get the details of a user")
                    .build());
            this.userRepository = userRepository;
        }

        @Override
        public RawIterator<Object[], ProcedureException> apply(Context ctx, Object[] input, ResourceTracker resourceTracker) throws ProcedureException {
            String username = input[0].toString();
            User user = userRepository.findByUsername(username);
            if (user == null) {
                return RawIterator.empty();
            } else {
                ZonedDateTime createdDate = ZonedDateTime.ofInstant(user.getCreateDate().toInstant(), ZoneId.systemDefault());
                RawIterator result = RawIterator.of((Object) new Object[] {
                        user.getUserId(),
                        user.getDisplayName(),
                        user.getEmailAddress(),
                        createdDate,
                        user.getCurrentWorkspaceId(),
                        user.getUiPreferences().toString(),
                        user.getCustomProperties()
                });
                return result;
            }
        }
    }
}
