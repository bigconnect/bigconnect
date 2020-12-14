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
package com.mware.ge.cypher.builtin.proc.spatial;

import com.mware.ge.cypher.exception.ProcedureException;
import com.mware.ge.cypher.procedure.impl.Neo4jTypes;
import com.mware.ge.cypher.procedure.impl.QualifiedName;
import com.mware.ge.cypher.procedure.impl.UserFunctionSignature;
import com.mware.ge.cypher.exception.Status;
import com.mware.ge.cypher.procedure.impl.Context;
import com.mware.ge.values.AnyValue;
import com.mware.ge.values.storable.GeoPointValue;
import com.mware.ge.values.storable.NoValue;
import com.mware.ge.values.storable.TextValue;
import com.mware.ge.values.storable.Values;

import java.util.Arrays;

import static java.util.Collections.singletonList;
import static com.mware.ge.cypher.procedure.impl.FieldSignature.inputField;

public class ToGeoPoint extends GeoFunction<GeoPointValue> {
    @Override
    public UserFunctionSignature signature() {
        return new UserFunctionSignature(
                new QualifiedName(new String[0], "toGeoPoint"),
                singletonList(inputField("input", Neo4jTypes.NTString)),
                Neo4jTypes.NTGeometry,
                null,
                new String[0],
                "Convert a string value 'lat,lon' to GeoPoint",
                true
        );
    }

    @Override
    public AnyValue apply(Context ctx, AnyValue[] input) throws ProcedureException {
        if (input != null && input.length == 1) {
            if (input[0] instanceof TextValue) {
                TextValue latLonPair = (TextValue) input[0];
                return GeoPointValue.of(latLonPair.stringValue());
            } else if (input[0] instanceof NoValue) {
                return Values.NO_VALUE;
            }
        }

        throw new ProcedureException(Status.Procedure.ProcedureCallFailed, "Invalid call signature for toGeoPoint: Provided input was " + Arrays.toString(input));
    }
}
