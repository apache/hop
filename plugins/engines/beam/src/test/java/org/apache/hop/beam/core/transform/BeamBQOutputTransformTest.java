/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.beam.core.transform;

import org.apache.hop.core.exception.HopException;
import org.junit.Test;

import static org.apache.hop.beam.core.transform.BeamBQOutputTransform.validateBQFieldName;

public class BeamBQOutputTransformTest {

    @Test
    public void testValidateBQFieldName() throws Exception {
        validateBQFieldName("_name1");
        validateBQFieldName("name2");
        validateBQFieldName("name_3");

        needsException(null, "no Exception thrown: BQ field name can not be null");
        needsException("", "no Exception thrown: BQ field name can not be empty");
        needsException("1name", "no Exception thrown: BQ field name starting with a digit");
        needsException(" name", "no Exception thrown: BQ field name starting with a space");
        needsException("_table_name", "no Exception thrown: BQ field name can't start with _table_");
        needsException("_file_name", "no Exception thrown: BQ field name can't start with _file_");
        needsException("_partition_", "no Exception thrown: BQ field name can't start with _partition_");
        needsException("$name", "no Exception thrown: BQ field name only start with letters or underscore");
        needsException("%name", "no Exception thrown: BQ field name only start with letters or underscore");
        needsException("first name", "no Exception thrown: BQ field name only contain letters, digits or underscore");
        needsException("some-name", "no Exception thrown: BQ field name only contain letters, digits or underscore");
        needsException("last*name", "no Exception thrown: BQ field name only contain letters, digits or underscore");
    }

    public void needsException(String fieldName, String exceptionDescription) throws HopException {
        try {
            validateBQFieldName(fieldName);
            throw new HopException(exceptionDescription);
        } catch(Exception e) {
            // OK!
        }
    }
}