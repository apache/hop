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

package org.apache.hop.age;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphVectorSearchDefinition;
import org.junit.jupiter.api.Test;

/** Apache AGE reads its schema (see AgeIT) and has no vector search. */
class AgeSchemaAndVectorSearchTest {

  @Test
  void testCapabilities() {
    AgeGraphDialect dialect = new AgeGraphDialect("graph");
    assertTrue(dialect.isSupportingSchemaIntrospection());
    assertFalse(dialect.isSupportingVectorSearch());
    assertFalse(dialect.isSupportingRelationshipVectorSearch());
    HopException e =
        assertThrows(
            HopException.class,
            () ->
                dialect.getVectorSearchStatement(
                    new GraphVectorSearchDefinition("index", null, null, 5, List.of(), null)));
    assertTrue(e.getMessage().contains("AGE"), e.getMessage());
  }
}
