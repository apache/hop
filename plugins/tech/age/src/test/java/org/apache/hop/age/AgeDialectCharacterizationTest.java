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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;

/** Characterization of the index statements of Apache AGE, recorded before the dialect SPI. */
class AgeDialectCharacterizationTest {

  @Test
  void createNodeIndex() {
    AgeGraphDatabase database = new AgeGraphDatabase();
    database.setGraphName("${GRAPH}");
    Variables variables = new Variables();
    variables.setVariable("GRAPH", "hop_graph");
    assertEquals(
        "CREATE INDEX IF NOT EXISTS \"idx_execution_id\" ON \"hop_graph\".\"Execution\""
            + " (ag_catalog.agtype_access_operator(VARIADIC ARRAY[properties,"
            + " '\"id\"'::ag_catalog.agtype]))",
        database
            .getGraphDialect(variables)
            .getCreateNodeIndexStatement("idx_execution_id", "Execution", List.of("id")));
    assertEquals(
        "CREATE INDEX IF NOT EXISTS \"i\"\"x\" ON \"hop_graph\".\"Exe\"\"cution\""
            + " (ag_catalog.agtype_access_operator(VARIADIC ARRAY[properties,"
            + " '\"na''me\"'::ag_catalog.agtype]),"
            + " ag_catalog.agtype_access_operator(VARIADIC ARRAY[properties,"
            + " '\"ty\\\"pe\"'::ag_catalog.agtype]))",
        database
            .getGraphDialect(variables)
            .getCreateNodeIndexStatement("i\"x", "Exe\"cution", List.of("na'me", "ty\"pe")));
  }
}
