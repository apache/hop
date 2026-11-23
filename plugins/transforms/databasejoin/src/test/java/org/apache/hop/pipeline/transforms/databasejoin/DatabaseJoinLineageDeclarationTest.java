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

package org.apache.hop.pipeline.transforms.databasejoin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.lineage.api.IDeclaresLineage;
import org.apache.hop.lineage.model.RelationalIoOperation;
import org.junit.jupiter.api.Test;

/** The query Database join declares, for publishing a pipeline without running it. */
class DatabaseJoinLineageDeclarationTest {

  @Test
  void theQueryIsDeclaredAsARunPreparesIt() {
    IVariables variables = new Variables();
    variables.setVariable("CONNECTION", "warehouse");
    variables.setVariable("TABLE", "public.customers");
    DatabaseJoinMeta meta = new DatabaseJoinMeta();
    meta.setConnection("${CONNECTION}");
    meta.setSql("SELECT name FROM ${TABLE} WHERE id = ?{customer_id}");

    meta.setReplaceVariables(true);
    assertEquals(
        List.of(
            new IDeclaresLineage.DeclaredSql(
                RelationalIoOperation.READ,
                "warehouse",
                "SELECT name FROM public.customers WHERE id = ?")),
        meta.declaredLineage(variables, null),
        "a named parameter is a ? as in the statement a run prepares");

    meta.setReplaceVariables(false);
    assertEquals(
        "SELECT name FROM ${TABLE} WHERE id = ?",
        ((IDeclaresLineage.DeclaredSql) meta.declaredLineage(variables, null).get(0)).sql(),
        "without variable replacement a run sends the query as it is");
  }

  @Test
  void withoutAQueryNothingIsDeclared() {
    DatabaseJoinMeta meta = new DatabaseJoinMeta();
    meta.setConnection("warehouse");
    meta.setSql("");

    assertTrue(meta.declaredLineage(new Variables(), null).isEmpty());
  }
}
