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

package org.apache.hop.ui.hopgui.perspective.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.variables.IVariables;
import org.junit.jupiter.api.Test;

class DatabaseWorkbenchPreviewSqlTest {

  @Test
  void previewSelectSqlAppendsDialectLimitClause() {
    DatabaseMeta meta = mock(DatabaseMeta.class);
    IVariables variables = mock(IVariables.class);
    when(meta.getQuotedSchemaTableCombination(any(), eq("public"), eq("customer")))
        .thenReturn("\"public\".\"customer\"");
    when(meta.getLimitClause(1000)).thenReturn(" LIMIT 1000");

    assertEquals(
        "SELECT * FROM \"public\".\"customer\" LIMIT 1000",
        DatabaseWorkbench.previewSelectSql(meta, variables, "public", "customer", 1000));
  }

  @Test
  void previewSelectSqlUsesConfiguredRowLimit() {
    DatabaseMeta meta = mock(DatabaseMeta.class);
    IVariables variables = mock(IVariables.class);
    when(meta.getQuotedSchemaTableCombination(any(), eq("public"), eq("customer")))
        .thenReturn("\"public\".\"customer\"");
    when(meta.getLimitClause(2000)).thenReturn(" LIMIT 2000");

    assertEquals(
        "SELECT * FROM \"public\".\"customer\" LIMIT 2000",
        DatabaseWorkbench.previewSelectSql(meta, variables, "public", "customer", 2000));
  }

  @Test
  void previewSelectSqlToleratesNullLimitClause() {
    DatabaseMeta meta = mock(DatabaseMeta.class);
    IVariables variables = mock(IVariables.class);
    when(meta.getQuotedSchemaTableCombination(any(), eq(null), eq("t"))).thenReturn("t");
    when(meta.getLimitClause(1000)).thenReturn(null);

    assertEquals(
        "SELECT * FROM t", DatabaseWorkbench.previewSelectSql(meta, variables, null, "t", 1000));
  }
}
