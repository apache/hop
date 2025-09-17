/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.workflow.actions.waitforsql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.apache.hop.workflow.actions.waitforsql.ActionWaitForSql.SuccessCondition;
import org.junit.Test;

/** Unit tests for wait for sql action. */
class ActionWaitForSqlTest {

  @Test
  void testSerialization() throws Exception {
    HopClientEnvironment.init();
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setName("unit-test-db");
    databaseMeta.setDatabaseType("NONE");
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    provider.getSerializer(DatabaseMeta.class).save(databaseMeta);

    ActionWaitForSql action =
        ActionSerializationTestUtil.testSerialization(
            "/wait-for-sql-action.xml", ActionWaitForSql.class, provider);

    assertEquals("unit-test-db", action.getConnection());
    assertEquals("SCHEMATEST", action.getSchemaName());
    assertEquals("TABLETEST", action.getTableName());
    assertEquals("select FIELD from TABLE", action.getCustomSql());
    assertEquals(SuccessCondition.ROWS_COUNT_DIFFERENT, action.getSuccessCondition());
    assertEquals("1", action.getRowsCountValue());
    assertEquals("60", action.getCheckCycleTime());
    assertEquals("99", action.getMaximumTimeout());
    assertFalse(action.isAddRowsResult());
    assertTrue(action.isCustomSqlEnabled());
    assertTrue(action.isUseVars());
    assertFalse(action.isSuccessOnTimeout());
    assertTrue(action.isClearResultList());
  }
}
