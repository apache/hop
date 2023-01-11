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

package org.apache.hop.workflow.actions.columnsexist;

import static org.junit.Assert.assertEquals;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.junit.Test;

/** Unit tests for column exist action. */
public class ActionColumnsExistTest {
 
  @Test
  public void testSerialization() throws Exception {
    HopClientEnvironment.init();
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setName("unit-test-db");
    databaseMeta.setDatabaseType("NONE");
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    provider.getSerializer(DatabaseMeta.class).save(databaseMeta);

    ActionColumnsExist action =
        ActionSerializationTestUtil.testSerialization(
            "/columns-exist-action.xml", ActionColumnsExist.class, provider);

    assertEquals("unit-test-db", action.getDatabaseMeta().getName());
    assertEquals("SCHEMA", action.getSchemaName());
    assertEquals("TABLE", action.getTableName());
    assertEquals(2, action.getColumns().size());
    assertEquals("F1", action.getColumns().get(0).getName());
    assertEquals("F2", action.getColumns().get(1).getName());
  }
}
