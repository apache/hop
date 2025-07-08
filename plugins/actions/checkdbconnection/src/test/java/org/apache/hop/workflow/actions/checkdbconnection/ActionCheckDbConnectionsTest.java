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
 *
 */

package org.apache.hop.workflow.actions.checkdbconnection;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ActionCheckDbConnectionsTest {

  @Test
  void testSerialization() throws Exception {
    HopClientEnvironment.init();
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setName("unit-test-db");
    databaseMeta.setDatabaseType("NONE");
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    provider.getSerializer(DatabaseMeta.class).save(databaseMeta);

    ActionCheckDbConnections action =
        ActionSerializationTestUtil.testSerialization(
            "/check-db-connections-action.xml", ActionCheckDbConnections.class, provider);

    Assertions.assertEquals(1, action.getConnections().size());
    Assertions.assertNotNull(action.getConnections().get(0).getName());
    Assertions.assertEquals("unit-test-db", action.getConnections().get(0).getName());
    Assertions.assertEquals("500", action.getConnections().get(0).getWaitTime());
    Assertions.assertEquals(
        ActionCheckDbConnections.WaitTimeUnit.MILLISECOND,
        action.getConnections().get(0).getWaitTimeUnit());
  }
}
