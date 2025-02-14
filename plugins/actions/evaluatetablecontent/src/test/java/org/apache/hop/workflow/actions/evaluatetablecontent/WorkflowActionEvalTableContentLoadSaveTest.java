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

package org.apache.hop.workflow.actions.evaluatetablecontent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.junit.jupiter.api.Test;

public class WorkflowActionEvalTableContentLoadSaveTest {
  @Test
  void testSerialization() throws Exception {
    ActionEvalTableContent meta =
        ActionSerializationTestUtil.testSerialization(
            "/evalueatetablecontent-action.xml", ActionEvalTableContent.class);

    assertEquals("unit-test-db", meta.getConnection());
    assertEquals("public", meta.getSchemaname());
    assertEquals("dimension", meta.getTableName());
    assertEquals("7", meta.getLimit());
    assertFalse(meta.isUseCustomSql());
    assertFalse(meta.isUseVars());
    assertFalse(meta.isAddRowsResult());
    assertTrue(meta.isClearResultList());
  }

  @Test
  void testClone() throws Exception {
    ActionEvalTableContent meta =
        ActionSerializationTestUtil.testSerialization(
            "/evalueatetablecontent-action.xml", ActionEvalTableContent.class);

    ActionEvalTableContent clone = (ActionEvalTableContent) meta.clone();
    assertEquals(clone.getConnection(), meta.getConnection());
    assertEquals(clone.getSchemaname(), meta.getSchemaname());
    assertEquals(clone.getTableName(), meta.getTableName());
    assertEquals(clone.getLimit(), meta.getLimit());
  }
}
