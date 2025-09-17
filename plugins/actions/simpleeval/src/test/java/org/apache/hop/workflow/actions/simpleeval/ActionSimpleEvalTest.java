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

package org.apache.hop.workflow.actions.simpleeval;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.apache.hop.workflow.actions.simpleeval.ActionSimpleEval.FieldType;
import org.apache.hop.workflow.actions.simpleeval.ActionSimpleEval.SuccessBooleanCondition;
import org.apache.hop.workflow.actions.simpleeval.ActionSimpleEval.SuccessNumberCondition;
import org.apache.hop.workflow.actions.simpleeval.ActionSimpleEval.SuccessStringCondition;
import org.apache.hop.workflow.actions.simpleeval.ActionSimpleEval.ValueType;
import org.junit.Test;

/** Unit tests for Simple Eval action. */
class ActionSimpleEvalTest {

  @Test
  void testSerialization() throws Exception {
    HopClientEnvironment.init();
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setName("unit-test-db");
    databaseMeta.setDatabaseType("NONE");
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    provider.getSerializer(DatabaseMeta.class).save(databaseMeta);

    ActionSimpleEval action =
        ActionSerializationTestUtil.testSerialization(
            "/simple-eval-action.xml", ActionSimpleEval.class, provider);

    assertEquals("2020", action.getCompareValue());
    assertEquals("YEAR", action.getVariableName());
    assertEquals(ValueType.VARIABLE, action.getValueType());
    assertEquals(FieldType.NUMBER, action.getFieldType());
    assertEquals("FieldTest", action.getFieldName());
    assertEquals(SuccessStringCondition.EQUAL, action.getSuccessStringCondition());
    assertEquals(SuccessNumberCondition.BETWEEN, action.getSuccessNumberCondition());
    assertEquals(SuccessBooleanCondition.FALSE, action.getSuccessBooleanCondition());

    assertEquals("100", action.getMinValue());
    assertEquals("200", action.getMaxValue());

    assertFalse(action.isSuccessWhenVarSet());
  }
}
