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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.apache.hop.workflow.actions.simpleeval.ActionSimpleEval.FieldType;
import org.apache.hop.workflow.actions.simpleeval.ActionSimpleEval.SuccessBooleanCondition;
import org.apache.hop.workflow.actions.simpleeval.ActionSimpleEval.SuccessNumberCondition;
import org.apache.hop.workflow.actions.simpleeval.ActionSimpleEval.SuccessStringCondition;
import org.apache.hop.workflow.actions.simpleeval.ActionSimpleEval.ValueType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Unit tests for Simple Eval action. */
class ActionSimpleEvalTest {

  @BeforeAll
  static void setUpBeforeClass() {
    HopLogStore.init();
  }

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

  @Test
  void testResultFilesSerialization() throws Exception {
    ActionSimpleEval action =
        ActionSerializationTestUtil.testSerialization(
            "/simple-eval-result-files-action.xml", ActionSimpleEval.class);

    assertEquals(ValueType.RESULT_FILES, action.getValueType());
    assertEquals(FieldType.NUMBER, action.getFieldType());
    assertEquals(SuccessNumberCondition.EQUAL, action.getSuccessNumberCondition());
    assertEquals("2", action.getCompareValue());
  }

  @Test
  void testLogTextSerialization() throws Exception {
    ActionSimpleEval action =
        ActionSerializationTestUtil.testSerialization(
            "/simple-eval-log-text-action.xml", ActionSimpleEval.class);

    assertEquals(ValueType.LOG_TEXT, action.getValueType());
    assertEquals(FieldType.STRING, action.getFieldType());
    assertEquals(SuccessStringCondition.CONTAINS, action.getSuccessStringCondition());
    assertEquals("Finished pipeline", action.getCompareValue());
  }

  @Test
  void testResultFilesEqualSuccess() throws Exception {
    ActionSimpleEval action = resultFilesAction(SuccessNumberCondition.EQUAL, "2");
    Result result = resultWithFiles(2);

    Result actual = action.execute(result, 0);

    assertTrue(actual.isResult());
    assertEquals(0, actual.getNrErrors());
  }

  @Test
  void testResultFilesEqualFailure() throws Exception {
    ActionSimpleEval action = resultFilesAction(SuccessNumberCondition.EQUAL, "3");
    Result result = resultWithFiles(2);

    Result actual = action.execute(result, 0);

    assertFalse(actual.isResult());
    assertEquals(0, actual.getNrErrors());
  }

  @Test
  void testResultFilesZeroWhenEmpty() throws Exception {
    ActionSimpleEval action = resultFilesAction(SuccessNumberCondition.EQUAL, "0");

    Result actual = action.execute(new Result(), 0);

    assertTrue(actual.isResult());
  }

  @Test
  void testResultFilesGreaterThan() throws Exception {
    ActionSimpleEval action = resultFilesAction(SuccessNumberCondition.GREATER, "1");
    Result result = resultWithFiles(2);

    assertTrue(action.execute(result, 0).isResult());
  }

  @Test
  void testResultFilesBetween() throws Exception {
    ActionSimpleEval action = new ActionSimpleEval("eval");
    action.setValueType(ValueType.RESULT_FILES);
    action.setSuccessNumberCondition(SuccessNumberCondition.BETWEEN);
    action.setMinValue("1");
    action.setMaxValue("3");

    assertTrue(action.execute(resultWithFiles(2), 0).isResult());
    assertFalse(action.execute(resultWithFiles(0), 0).isResult());
    assertFalse(action.execute(resultWithFiles(4), 0).isResult());
  }

  @Test
  void testResultFilesIgnoresConfiguredFieldType() throws Exception {
    ActionSimpleEval action = resultFilesAction(SuccessNumberCondition.EQUAL, "1");
    action.setFieldType(FieldType.STRING);

    assertEquals(FieldType.NUMBER, action.getEvaluationFieldType());
    assertTrue(action.execute(resultWithFiles(1), 0).isResult());
  }

  @Test
  void testLogTextContainsSuccess() throws Exception {
    ActionSimpleEval action = logTextAction(SuccessStringCondition.CONTAINS, "Abort this workflow");
    Result result = resultWithLog("Starting\nAbort this workflow\nERROR\n");

    Result actual = action.execute(result, 0);

    assertTrue(actual.isResult());
    assertEquals(0, actual.getNrErrors());
  }

  @Test
  void testLogTextContainsFailure() throws Exception {
    ActionSimpleEval action = logTextAction(SuccessStringCondition.CONTAINS, "missing snippet");
    Result result = resultWithLog("Starting\nAbort this workflow\nERROR\n");

    assertFalse(action.execute(result, 0).isResult());
  }

  @Test
  void testLogTextNotContainsSuccess() throws Exception {
    ActionSimpleEval action =
        logTextAction(SuccessStringCondition.NOT_CONTAINS, "Success: we should not see this");
    Result result = resultWithLog("Starting\nAbort this workflow\nERROR\n");

    assertTrue(action.execute(result, 0).isResult());
  }

  @Test
  void testLogTextNotContainsFailure() throws Exception {
    ActionSimpleEval action = logTextAction(SuccessStringCondition.NOT_CONTAINS, "ERROR");
    Result result = resultWithLog("Starting\nAbort this workflow\nERROR\n");

    assertFalse(action.execute(result, 0).isResult());
  }

  @Test
  void testLogTextNullTreatedAsEmpty() throws Exception {
    ActionSimpleEval action = logTextAction(SuccessStringCondition.NOT_CONTAINS, "ERROR");

    assertTrue(action.execute(new Result(), 0).isResult());
  }

  @Test
  void testLogTextRegex() throws Exception {
    ActionSimpleEval action = logTextAction(SuccessStringCondition.REGEX, "(?s).*ERROR.*");
    Result result = resultWithLog("line 1\nERROR: boom\nline 3");

    assertTrue(action.execute(result, 0).isResult());
  }

  @Test
  void testLogTextIgnoresConfiguredFieldType() throws Exception {
    ActionSimpleEval action = logTextAction(SuccessStringCondition.CONTAINS, "ok");
    action.setFieldType(FieldType.NUMBER);

    assertEquals(FieldType.STRING, action.getEvaluationFieldType());
    assertTrue(action.execute(resultWithLog("this is ok"), 0).isResult());
  }

  private static ActionSimpleEval resultFilesAction(
      SuccessNumberCondition condition, String compareValue) {
    ActionSimpleEval action = new ActionSimpleEval("eval");
    action.setValueType(ValueType.RESULT_FILES);
    action.setSuccessNumberCondition(condition);
    action.setCompareValue(compareValue);
    return action;
  }

  private static ActionSimpleEval logTextAction(
      SuccessStringCondition condition, String compareValue) {
    ActionSimpleEval action = new ActionSimpleEval("eval");
    action.setValueType(ValueType.LOG_TEXT);
    action.setSuccessStringCondition(condition);
    action.setCompareValue(compareValue);
    return action;
  }

  private static Result resultWithFiles(int count) {
    Result result = new Result();
    for (int i = 0; i < count; i++) {
      result.getResultFiles().put("file-" + i, mock(ResultFile.class));
    }
    return result;
  }

  private static Result resultWithLog(String logText) {
    Result result = new Result();
    result.setLogText(logText);
    return result;
  }
}
