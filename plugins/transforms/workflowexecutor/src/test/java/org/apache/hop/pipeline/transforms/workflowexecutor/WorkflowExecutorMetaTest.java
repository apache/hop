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

package org.apache.hop.pipeline.transforms.workflowexecutor;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Test;

class WorkflowExecutorMetaTest {

  @Test
  void testSerialization() throws Exception {
    WorkflowExecutorMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/workflow-executor-transform.xml", WorkflowExecutorMeta.class);

    assertEquals("${PROJECT_HOME}/loops/child-workflow-executor.hwf", meta.getFilename());
    assertEquals("ExecutionTime", meta.getExecutionTimeField());
    assertEquals("ExecutionResult", meta.getExecutionResultField());
    assertEquals("ExecutionNrErrors", meta.getExecutionNrErrorsField());
    assertEquals("ExecutionLinesRead", meta.getExecutionLinesReadField());
    assertEquals("ExecutionLinesWritten", meta.getExecutionLinesWrittenField());
    assertEquals("ExecutionLinesInput", meta.getExecutionLinesInputField());
    assertEquals("ExecutionLinesOutput", meta.getExecutionLinesOutputField());
    assertEquals("ExecutionLinesRejected", meta.getExecutionLinesRejectedField());
    assertEquals("ExecutionLinesUpdated", meta.getExecutionLinesUpdatedField());
    assertEquals("ExecutionLinesDeleted", meta.getExecutionLinesDeletedField());
    assertEquals("ExecutionFilesRetrieved", meta.getExecutionFilesRetrievedField());
    assertEquals("ExecutionExitStatus", meta.getExecutionExitStatusField());
    assertEquals("ExecutionLogText", meta.getExecutionLogTextField());
    assertEquals("ExecutionLogChannelId", meta.getExecutionLogChannelIdField());
    assertEquals("result rows after execution", meta.getResultRowsTargetTransform());
    assertEquals("result file names after execution", meta.getResultFilesTargetTransform());
    assertEquals("FileName", meta.getResultFilesFileNameField());
    assertEquals(2, meta.getResultRowsField().size());
    assertEquals(2, meta.getParameters().size());
  }

  @Test
  void testClone() throws Exception {
    WorkflowExecutorMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/workflow-executor-transform.xml", WorkflowExecutorMeta.class);

    WorkflowExecutorMeta clone = (WorkflowExecutorMeta) meta.clone();

    assertEquals(clone.getFilename(), meta.getFilename());
    assertEquals(clone.getExecutionTimeField(), meta.getExecutionTimeField());
    assertEquals(clone.getExecutionResultField(), meta.getExecutionResultField());
    assertEquals(clone.getExecutionNrErrorsField(), meta.getExecutionNrErrorsField());
    assertEquals(clone.getExecutionLinesReadField(), meta.getExecutionLinesReadField());
    assertEquals(clone.getExecutionLinesWrittenField(), meta.getExecutionLinesWrittenField());
    assertEquals(clone.getExecutionLinesInputField(), meta.getExecutionLinesInputField());
    assertEquals(clone.getExecutionLinesOutputField(), meta.getExecutionLinesOutputField());
    assertEquals(clone.getExecutionLinesRejectedField(), meta.getExecutionLinesRejectedField());
    assertEquals(clone.getExecutionLinesUpdatedField(), meta.getExecutionLinesUpdatedField());
    assertEquals(clone.getExecutionLinesDeletedField(), meta.getExecutionLinesDeletedField());
    assertEquals(clone.getExecutionFilesRetrievedField(), meta.getExecutionFilesRetrievedField());
    assertEquals(clone.getExecutionExitStatusField(), meta.getExecutionExitStatusField());
    assertEquals(clone.getExecutionLogTextField(), meta.getExecutionLogTextField());
    assertEquals(clone.getExecutionLogChannelIdField(), meta.getExecutionLogChannelIdField());
    assertEquals(clone.getResultRowsTargetTransform(), meta.getResultRowsTargetTransform());
    assertEquals(clone.getResultFilesTargetTransform(), meta.getResultFilesTargetTransform());
    assertEquals(clone.getResultFilesFileNameField(), meta.getResultFilesFileNameField());
    assertEquals(clone.getResultRowsField().size(), meta.getResultRowsField().size());
    assertEquals(clone.getParameters().size(), meta.getParameters().size());
  }
}
