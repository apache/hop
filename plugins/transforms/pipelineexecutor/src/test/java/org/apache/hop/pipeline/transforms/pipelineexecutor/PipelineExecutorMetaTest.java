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

package org.apache.hop.pipeline.transforms.pipelineexecutor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Test;

class PipelineExecutorMetaTest {

  @Test
  void testSerialization() throws Exception {
    PipelineExecutorMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/pipeline-executor-transform.xml", PipelineExecutorMeta.class);

    assertEquals("${PROJECT_HOME}/loops/child-loops-log-counter.hpl", meta.getFilename());
    assertEquals("execution results", meta.getExecutionResultTargetTransform());
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
    assertNull(meta.getOutputRowsSourceTransform());
    assertEquals("result file names after execution", meta.getResultFilesTargetTransform());
    assertEquals("FileName", meta.getResultFilesFileNameField());
    assertEquals("copy of input rows", meta.getExecutorsOutputTransform());
    assertEquals(2, meta.getParameters().size());
    assertEquals(2, meta.getResultRows().size());
  }

  @Test
  void testClone() throws Exception {
    PipelineExecutorMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/pipeline-executor-transform.xml", PipelineExecutorMeta.class);

    PipelineExecutorMeta clone = (PipelineExecutorMeta) meta.clone();

    assertEquals(
        meta.getExecutionResultTargetTransform(), clone.getExecutionResultTargetTransform());
    assertEquals(meta.getExecutionTimeField(), clone.getExecutionTimeField());
    assertEquals(meta.getExecutionResultField(), clone.getExecutionResultField());
    assertEquals(meta.getExecutionNrErrorsField(), clone.getExecutionNrErrorsField());
    assertEquals(meta.getExecutionLinesReadField(), clone.getExecutionLinesReadField());
    assertEquals(meta.getExecutionLinesWrittenField(), clone.getExecutionLinesWrittenField());
    assertEquals(meta.getExecutionLinesInputField(), clone.getExecutionLinesInputField());
    assertEquals(meta.getExecutionLinesOutputField(), clone.getExecutionLinesOutputField());
    assertEquals(meta.getExecutionLinesRejectedField(), clone.getExecutionLinesRejectedField());
    assertEquals(meta.getExecutionLinesUpdatedField(), clone.getExecutionLinesUpdatedField());
    assertEquals(meta.getExecutionLinesDeletedField(), clone.getExecutionLinesDeletedField());
    assertEquals(meta.getExecutionFilesRetrievedField(), clone.getExecutionFilesRetrievedField());
    assertEquals(meta.getExecutionExitStatusField(), clone.getExecutionExitStatusField());
    assertEquals(meta.getExecutionLogTextField(), clone.getExecutionLogTextField());
    assertEquals(meta.getExecutionLogChannelIdField(), clone.getExecutionLogChannelIdField());
    assertEquals(meta.getOutputRowsSourceTransform(), clone.getOutputRowsSourceTransform());
    assertEquals(meta.getResultFilesTargetTransform(), clone.getResultFilesTargetTransform());
    assertEquals(meta.getResultFilesFileNameField(), clone.getResultFilesFileNameField());
    assertEquals(meta.getExecutorsOutputTransform(), clone.getExecutorsOutputTransform());
    assertEquals(meta.getParameters().size(), clone.getParameters().size());
    assertEquals(meta.getResultRows().size(), clone.getResultRows().size());
  }
}
