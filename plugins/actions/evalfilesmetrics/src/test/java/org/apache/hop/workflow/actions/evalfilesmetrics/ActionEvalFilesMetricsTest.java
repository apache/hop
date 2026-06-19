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

package org.apache.hop.workflow.actions.evalfilesmetrics;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActionEvalFilesMetricsTest {
  @BeforeEach
  void setUp() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void testSerializationRoundTrip() throws Exception {
    ActionEvalFilesMetrics action =
        ActionSerializationTestUtil.testSerialization(
            "/action-evaluate-file-metrics.xml", ActionEvalFilesMetrics.class);

    assertEquals("wildcard", action.getResultFilenamesWildcard());
    assertEquals("file-result-field", action.getResultFieldFile());
    assertEquals("wildcard-result-field", action.getResultFieldWildcard());
    assertEquals("include-result-field", action.getResultFieldIncludeSubFolders());
    assertEquals("12345", action.getCompareValue());
    assertEquals("1000", action.getMinValue());
    assertEquals("9999", action.getMaxValue());
    assertEquals(
        ActionEvalFilesMetrics.SuccesConditionType.GREATER_EQUAL, action.getSuccessConditionType());
    assertEquals(
        ActionEvalFilesMetrics.SourceFilesType.PREVIOUS_RESULT, action.getSourceFilesType());
    assertEquals(ActionEvalFilesMetrics.EvaluationType.SIZE, action.getEvaluationType());
    assertEquals(ActionEvalFilesMetrics.Scale.BYTES, action.getScale());

    assertEquals(3, action.getSourceFiles().size());
    ActionEvalFilesMetrics.SourceFile f = action.getSourceFiles().getFirst();
    assertEquals("folder1", f.getSourceFileFolder());
    assertEquals("wildcard1", f.getSourceWildcard());
    assertEquals("Y", f.getSourceIncludeSubfolders());

    f = action.getSourceFiles().get(1);
    assertEquals("folder2", f.getSourceFileFolder());
    assertEquals("wildcard2", f.getSourceWildcard());
    assertEquals("N", f.getSourceIncludeSubfolders());

    f = action.getSourceFiles().getLast();
    assertEquals("folder3", f.getSourceFileFolder());
    assertEquals("wildcard3", f.getSourceWildcard());
    assertEquals("Y", f.getSourceIncludeSubfolders());
  }
}
