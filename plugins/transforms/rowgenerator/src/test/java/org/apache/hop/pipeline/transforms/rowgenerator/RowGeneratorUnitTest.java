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

package org.apache.hop.pipeline.transforms.rowgenerator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class RowGeneratorUnitTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<RowGeneratorMeta, RowGeneratorData> transformMockHelper;

  @BeforeEach
  void setup() {
    transformMockHelper =
        new TransformMockHelper(
            "RowGenerator TEST", RowGeneratorMeta.class, RowGeneratorData.class);
    when(transformMockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(transformMockHelper.iLogChannel);
    when(transformMockHelper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    transformMockHelper.cleanUp();
  }

  @BeforeAll
  static void initEnvironment() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void testReadRowLimitAsPipelineVar() throws HopException {
    RowGenerator rowGenerator =
        new RowGenerator(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            transformMockHelper.iTransformData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);

    // add variable to pipeline variable variables
    Variables variables = new Variables();
    variables.setVariable("ROW_LIMIT", "1440");
    transformMockHelper.pipeline.setVariables(variables);

    when(transformMockHelper.iTransformMeta.getRowLimit()).thenReturn("${ROW_LIMIT}");

    rowGenerator.initializeFrom(transformMockHelper.pipeline);
    rowGenerator.init();

    long rowLimit = rowGenerator.getData().rowLimit;
    assertEquals(1440, rowLimit);
  }
}
