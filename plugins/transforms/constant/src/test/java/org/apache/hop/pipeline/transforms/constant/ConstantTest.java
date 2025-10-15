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

package org.apache.hop.pipeline.transforms.constant;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;

class ConstantTest {

  private TransformMockHelper<ConstantMeta, ConstantData> mockHelper;
  private RowMetaAndData rowMetaAndData = mock(RowMetaAndData.class);
  private Constant constantSpy;

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void setUpBeforeClass() throws HopPluginException {
    ValueMetaPluginType.getInstance().searchPlugins();
  }

  @BeforeEach
  void setUp() {

    mockHelper = new TransformMockHelper<>("Add Constants", ConstantMeta.class, ConstantData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);

    doReturn(rowMetaAndData).when(mockHelper.iTransformData).getConstants();
    constantSpy =
        Mockito.spy(
            new Constant(
                mockHelper.transformMeta,
                mockHelper.iTransformMeta,
                mockHelper.iTransformData,
                0,
                mockHelper.pipelineMeta,
                mockHelper.pipeline));
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void testProcessRowSuccess() throws Exception {

    doReturn(new Object[1]).when(constantSpy).getRow();
    doReturn(new RowMeta()).when(constantSpy).getInputRowMeta();
    doReturn(new Object[1]).when(rowMetaAndData).getData();

    boolean success = constantSpy.processRow();
    assertTrue(success);
  }

  @Test
  void testProcessRow_fail() throws Exception {

    doReturn(null).when(constantSpy).getRow();
    doReturn(null).when(constantSpy).getInputRowMeta();

    boolean success = constantSpy.processRow();
    assertFalse(success);
  }
}
