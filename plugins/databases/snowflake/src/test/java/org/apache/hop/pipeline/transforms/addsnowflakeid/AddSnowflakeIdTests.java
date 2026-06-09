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
package org.apache.hop.pipeline.transforms.addsnowflakeid;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.plugins.Plugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.pipeline.PipelineTestingUtil;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** AddSnowflakeId test */
class AddSnowflakeIdTests {
  private TransformMockHelper<AddSnowflakeIdMeta, AddSnowflakeIdData> transformMockHelper;

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    Map<Class<?>, String> map = new HashMap<>();
    map.put(HopTwoWayPasswordEncoder.class, HopTwoWayPasswordEncoder.class.getName());
    List<String> libraries = new ArrayList<>();

    Plugin plugin =
        new Plugin(
            new String[] {"Hop"},
            TwoWayPasswordEncoderPluginType.class,
            HopTwoWayPasswordEncoder.class,
            "Encryption",
            "Hop",
            null,
            null,
            false,
            false,
            map,
            libraries,
            null,
            null,
            null,
            false);

    PluginRegistry.getInstance().registerPlugin(TwoWayPasswordEncoderPluginType.class, plugin);

    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    transformMockHelper =
        new TransformMockHelper<>(
            "AddSnowflakeId", AddSnowflakeIdMeta.class, AddSnowflakeIdData.class);
    when(transformMockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(transformMockHelper.iLogChannel);
    when(transformMockHelper.pipeline.isRunning()).thenReturn(true);
    when(transformMockHelper.transformMeta.getName()).thenReturn("AddSnowflakeId");
  }

  @Test
  void processRow() throws Exception {
    when(transformMockHelper.iTransformMeta.getValueName()).thenReturn("id");
    when(transformMockHelper.iTransformMeta.getDataCenterId()).thenReturn(10);
    when(transformMockHelper.iTransformMeta.getMachineId()).thenReturn(20);
    when(transformMockHelper.pipeline.getContainerId())
        .thenReturn("cId-" + System.currentTimeMillis());

    AddSnowflakeId addSnowflakeId =
        new AddSnowflakeId(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            transformMockHelper.iTransformData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);

    // init success.
    boolean result = addSnowflakeId.init();
    assertTrue(result);

    // Set up input row meta
    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaInteger("No"));
    inputRowMeta.addValueMeta(new ValueMetaInteger("Id"));
    addSnowflakeId.setInputRowMeta(inputRowMeta);

    // spy
    addSnowflakeId = spy(addSnowflakeId);
    doReturn(new Object[] {1000L})
        .doReturn(new Object[] {2000L})
        .doReturn(new Object[] {3000L})
        .doReturn(null)
        .when(addSnowflakeId)
        .getRow();

    List<Object[]> execCount = PipelineTestingUtil.execute(addSnowflakeId, 3, false);
    assertEquals(3, execCount.size());
    assertTrue((long) execCount.get(1)[1] > (long) execCount.get(0)[1]);

    IRowMeta outputRowMeta = transformMockHelper.iTransformData.outputRowMeta;
    assertEquals(2, outputRowMeta.size());

    addSnowflakeId.dispose();
  }

  @AfterEach
  void tearDown() {
    transformMockHelper.cleanUp();
  }
}
