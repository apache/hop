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

package org.apache.hop.pipeline.transforms.streamlookup;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.text.DateFormat;
import java.util.Date;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class StreamLookupTest {

  private TransformMockHelper<StreamLookupMeta, StreamLookupData> mockHelper;

  @BeforeEach
  void setUp() throws Exception {
    ValueMetaPluginType.getInstance().searchPlugins();
    mockHelper =
        new TransformMockHelper<>("lookup", StreamLookupMeta.class, StreamLookupData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.logChannelFactory.create(any())).thenReturn(mockHelper.iLogChannel);
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void timestampDefaultValueDoesNotThrowConversionNotImplemented() throws Exception {
    StreamLookupMeta meta = new StreamLookupMeta();
    StreamLookupMeta.ReturnValue returnValue = new StreamLookupMeta.ReturnValue();
    returnValue.setValue("ts");
    returnValue.setValueName("ts");
    returnValue.setValueDefault(DateFormat.getInstance().format(new Date()));
    returnValue.setValueDefaultType(IValueMeta.TYPE_TIMESTAMP);
    meta.getLookup().getReturnValues().add(returnValue);

    StreamLookupData data = new StreamLookupData();
    data.valueDefault = new String[] {returnValue.getValueDefault()};

    StreamLookup transform =
        new StreamLookup(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);

    Method handleNullIf = StreamLookup.class.getDeclaredMethod("handleNullIf");
    handleNullIf.setAccessible(true);
    assertDoesNotThrow(() -> handleNullIf.invoke(transform));
    assertNotNull(data.nullIf);
    assertNotNull(data.nullIf[0]);
  }
}
