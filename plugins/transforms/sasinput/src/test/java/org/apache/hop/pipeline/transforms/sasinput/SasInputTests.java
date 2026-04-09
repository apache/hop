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

package org.apache.hop.pipeline.transforms.sasinput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.epam.parso.SasFileProperties;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

/** Unit test for {@link SasInput} */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class SasInputTests {

  private SasInput input;

  @BeforeEach
  void setup() {
    TransformMockHelper<SasInputMeta, SasInputData> helper =
        new TransformMockHelper<>("sas_input_test", SasInputMeta.class, SasInputData.class);
    Mockito.doReturn(helper.iLogChannel)
        .when(helper.logChannelFactory)
        .create(any(), any(ILoggingObject.class));

    when(helper.pipeline.isRunning()).thenReturn(true);

    SasInputMeta meta = helper.iTransformMeta;
    SasInputData data = helper.iTransformData;
    PipelineMeta plMeta = helper.pipelineMeta;
    Pipeline pipeline = helper.pipeline;
    input = new SasInput(helper.transformMeta, meta, data, 0, plMeta, pipeline);
  }

  @Test
  void testByteArray_withEncoding() {
    byte[] bytes = "Bob".getBytes(StandardCharsets.UTF_8);
    SasInput.ConvertedValue result = input.convertSasValue(bytes, "name", property("UTF-8"));

    assertNotNull(result);
    assertInstanceOf(ValueMetaString.class, result.meta());
    assertEquals("Bob", result.value());
  }

  @Test
  void testByteArray_defaultEncoding() {
    byte[] bytes = "world".getBytes(StandardCharsets.UTF_8);

    SasInput.ConvertedValue result = input.convertSasValue(bytes, "field1", property(null));
    assertNotNull(result);
    assertEquals("world", result.value());
  }

  @Test
  void testString() {
    SasInput.ConvertedValue result = input.convertSasValue("abc", "field1", property(null));

    assertInstanceOf(ValueMetaString.class, result.meta());
    assertEquals("abc", result.value());
  }

  @Test
  void testDouble() {
    SasInput.ConvertedValue result = input.convertSasValue(10.5d, "field1", property(null));

    assertInstanceOf(ValueMetaNumber.class, result.meta());
    assertEquals(10.5d, result.value());
  }

  @Test
  void testFloat() {
    SasInput.ConvertedValue result = input.convertSasValue(3.14f, "field1", property(null));

    assertInstanceOf(ValueMetaNumber.class, result.meta());
    assertEquals(3.14f, result.value());
  }

  @Test
  void testLong() {
    SasInput.ConvertedValue result = input.convertSasValue(100L, "field1", property(null));

    assertInstanceOf(ValueMetaInteger.class, result.meta());
    assertEquals(100L, result.value());
  }

  @Test
  void testInteger() {
    SasInput.ConvertedValue result = input.convertSasValue(42, "field1", property(null));

    assertInstanceOf(ValueMetaInteger.class, result.meta());
    assertEquals(42L, result.value());
  }

  @Test
  void testDate() {
    Date now = new Date();
    SasInput.ConvertedValue result = input.convertSasValue(now, "field1", property(null));

    assertInstanceOf(ValueMetaDate.class, result.meta());
    assertEquals(now, result.value());
  }

  @Test
  void testNull() {
    SasInput.ConvertedValue result = input.convertSasValue(null, "field1", property(null));

    assertNull(result);
  }

  @Test
  void testDefaultUnknownType() {
    SasInput.ConvertedValue result = input.convertSasValue(new Object(), "field1", property(null));

    assertNull(result);
  }

  private SasFileProperties property(String encoding) {
    SasFileProperties p = new SasFileProperties();
    p.setEncoding(encoding);
    return p;
  }
}
