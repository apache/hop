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

package org.apache.hop.pipeline.transforms.execprocess;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class ExecProcessMetaTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @Test
  void testSerialization() throws Exception {
    ExecProcessMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/execute-process-transform.xml", ExecProcessMeta.class);

    assertEquals("script", meta.getProcessField());
    assertEquals("output", meta.getResultFieldName());
    assertEquals("error", meta.getErrorFieldName());
    assertEquals("exit", meta.getExitValueFieldName());
    assertTrue(meta.isFailWhenNotSuccess());
    assertEquals(2, meta.getArgumentFields().size());
    assertEquals("value1", meta.getArgumentFields().get(0).getName());
    assertEquals("value2", meta.getArgumentFields().get(1).getName());
  }

  @Test
  void testCloneCopiesArgumentFieldsAndFlags() throws Exception {
    HopEnvironment.init();

    ExecProcessMeta a = new ExecProcessMeta();
    a.setDefault();
    a.setProcessField("p");
    a.setArgumentsInFields(true);
    ExecProcessMeta.EPField f = new ExecProcessMeta.EPField();
    f.setName("arg0");
    a.getArgumentFields().add(f);
    a.setOutputLineDelimiter("|");

    ExecProcessMeta b = a.clone();
    assertNotSame(a, b);
    assertEquals(a.getProcessField(), b.getProcessField());
    assertTrue(b.isArgumentsInFields());
    assertEquals(1, b.getArgumentFields().size());
    assertEquals("arg0", b.getArgumentFields().get(0).getName());
    assertEquals("|", b.getOutputLineDelimiter());
  }

  @Test
  void testGetFieldsAppendsResultColumns() throws Exception {
    HopEnvironment.init();

    ExecProcessMeta meta = new ExecProcessMeta();
    meta.setDefault();
    meta.setResultFieldName("out");
    meta.setErrorFieldName("err");
    meta.setExitValueFieldName("exit");

    IRowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("in1"));

    meta.getFields(
        row,
        "ExecProcess",
        null,
        new TransformMeta(),
        new Variables(),
        (IHopMetadataProvider) null);

    assertEquals(4, row.size());
    assertEquals("in1", row.getValueMeta(0).getName());
    assertEquals("out", row.getValueMeta(1).getName());
    assertEquals(ValueMetaString.TYPE_STRING, row.getValueMeta(1).getType());
    assertEquals("err", row.getValueMeta(2).getName());
    assertEquals("exit", row.getValueMeta(3).getName());
    assertEquals(ValueMetaInteger.TYPE_INTEGER, row.getValueMeta(3).getType());
  }

  @Test
  void testSupportsErrorHandlingMatchesFailWhenNotSuccess() throws Exception {
    HopEnvironment.init();

    ExecProcessMeta meta = new ExecProcessMeta();
    meta.setFailWhenNotSuccess(false);
    assertFalse(meta.supportsErrorHandling());
    meta.setFailWhenNotSuccess(true);
    assertTrue(meta.supportsErrorHandling());
  }
}
