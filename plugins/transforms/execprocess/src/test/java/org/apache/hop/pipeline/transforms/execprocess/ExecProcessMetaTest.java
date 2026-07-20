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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Unit test for {@link ExecProcessMeta} */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class ExecProcessMetaTest {

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
    assertTrue(meta.isArgumentsInFields());
    assertEquals(2, meta.getArgumentFields().size());
    assertEquals("value1", meta.getArgumentFields().get(0).getName());
    assertEquals("value2", meta.getArgumentFields().get(1).getName());
  }

  @Test
  void testSetDefault() {
    ExecProcessMeta meta = new ExecProcessMeta();
    meta.setDefault();

    assertEquals("Result output", meta.getResultFieldName());
    assertEquals("Error output", meta.getErrorFieldName());
    assertEquals("Exit value", meta.getExitValueFieldName());
    assertFalse(meta.isFailWhenNotSuccess());
    assertEquals("", meta.getOutputLineDelimiter());
    assertNotNull(meta.getArgumentFields());
    assertTrue(meta.getArgumentFields().isEmpty());
  }

  @Test
  void testGettersAndSetters() {
    ExecProcessMeta meta = new ExecProcessMeta();

    meta.setProcessField("cmd");
    assertEquals("cmd", meta.getProcessField());

    meta.setResultFieldName("out");
    assertEquals("out", meta.getResultFieldName());

    meta.setErrorFieldName("err");
    assertEquals("err", meta.getErrorFieldName());

    meta.setExitValueFieldName("exit");
    assertEquals("exit", meta.getExitValueFieldName());

    meta.setFailWhenNotSuccess(true);
    assertTrue(meta.isFailWhenNotSuccess());

    meta.setOutputLineDelimiter("|");
    assertEquals("|", meta.getOutputLineDelimiter());

    meta.setArgumentsInFields(true);
    assertTrue(meta.isArgumentsInFields());

    List<ExecProcessMeta.EPField> fields = new ArrayList<>();
    ExecProcessMeta.EPField field = new ExecProcessMeta.EPField();
    field.setName("arg0");
    fields.add(field);
    meta.setArgumentFields(fields);
    assertEquals(1, meta.getArgumentFields().size());
    assertEquals("arg0", meta.getArgumentFields().get(0).getName());
  }

  @Test
  void testEpFieldGettersAndSetters() {
    ExecProcessMeta.EPField field = new ExecProcessMeta.EPField();
    field.setName("argument");
    assertEquals("argument", field.getName());

    ExecProcessMeta.EPField copy = new ExecProcessMeta.EPField(field);
    assertNotSame(field, copy);
    assertEquals(field.getName(), copy.getName());
  }

  @Test
  void testCloneCopiesArgumentFieldsAndFlags() {
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
    assertNotSame(a.getArgumentFields(), b.getArgumentFields());
    assertNotSame(a.getArgumentFields().get(0), b.getArgumentFields().get(0));
    assertEquals(a.getProcessField(), b.getProcessField());
    assertTrue(b.isArgumentsInFields());
    assertEquals(1, b.getArgumentFields().size());
    assertEquals("arg0", b.getArgumentFields().get(0).getName());
    assertEquals("|", b.getOutputLineDelimiter());
  }

  @Test
  void testCopyConstructor() {
    ExecProcessMeta meta = new ExecProcessMeta();
    meta.setProcessField("p");
    meta.setResultFieldName("out");
    meta.setErrorFieldName("err");
    meta.setExitValueFieldName("exit");
    meta.setFailWhenNotSuccess(true);
    meta.setOutputLineDelimiter(";");
    meta.setArgumentsInFields(true);
    ExecProcessMeta.EPField f = new ExecProcessMeta.EPField();
    f.setName("arg1");
    meta.getArgumentFields().add(f);

    ExecProcessMeta copy = new ExecProcessMeta(meta);

    assertEquals(meta.getProcessField(), copy.getProcessField());
    assertEquals(meta.getResultFieldName(), copy.getResultFieldName());
    assertEquals(meta.getErrorFieldName(), copy.getErrorFieldName());
    assertEquals(meta.getExitValueFieldName(), copy.getExitValueFieldName());
    assertEquals(meta.isFailWhenNotSuccess(), copy.isFailWhenNotSuccess());
    assertEquals(meta.getOutputLineDelimiter(), copy.getOutputLineDelimiter());
    assertEquals(meta.isArgumentsInFields(), copy.isArgumentsInFields());
    assertEquals(1, copy.getArgumentFields().size());
    assertEquals("arg1", copy.getArgumentFields().get(0).getName());
    assertNotSame(meta.getArgumentFields().get(0), copy.getArgumentFields().get(0));
  }

  @Test
  void testGetFieldsAppendsResultColumns() {
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
  void testGetFieldsSkipsEmptyFieldNames() {
    ExecProcessMeta meta = new ExecProcessMeta();
    meta.setResultFieldName("");
    meta.setErrorFieldName("");
    meta.setExitValueFieldName("");

    IRowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("in1"));

    meta.getFields(
        row,
        "ExecProcess",
        null,
        new TransformMeta(),
        new Variables(),
        (IHopMetadataProvider) null);

    assertEquals(1, row.size());
  }

  @Test
  void testSupportsErrorHandlingMatchesFailWhenNotSuccess() {
    ExecProcessMeta meta = new ExecProcessMeta();
    meta.setFailWhenNotSuccess(false);
    assertFalse(meta.supportsErrorHandling());
    meta.setFailWhenNotSuccess(true);
    assertTrue(meta.supportsErrorHandling());
  }

  @Test
  void checkWithoutResultAndProcessFieldsReportsErrors() {
    ExecProcessMeta meta = new ExecProcessMeta();
    List<ICheckResult> remarks = new ArrayList<>();

    meta.check(
        remarks,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        new RowMeta(),
        new String[0],
        new String[0],
        mock(IRowMeta.class),
        null,
        mock(IHopMetadataProvider.class));

    assertTrue(
        remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR),
        "Expected error when result/process fields are missing and no input is connected");
  }

  @Test
  void checkWithValidFieldsAndInputReportsOk() {
    ExecProcessMeta meta = new ExecProcessMeta();
    meta.setDefault();
    meta.setProcessField("cmd");

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        new RowMeta(),
        new String[] {"in"},
        new String[0],
        mock(IRowMeta.class),
        null,
        mock(IHopMetadataProvider.class));

    assertTrue(
        remarks.stream().allMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_OK),
        "Expected only OK remarks when required fields are set and input is connected");
  }

  @Test
  void checkWithoutInputReportsError() {
    ExecProcessMeta meta = new ExecProcessMeta();
    meta.setDefault();
    meta.setProcessField("cmd");

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        new RowMeta(),
        new String[0],
        new String[0],
        mock(IRowMeta.class),
        null,
        mock(IHopMetadataProvider.class));

    assertTrue(
        remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR),
        "Expected error when no input transforms are connected");
  }
}
