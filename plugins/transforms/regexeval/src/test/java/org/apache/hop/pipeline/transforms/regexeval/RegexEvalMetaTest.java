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

package org.apache.hop.pipeline.transforms.regexeval;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentCaptor;

class RegexEvalMetaTest {
  IRowMeta mockInputRowMeta;
  IVariables mockVariableSpace;

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void setupClass() throws HopException {
    ValueMetaPluginType.getInstance().searchPlugins();
  }

  @BeforeEach
  void setup() throws Exception {
    mockInputRowMeta = mock(IRowMeta.class);
    mockVariableSpace = mock(IVariables.class);
  }

  @Test
  void testGetFieldsReplacesResultFieldIfItExists() throws HopTransformException {
    RegexEvalMeta regexEvalMeta = new RegexEvalMeta();
    String name = "TEST_NAME";
    String resultField = "result";
    regexEvalMeta.setResultFieldName(resultField);
    when(mockInputRowMeta.indexOfValue(resultField)).thenReturn(0);
    IValueMeta mockValueMeta = mock(IValueMeta.class);
    String mockName = "MOCK_NAME";
    when(mockValueMeta.getName()).thenReturn(mockName);
    when(mockInputRowMeta.getValueMeta(0)).thenReturn(mockValueMeta);
    regexEvalMeta.setReplacingFields(true);
    regexEvalMeta.getFields(mockInputRowMeta, name, null, null, mockVariableSpace, null);
    ArgumentCaptor<IValueMeta> captor = ArgumentCaptor.forClass(IValueMeta.class);
    verify(mockInputRowMeta).setValueMeta(eq(0), captor.capture());
    assertEquals(mockName, captor.getValue().getName());
  }

  @Test
  void testGetFieldsAddsResultFieldIfDoesntExist() throws HopTransformException {
    RegexEvalMeta regexEvalMeta = new RegexEvalMeta();
    String name = "TEST_NAME";
    String resultField = "result";
    regexEvalMeta.setResultFieldName(resultField);
    when(mockInputRowMeta.indexOfValue(resultField)).thenReturn(-1);
    IValueMeta mockValueMeta = mock(IValueMeta.class);
    String mockName = "MOCK_NAME";
    when(mockVariableSpace.resolve(resultField)).thenReturn(mockName);
    when(mockInputRowMeta.getValueMeta(0)).thenReturn(mockValueMeta);
    regexEvalMeta.setReplacingFields(true);
    regexEvalMeta.getFields(mockInputRowMeta, name, null, null, mockVariableSpace, null);
    ArgumentCaptor<IValueMeta> captor = ArgumentCaptor.forClass(IValueMeta.class);
    verify(mockInputRowMeta).addValueMeta(captor.capture());
    assertEquals(mockName, captor.getValue().getName());
  }

  @Test
  void testSerializationRoundTrip() throws Exception {
    RegexEvalMeta meta =
        TransformSerializationTestUtil.testSerialization("/regex-eval.xml", RegexEvalMeta.class);

    assertEquals("regular-expression", meta.getScript());
    assertEquals("regex-field", meta.getMatcher());
    assertEquals("result", meta.getResultFieldName());
    assertTrue(meta.isUsingVariables());
    assertTrue(meta.isAllowingCaptureGroups());
    assertTrue(meta.isReplacingFields());
    assertTrue(meta.isCanonicalEqualityEnabled());
    assertTrue(meta.isCaseInsensitive());
    assertTrue(meta.isCommentingEnabled());
    assertTrue(meta.isDotAllEnabled());
    assertTrue(meta.isMultiLine());
    assertTrue(meta.isUnicode());
    assertTrue(meta.isUnixLineEndings());
    assertEquals(3, meta.getRegexFields().size());

    RegexEvalMeta.RegexField f = meta.getRegexFields().getFirst();
    assertEquals("field1", f.getFieldName());
    assertEquals(IValueMeta.TYPE_STRING, f.getFieldType());
    assertEquals(100, f.getFieldLength());
    assertEquals(-1, f.getFieldPrecision());
    assertEquals(IValueMeta.TRIM_TYPE_LEFT, f.getFieldTrimType());

    f = meta.getRegexFields().get(1);
    assertEquals("field2", f.getFieldName());
    assertEquals(IValueMeta.TYPE_INTEGER, f.getFieldType());
    assertEquals(7, f.getFieldLength());
    assertEquals(0, f.getFieldPrecision());
    assertEquals(IValueMeta.TRIM_TYPE_RIGHT, f.getFieldTrimType());

    f = meta.getRegexFields().getLast();
    assertEquals("field3", f.getFieldName());
    assertEquals(IValueMeta.TYPE_NUMBER, f.getFieldType());
    assertEquals(9, f.getFieldLength());
    assertEquals(2, f.getFieldPrecision());
    assertEquals(IValueMeta.TRIM_TYPE_BOTH, f.getFieldTrimType());
  }
}
