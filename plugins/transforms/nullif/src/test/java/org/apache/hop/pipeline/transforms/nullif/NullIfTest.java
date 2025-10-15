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

package org.apache.hop.pipeline.transforms.nullif;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class NullIfTest {
  TransformMockHelper<NullIfMeta, NullIfData> smh;

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeEach
  void setUp() {
    smh = new TransformMockHelper<>("Field NullIf processor", NullIfMeta.class, NullIfData.class);
    when(smh.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(smh.iLogChannel);
    when(smh.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void cleanUp() {
    smh.cleanUp();
  }

  private IRowSet mockInputRowSet() {
    return smh.getMockInputRowSet(new Object[][] {{"value1", "nullable-value", "value3"}});
  }

  private NullIfMeta mockProcessRowMeta() {
    NullIfMeta processRowMeta = smh.iTransformMeta;
    List<NullIfField> fields = createArrayWithOneField("nullable-field", "nullable-value");
    doReturn(fields).when(processRowMeta).getFields();
    doCallRealMethod()
        .when(processRowMeta)
        .getFields(
            any(IRowMeta.class),
            anyString(),
            any(IRowMeta[].class),
            any(TransformMeta.class),
            any(IVariables.class),
            any(IHopMetadataProvider.class));

    return processRowMeta;
  }

  private RowMeta getInputRowMeta() {
    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("some-field"));
    inputRowMeta.addValueMeta(new ValueMetaString("nullable-field"));
    inputRowMeta.addValueMeta(new ValueMetaString("another-field"));

    return inputRowMeta;
  }

  @Test
  void test() throws HopException {
    HopEnvironment.init();

    NullIf transform =
        new NullIf(
            smh.transformMeta,
            mockProcessRowMeta(),
            smh.iTransformData,
            0,
            smh.pipelineMeta,
            smh.pipeline);
    transform.init();
    transform.setInputRowMeta(getInputRowMeta());
    transform.addRowSetToInputRowSets(mockInputRowSet());
    transform.addRowSetToOutputRowSets(new QueueRowSet());

    boolean hasMoreRows;
    do {
      hasMoreRows = transform.processRow();
    } while (hasMoreRows);

    IRowSet outputRowSet = transform.getOutputRowSets().get(0);
    Object[] actualRow = outputRowSet.getRow();
    Object[] expectedRow = new Object[] {"value1", null, "value3"};

    assertEquals(
        expectedRow.length,
        outputRowSet.getRowMeta().size(),
        "Output row is of an unexpected length");

    for (int i = 0; i < expectedRow.length; i++) {
      assertEquals(expectedRow[i], actualRow[i], "Unexpected output value at index " + i);
    }
  }

  private static List<NullIfField> createArrayWithOneField(String name, String value) {
    List<NullIfField> fields = new ArrayList<>();
    NullIfField field = new NullIfField();
    field.setName(name);
    field.setValue(value);
    fields.add(field);
    return fields;
  }

  private RowMeta getInputRowMeta2() {
    RowMeta inputRowMeta = new RowMeta();
    ValueMetaDate vmd1 = new ValueMetaDate("value1");
    vmd1.setConversionMask("yyyyMMdd");
    inputRowMeta.addValueMeta(vmd1);
    ValueMetaDate vmd2 = new ValueMetaDate("value2");
    vmd2.setConversionMask("yyyy/MM/dd HH:mm:ss.SSS");
    inputRowMeta.addValueMeta(vmd2);
    ValueMetaDate vmd3 = new ValueMetaDate("value3");
    vmd3.setConversionMask("yyyyMMdd");
    inputRowMeta.addValueMeta(vmd3);
    ValueMetaDate vmd4 = new ValueMetaDate("value4");
    vmd4.setConversionMask("yyyy/MM/dd HH:mm:ss.SSS");
    inputRowMeta.addValueMeta(vmd4);

    return inputRowMeta;
  }

  private NullIfMeta mockProcessRowMeta2() {
    NullIfMeta processRowMeta = smh.iTransformMeta;
    List<NullIfField> fields = new ArrayList<>();
    fields.add(new NullIfField("value1", "20150606"));
    fields.add(new NullIfField("value2", "2015/06/06 00:00:00.000"));
    fields.add(new NullIfField("value3", "20150606"));
    fields.add(new NullIfField("value4", "2015/06/06 00:00:00.000"));
    doReturn(fields).when(processRowMeta).getFields();
    doCallRealMethod()
        .when(processRowMeta)
        .getFields(
            any(IRowMeta.class),
            anyString(),
            any(IRowMeta[].class),
            any(TransformMeta.class),
            any(IVariables.class),
            any(IHopMetadataProvider.class));

    return processRowMeta;
  }

  @Test
  void testDateWithFormat() throws HopException {
    HopEnvironment.init();

    NullIf transform =
        new NullIf(
            smh.transformMeta,
            mockProcessRowMeta2(),
            smh.iTransformData,
            0,
            smh.pipelineMeta,
            smh.pipeline);
    transform.init();
    transform.setInputRowMeta(getInputRowMeta2());
    Date d1 = null;
    Date d2 = null;
    Date d3 = null;
    Date d4 = null;
    try {
      DateFormat formatter = new SimpleDateFormat("yyyyMMdd");
      d1 = formatter.parse("20150606");
      d3 = formatter.parse("20150607");
      formatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
      d2 = formatter.parse("2015/06/06 00:00:00.000");
      d4 = formatter.parse("2015/07/06 00:00:00.000");
    } catch (ParseException e) {
      e.printStackTrace();
    }
    transform.addRowSetToInputRowSets(smh.getMockInputRowSet(new Object[][] {{d1, d2, d3, d4}}));
    transform.addRowSetToOutputRowSets(new QueueRowSet());
    boolean hasMoreRows;
    do {
      hasMoreRows = transform.processRow();
    } while (hasMoreRows);

    IRowSet outputRowSet = transform.getOutputRowSets().get(0);
    Object[] actualRow = outputRowSet.getRow();
    Object[] expectedRow = new Object[] {null, null, d3, d4};

    assertEquals(
        expectedRow.length,
        outputRowSet.getRowMeta().size(),
        "Output row is of an unexpected length");

    for (int i = 0; i < expectedRow.length; i++) {
      assertEquals(expectedRow[i], actualRow[i], "Unexpected output value at index " + i);
    }
  }
}
