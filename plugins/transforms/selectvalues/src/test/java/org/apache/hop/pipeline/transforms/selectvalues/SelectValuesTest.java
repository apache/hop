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

package org.apache.hop.pipeline.transforms.selectvalues;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopConversionException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class SelectValuesTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private static final String SELECTED_FIELD = "field";

  private final Object[] inputRow = new Object[] {"a string"};

  private SelectValues transform;
  private TransformMockHelper<SelectValuesMeta, SelectValuesData> helper;

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    helper =
        TransformMockUtil.getTransformMockHelper(
            SelectValuesMeta.class, SelectValuesData.class, "SelectValuesTest");
    when(helper.transformMeta.isDoingErrorHandling()).thenReturn(true);
  }

  private void configureTransform(SelectValuesMeta meta, SelectValuesData data) throws Exception {
    transform =
        new SelectValues(helper.transformMeta, meta, data, 1, helper.pipelineMeta, helper.pipeline);
    transform = spy(transform);
    doReturn(inputRow).when(transform).getRow();
    doNothing()
        .when(transform)
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            anyLong(),
            anyString(),
            anyString(),
            anyString());

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString(SELECTED_FIELD));
    transform.setInputRowMeta(inputRowMeta);
  }

  @AfterEach
  void cleanUp() {
    helper.cleanUp();
  }

  @Test
  void testPDI16368() throws Exception {
    SelectValuesHandler transform2 = null;
    Object[] inputRow2 = null;
    RowMeta inputRowMeta = null;
    SelectValuesMeta transformMeta = null;
    SelectValuesData transformData = null;
    IValueMeta iValueMeta = null;

    // First, test current behavior (it's worked this way since 5.x or so)
    //
    transformMeta = new SelectValuesMeta();
    SelectMetadataChange sf =
        new SelectMetadataChange(
            SELECTED_FIELD,
            null,
            "Integer",
            -2,
            -2,
            "Normal",
            null,
            false,
            null,
            null,
            false,
            null,
            null,
            null);
    transformMeta.setSelectOption(new SelectOptions());
    transformMeta.getSelectOption().getMeta().add(sf);

    transformData = new SelectValuesData();
    transformData.select = true;
    transformData.metadata = true;
    transformData.firstselect = true;
    transformData.firstmetadata = true;

    transform2 =
        new SelectValuesHandler(
            helper.transformMeta,
            transformMeta,
            transformData,
            1,
            helper.pipelineMeta,
            helper.pipeline);
    transform2 = spy(transform2);
    inputRow2 = new Object[] {new BigDecimal("589")}; // Starting with a BigDecimal (no places)
    doReturn(inputRow2).when(transform2).getRow();
    doNothing()
        .when(transform2)
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            anyLong(),
            anyString(),
            anyString(),
            anyString());
    inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaBigNumber(SELECTED_FIELD));
    transform2.setInputRowMeta(inputRowMeta);

    transform2.processRow();

    iValueMeta = transform2.rowMeta.getValueMeta(0);
    assertEquals(ValueMetaBase.DEFAULT_BIG_NUMBER_FORMAT_MASK, iValueMeta.getConversionMask());

    // Another test...
    //
    transformMeta = new SelectValuesMeta();
    SelectMetadataChange sf1 =
        new SelectMetadataChange(
            SELECTED_FIELD,
            null,
            "Number",
            -2,
            -2,
            "Normal",
            null,
            false,
            null,
            null,
            false,
            null,
            null,
            null);
    transformMeta.setSelectOption(new SelectOptions());
    transformMeta.getSelectOption().getMeta().add(sf1);

    transformData = new SelectValuesData();
    transformData.select = true;
    transformData.metadata = true;
    transformData.firstselect = true;
    transformData.firstmetadata = true;

    transform2 =
        new SelectValuesHandler(
            helper.transformMeta,
            transformMeta,
            transformData,
            1,
            helper.pipelineMeta,
            helper.pipeline);
    transform2 = spy(transform2);
    doReturn(inputRow2).when(transform2).getRow();
    doNothing()
        .when(transform2)
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            anyLong(),
            anyString(),
            anyString(),
            anyString());

    inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaBigNumber(SELECTED_FIELD));
    transform2.setInputRowMeta(inputRowMeta);

    transform2.processRow();

    iValueMeta = transform2.rowMeta.getValueMeta(0);
    assertEquals(ValueMetaBase.DEFAULT_BIG_NUMBER_FORMAT_MASK, iValueMeta.getConversionMask());

    // Another test
    //
    transformMeta = new SelectValuesMeta();
    SelectMetadataChange sf2 =
        new SelectMetadataChange(
            SELECTED_FIELD,
            null,
            "BigNumber",
            -2,
            -2,
            "Normal",
            null,
            false,
            null,
            null,
            false,
            null,
            null,
            null);
    transformMeta.setSelectOption(new SelectOptions());
    transformMeta.getSelectOption().getMeta().add(sf2);

    transformData = new SelectValuesData();
    transformData.select = true;
    transformData.metadata = true;
    transformData.firstselect = true;
    transformData.firstmetadata = true;

    transform2 =
        new SelectValuesHandler(
            helper.transformMeta,
            transformMeta,
            transformData,
            1,
            helper.pipelineMeta,
            helper.pipeline);
    transform2 = spy(transform2);
    inputRow2 = new Object[] {Long.valueOf("589")}; // Starting with a Long
    doReturn(inputRow2).when(transform2).getRow();
    doNothing()
        .when(transform2)
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            anyLong(),
            anyString(),
            anyString(),
            anyString());

    inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaInteger(SELECTED_FIELD));
    transform2.setInputRowMeta(inputRowMeta);

    transform2.processRow();

    iValueMeta = transform2.rowMeta.getValueMeta(0);
    assertEquals(ValueMetaBase.DEFAULT_INTEGER_FORMAT_MASK, iValueMeta.getConversionMask());
  }

  @Test
  void errorRowSetObtainsFieldName() throws Exception {
    SelectValuesMeta transformMeta = new SelectValuesMeta();
    SelectField field = new SelectField();
    field.setName(SELECTED_FIELD);
    SelectMetadataChange sf3 =
        new SelectMetadataChange(
            SELECTED_FIELD,
            null,
            "Integer",
            -2,
            -2,
            "Normal",
            null,
            false,
            null,
            null,
            false,
            null,
            null,
            null);
    transformMeta.setSelectOption(new SelectOptions());
    transformMeta.getSelectOption().getMeta().add(sf3);
    transformMeta.getSelectOption().getSelectFields().add(field);

    SelectValuesData transformData = new SelectValuesData();
    transformData.select = true;
    transformData.metadata = true;
    transformData.firstselect = true;
    transformData.firstmetadata = true;

    configureTransform(transformMeta, transformData);

    transform.processRow();

    verify(transform)
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            anyLong(),
            anyString(),
            eq(SELECTED_FIELD),
            anyString());

    // additionally ensure conversion error causes HopConversionError
    boolean properException = false;
    try {
      transform.metadataValues(transform.getInputRowMeta(), inputRow);
    } catch (HopConversionException e) {
      properException = true;
    }
    assertTrue(properException);
  }

  public class SelectValuesHandler extends SelectValues {
    private Object[] resultRow;
    private IRowMeta rowMeta;
    private IRowSet rowset;

    public SelectValuesHandler(
        TransformMeta transformMeta,
        SelectValuesMeta meta,
        SelectValuesData data,
        int copyNr,
        PipelineMeta pipelineMeta,
        Pipeline pipeline) {
      super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
    }

    @Override
    public void putRow(IRowMeta rm, Object[] row) throws HopTransformException {
      resultRow = row;
      rowMeta = rm;
    }

    /**
     * Find input row set.
     *
     * @param sourceTransformName the source transform
     * @return the row set
     * @throws HopTransformException the hop transform exception
     */
    @Override
    public IRowSet findInputRowSet(String sourceTransformName) throws HopTransformException {
      return rowset;
    }
  }
}
