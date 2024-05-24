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

package org.apache.hop.pipeline.transforms.ifnull;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import junit.framework.Assert;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Tests for IfNull transform */
public class IfNullTest {
  TransformMockHelper<IfNullMeta, IfNullData> smh;
  private RestoreHopEngineEnvironment env;

  @BeforeClass
  public static void beforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Before
  public void setUp() {
    env = new RestoreHopEngineEnvironment();
    smh = new TransformMockHelper<>("Field IfNull processor", IfNullMeta.class, IfNullData.class);
    when(smh.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(smh.iLogChannel);
    when(smh.pipeline.isRunning()).thenReturn(true);
  }

  @After
  public void clean()
      throws NoSuchFieldException,
          SecurityException,
          IllegalArgumentException,
          IllegalAccessException {
    smh.cleanUp();
  }

  private IRowSet buildInputRowSet(Object... row) {
    return smh.getMockInputRowSet(new Object[][] {row});
  }

  private IfNullMeta mockProcessRowMeta() throws HopTransformException {
    IfNullMeta processRowMeta = smh.iTransformMeta;
    doReturn(createFields("null-field", "empty-field", "space-field"))
        .when(processRowMeta)
        .getFields();
    doReturn("replace-value").when(processRowMeta).getReplaceAllByValue();
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

  private static List<Field> createFields(String... fieldNames) {
    List<Field> fields = new ArrayList<>();
    for (int i = 0; i < fieldNames.length; i++) {
      Field field = new Field();
      field.setName(fieldNames[i]);
      fields.add(field);
    }
    return fields;
  }

  private RowMeta buildInputRowMeta(IValueMeta... iValueMeta) {
    RowMeta inputRowMeta = new RowMeta();
    for (IValueMeta iValuMetaInterface : iValueMeta) {
      inputRowMeta.addValueMeta(iValuMetaInterface);
    }
    return inputRowMeta;
  }

  @Test
  public void testStringEmptyIsNull() throws HopException {
    System.setProperty(Const.HOP_EMPTY_STRING_DIFFERS_FROM_NULL, "N");
    IfNull transform =
        new IfNull(
            smh.transformMeta,
            mockProcessRowMeta(),
            smh.iTransformData,
            0,
            smh.pipelineMeta,
            smh.pipeline);
    transform.init();
    final RowMeta inputRowMeta =
        buildInputRowMeta( //
            new ValueMetaString("some-field"), //
            new ValueMetaString("null-field"), //
            new ValueMetaString("empty-field"), //
            new ValueMetaString("space-field"), //
            new ValueMetaString("another-field") //
            );
    transform.setInputRowMeta(inputRowMeta);

    final Object[] inputRow = new Object[] {"value1", null, "", "    ", "value5"};
    final Object[] expectedRow =
        new Object[] {"value1", "replace-value", "replace-value", "    ", "value5"};

    transform.addRowSetToInputRowSets(buildInputRowSet(inputRow));
    transform.addRowSetToOutputRowSets(new QueueRowSet());

    boolean hasMoreRows;
    do {
      hasMoreRows = transform.processRow();
    } while (hasMoreRows);

    IRowSet outputRowSet = transform.getOutputRowSets().get(0);

    assertRowSetMatches("", expectedRow, outputRowSet);
  }

  @Test
  public void testStringEmptyIsNotNull() throws HopException {
    System.setProperty(Const.HOP_EMPTY_STRING_DIFFERS_FROM_NULL, "Y");
    IfNull transform =
        new IfNull(
            smh.transformMeta,
            mockProcessRowMeta(),
            smh.iTransformData,
            0,
            smh.pipelineMeta,
            smh.pipeline);
    transform.init();
    final RowMeta inputRowMeta =
        buildInputRowMeta( //
            new ValueMetaString("some-field"), //
            new ValueMetaString("null-field"), //
            new ValueMetaString("empty-field"), //
            new ValueMetaString("space-field"), //
            new ValueMetaString("another-field") //
            );
    transform.setInputRowMeta(inputRowMeta);

    final Object[] inputRow = new Object[] {"value1", null, "", "    ", "value5"};
    final Object[] expectedRow = new Object[] {"value1", "replace-value", "", "    ", "value5"};

    transform.addRowSetToInputRowSets(buildInputRowSet(inputRow));
    transform.addRowSetToOutputRowSets(new QueueRowSet());

    boolean hasMoreRows;
    do {
      hasMoreRows = transform.processRow();
    } while (hasMoreRows);

    IRowSet outputRowSet = transform.getOutputRowSets().get(0);

    assertRowSetMatches("", expectedRow, outputRowSet);
  }

  private void assertRowSetMatches(String msg, Object[] expectedRow, IRowSet outputRowSet) {
    Object[] actualRow = outputRowSet.getRow();
    Assert.assertEquals(
        msg + ". Output row is of an unexpected length",
        expectedRow.length,
        outputRowSet.getRowMeta().size());

    for (int i = 0; i < expectedRow.length; i++) {
      Assert.assertEquals(
          msg + ". Unexpected output value at index " + i, expectedRow[i], actualRow[i]);
    }
  }
}
