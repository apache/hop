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

package org.apache.hop.pipeline.transforms.denormaliser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class DenormaliserAggregationsTest {

  static final String JUNIT = "JUNIT";

  static TransformMockHelper<DenormaliserMeta, DenormaliserData> mockHelper;
  Denormaliser transform;
  DenormaliserData data = new DenormaliserData();
  DenormaliserMeta meta = new DenormaliserMeta();

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    mockHelper =
        new TransformMockHelper<>("Denormaliser", DenormaliserMeta.class, DenormaliserData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterAll
  static void cleanUp() {
    mockHelper.cleanUp();
  }

  @BeforeEach
  void setUp() throws Exception {
    Mockito.when(mockHelper.transformMeta.getTransform()).thenReturn(meta);
    transform =
        new Denormaliser(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
  }

  /**
   * 100+null=100 , null+100=100
   *
   * @throws HopValueException
   */
  @Test
  void testDenormalizeSum100PlusNull() throws HopValueException {
    // prevTargetData
    Long sto = Long.valueOf(100);
    data.targetResult = new Object[] {sto};

    transform.deNormalise(
        testSumPreconditions(DenormaliserTargetField.DenormaliseAggregation.TYPE_AGGR_SUM),
        new Object[] {JUNIT, null});

    assertEquals(sto, data.targetResult[0], "100 + null = 100 ");
  }

  @Test
  void testDenormalizeSumNullPlus100() throws HopValueException {
    // prevTargetData
    Long sto = Long.valueOf(100);
    data.targetResult = new Object[] {null};

    transform.deNormalise(
        testSumPreconditions(DenormaliserTargetField.DenormaliseAggregation.TYPE_AGGR_SUM),
        new Object[] {JUNIT, sto});

    assertEquals(sto, data.targetResult[0], "null + 100 = 100 ");
  }

  /**
   * respect of new variable for null comparison
   *
   * @throws HopValueException
   */
  @Test
  void testDenormalizeMinValueY() throws HopValueException {
    transform.setMinNullIsValued(true);

    Long trinadzat = Long.valueOf(-13);
    data.targetResult = new Object[] {trinadzat};

    transform.deNormalise(
        testSumPreconditions(DenormaliserTargetField.DenormaliseAggregation.TYPE_AGGR_MIN),
        new Object[] {JUNIT, null});

    assertNull(data.targetResult[0], "Null now is new minimal");
  }

  /**
   * respect of new variable for null comparison
   *
   * @throws HopValueException
   */
  @Test
  void testDenormalizeMinValueN() throws HopValueException {
    transform.setVariable(Const.HOP_AGGREGATION_MIN_NULL_IS_VALUED, "N");

    Long sto = Long.valueOf(100);
    data.targetResult = new Object[] {sto};

    transform.deNormalise(
        testSumPreconditions(DenormaliserTargetField.DenormaliseAggregation.TYPE_AGGR_MIN),
        new Object[] {JUNIT, null});

    assertEquals(sto, data.targetResult[0], "Null is ignored");
  }

  /**
   * This is extracted common part for sum tests
   *
   * @return
   */
  IRowMeta testSumPreconditions(DenormaliserTargetField.DenormaliseAggregation agg) {

    // create rmi for one string and 2 integers
    IRowMeta rmi = new RowMeta();
    List<IValueMeta> list = new ArrayList<>();
    list.add(new ValueMetaString("a"));
    list.add(new ValueMetaInteger("b"));
    list.add(new ValueMetaInteger("d"));
    rmi.setValueMetaList(list);

    // denormalizer key field will be String 'Junit'
    data.keyValue = new HashMap<>();
    List<Integer> listInt = new ArrayList<>();
    listInt.add(0);
    data.keyValue.put(JUNIT, listInt);

    // we will calculate sum for second field ( like ["JUNIT", 1] )
    data.fieldNameIndex = new int[] {1};
    data.inputRowMeta = rmi;
    data.outputRowMeta = rmi;
    data.removeNrs = new int[] {-1};

    // we do create internal instance of output field wiht sum aggregation
    DenormaliserTargetField tField = new DenormaliserTargetField();
    tField.setTargetAggregationType(agg);
    ArrayList<DenormaliserTargetField> pivotField = new ArrayList<>();
    pivotField.add(new DenormaliserTargetField(tField));
    meta.setDenormaliserTargetFields(pivotField);

    // return row meta interface to pass into denormalize method
    return rmi;
  }

  /**
   * respect to HOP_AGGREGATION_ALL_NULLS_ARE_ZERO variable
   *
   * @throws HopValueException
   */
  @Test
  void testBuildResultWithNullsY() throws HopValueException {
    transform.setAllNullsAreZero(true);

    Object[] rowData = new Object[10];
    data.targetResult = new Object[1];
    // this removal of input rows?
    IRowMeta rmi =
        testSumPreconditions(DenormaliserTargetField.DenormaliseAggregation.TYPE_AGGR_NONE);
    data.removeNrs = new int[] {0};
    Object[] outputRowData = transform.buildResult(rmi, rowData);

    assertEquals(Long.valueOf(0), outputRowData[2], "Output row: nulls are zeros");
  }

  @Test
  void testBuildResultWithNullsN() throws HopValueException {
    transform.setAllNullsAreZero(false);

    Object[] rowData = new Object[10];
    data.targetResult = new Object[1];
    Object[] outputRowData =
        transform.buildResult(
            testSumPreconditions(DenormaliserTargetField.DenormaliseAggregation.TYPE_AGGR_NONE),
            rowData);

    assertNull(outputRowData[3], "Output row: nulls are nulls");
  }

  /**
   * Method newGroup should not initialize result by default for MIN
   *
   * @throws Exception
   */
  @Test
  void testNewGroup() throws Exception {
    DenormaliserTargetField field1 = new DenormaliserTargetField();
    field1.setTargetAggregationType(DenormaliserTargetField.DenormaliseAggregation.TYPE_AGGR_MIN);

    DenormaliserTargetField field2 = new DenormaliserTargetField();
    field2.setTargetAggregationType(DenormaliserTargetField.DenormaliseAggregation.TYPE_AGGR_MIN);

    DenormaliserTargetField field3 = new DenormaliserTargetField();
    field3.setTargetAggregationType(DenormaliserTargetField.DenormaliseAggregation.TYPE_AGGR_MIN);

    ArrayList<DenormaliserTargetField> pivotField = new ArrayList<>();
    pivotField.add(field1);
    pivotField.add(field2);
    pivotField.add(field3);
    meta.setDenormaliserTargetFields(pivotField);
    data.counters = new long[3];
    data.sum = new Object[3];

    Method newGroupMethod = transform.getClass().getDeclaredMethod("newGroup");
    newGroupMethod.setAccessible(true);
    newGroupMethod.invoke(transform);

    assertEquals(3, data.targetResult.length);

    for (Object result : data.targetResult) {
      assertNull(result);
    }
  }
}
