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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DenormaliserTest {

  private TransformMockHelper<DenormaliserMeta, DenormaliserData> mockHelper;

  @BeforeEach
  void init() {
    mockHelper =
        new TransformMockHelper<>("Denormalizer", DenormaliserMeta.class, DenormaliserData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
  }

  @AfterEach
  void cleanUp() {
    mockHelper.cleanUp();
  }

  @Test
  void testDeNormalise() throws Exception {

    // init transform data
    DenormaliserData data = new DenormaliserData();
    data.keyFieldNr = 0;
    data.keyValue = new HashMap<>();
    data.keyValue.put("1", Arrays.asList(new Integer[] {0, 1}));
    data.fieldNameIndex = new int[] {1, 2};
    data.inputRowMeta = new RowMeta();
    ValueMetaDate outDateField1 = new ValueMetaDate("date_field[yyyy-MM-dd]");
    ValueMetaDate outDateField2 = new ValueMetaDate("date_field[yyyy/MM/dd]");
    data.outputRowMeta = new RowMeta();
    data.outputRowMeta.addValueMeta(0, outDateField1);
    data.outputRowMeta.addValueMeta(1, outDateField2);
    data.removeNrs = new int[] {};
    data.targetResult = new Object[] {null, null};

    // init transform meta
    DenormaliserMeta meta = new DenormaliserMeta();
    ArrayList<DenormaliserTargetField> denormaliserTargetFields = new ArrayList<>();
    DenormaliserTargetField targetField1 = new DenormaliserTargetField();
    DenormaliserTargetField targetField2 = new DenormaliserTargetField();
    targetField1.setTargetFormat("yyyy-MM-dd");
    targetField2.setTargetFormat("yyyy/MM/dd");
    denormaliserTargetFields.add(targetField1);
    denormaliserTargetFields.add(targetField2);
    meta.setDenormaliserTargetFields(denormaliserTargetFields);

    // init row meta
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(0, new ValueMetaInteger("key"));
    rowMeta.addValueMeta(1, new ValueMetaString("stringDate1"));
    rowMeta.addValueMeta(2, new ValueMetaString("stringDate2"));

    // init row data
    Object[] rowData = new Object[] {1L, "2000-10-20", "2000/10/20"};

    // init transform
    Denormaliser denormaliser =
        new Denormaliser(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);

    // call tested method
    Method deNormalise =
        denormaliser.getClass().getDeclaredMethod("deNormalise", IRowMeta.class, Object[].class);
    assertNotNull(deNormalise, "Can't find a method 'deNormalise' in class Denormalizer");
    deNormalise.setAccessible(true);
    deNormalise.invoke(denormaliser, rowMeta, rowData);

    // vefiry
    for (Object res : data.targetResult) {
      assertNotNull(res, "Date is null");
    }
  }

  @Test
  void averageOfBigNumbersDoesNotFailOnANonTerminatingQuotient() throws Exception {
    // No exact decimal result: rounded like the Group by average, to the larger precision of the
    // sum and the count
    assertEquals(new BigDecimal("0.3"), bigNumberAverage(BigDecimal.ONE, 3));
    assertEquals(new BigDecimal("33.333"), bigNumberAverage(new BigDecimal("100.00"), 3));
  }

  @Test
  void averageOfBigNumbersStaysExactWhenThereIsAnExactResult() throws Exception {
    // These averages were exact before the fix and must not change
    assertEquals(new BigDecimal("1.5"), bigNumberAverage(new BigDecimal("3"), 2));
    assertEquals(new BigDecimal("3.75"), bigNumberAverage(new BigDecimal("15"), 4));
    assertEquals(new BigDecimal("12.375"), bigNumberAverage(new BigDecimal("99"), 8));
  }

  private Object bigNumberAverage(BigDecimal sum, long count) throws Exception {
    DenormaliserData data = new DenormaliserData();
    data.outputRowMeta = new RowMeta();
    data.outputRowMeta.addValueMeta(new ValueMetaBigNumber("avg"));
    data.removeNrs = new int[] {};
    data.targetResult = new Object[] {null};
    data.counters = new long[] {count};
    data.sum = new Object[] {sum};

    DenormaliserMeta meta = new DenormaliserMeta();
    DenormaliserTargetField average = new DenormaliserTargetField();
    average.setTargetAggregationType(
        DenormaliserTargetField.DenormaliseAggregation.TYPE_AGGR_AVERAGE);
    ArrayList<DenormaliserTargetField> fields = new ArrayList<>();
    fields.add(average);
    meta.setDenormaliserTargetFields(fields);

    Denormaliser denormaliser =
        new Denormaliser(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);

    return denormaliser.buildResult(new RowMeta(), new Object[] {})[0];
  }
}
