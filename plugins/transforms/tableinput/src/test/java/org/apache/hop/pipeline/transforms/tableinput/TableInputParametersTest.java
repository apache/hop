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

package org.apache.hop.pipeline.transforms.tableinput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class TableInputParametersTest {
  @BeforeAll
  static void setUpClass() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void concatenateAssemblesPositionalInListFromSingleFieldRows() {
    IRowMeta oneField = new RowMeta();
    oneField.addValueMeta(new ValueMetaInteger("bar"));

    RowMetaAndData assembled =
        TableInputParameters.concatenate(
            oneField, new Object[] {1L}, new Object[] {2L}, new Object[] {3L});

    assertEquals(3, assembled.getRowMeta().size());
    assertEquals("bar", assembled.getRowMeta().getValueMeta(0).getName());
    assertEquals("bar_1", assembled.getRowMeta().getValueMeta(1).getName());
    assertEquals("bar_2", assembled.getRowMeta().getValueMeta(2).getName());
    assertEquals(1L, assembled.getData()[0]);
    assertEquals(2L, assembled.getData()[1]);
    assertEquals(3L, assembled.getData()[2]);
  }

  @Test
  void concatenateMergesRowsFromMultipleSourcesWithTheSameLayout() {
    IRowMeta layout = new RowMeta();
    layout.addValueMeta(new ValueMetaString("id"));

    RowMetaAndData fromFirst = TableInputParameters.concatenate(layout, new Object[] {"a"});
    RowMetaAndData merged = new RowMetaAndData(fromFirst.getRowMeta(), fromFirst.getData());
    Object[] withSecond =
        TableInputParameters.append(
            merged.getRowMeta(), merged.getData(), layout, new Object[] {"b"});

    assertEquals(2, merged.getRowMeta().size());
    assertEquals("a", withSecond[0]);
    assertEquals("b", withSecond[1]);
  }

  @Test
  void appendIgnoresNullRows() {
    IRowMeta meta = new RowMeta();
    meta.addValueMeta(new ValueMetaInteger("bar"));
    Object[] original = new Object[] {9L};

    Object[] result = TableInputParameters.append(meta, original, meta, null);

    assertSame(original, result);
    assertEquals(1, meta.size());
  }
}
