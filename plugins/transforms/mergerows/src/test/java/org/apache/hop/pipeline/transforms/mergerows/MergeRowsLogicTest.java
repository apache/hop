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

package org.apache.hop.pipeline.transforms.mergerows;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRowException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class MergeRowsLogicTest {

  @BeforeAll
  static void initHop() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void alignedRowsProduceChangedFlagWhenValuesDiffer() throws Exception {
    IRowMeta reference = row("id", "name");
    IRowMeta compare = row("id", "extra");

    MergeRowsAlignment.SchemaMapping mapping =
        MergeRowsAlignment.buildSchemaMapping(reference, compare);
    IRowMeta aligned = mapping.getOutputRowMeta();

    Object[] refRow = mapping.mapRow(0, new Object[] {"1", "Alice"});
    Object[] cmpRow = mapping.mapRow(1, new Object[] {"1", "extra-value"});

    int[] keyNrs = new int[] {aligned.indexOfValue("id")};
    int[] valueNrs = new int[] {aligned.indexOfValue("name")};

    assertEquals(0, aligned.compare(refRow, cmpRow, keyNrs));
    assertEquals(MergeRows.VALUE_CHANGED, flagForRows(aligned, refRow, cmpRow, keyNrs, valueNrs));
  }

  @Test
  void alignedRowsProduceIdenticalFlagWhenValuesMatch() throws Exception {
    IRowMeta reference = row("id", "name", "status");
    IRowMeta compare = row("id", "name");

    MergeRowsAlignment.SchemaMapping mapping =
        MergeRowsAlignment.buildSchemaMapping(reference, compare);
    IRowMeta aligned = mapping.getOutputRowMeta();

    Object[] refRow = mapping.mapRow(0, new Object[] {"1", "Alice", "active"});
    Object[] cmpRow = mapping.mapRow(1, new Object[] {"1", "Alice"});

    int[] keyNrs = new int[] {aligned.indexOfValue("id")};
    int[] valueNrs = new int[] {aligned.indexOfValue("name")};

    assertEquals(MergeRows.VALUE_IDENTICAL, flagForRows(aligned, refRow, cmpRow, keyNrs, valueNrs));
  }

  @Test
  void alignedRowsProduceNewAndDeletedFlagsForKeyOrdering() throws Exception {
    IRowMeta reference = row("id", "name");
    IRowMeta compare = row("id", "name");

    MergeRowsAlignment.SchemaMapping mapping =
        MergeRowsAlignment.buildSchemaMapping(reference, compare);
    IRowMeta aligned = mapping.getOutputRowMeta();
    int[] keyNrs = new int[] {aligned.indexOfValue("id")};
    int[] valueNrs = new int[] {aligned.indexOfValue("name")};

    Object[] refOnly = mapping.mapRow(0, new Object[] {"1", "Alice"});
    Object[] cmpLater = mapping.mapRow(1, new Object[] {"2", "Bob"});

    assertEquals(
        MergeRows.VALUE_DELETED, flagForRows(aligned, refOnly, cmpLater, keyNrs, valueNrs));

    Object[] refLater = mapping.mapRow(0, new Object[] {"2", "Bob"});
    Object[] cmpOnly = mapping.mapRow(1, new Object[] {"1", "Alice"});

    assertEquals(MergeRows.VALUE_NEW, flagForRows(aligned, refLater, cmpOnly, keyNrs, valueNrs));
  }

  @Test
  void checkInputLayoutValidRejectsMismatchedLayouts() {
    IRowMeta reference = row("id", "name");
    IRowMeta compare = row("id", "extra");

    assertThrows(HopRowException.class, () -> MergeRows.checkInputLayoutValid(reference, compare));
  }

  private static String flagForRows(
      IRowMeta aligned, Object[] referenceRow, Object[] compareRow, int[] keyNrs, int[] valueNrs)
      throws HopValueException {
    if (referenceRow == null && compareRow != null) {
      return MergeRows.VALUE_NEW;
    }
    if (referenceRow != null && compareRow == null) {
      return MergeRows.VALUE_DELETED;
    }

    int compare = aligned.compare(referenceRow, compareRow, keyNrs);
    if (compare == 0) {
      if (aligned.compare(referenceRow, compareRow, valueNrs) == 0) {
        return MergeRows.VALUE_IDENTICAL;
      }
      return MergeRows.VALUE_CHANGED;
    }
    return compare < 0 ? MergeRows.VALUE_DELETED : MergeRows.VALUE_NEW;
  }

  private static IRowMeta row(String... fieldNames) {
    RowMeta rowMeta = new RowMeta();
    for (String fieldName : fieldNames) {
      rowMeta.addValueMeta(new ValueMetaString(fieldName));
    }
    return rowMeta;
  }
}
