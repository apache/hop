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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class MergeRowsAlignmentTest {

  @BeforeAll
  static void initHop() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void buildSchemaMappingUnionsFieldsAndNullFillsMissingColumns() throws HopTransformException {
    IRowMeta reference = row("hub_hk", "attr_a");
    IRowMeta compare = row("hub_hk", "attr_b");

    MergeRowsAlignment.SchemaMapping mapping =
        MergeRowsAlignment.buildSchemaMapping(reference, compare);

    assertEquals(3, mapping.getOutputRowMeta().size());
    Object[] mappedReference = mapping.mapRow(0, new Object[] {"hk-1", "value-a"});
    Object[] mappedCompare = mapping.mapRow(1, new Object[] {"hk-1", "value-b"});

    assertEquals("hk-1", mappedReference[index(mapping, "hub_hk")]);
    assertEquals("value-a", mappedReference[index(mapping, "attr_a")]);
    assertNull(mappedReference[index(mapping, "attr_b")]);

    assertEquals("hk-1", mappedCompare[index(mapping, "hub_hk")]);
    assertNull(mappedCompare[index(mapping, "attr_a")]);
    assertEquals("value-b", mappedCompare[index(mapping, "attr_b")]);
  }

  @Test
  void buildSchemaMappingKeepsReferenceTypeForSharedFields() {
    IRowMeta reference = new RowMeta();
    reference.addValueMeta(new ValueMetaString("shared"));

    IRowMeta compare = new RowMeta();
    compare.addValueMeta(new ValueMetaInteger("shared"));

    MergeRowsAlignment.SchemaMapping mapping =
        MergeRowsAlignment.buildSchemaMapping(reference, compare);

    IValueMeta sharedField = mapping.getOutputRowMeta().getValueMeta(index(mapping, "shared"));
    assertEquals(IValueMeta.TYPE_STRING, sharedField.getType());
  }

  @Test
  void mapCompareRowConvertsCompatibleValuesToReferenceType() throws HopTransformException {
    IRowMeta reference = new RowMeta();
    reference.addValueMeta(new ValueMetaInteger("amount"));

    IRowMeta compare = new RowMeta();
    compare.addValueMeta(new ValueMetaString("amount"));

    MergeRowsAlignment.SchemaMapping mapping =
        MergeRowsAlignment.buildSchemaMapping(reference, compare);

    Object[] mappedCompare = mapping.mapRow(1, new Object[] {"42"});
    assertEquals(42L, mappedCompare[index(mapping, "amount")]);
  }

  @Test
  void mapCompareRowFailsWithClearErrorForIncompatibleConversion() {
    IRowMeta reference = new RowMeta();
    reference.addValueMeta(new ValueMetaInteger("amount"));

    IRowMeta compare = new RowMeta();
    compare.addValueMeta(new ValueMetaString("amount"));

    MergeRowsAlignment.SchemaMapping mapping =
        MergeRowsAlignment.buildSchemaMapping(reference, compare);

    HopTransformException error =
        assertThrows(
            HopTransformException.class, () -> mapping.mapRow(1, new Object[] {"not-a-number"}));
    assertTrue(error.getMessage().contains("amount"));
    assertTrue(error.getMessage().contains("String"));
    assertTrue(error.getMessage().contains("Integer"));
  }

  @Test
  void compareOnlyFieldKeepsCompareType() {
    IRowMeta reference = row("hub_hk");
    IRowMeta compare = row("hub_hk", "attr_b");

    MergeRowsAlignment.SchemaMapping mapping =
        MergeRowsAlignment.buildSchemaMapping(reference, compare);

    assertEquals(
        IValueMeta.TYPE_STRING,
        mapping.getOutputRowMeta().getValueMeta(index(mapping, "attr_b")).getType());
  }

  private static int index(MergeRowsAlignment.SchemaMapping mapping, String fieldName) {
    return mapping.getOutputRowMeta().indexOfValue(fieldName);
  }

  private static IRowMeta row(String... fieldNames) {
    RowMeta rowMeta = new RowMeta();
    for (String fieldName : fieldNames) {
      rowMeta.addValueMeta(new ValueMetaString(fieldName));
    }
    return rowMeta;
  }
}
