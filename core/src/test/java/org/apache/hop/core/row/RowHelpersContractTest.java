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

package org.apache.hop.core.row;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** The equals/hashCode contracts and the copy semantics of the row helpers. */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class RowHelpersContractTest {

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  private static IRowMeta rowMeta(String... names) {
    IRowMeta rowMeta = new RowMeta();
    for (String name : names) {
      rowMeta.addValueMeta(new ValueMetaString(name));
    }
    return rowMeta;
  }

  @Test
  void rowMetaEqualsIsSymmetricForAPrefix() {
    IRowMeta shorter = rowMeta("a");
    IRowMeta longer = rowMeta("a", "b");

    assertNotEquals(shorter, longer);
    assertNotEquals(longer, shorter);
    assertEquals(rowMeta("a", "b"), longer);
  }

  @Test
  void rowHashIgnoresPadding() throws Exception {
    IRowMeta rowMeta = rowMeta("a", "b");
    Object[] exact = new Object[] {"x", "y"};
    Object[] padded = RowDataUtil.allocateRowData(rowMeta.size());
    padded[0] = "x";
    padded[1] = "y";

    assertEquals(rowMeta.hashCode(exact), rowMeta.hashCode(padded));
    // Exact-size rows keep the value they always had: Checksum and Combination lookup store it.
    assertEquals(Arrays.deepHashCode(exact), rowMeta.hashCode(exact));
  }

  @Test
  void rowMetaBuilderKeepsPrecision() {
    IRowMeta rowMeta = new RowMetaBuilder().addNumber("n", 9, 2).addBigNumber("b", 20, 4).build();

    assertEquals(2, rowMeta.getValueMeta(0).getPrecision());
    assertEquals(4, rowMeta.getValueMeta(1).getPrecision());
  }

  @Test
  void createResizedCopyOfNullIsAnEmptyRow() {
    Object[] row = RowDataUtil.createResizedCopy((Object[]) null, 3);

    assertTrue(row.length >= 3);
    assertTrue(Arrays.stream(row).allMatch(v -> v == null));
  }

  @Test
  void equalRowBuffersHaveEqualHashCodes() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    List<Object[]> rows1 = new ArrayList<>();
    rows1.add(new Object[] {1L});
    List<Object[]> rows2 = new ArrayList<>();
    rows2.add(new Object[] {1L});

    RowBuffer buffer1 = new RowBuffer(rowMeta, rows1);
    RowBuffer buffer2 = new RowBuffer(rowMeta, rows2);

    assertEquals(buffer1, buffer2);
    assertEquals(buffer1.hashCode(), buffer2.hashCode());
  }

  @Test
  void rowMetaAndDataIsNotEqualToNullOrAnotherType() {
    RowMetaAndData row = new RowMetaAndData(rowMeta("a"), "x");

    assertFalse(row.equals(null));
    assertFalse(row.equals("x"));
    assertTrue(row.equals(row));
  }

  @Test
  void equalValueMetasHaveEqualHashCodes() {
    ValueMetaString one = new ValueMetaString("name");
    one.setOrigin("Transform 1");
    one.setComments("first");
    ValueMetaString two = new ValueMetaString("name");
    two.setOrigin("Transform 2");
    two.setComments("second");

    assertEquals(one, two);
    assertEquals(one.hashCode(), two.hashCode());

    // getPrecision() is fixed for these types, so the stored precision must not change the hash.
    ValueMetaInteger plain = new ValueMetaInteger("id");
    ValueMetaInteger withPrecision = new ValueMetaInteger("id");
    withPrecision.setPrecision(0);
    assertEquals(plain, withPrecision);
    assertEquals(plain.hashCode(), withPrecision.hashCode());

    ValueMetaString text = new ValueMetaString("text");
    ValueMetaString textWithPrecision = new ValueMetaString("text");
    textWithPrecision.setPrecision(3);
    assertEquals(text, textWithPrecision);
    assertEquals(text.hashCode(), textWithPrecision.hashCode());
  }
}
