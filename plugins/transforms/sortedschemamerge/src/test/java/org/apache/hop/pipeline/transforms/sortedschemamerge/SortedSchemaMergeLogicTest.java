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
 *
 */

package org.apache.hop.pipeline.transforms.sortedschemamerge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class SortedSchemaMergeLogicTest {

  @BeforeAll
  static void initHop() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void buildSchemaMappingUnionsFieldsAndMapsSparseRows() throws HopPluginException {
    IRowMeta streamA = new RowMeta();
    streamA.addValueMeta(new ValueMetaString("hub_hk"));
    streamA.addValueMeta(new ValueMetaString("attr_a"));

    IRowMeta streamB = new RowMeta();
    streamB.addValueMeta(new ValueMetaString("hub_hk"));
    streamB.addValueMeta(new ValueMetaString("attr_b"));

    SortedSchemaMergeLogic.SchemaMapping mapping =
        SortedSchemaMergeLogic.buildSchemaMapping(new IRowMeta[] {streamA, streamB});

    assertEquals(3, mapping.getOutputRowMeta().size());
    assertTrue(mapping.getOutputRowMeta().indexOfValue("hub_hk") >= 0);
    assertTrue(mapping.getOutputRowMeta().indexOfValue("attr_a") >= 0);
    assertTrue(mapping.getOutputRowMeta().indexOfValue("attr_b") >= 0);

    Object[] mappedA = mapping.mapRow(0, new Object[] {"hk-1", "value-a"});
    Object[] mappedB = mapping.mapRow(1, new Object[] {"hk-1", "value-b"});

    assertEquals("hk-1", mappedA[mapping.getOutputRowMeta().indexOfValue("hub_hk")]);
    assertEquals("value-a", mappedA[mapping.getOutputRowMeta().indexOfValue("attr_a")]);
    assertEquals(null, mappedA[mapping.getOutputRowMeta().indexOfValue("attr_b")]);

    assertEquals("hk-1", mappedB[mapping.getOutputRowMeta().indexOfValue("hub_hk")]);
    assertEquals(null, mappedB[mapping.getOutputRowMeta().indexOfValue("attr_a")]);
    assertEquals("value-b", mappedB[mapping.getOutputRowMeta().indexOfValue("attr_b")]);
  }

  @Test
  void buildSchemaMappingCoercesConflictingTypesToString() throws HopPluginException {
    IRowMeta streamA = new RowMeta();
    streamA.addValueMeta(new ValueMetaString("shared"));

    IRowMeta streamB = new RowMeta();
    streamB.addValueMeta(new ValueMetaInteger("shared"));

    SortedSchemaMergeLogic.SchemaMapping mapping =
        SortedSchemaMergeLogic.buildSchemaMapping(new IRowMeta[] {streamA, streamB});

    assertEquals(
        ValueMetaString.TYPE_STRING,
        mapping
            .getOutputRowMeta()
            .getValueMeta(mapping.getOutputRowMeta().indexOfValue("shared"))
            .getType());
  }

  @Test
  void resolveSortKeyIndicesRequiresFieldOnEveryStream() {
    IRowMeta streamA = row("hub_hk", "load_dts");
    IRowMeta streamB = row("hub_hk");

    List<SortedSchemaMergeSortKey> sortKeys =
        SortedSchemaMergeMetaFactory.sortKeys("hub_hk", "load_dts");

    assertThrows(
        HopTransformException.class,
        () ->
            SortedSchemaMergeLogic.resolveSortKeyIndices(
                new IRowMeta[] {streamA, streamB}, sortKeys));
  }

  @Test
  void compareRowsUsesSortKeysAndStreamIndexTieBreak() throws Exception {
    IRowMeta streamA = row("hub_hk", "load_dts");
    IRowMeta streamB = row("hub_hk", "load_dts");

    List<SortedSchemaMergeSortKey> sortKeys =
        SortedSchemaMergeMetaFactory.sortKeys("hub_hk", "load_dts");
    int[][] sortKeyIndices =
        SortedSchemaMergeLogic.resolveSortKeyIndices(new IRowMeta[] {streamA, streamB}, sortKeys);

    SortedSchemaMergeRow left =
        new SortedSchemaMergeRow(0, null, streamA, new Object[] {"hk-a", "2024-01-01"});
    SortedSchemaMergeRow right =
        new SortedSchemaMergeRow(1, null, streamB, new Object[] {"hk-b", "2024-01-02"});

    assertTrue(SortedSchemaMergeLogic.compareRows(left, right, sortKeyIndices, sortKeys) < 0);

    SortedSchemaMergeRow tieLeft =
        new SortedSchemaMergeRow(0, null, streamA, new Object[] {"hk-same", "2024-01-01"});
    SortedSchemaMergeRow tieRight =
        new SortedSchemaMergeRow(1, null, streamB, new Object[] {"hk-same", "2024-01-01"});

    assertTrue(SortedSchemaMergeLogic.compareRows(tieLeft, tieRight, sortKeyIndices, sortKeys) < 0);
  }

  @Test
  void compareRowsHonorsDescendingSortKey() throws Exception {
    IRowMeta layout = row("seq");

    List<SortedSchemaMergeSortKey> sortKeys = List.of(new SortedSchemaMergeSortKey("seq", false));
    int[][] sortKeyIndices =
        SortedSchemaMergeLogic.resolveSortKeyIndices(new IRowMeta[] {layout}, sortKeys);

    SortedSchemaMergeRow low = new SortedSchemaMergeRow(0, null, layout, new Object[] {1L});
    SortedSchemaMergeRow high = new SortedSchemaMergeRow(0, null, layout, new Object[] {2L});

    assertTrue(SortedSchemaMergeLogic.compareRows(high, low, sortKeyIndices, sortKeys) < 0);
  }

  private static IRowMeta row(String... names) {
    IRowMeta rowMeta = new RowMeta();
    for (String name : names) {
      if ("seq".equals(name)) {
        rowMeta.addValueMeta(new ValueMetaInteger(name));
      } else {
        rowMeta.addValueMeta(new ValueMetaString(name));
      }
    }
    return rowMeta;
  }
}
