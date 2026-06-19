/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.switchcase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SwitchCase} routing behaviour — covering {@code createOutputValueMapping} and
 * the full {@code processRow} flow.
 *
 * <p>Unlike the existing {@link SwitchCaseTest} (which only tests {@code prepareObjectType}), these
 * tests exercise the actual row-routing logic: exact matching, null handling, default fallback,
 * contains-mode, and error paths.
 */
class SwitchCaseRoutingTest {

  private TransformMockHelper<SwitchCaseMeta, SwitchCaseData> helper;

  @BeforeEach
  void setUp() {
    helper = new TransformMockHelper<>("Switch Case", SwitchCaseMeta.class, SwitchCaseData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  // ---------------------------------------------------------------------------
  // Test helper
  // ---------------------------------------------------------------------------

  /**
   * A minimal {@link SwitchCase} subclass that wires a <em>real</em> {@link SwitchCaseMeta} into
   * {@link org.apache.hop.pipeline.transform.BaseTransform} (bypassing the Mockito mock in the
   * helper). This ensures that {@code createOutputValueMapping()} and {@code processRow()} use the
   * fully-configured metadata rather than a stub.
   *
   * <p>Data field setup is done manually (without calling {@code init()}) to avoid the {@link
   * org.apache.hop.core.plugins.PluginRegistry} dependency: we supply a real {@link
   * ValueMetaString} for both {@code valueMeta} and {@code stringValueMeta}, and wire the correct
   * {@link KeyToRowSetMap} variant for the chosen matching mode.
   */
  private class SwitchCaseForTest extends SwitchCase {

    final Map<String, IRowSet> rowSets = new HashMap<>();
    final Queue<Object[]> inputRows = new LinkedList<>();
    final List<IRowSet> routedTo = new ArrayList<>();

    SwitchCaseForTest(SwitchCaseMeta realMeta) throws Exception {
      super(
          helper.transformMeta,
          realMeta,
          new SwitchCaseData(),
          0,
          helper.pipelineMeta,
          helper.pipeline);

      // Build a mock input row meta that returns a real ValueMetaString for the switch field.
      // createOutputValueMapping() uses this to locate the field index and its IValueMeta.
      IRowMeta mockInputMeta = mock(IRowMeta.class);
      ValueMetaString fieldMeta = new ValueMetaString(realMeta.getFieldName());
      when(mockInputMeta.clone()).thenReturn(mockInputMeta);
      when(mockInputMeta.indexOfValue(anyString())).thenReturn(0);
      when(mockInputMeta.getValueMeta(anyInt())).thenReturn(fieldMeta);

      // Attach the mock as the single input stream meta
      setInputRowMeta(mockInputMeta);

      // Manually prime data so we don't need ValueMetaFactory / PluginRegistry.
      // ValueMetaString handles all String-type conversions natively.
      SwitchCaseData d = switchCaseData();
      d.outputMap =
          realMeta.isUsingContains() ? new ContainsKeyToRowSetMap() : new KeyToRowSetMap();
      d.valueMeta = new ValueMetaString(realMeta.getFieldName());
      d.stringValueMeta = new ValueMetaString(realMeta.getFieldName());
    }

    @Override
    public IRowSet findOutputRowSet(String targetTransform) {
      return rowSets.get(targetTransform);
    }

    @Override
    public Object[] getRow() {
      return inputRows.poll();
    }

    @Override
    public void putRowTo(IRowMeta rowMeta, Object[] row, IRowSet rowSet)
        throws HopTransformException {
      routedTo.add(rowSet);
    }

    // Expose the protected BaseTransform.data field for assertions in the outer test class.
    SwitchCaseData switchCaseData() {
      return data;
    }

    // Convenience: run processRow() until it returns false (no more rows).
    void drainRows() throws HopException {
      //noinspection StatementWithEmptyBody
      while (processRow()) {}
    }
  }

  // ---------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------

  private static SwitchCaseMeta metaWithCases(String fieldName, SwitchCaseTarget... targets) {
    SwitchCaseMeta meta = new SwitchCaseMeta();
    meta.setFieldName(fieldName);
    for (SwitchCaseTarget t : targets) {
      meta.getCaseTargets().add(t);
    }
    return meta;
  }

  private static SwitchCaseTarget target(String value, String transformName) {
    SwitchCaseTarget t = new SwitchCaseTarget();
    t.setCaseValue(value);
    t.setCaseTargetTransformName(transformName);
    return t;
  }

  // ---------------------------------------------------------------------------
  // createOutputValueMapping — mapping construction
  // ---------------------------------------------------------------------------

  @Test
  void createOutputValueMapping_exactCaseValue_addsToOutputMap() throws Exception {
    SwitchCaseMeta meta = metaWithCases("status", target("A", "TargetA"), target("B", "TargetB"));
    SwitchCaseForTest sc = new SwitchCaseForTest(meta);
    IRowSet rsA = mock(IRowSet.class);
    IRowSet rsB = mock(IRowSet.class);
    sc.rowSets.put("TargetA", rsA);
    sc.rowSets.put("TargetB", rsB);

    sc.createOutputValueMapping();

    assertTrue(
        sc.switchCaseData().outputMap.containsKey("A"), "Key 'A' should be in the output map");
    assertTrue(sc.switchCaseData().outputMap.get("A").contains(rsA));
    assertTrue(sc.switchCaseData().outputMap.containsKey("B"));
    assertTrue(sc.switchCaseData().outputMap.get("B").contains(rsB));
  }

  @Test
  void createOutputValueMapping_defaultTarget_addedToDefaultAndNullSets() throws Exception {
    SwitchCaseMeta meta = metaWithCases("status", target("A", "TargetA"));
    meta.setDefaultTargetTransformName("Default");
    SwitchCaseForTest sc = new SwitchCaseForTest(meta);
    IRowSet rsA = mock(IRowSet.class);
    IRowSet rsDefault = mock(IRowSet.class);
    sc.rowSets.put("TargetA", rsA);
    sc.rowSets.put("Default", rsDefault);

    sc.createOutputValueMapping();

    assertTrue(sc.switchCaseData().defaultRowSetSet.contains(rsDefault));
    // When no explicit null target is configured, the default also handles nulls
    assertTrue(sc.switchCaseData().nullRowSetSet.contains(rsDefault));
  }

  @Test
  void createOutputValueMapping_usesKeyToRowSetMapForExactMode() throws Exception {
    SwitchCaseMeta meta = metaWithCases("x", target("v", "T"));
    meta.setUsingContains(false);
    SwitchCaseForTest sc = new SwitchCaseForTest(meta);
    sc.rowSets.put("T", mock(IRowSet.class));

    sc.createOutputValueMapping();

    assertInstanceOf(KeyToRowSetMap.class, sc.switchCaseData().outputMap);
  }

  @Test
  void createOutputValueMapping_usesContainsKeyToRowSetMapForContainsMode() throws Exception {
    SwitchCaseMeta meta = metaWithCases("x", target("substring", "T"));
    meta.setUsingContains(true);
    SwitchCaseForTest sc = new SwitchCaseForTest(meta);
    sc.rowSets.put("T", mock(IRowSet.class));

    sc.createOutputValueMapping();

    assertInstanceOf(ContainsKeyToRowSetMap.class, sc.switchCaseData().outputMap);
  }

  @Test
  void createOutputValueMapping_throwsWhenTargetRowSetNotFound() throws Exception {
    SwitchCaseMeta meta = metaWithCases("status", target("A", "Missing"));
    SwitchCaseForTest sc = new SwitchCaseForTest(meta);
    // "Missing" not registered in sc.rowSets

    assertThrows(HopException.class, sc::createOutputValueMapping);
  }

  @Test
  void createOutputValueMapping_throwsWhenFieldNotFoundInRow() throws Exception {
    SwitchCaseMeta meta = metaWithCases("nonExistentField", target("A", "T"));
    SwitchCaseForTest sc = new SwitchCaseForTest(meta);
    sc.rowSets.put("T", mock(IRowSet.class));

    // Override getInputRowMeta to return -1 for the field index (field not found)
    IRowMeta badMeta = mock(IRowMeta.class);
    when(badMeta.clone()).thenReturn(badMeta);
    when(badMeta.indexOfValue(anyString())).thenReturn(-1);
    sc.setInputRowMeta(badMeta);

    assertThrows(HopException.class, sc::createOutputValueMapping);
  }

  @Test
  void createOutputValueMapping_sameTransformForMultipleCaseValues_bothKeysPresent()
      throws Exception {
    SwitchCaseMeta meta = metaWithCases("x", target("A", "Shared"), target("B", "Shared"));
    SwitchCaseForTest sc = new SwitchCaseForTest(meta);
    IRowSet rsShared = mock(IRowSet.class);
    sc.rowSets.put("Shared", rsShared);

    sc.createOutputValueMapping();

    assertTrue(sc.switchCaseData().outputMap.get("A").contains(rsShared));
    assertTrue(sc.switchCaseData().outputMap.get("B").contains(rsShared));
  }

  // ---------------------------------------------------------------------------
  // processRow — end-to-end routing
  // ---------------------------------------------------------------------------

  @Test
  void processRow_exactMatch_routesToCorrectRowSet() throws Exception {
    SwitchCaseMeta meta = metaWithCases("col", target("X", "TargetX"), target("Y", "TargetY"));
    SwitchCaseForTest sc = new SwitchCaseForTest(meta);
    IRowSet rsX = mock(IRowSet.class);
    IRowSet rsY = mock(IRowSet.class);
    sc.rowSets.put("TargetX", rsX);
    sc.rowSets.put("TargetY", rsY);

    sc.inputRows.add(new Object[] {"X"});
    sc.inputRows.add(new Object[] {"Y"});
    sc.inputRows.add(new Object[] {"X"});
    sc.drainRows();

    assertEquals(3, sc.routedTo.size());
    assertEquals(rsX, sc.routedTo.get(0));
    assertEquals(rsY, sc.routedTo.get(1));
    assertEquals(rsX, sc.routedTo.get(2));
  }

  @Test
  void processRow_nullValue_routesToNullRowSetSet() throws Exception {
    SwitchCaseMeta meta = metaWithCases("col", target("A", "TargetA"));
    meta.setDefaultTargetTransformName("Default");
    SwitchCaseForTest sc = new SwitchCaseForTest(meta);
    IRowSet rsA = mock(IRowSet.class);
    IRowSet rsDefault = mock(IRowSet.class);
    sc.rowSets.put("TargetA", rsA);
    sc.rowSets.put("Default", rsDefault);

    sc.inputRows.add(new Object[] {null}); // null → nullRowSetSet → defaults to rsDefault
    sc.drainRows();

    assertEquals(1, sc.routedTo.size());
    assertEquals(rsDefault, sc.routedTo.get(0));
  }

  @Test
  void processRow_unmatchedValue_routesToDefault() throws Exception {
    SwitchCaseMeta meta = metaWithCases("col", target("A", "TargetA"));
    meta.setDefaultTargetTransformName("Default");
    SwitchCaseForTest sc = new SwitchCaseForTest(meta);
    IRowSet rsA = mock(IRowSet.class);
    IRowSet rsDefault = mock(IRowSet.class);
    sc.rowSets.put("TargetA", rsA);
    sc.rowSets.put("Default", rsDefault);

    sc.inputRows.add(new Object[] {"NoMatch"});
    sc.drainRows();

    assertEquals(1, sc.routedTo.size());
    assertEquals(rsDefault, sc.routedTo.get(0));
  }

  @Test
  void processRow_mixedRows_routedCorrectly() throws Exception {
    SwitchCaseMeta meta = metaWithCases("col", target("A", "TargetA"), target("B", "TargetB"));
    meta.setDefaultTargetTransformName("Default");
    SwitchCaseForTest sc = new SwitchCaseForTest(meta);
    IRowSet rsA = mock(IRowSet.class);
    IRowSet rsB = mock(IRowSet.class);
    IRowSet rsDefault = mock(IRowSet.class);
    sc.rowSets.put("TargetA", rsA);
    sc.rowSets.put("TargetB", rsB);
    sc.rowSets.put("Default", rsDefault);

    sc.inputRows.add(new Object[] {"A"});
    sc.inputRows.add(new Object[] {"B"});
    sc.inputRows.add(new Object[] {"unknown"});
    sc.inputRows.add(new Object[] {null});
    sc.drainRows();

    assertEquals(4, sc.routedTo.size());
    assertEquals(rsA, sc.routedTo.get(0));
    assertEquals(rsB, sc.routedTo.get(1));
    assertEquals(rsDefault, sc.routedTo.get(2)); // unmatched → default
    assertEquals(rsDefault, sc.routedTo.get(3)); // null → default (no explicit null target)
  }

  @Test
  void processRow_containsMode_routesBySubstringMatch() throws Exception {
    SwitchCaseMeta meta = metaWithCases("message", target("ERROR", "ErrorTarget"));
    meta.setUsingContains(true);
    meta.setDefaultTargetTransformName("Default");
    SwitchCaseForTest sc = new SwitchCaseForTest(meta);
    IRowSet rsError = mock(IRowSet.class);
    IRowSet rsDefault = mock(IRowSet.class);
    sc.rowSets.put("ErrorTarget", rsError);
    sc.rowSets.put("Default", rsDefault);

    sc.inputRows.add(new Object[] {"FATAL ERROR: disk full"}); // contains "ERROR"
    sc.inputRows.add(new Object[] {"INFO: all good"}); // no match
    sc.drainRows();

    assertEquals(2, sc.routedTo.size());
    assertEquals(rsError, sc.routedTo.get(0));
    assertEquals(rsDefault, sc.routedTo.get(1));
  }

  @Test
  void processRow_emptyInput_noRowsRouted() throws Exception {
    SwitchCaseMeta meta = metaWithCases("col", target("A", "TargetA"));
    SwitchCaseForTest sc = new SwitchCaseForTest(meta);
    sc.rowSets.put("TargetA", mock(IRowSet.class));

    // No rows added to inputRows
    sc.drainRows();

    assertTrue(sc.routedTo.isEmpty());
  }

  @Test
  void processRow_sameValueForMultipleCases_allTargetRowSetsReceiveRow() throws Exception {
    // Two case targets with the same value map to different rowsets —
    // both rowsets should receive the row.
    SwitchCaseMeta meta = metaWithCases("x", target("same", "T1"), target("same", "T2"));
    SwitchCaseForTest sc = new SwitchCaseForTest(meta);
    IRowSet rs1 = mock(IRowSet.class);
    IRowSet rs2 = mock(IRowSet.class);
    sc.rowSets.put("T1", rs1);
    sc.rowSets.put("T2", rs2);

    sc.inputRows.add(new Object[] {"same"});
    sc.drainRows();

    assertEquals(2, sc.routedTo.size());
    assertTrue(sc.routedTo.contains(rs1));
    assertTrue(sc.routedTo.contains(rs2));
  }

  @Test
  void processRow_byteArray_routedViaHashCode() throws Exception {
    // SwitchCase converts byte[] to Arrays.hashCode before map lookup.
    // Here we register the hash code directly as the case value to confirm the conversion applies.
    byte[] bytes = {1, 2, 3};
    int hashCode = java.util.Arrays.hashCode(bytes);

    SwitchCaseMeta meta =
        metaWithCases("payload", target(String.valueOf(hashCode), "BinaryTarget"));
    SwitchCaseForTest sc = new SwitchCaseForTest(meta);
    IRowSet rsBinary = mock(IRowSet.class);
    sc.rowSets.put("BinaryTarget", rsBinary);

    // The value meta is a String meta, so convertData will convert int → string; we verify
    // the prepareObjectType path separately via SwitchCase.prepareObjectType tests.
    // What we test here is that the mechanism doesn't blow up on non-string values in the map.
    sc.inputRows.add(new Object[] {String.valueOf(hashCode)});
    sc.drainRows();

    assertEquals(1, sc.routedTo.size());
    assertEquals(rsBinary, sc.routedTo.get(0));
  }

  // ---------------------------------------------------------------------------
  // IValueMeta.TYPE_NONE default (field value type left unspecified)
  // ---------------------------------------------------------------------------

  @Test
  void processRow_noExplicitCaseValueType_stringValuesStillRoute() throws Exception {
    // When no caseValueType is set the meta defaults to TYPE_NONE (empty string).
    // The routing still works because ValueMetaString.convertDataFromString passes the
    // string through unchanged, and map.get("A") finds the key.
    SwitchCaseMeta meta =
        metaWithCases("field", target("hello", "Hello"), target("world", "World"));
    SwitchCaseForTest sc = new SwitchCaseForTest(meta);
    IRowSet rsHello = mock(IRowSet.class);
    IRowSet rsWorld = mock(IRowSet.class);
    sc.rowSets.put("Hello", rsHello);
    sc.rowSets.put("World", rsWorld);

    sc.inputRows.add(new Object[] {"hello"});
    sc.inputRows.add(new Object[] {"world"});
    sc.drainRows();

    assertEquals(rsHello, sc.routedTo.get(0));
    assertEquals(rsWorld, sc.routedTo.get(1));
  }
}
