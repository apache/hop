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

package org.apache.hop.pipeline.transforms.sortedmerge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Unit test for {@link SortedMerge} */
class SortedMergeTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<SortedMergeMeta, SortedMergeData> mockHelper;

  @BeforeAll
  static void setUpClass() throws HopException {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    mockHelper =
        new TransformMockHelper<>("Sorted Merge", SortedMergeMeta.class, SortedMergeData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void processRowMergesTwoSortedStreamsByKey() throws Exception {
    IRowMeta rowMeta = stringKeyMeta();
    SortedMerge transform =
        createTransform(
            rowMeta,
            new Object[][] {{"alpha"}, {"delta"}},
            new Object[][] {{"bravo"}, {"charlie"}});

    List<Object[]> rows = runToCompletion(transform);

    assertEquals(List.of("alpha", "bravo", "charlie", "delta"), keys(rows));
  }

  @Test
  void processRowKeepsEqualKeysWhenCompareSucceeds() throws Exception {
    IRowMeta rowMeta = stringKeyMeta();
    SortedMerge transform =
        createTransform(rowMeta, new Object[][] {{"same"}}, new Object[][] {{"same"}});

    List<Object[]> rows = runToCompletion(transform);

    assertEquals(List.of("same", "same"), keys(rows));
  }

  @Test
  void processRowReturnsFalseWhenAllStreamsAreEmpty() throws Exception {
    SortedMerge transform = createTransform(stringKeyMeta(), new Object[][] {}, new Object[][] {});

    assertFalse(transform.processRow());
    assertTrue(drain(transform.getOutputRowSets().getFirst()).isEmpty());
  }

  @Test
  void processRowFailsWhenSortKeyIsMissing() {
    IRowMeta rowMeta = stringKeyMeta();
    SortedMergeMeta meta = new SortedMergeMeta();
    meta.getMergeFields().add(new SortedMergeMeta.MergeField("missing", true));

    SortedMerge transform = createTransform(meta, rowMeta, new Object[][] {{"left"}});

    HopTransformException thrown = assertThrows(HopTransformException.class, transform::processRow);
    assertTrue(thrown.getMessage().contains("missing"));
  }

  @Test
  void processRowFailsWhenCompareThrowsHopValueException() throws Exception {
    RowMeta rowMeta = spy(new RowMeta());
    rowMeta.addValueMeta(new ValueMetaString("key"));
    doThrow(new HopValueException("compare failed"))
        .when(rowMeta)
        .compare(any(Object[].class), any(Object[].class), any(int[].class));

    SortedMerge transform =
        createTransform(rowMeta, new Object[][] {{"left"}}, new Object[][] {{"right"}});

    HopRuntimeException thrown = assertThrows(HopRuntimeException.class, transform::processRow);
    assertInstanceOf(HopValueException.class, thrown.getCause());
  }

  @Test
  void processRowFailsWhenValueMetaCompareThrows() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(throwingKeyMeta());

    SortedMerge transform =
        createTransform(rowMeta, new Object[][] {{"left"}}, new Object[][] {{"right"}});

    HopRuntimeException thrown = assertThrows(HopRuntimeException.class, transform::processRow);
    assertInstanceOf(HopValueException.class, thrown.getCause());
  }

  private SortedMerge createTransform(IRowMeta rowMeta, Object[][]... streams) {
    SortedMergeMeta meta = new SortedMergeMeta();
    meta.getMergeFields().add(new SortedMergeMeta.MergeField("key", true));
    return createTransform(meta, rowMeta, streams);
  }

  private SortedMerge createTransform(
      SortedMergeMeta meta, IRowMeta rowMeta, Object[][]... streams) {
    SortedMerge transform =
        new SortedMerge(
            mockHelper.transformMeta,
            meta,
            new SortedMergeData(),
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);
    transform.init();
    transform.setInputRowMeta(rowMeta);

    for (Object[][] rows : streams) {
      QueueRowSet input = new QueueRowSet();
      for (Object[] row : rows) {
        input.putRow(rowMeta, row);
      }
      input.setDone();
      transform.addRowSetToInputRowSets(input);
    }
    transform.addRowSetToOutputRowSets(new QueueRowSet());
    return transform;
  }

  private static IRowMeta stringKeyMeta() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("key"));
    return rowMeta;
  }

  private static IValueMeta throwingKeyMeta() {
    return new ValueMetaString("key") {
      @Override
      public int compare(Object data1, Object data2) throws HopValueException {
        throw new HopValueException("compare failed");
      }
    };
  }

  private static List<Object[]> runToCompletion(SortedMerge transform) throws HopException {
    while (transform.processRow()) {
      // drain all merged rows
    }
    return drain(transform.getOutputRowSets().getFirst());
  }

  private static List<Object[]> drain(IRowSet output) {
    List<Object[]> rows = new ArrayList<>();
    Object[] row = output.getRow();
    while (row != null) {
      rows.add(row);
      row = output.getRow();
    }
    return rows;
  }

  private static List<String> keys(List<Object[]> rows) {
    List<String> keys = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      keys.add((String) row[0]);
    }
    return keys;
  }
}
