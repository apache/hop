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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.apache.hop.pipeline.transforms.sortedmerge.SortedMergeMeta.MergeField;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class SortedMergeMetaTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void setUpClass() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void testSerializationRoundTrip() throws Exception {
    SortedMergeMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/sorted-merge.xml", SortedMergeMeta.class);

    assertEquals(2, meta.getMergeFields().size());
    assertEquals("field1", meta.getMergeFields().getFirst().getFieldName());
    assertTrue(meta.getMergeFields().getFirst().isAscending());
    assertEquals("field2", meta.getMergeFields().getLast().getFieldName());
    assertFalse(meta.getMergeFields().getLast().isAscending());
  }

  @Test
  void mergeFieldCopyConstructorCopiesValuesIndependently() {
    MergeField original = new MergeField("key", true);
    MergeField copy = new MergeField(original);

    assertEquals("key", copy.getFieldName());
    assertTrue(copy.isAscending());

    copy.setFieldName("other");
    copy.setAscending(false);
    assertEquals("key", original.getFieldName());
    assertTrue(original.isAscending());
  }

  @Test
  void getSupportedPipelineTypesIsNormalOnly() {
    PipelineType[] types = new SortedMergeMeta().getSupportedPipelineTypes();
    assertEquals(1, types.length);
    assertEquals(PipelineType.Normal, types[0]);
  }

  @Test
  void getFieldsSetsSortedDescendingToMatchRuntime() throws Exception {
    SortedMergeMeta meta = new SortedMergeMeta();
    meta.getMergeFields().add(new MergeField("ascKey", true));
    meta.getMergeFields().add(new MergeField("descKey", false));
    meta.getMergeFields().add(new MergeField("missing", true));

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("ascKey"));
    rowMeta.addValueMeta(new ValueMetaString("descKey"));
    rowMeta.addValueMeta(new ValueMetaString("other"));

    meta.getFields(rowMeta, "Sorted Merge", null, null, new Variables(), null);

    assertFalse(rowMeta.searchValueMeta("ascKey").isSortedDescending());
    assertTrue(rowMeta.searchValueMeta("descKey").isSortedDescending());
    assertFalse(rowMeta.searchValueMeta("other").isSortedDescending());
  }

  @Test
  void checkReportsErrorWhenNoPreviousFields() {
    SortedMergeMeta meta = new SortedMergeMeta();
    List<ICheckResult> remarks = check(meta, null, new String[] {"in"});

    assertEquals(ICheckResult.TYPE_RESULT_ERROR, remarks.get(0).getType());
    assertEquals(ICheckResult.TYPE_RESULT_OK, remarks.get(1).getType());
  }

  @Test
  void checkReportsErrorWhenNoInputStreams() {
    SortedMergeMeta meta = new SortedMergeMeta();
    meta.getMergeFields().add(new MergeField("key", true));
    List<ICheckResult> remarks = check(meta, stringMeta("key"), new String[0]);

    assertTrue(hasType(remarks, ICheckResult.TYPE_RESULT_OK));
    assertTrue(hasType(remarks, ICheckResult.TYPE_RESULT_ERROR));
  }

  @Test
  void checkReportsOkWhenAllSortKeysArePresent() {
    SortedMergeMeta meta = new SortedMergeMeta();
    meta.getMergeFields().add(new MergeField("key", true));
    List<ICheckResult> remarks = check(meta, stringMeta("key"), new String[] {"in"});

    assertEquals(3, remarks.size());
    assertTrue(remarks.stream().allMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_OK));
  }

  @Test
  void checkReportsErrorWhenSortKeyIsMissing() {
    SortedMergeMeta meta = new SortedMergeMeta();
    meta.getMergeFields().add(new MergeField("missing", true));
    List<ICheckResult> remarks = check(meta, stringMeta("key"), new String[] {"in"});

    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && r.getText().contains("missing")));
  }

  @Test
  void checkReportsErrorWhenNoSortKeysEntered() {
    SortedMergeMeta meta = new SortedMergeMeta();
    List<ICheckResult> remarks = check(meta, stringMeta("key"), new String[] {"in"});

    assertTrue(hasType(remarks, ICheckResult.TYPE_RESULT_ERROR));
    assertTrue(hasType(remarks, ICheckResult.TYPE_RESULT_OK));
  }

  private static List<ICheckResult> check(SortedMergeMeta meta, IRowMeta prev, String[] input) {
    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        prev,
        input,
        new String[0],
        null,
        new Variables(),
        null);
    return remarks;
  }

  private static IRowMeta stringMeta(String... names) {
    RowMeta rowMeta = new RowMeta();
    for (String name : names) {
      rowMeta.addValueMeta(new ValueMetaString(name));
    }
    return rowMeta;
  }

  private static boolean hasType(List<ICheckResult> remarks, int type) {
    return remarks.stream().anyMatch(r -> r.getType() == type);
  }
}
