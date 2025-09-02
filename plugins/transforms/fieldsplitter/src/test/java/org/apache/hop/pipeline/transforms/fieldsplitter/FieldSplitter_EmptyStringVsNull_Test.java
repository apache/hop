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

package org.apache.hop.pipeline.transforms.fieldsplitter;

import static org.apache.hop.pipeline.transforms.fieldsplitter.FieldSplitterMeta.FSField;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.PipelineTestingUtil;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class FieldSplitter_EmptyStringVsNull_Test {
  private TransformMockHelper<FieldSplitterMeta, ITransformData> helper;

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    helper =
        TransformMockUtil.getTransformMockHelper(
            FieldSplitterMeta.class, "FieldSplitter_EmptyStringVsNull_Test");
  }

  @AfterEach
  void cleanUp() {
    helper.cleanUp();
  }

  @Test
  void emptyAndNullsAreNotDifferent() throws Exception {
    System.setProperty(Const.HOP_EMPTY_STRING_DIFFERS_FROM_NULL, "N");
    List<Object[]> expected =
        Arrays.asList(
            new Object[] {"a", "", "a"}, new Object[] {"b", null, "b"}, new Object[] {null});
    executeAndAssertResults(expected);
  }

  @Test
  void emptyAndNullsAreDifferent() throws Exception {
    System.setProperty(Const.HOP_EMPTY_STRING_DIFFERS_FROM_NULL, "Y");
    List<Object[]> expected =
        Arrays.asList(
            new Object[] {"a", "", "a"}, new Object[] {"b", "", "b"}, new Object[] {"", "", ""});
    executeAndAssertResults(expected);
  }

  private void executeAndAssertResults(List<Object[]> expected) throws Exception {
    FieldSplitterMeta meta = new FieldSplitterMeta();
    meta.setSplitField("string");
    meta.setDelimiter(",");

    FSField f1 = new FSField();
    f1.setName("s1");
    f1.setType("String");
    FSField f2 = new FSField();
    f2.setName("s2");
    f2.setType("String");
    FSField f3 = new FSField();
    f3.setName("s3");
    f3.setType("String");
    meta.getFields().addAll(List.of(f1, f2, f3));

    FieldSplitterData data = new FieldSplitterData();

    FieldSplitter transform = createAndInitTransform(meta, data);

    RowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaString("string"));
    transform.setInputRowMeta(input);

    transform = spy(transform);
    doReturn(new String[] {"a, ,a"})
        .doReturn(new String[] {"b,,b"})
        .doReturn(new String[] {null})
        .when(transform)
        .getRow();

    List<Object[]> actual = PipelineTestingUtil.execute(transform, 3, false);
    PipelineTestingUtil.assertResult(expected, actual);
  }

  private FieldSplitter createAndInitTransform(FieldSplitterMeta meta, FieldSplitterData data)
      throws Exception {
    when(helper.transformMeta.getTransform()).thenReturn(meta);

    FieldSplitter transform =
        new FieldSplitter(
            helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline);
    transform.init();
    return transform;
  }
}
