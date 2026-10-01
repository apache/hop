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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.row.IRowMeta;
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

/** Unit test for {@link FieldSplitterMeta} */
class FieldSplitterKeepSplitFieldTest {
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
            FieldSplitterMeta.class, "FieldSplitterKeepSplitFieldTest");
  }

  @AfterEach
  void cleanUp() {
    helper.cleanUp();
  }

  @Test
  void getFieldsKeepsOriginalAndAppendsNewFields() throws Exception {
    FieldSplitterMeta meta = createMeta(true);

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("id"));
    rowMeta.addValueMeta(new ValueMetaString("movies"));
    rowMeta.addValueMeta(new ValueMetaString("character"));

    meta.getFields(rowMeta, "splitter", null, null, null, null);

    assertEquals(5, rowMeta.size());
    assertEquals("id", rowMeta.getValueMeta(0).getName());
    assertEquals("movies", rowMeta.getValueMeta(1).getName());
    assertEquals("character", rowMeta.getValueMeta(2).getName());
    assertEquals("movie1", rowMeta.getValueMeta(3).getName());
    assertEquals("movie2", rowMeta.getValueMeta(4).getName());
  }

  @Test
  void getFieldsReplacesInPlaceByDefault() throws Exception {
    FieldSplitterMeta meta = createMeta(false);

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("id"));
    rowMeta.addValueMeta(new ValueMetaString("movies"));
    rowMeta.addValueMeta(new ValueMetaString("character"));

    meta.getFields(rowMeta, "splitter", null, null, null, null);

    assertEquals(4, rowMeta.size());
    assertEquals("id", rowMeta.getValueMeta(0).getName());
    assertEquals("movie1", rowMeta.getValueMeta(1).getName());
    assertEquals("movie2", rowMeta.getValueMeta(2).getName());
    assertEquals("character", rowMeta.getValueMeta(3).getName());
  }

  @Test
  void processRowKeepsOriginalAndAppendsValues() throws Exception {
    FieldSplitterMeta meta = createMeta(true);
    FieldSplitterData data = new FieldSplitterData();
    FieldSplitter transform = createAndInitTransform(meta, data);

    RowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaString("id"));
    input.addValueMeta(new ValueMetaString("movies"));
    input.addValueMeta(new ValueMetaString("character"));
    transform.setInputRowMeta(input);

    transform = spy(transform);
    doReturn(new Object[] {"3", "A,B", "Captain America"}).when(transform).getRow();

    List<Object[]> actual = PipelineTestingUtil.execute(transform, 1, false);
    PipelineTestingUtil.assertResult(
        List.<Object[]>of(new Object[] {"3", "A,B", "Captain America", "A", "B"}), actual);
  }

  private static FieldSplitterMeta createMeta(boolean keepSplitField) {
    FieldSplitterMeta meta = new FieldSplitterMeta();
    meta.setSplitField("movies");
    meta.setDelimiter(",");
    meta.setKeepSplitField(keepSplitField);

    FSField f1 = new FSField();
    f1.setName("movie1");
    f1.setType("String");
    FSField f2 = new FSField();
    f2.setName("movie2");
    f2.setType("String");
    meta.getFields().addAll(List.of(f1, f2));
    return meta;
  }

  private FieldSplitter createAndInitTransform(FieldSplitterMeta meta, FieldSplitterData data) {
    when(helper.transformMeta.getTransform()).thenReturn(meta);

    FieldSplitter transform =
        new FieldSplitter(
            helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline);
    transform.init();
    return transform;
  }
}
