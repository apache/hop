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

package org.apache.hop.pipeline.transforms.transformsmetrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class TransformsMetricsMetaTest {

  @BeforeAll
  static void beforeAll() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void testXmlRoundTrip() throws Exception {
    TransformsMetricsMeta meta = new TransformsMetricsMeta();
    meta.setDefault();
    meta.getMetricTransforms().add(new MetricTransform("A", "0", true));
    meta.getMetricTransforms().add(new MetricTransform("B", "1", false));
    meta.setTransformNameField("Transform name");
    meta.setLinesWrittenField("Lines written");
    meta.setDurationField("");

    String transformXml =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + meta.getXml()
            + XmlHandler.closeTag(TransformMeta.XML_TAG);
    TransformsMetricsMeta loaded = new TransformsMetricsMeta();
    loaded.loadXml(XmlHandler.loadXmlString(transformXml, TransformMeta.XML_TAG), null);

    assertEquals(2, loaded.getMetricTransforms().size());
    assertEquals("A", loaded.getMetricTransforms().get(0).getName());
    assertEquals("0", loaded.getMetricTransforms().get(0).getCopyNr());
    assertTrue(loaded.getMetricTransforms().get(0).isRequired());
    assertEquals("B", loaded.getMetricTransforms().get(1).getName());
    assertFalse(loaded.getMetricTransforms().get(1).isRequired());
    assertEquals("Transform name", loaded.getTransformNameField());
    assertEquals("Lines written", loaded.getLinesWrittenField());
    assertTrue(StringUtils.isBlank(loaded.getDurationField()));
    assertTrue(transformXml.contains("<transforms>"));
    assertTrue(transformXml.contains("<transformnamefield>"));
    assertTrue(transformXml.contains("<transformlineswrittenfield>"));
  }

  @Test
  void testGetFieldsClearsIncomingAndOmitsEmptyNames() throws Exception {
    TransformsMetricsMeta meta = new TransformsMetricsMeta();
    meta.setDefault();
    meta.setTransformIdField("");
    meta.setLinesInputField(null);
    meta.setDurationField("   ");

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("incoming"));
    meta.getFields(rowMeta, "metrics", null, null, new Variables(), null);

    List<String> names = new ArrayList<>();
    for (int i = 0; i < rowMeta.size(); i++) {
      names.add(rowMeta.getValueMeta(i).getName());
    }
    assertFalse(names.contains("incoming"));
    assertTrue(names.contains(TransformsMetricsMeta.DEFAULT_TRANSFORM_NAME_FIELD));
    assertFalse(names.contains(TransformsMetricsMeta.DEFAULT_TRANSFORM_ID_FIELD));
    assertFalse(names.contains(TransformsMetricsMeta.DEFAULT_LINES_INPUT_FIELD));
    assertTrue(names.contains(TransformsMetricsMeta.DEFAULT_LINES_WRITTEN_FIELD));
    assertEquals(IValueMeta.TYPE_INTEGER, rowMeta.searchValueMeta("Lines written").getType());
  }

  @Test
  void testCheckEmptyListAndIncomingHops() {
    TransformsMetricsMeta meta = new TransformsMetricsMeta();
    meta.setDefault();
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("metrics");
    PipelineMeta pipelineMeta = new PipelineMeta();

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        pipelineMeta,
        transformMeta,
        new RowMeta(),
        new String[] {"upstream"},
        new String[0],
        new RowMeta(),
        new Variables(),
        null);
    assertTrue(
        remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR),
        "incoming hops should be an error");
    assertTrue(
        remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR),
        "empty watch list should be an error");
  }

  @Test
  void testCheckMissingWatchedTransformIsWarning() {
    TransformsMetricsMeta meta = new TransformsMetricsMeta();
    meta.setDefault();
    meta.getMetricTransforms().add(new MetricTransform("missing", "0", false));
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("metrics");
    PipelineMeta pipelineMeta = new PipelineMeta();

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        pipelineMeta,
        transformMeta,
        new RowMeta(),
        new String[0],
        new String[0],
        new RowMeta(),
        new Variables(),
        null);
    assertTrue(remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_WARNING));
  }
}
