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

package org.apache.hop.pipeline.transforms.xml.xmloutput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.apache.hop.pipeline.transforms.xml.xmloutput.XmlField.ContentType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** XML output in a real pipeline: Injector, then XML output, then Dummy. */
class XmlOutputRowMetaTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(60);
  private static final String INPUT = "input";
  private static final String XML_OUTPUT = "xml output";

  @TempDir Path tempDir;

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
    PluginRegistry registry = PluginRegistry.getInstance();
    for (ITransformMeta meta : List.of(new XmlOutputMeta(), new InjectorMeta(), new DummyMeta())) {
      if (registry.getPluginId(TransformPluginType.class, meta) == null) {
        registry.registerPluginClass(
            meta.getClass().getName(), TransformPluginType.class, Transform.class);
      }
    }
  }

  /** The rows are passed on with one metadata object, which still has the field lengths set. */
  @Test
  void rowsArePassedOnWithOneRowMeta() {
    List<IRowMeta> passedOn =
        assertTimeoutPreemptively(TIMEOUT, () -> run(false, rows(5)).passedOn());

    assertEquals(5, passedOn.size());
    for (IRowMeta rowMeta : passedOn) {
      assertSame(passedOn.get(0), rowMeta);
    }
    IValueMeta qty = passedOn.get(0).searchValueMeta("qty");
    assertEquals(7, qty.getLength());
    assertEquals(0, qty.getPrecision());
  }

  @Test
  void omitNullValuesLeavesOutNullElements() throws Exception {
    String xml = assertTimeoutPreemptively(TIMEOUT, () -> run(true, rowsWithANullName()).xml());

    assertEquals(1, count(xml, "<name>"), xml);
    assertEquals(2, count(xml, "<qty>"), xml);
  }

  @Test
  void nullElementsAreWrittenWhenNotOmitted() throws Exception {
    String xml = assertTimeoutPreemptively(TIMEOUT, () -> run(false, rowsWithANullName()).xml());

    assertEquals(2, count(xml, "<name"), xml);
    assertEquals(2, count(xml, "<qty>"), xml);
  }

  private record Run(List<IRowMeta> passedOn, String xml) {}

  private Run run(boolean omitNullValues, List<Object[]> rows) throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("xml-output");

    XmlOutputMeta xmlOutputMeta = new XmlOutputMeta();
    xmlOutputMeta.getFileDetails().setFileName(tempDir.resolve("out").toString());
    xmlOutputMeta.getFileDetails().setExtension("xml");
    xmlOutputMeta.getFileDetails().setOmitNullValues(omitNullValues);
    List<XmlField> fields = new ArrayList<>();
    fields.add(new XmlField(ContentType.Element, "name", "name", IValueMeta.TYPE_STRING, -1, -1));
    fields.add(new XmlField(ContentType.Element, "qty", "qty", IValueMeta.TYPE_INTEGER, 7, 0));
    xmlOutputMeta.setOutputFields(fields);

    TransformMeta input = addTransform(pipelineMeta, INPUT, new InjectorMeta());
    TransformMeta xmlOutput = addTransform(pipelineMeta, XML_OUTPUT, xmlOutputMeta);
    TransformMeta output = addTransform(pipelineMeta, "output", new DummyMeta());
    pipelineMeta.addPipelineHop(new PipelineHopMeta(input, xmlOutput));
    pipelineMeta.addPipelineHop(new PipelineHopMeta(xmlOutput, output));

    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);
    pipeline.prepareExecution();
    List<IRowMeta> passedOn = new ArrayList<>();
    pipeline
        .getTransform(XML_OUTPUT, 0)
        .addRowListener(
            new RowAdapter() {
              @Override
              public void rowWrittenEvent(IRowMeta rowMeta, Object[] row)
                  throws HopTransformException {
                passedOn.add(rowMeta);
              }
            });
    RowProducer producer = pipeline.addRowProducer(INPUT, 0);
    pipeline.startThreads();

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));
    rowMeta.addValueMeta(new ValueMetaInteger("qty"));
    for (Object[] row : rows) {
      producer.putRow(rowMeta, row);
    }
    producer.finished();
    pipeline.waitUntilFinished();
    assertEquals(0, pipeline.getErrors());

    String xml = Files.readString(tempDir.resolve("out.xml"), StandardCharsets.UTF_8);
    return new Run(passedOn, xml);
  }

  private static List<Object[]> rows(int nrRows) {
    List<Object[]> rows = new ArrayList<>();
    for (long i = 0; i < nrRows; i++) {
      rows.add(new Object[] {"name-" + i, i});
    }
    return rows;
  }

  private static List<Object[]> rowsWithANullName() {
    return List.of(new Object[] {"a", 1L}, new Object[] {null, 2L});
  }

  private static int count(String text, String part) {
    int count = 0;
    for (int i = text.indexOf(part); i >= 0; i = text.indexOf(part, i + part.length())) {
      count++;
    }
    return count;
  }

  private static TransformMeta addTransform(
      PipelineMeta pipelineMeta, String name, ITransformMeta meta) {
    String pluginId = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    TransformMeta transformMeta = new TransformMeta(pluginId, name, meta);
    pipelineMeta.addTransform(transformMeta);
    return transformMeta;
  }
}
