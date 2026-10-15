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

package org.apache.hop.pipeline.transforms.xml.getxmldata;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.xml.RowTransformCollector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Issue #8936: the element type of a field is stored as the description of the UI language it was
 * saved in. An attribute field must read its attribute whatever that language was.
 */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class GetXmlDataElementTypeTest {

  private static final String XML =
      "<orders><order id=\"42\"><customer>Alice</customer></order></orders>";

  @BeforeEach
  void init() throws Exception {
    HopEnvironment.init();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "Attribute", // English description
        "attribute", // code
        "Attribut", // French description, and the old code
        "Attributo", // Italian description
        "Atributo", // Spanish and Portuguese description
        "属性", // Japanese and Chinese description
        "ATTRIBUTE"
      })
  void attributeFieldIsReadWhateverTheElementTypeLanguage(String elementType, @TempDir Path dir)
      throws Exception {
    Files.writeString(dir.resolve("orders.xml"), XML);

    GetXmlDataField id = new GetXmlDataField("id");
    id.setXPath("id");
    id.setElementType(elementType);

    GetXmlDataField customer = new GetXmlDataField("customer");
    customer.setXPath("customer");

    List<Object[]> rows = read(dir, id, customer);

    assertEquals(1, rows.size());
    assertEquals("42", rows.get(0)[0], "attribute field with element type '" + elementType + "'");
    assertEquals("Alice", rows.get(0)[1]);
  }

  @ParameterizedTest
  @ValueSource(strings = {"Node", "node", "Noeud", "Nodo", ""})
  void nodeFieldStaysANode(String elementType, @TempDir Path dir) throws Exception {
    Files.writeString(dir.resolve("orders.xml"), XML);

    GetXmlDataField customer = new GetXmlDataField("customer");
    customer.setXPath("customer");
    customer.setElementType(elementType);

    List<Object[]> rows = read(dir, customer);

    assertEquals(1, rows.size());
    assertEquals("Alice", rows.get(0)[0], "node field with element type '" + elementType + "'");
  }

  private List<Object[]> read(Path dir, GetXmlDataField... fields) throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("getxmldata-element-type");
    PluginRegistry registry = PluginRegistry.getInstance();

    GetXmlDataMeta meta = new GetXmlDataMeta();
    meta.setEncoding(Const.UTF_8);
    meta.setLoopXPath("/orders/order");
    meta.setInputFields(Arrays.asList(fields));
    meta.setFilesList(
        Collections.singletonList(new GetXmlFileItem(dir.toString(), ".*\\.xml$", "", "N", "N")));
    TransformMeta getXmlData =
        new TransformMeta(
            registry.getPluginId(TransformPluginType.class, meta), "get xml data", meta);
    pipelineMeta.addTransform(getXmlData);

    DummyMeta dummyMeta = new DummyMeta();
    TransformMeta dummy =
        new TransformMeta(
            registry.getPluginId(TransformPluginType.class, dummyMeta), "dummy", dummyMeta);
    pipelineMeta.addTransform(dummy);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(getXmlData, dummy));

    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);
    pipeline.prepareExecution();
    RowTransformCollector collector = new RowTransformCollector();
    pipeline.getTransform("dummy", 0).addRowListener(collector);
    pipeline.startThreads();
    pipeline.waitUntilFinished();

    assertEquals(0, pipeline.getResult().getNrErrors(), "pipeline errors");
    return collector.getRowsWritten().stream().map(r -> r.getData()).toList();
  }
}
