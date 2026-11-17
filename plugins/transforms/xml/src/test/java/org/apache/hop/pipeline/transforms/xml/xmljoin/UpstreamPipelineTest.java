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

package org.apache.hop.pipeline.transforms.xml.xmljoin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.function.Executable;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Load previews the target transform. The pipeline it previews must hold only that transform and
 * what leads to it, so neither the join nor anything after it runs. The transforms they send rows
 * to stay as empty Dummies, so that a transform that writes to them by name still finds them.
 */
class UpstreamPipelineTest {
  private static final Class<?> PKG = XmlJoinMeta.class;

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void keepsTheTransformAndWhatLeadsToIt() throws HopException {
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.setMetadataProvider(new MemoryMetadataProvider());
    pipeline.setFilename("/projects/orders/join.hpl");
    TransformMeta generate = add(pipeline, "generate");
    TransformMeta enrich = add(pipeline, "enrich");
    TransformMeta lookup = add(pipeline, "lookup");
    TransformMeta target = add(pipeline, "target");
    TransformMeta source = add(pipeline, "source");
    TransformMeta join = add(pipeline, "join");
    TransformMeta output = add(pipeline, "output");
    TransformMeta side = add(pipeline, "side");
    TransformMeta disabled = add(pipeline, "disabled");
    pipeline.addPipelineHop(new PipelineHopMeta(generate, enrich));
    pipeline.addPipelineHop(new PipelineHopMeta(lookup, enrich));
    pipeline.addPipelineHop(new PipelineHopMeta(enrich, target));
    pipeline.addPipelineHop(new PipelineHopMeta(generate, side));
    pipeline.addPipelineHop(new PipelineHopMeta(target, join));
    pipeline.addPipelineHop(new PipelineHopMeta(source, join));
    pipeline.addPipelineHop(new PipelineHopMeta(join, output));
    pipeline.addPipelineHop(new PipelineHopMeta(disabled, target, false));

    PipelineMeta upstream = UpstreamPipeline.of(pipeline, new Variables(), "target");

    assertEquals(
        List.of("generate", "enrich", "lookup", "target", "join", "side"),
        List.of(upstream.getTransformNames()));
    assertEquals(
        List.of(
            "generate>enrich", "lookup>enrich", "enrich>target", "generate>side", "target>join"),
        hopsOf(upstream));
    for (String kept : List.of("generate", "enrich", "lookup", "target")) {
      assertInstanceOf(InjectorMeta.class, upstream.findTransform(kept).getTransform(), kept);
    }
    for (String placeholder : List.of("join", "side")) {
      TransformMeta dummy = upstream.findTransform(placeholder);
      assertInstanceOf(DummyMeta.class, dummy.getTransform(), placeholder);
      assertEquals("Dummy", dummy.getTransformPluginId());
    }
    assertEquals("/projects/orders/join.hpl", upstream.getFilename());
    assertNotSame(target, upstream.findTransform("target"));

    // The pipeline in the editor is left as it was
    assertEquals(9, pipeline.nrTransforms());
    assertEquals(8, pipeline.nrPipelineHops());
    assertInstanceOf(InjectorMeta.class, pipeline.findTransform("join").getTransform());
  }

  /**
   * The error rows of a kept transform still have somewhere to go: the target of its error handling
   * stays, as an empty Dummy, and so does the error handling. What comes after it is left out.
   */
  @Test
  void theTargetOfErrorHandlingStaysAsAnEmptyDummy() throws HopException {
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.setMetadataProvider(new MemoryMetadataProvider());
    TransformMeta target = add(pipeline, "target");
    TransformMeta errors = add(pipeline, "errors");
    TransformMeta log = add(pipeline, "log");
    pipeline.addPipelineHop(new PipelineHopMeta(target, errors));
    pipeline.addPipelineHop(new PipelineHopMeta(errors, log));
    TransformErrorMeta errorMeta = new TransformErrorMeta(target, errors);
    errorMeta.setEnabled(true);
    target.setTransformErrorMeta(errorMeta);
    TransformErrorMeta errorsOfErrors = new TransformErrorMeta(errors, log);
    errorsOfErrors.setEnabled(true);
    errors.setTransformErrorMeta(errorsOfErrors);

    PipelineMeta upstream = UpstreamPipeline.of(pipeline, new Variables(), "target");

    assertEquals(List.of("target", "errors"), List.of(upstream.getTransformNames()));
    assertEquals(List.of("target>errors"), hopsOf(upstream));
    TransformMeta dummy = upstream.findTransform("errors");
    assertInstanceOf(DummyMeta.class, dummy.getTransform());
    assertNull(dummy.getTransformErrorMeta());
    TransformErrorMeta kept = upstream.findTransform("target").getTransformErrorMeta();
    assertSame(dummy, kept.getTargetTransform());
  }

  @Test
  void anUnknownTransformIsReported() {
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.setMetadataProvider(new MemoryMetadataProvider());
    add(pipeline, "target");

    assertThrows(
        HopException.class, () -> UpstreamPipeline.of(pipeline, new Variables(), "missing"));
  }

  /**
   * The document is read from the field of the first row the way the transform reads its target,
   * and a row that holds no document says why.
   */
  @Test
  void theDocumentIsReadFromTheFieldOfTheRow() throws HopException {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("doc"));

    Document document =
        UpstreamPipeline.documentIn(
            rowMeta, new Object[] {1L, "<orders><order id=\"7\"/></orders>"}, "target", "doc");
    assertEquals("orders", document.getDocumentElement().getNodeName());
    assertEquals(
        "7", ((Element) document.getElementsByTagName("order").item(0)).getAttribute("id"));

    Object[] row = {1L, "<orders/>"};
    assertFails(
        message("XmlJoin.Exception.FieldNotFound", "xml"),
        () -> UpstreamPipeline.documentIn(rowMeta, row, "target", "xml"));
    for (String empty : new String[] {null, ""}) {
      assertFails(
          message("XmlJoin.Document.EmptyDocument", "doc", "target"),
          () -> UpstreamPipeline.documentIn(rowMeta, new Object[] {1L, empty}, "target", "doc"));
    }
    assertFails(
        message("XmlJoin.Document.NotXml", "doc", "target"),
        () -> UpstreamPipeline.documentIn(rowMeta, new Object[] {1L, "orders"}, "target", "doc"));
  }

  private static void assertFails(String expected, Executable executable) {
    HopException e = assertThrows(HopException.class, executable);
    assertTrue(e.getMessage().contains(expected), e.getMessage());
  }

  /** A message as Load shows it; BaseMessages shows one it can't find or format as !key!. */
  private static String message(String key, Object... arguments) {
    String text = BaseMessages.getString(PKG, key, arguments);
    assertFalse(text.startsWith("!") && text.endsWith("!"), "no message " + key + ": " + text);
    return text;
  }

  /** Injectors, so that the transforms that become a Dummy can be told apart. */
  private static TransformMeta add(PipelineMeta pipeline, String name) {
    TransformMeta transformMeta = new TransformMeta("Injector", name, new InjectorMeta());
    pipeline.addTransform(transformMeta);
    return transformMeta;
  }

  private static List<String> hopsOf(PipelineMeta pipeline) {
    return pipeline.getPipelineHops().stream()
        .map(h -> h.getFromTransform().getName() + ">" + h.getToTransform().getName())
        .toList();
  }
}
