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

package org.apache.hop.beam.transforms.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.coder.HopRowCoder;
import org.apache.hop.beam.engines.direct.BeamDirectPipelineEngine;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class BeamMqttGraphTest {
  @BeforeAll
  static void init() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  private BeamMqttInputMeta source() {
    var m = new BeamMqttInputMeta();
    m.setServerUri("tcp://localhost:1883");
    m.setTopic("sensors/#");
    return m;
  }

  private BeamMqttOutputMeta sink() {
    var m = new BeamMqttOutputMeta();
    m.setServerUri("tcp://localhost:1883");
    m.setTopic("sensors/data");
    return m;
  }

  private RowMeta row() {
    var row = new RowMeta();
    row.addValueMeta(new ValueMetaString("message"));
    return row;
  }

  @Test
  void discoveredPluginsAreSupportedByBeamEngine() {
    for (Object meta : new Object[] {source(), sink()}) {
      assertInstanceOf(IBeamPipelineTransformHandler.class, meta);
      var plugin = PluginRegistry.getInstance().getPlugin(TransformPluginType.class, meta);
      assertNotNull(plugin);
      assertTrue(new BeamDirectPipelineEngine().supports(plugin).isSupported());
    }
  }

  @Test
  void sourceHandlerRegistersCodedUnboundedCollection() throws Exception {
    var meta = source();
    var hop = new PipelineMeta();
    var transform = new TransformMeta("BeamMqttInput", "read", meta);
    hop.addTransform(transform);
    var map = new HashMap<String, PCollection<HopRow>>();
    Pipeline p = Pipeline.create();
    meta.handleTransform(
        null,
        new Variables(),
        null,
        null,
        null,
        null,
        hop,
        transform,
        map,
        p,
        row(),
        List.of(),
        null,
        null);
    assertInstanceOf(HopRowCoder.class, map.get("read").getCoder());
    assertEquals(PCollection.IsBounded.UNBOUNDED, map.get("read").isBounded());
    assertTrue(hasTransform(p, "org.apache.beam.sdk.io.mqtt.MqttIO$Read"));
  }

  @Test
  void recordAndTimeLimitsMakeSourceBoundedAndResolveVariables() throws Exception {
    var m = source();
    var vars = new Variables();
    vars.setVariable("LIMIT", "2");
    vars.setVariable("URI", "ssl://localhost:8883");
    m.setServerUri("${URI}");
    m.setMaxNumRecords("${LIMIT}");
    m.setMaxReadTime("3");
    var transform = m.buildInputTransform(vars, "read");
    var p = Pipeline.create();
    var rows = p.apply(transform);
    assertEquals(PCollection.IsBounded.BOUNDED, rows.isBounded());
    var display = org.apache.beam.sdk.transforms.display.DisplayData.from(transform.getRead());
    assertTrue(
        display.items().stream()
            .anyMatch(
                i ->
                    i.getKey().equals("serverUri") && i.getValue().equals("ssl://localhost:8883")));
    assertTrue(
        display.items().stream()
            .anyMatch(i -> i.getKey().equals("maxNumRecords") && i.getValue().equals(2L)));
    assertTrue(display.items().stream().anyMatch(i -> i.getKey().equals("maxReadTime")));
  }

  @Test
  void timeLimitAloneMakesSourceBounded() throws Exception {
    var m = source();
    m.setMaxReadTime("1");
    assertEquals(
        PCollection.IsBounded.BOUNDED,
        Pipeline.create().apply(m.buildInputTransform(new Variables(), "read")).isBounded());
  }

  @Test
  void outputHandlerBuildsActualMqttSinkWithoutRegisteringRows() throws Exception {
    var m = sink();
    m.setRetained(true);
    var p = Pipeline.create();
    var input =
        p.apply(Create.of(new HopRow(new Object[] {"payload"})).withCoder(new HopRowCoder()));
    var map = new HashMap<String, PCollection<HopRow>>();
    var transform = new TransformMeta("BeamMqttOutput", "write", m);
    m.handleTransform(
        null,
        new Variables(),
        null,
        null,
        null,
        null,
        new PipelineMeta(),
        transform,
        map,
        p,
        row(),
        List.of(new TransformMeta()),
        input,
        null);
    assertTrue(map.isEmpty());
    assertTrue(hasTransform(p, "org.apache.beam.sdk.io.mqtt.MqttIO$Write"));
    var display =
        org.apache.beam.sdk.transforms.display.DisplayData.from(
            m.buildOutputTransform(new Variables(), "write", row()).getWrite());
    assertTrue(
        display.items().stream()
            .anyMatch(i -> i.getKey().equals("retained") && Boolean.TRUE.equals(i.getValue())));
  }

  @Test
  void missingOrUnsafeConnectionAndTopicFailBeforeGraphSubmission() {
    for (String uri :
        new String[] {
          "",
          "http://localhost:1883",
          "tcp://user:secret@localhost:1883",
          "tcp://localhost:99999",
          "tcp://localhost:1883/path"
        }) {
      var m = source();
      m.setServerUri(uri);
      var e =
          assertThrows(HopException.class, () -> m.buildInputTransform(new Variables(), "read"));
      assertFalse(e.getMessage().contains("secret"));
    }
    var blank = source();
    blank.setTopic(" ");
    assertThrows(HopException.class, () -> blank.buildInputTransform(new Variables(), "read"));
    var out = sink();
    out.setTopic("sensors/#");
    assertThrows(
        HopException.class, () -> out.buildOutputTransform(new Variables(), "write", row()));
    var withPassword = source();
    withPassword.setPassword("secret");
    assertThrows(
        HopException.class, () -> withPassword.buildInputTransform(new Variables(), "read"));
  }

  @Test
  void malformedAndNegativeLimitsAreNotSilentlyUnlimited() {
    for (String limit : new String[] {"-1", "abc", "${UNSET}", "999999999999999999999999999"}) {
      var m = source();
      m.setMaxNumRecords(limit);
      assertThrows(HopException.class, () -> m.buildInputTransform(new Variables(), "read"));
      m.setMaxNumRecords("0");
      m.setMaxReadTime(limit);
      assertThrows(HopException.class, () -> m.buildInputTransform(new Variables(), "read"));
    }
  }

  @Test
  void invalidPayloadAndMissingOutputFieldFailAtGraphConstruction() {
    var m = sink();
    m.setPayloadField("absent");
    assertThrows(HopException.class, () -> m.buildOutputTransform(new Variables(), "write", row()));
    m.setPayloadField("message");
    m.setPayloadType("Binary");
    assertThrows(HopException.class, () -> m.buildOutputTransform(new Variables(), "write", row()));
    m.setPayloadType("json");
    assertThrows(HopException.class, () -> m.buildOutputTransform(new Variables(), "write", row()));
    var in = source();
    in.setPayloadField(" ");
    assertThrows(HopException.class, () -> in.buildInputTransform(new Variables(), "read"));
  }

  @Test
  void malformedSourceSchemaFailsWithoutErasingExistingFields() {
    var m = source();
    var row = row();
    m.setPayloadType("json");
    assertThrows(
        org.apache.hop.core.exception.HopTransformException.class,
        () -> m.getFields(row, "read", null, null, new Variables(), null));
    assertEquals(1, row.size());
    m.setPayloadType("String");
    m.setPayloadField(" ");
    assertThrows(
        org.apache.hop.core.exception.HopTransformException.class,
        () -> m.getFields(row, "read", null, null, new Variables(), null));
  }

  @Test
  void pipelineChecksReportMissingRequiredOptionsAndMissingIncomingRows() {
    var m = new BeamMqttInputMeta();
    var remarks = new ArrayList<org.apache.hop.core.ICheckResult>();
    m.check(
        remarks,
        new PipelineMeta(),
        new TransformMeta(),
        row(),
        new String[0],
        new String[0],
        null,
        new Variables(),
        null);
    assertTrue(
        remarks.stream()
            .anyMatch(r -> r.getType() == org.apache.hop.core.ICheckResult.TYPE_RESULT_ERROR));
    remarks.clear();
    var out = sink();
    out.check(
        remarks,
        new PipelineMeta(),
        new TransformMeta(),
        row(),
        new String[0],
        new String[0],
        null,
        new Variables(),
        null);
    assertTrue(
        remarks.stream()
            .anyMatch(r -> r.getType() == org.apache.hop.core.ICheckResult.TYPE_RESULT_ERROR));
  }

  @Test
  void hopConnectionSerializationDoesNotContainResolvedPassword() throws Exception {
    var vars = new Variables();
    vars.setVariable("SECRET", "local-test-secret");
    var connection =
        org.apache.hop.beam.core.transform.BeamMqttConnection.resolve(
            vars, "tcp://localhost:1883", "topic", "prefix", "user", "${SECRET}", true);
    var bytes = new java.io.ByteArrayOutputStream();
    try (var stream = new java.io.ObjectOutputStream(bytes)) {
      stream.writeObject(connection);
    }
    assertFalse(
        bytes.toString(java.nio.charset.StandardCharsets.ISO_8859_1).contains("local-test-secret"));
    var display =
        org.apache.beam.sdk.transforms.display.DisplayData.from(
            source().buildInputTransform(vars, "read").getRead());
    assertFalse(display.items().stream().anyMatch(item -> item.getKey().equals("password")));
  }

  @Test
  void handlersRejectUnexpectedIncomingTopology() throws Exception {
    var p = Pipeline.create();
    var input = p.apply(Create.of(new HopRow(new Object[] {"value"})).withCoder(new HopRowCoder()));
    var map = new HashMap<String, PCollection<HopRow>>();
    var vars = new Variables();
    var transform = new TransformMeta();
    assertThrows(
        HopException.class,
        () ->
            source()
                .handleTransform(
                    null,
                    vars,
                    null,
                    null,
                    null,
                    null,
                    new PipelineMeta(),
                    transform,
                    map,
                    p,
                    row(),
                    List.of(new TransformMeta()),
                    input,
                    null));
    assertThrows(
        HopException.class,
        () ->
            sink()
                .handleTransform(
                    null,
                    vars,
                    null,
                    null,
                    null,
                    null,
                    new PipelineMeta(),
                    transform,
                    map,
                    p,
                    row(),
                    List.of(),
                    null,
                    null));
    assertThrows(
        HopException.class,
        () ->
            sink()
                .handleTransform(
                    null,
                    vars,
                    null,
                    null,
                    null,
                    null,
                    new PipelineMeta(),
                    transform,
                    map,
                    p,
                    row(),
                    List.of(new TransformMeta(), new TransformMeta()),
                    input,
                    null));
  }

  private boolean hasTransform(Pipeline p, String name) {
    var found = new boolean[1];
    p.traverseTopologically(
        new Pipeline.PipelineVisitor.Defaults() {
          @Override
          public Pipeline.PipelineVisitor.CompositeBehavior enterCompositeTransform(
              TransformHierarchy.Node node) {
            if (node.getTransform() != null
                && node.getTransform()
                    .getClass()
                    .getName()
                    .startsWith(
                        "org.apache.beam.sdk.io.mqtt.AutoValue_"
                            + name.substring(name.lastIndexOf('.') + 1).replace('$', '_')))
              found[0] = true;
            return Pipeline.PipelineVisitor.CompositeBehavior.ENTER_TRANSFORM;
          }
        });
    return found[0];
  }
}
