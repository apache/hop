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

package org.apache.hop.beam.pipeline;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.hop.beam.core.coder.HopRowCoder;
import org.apache.hop.beam.engines.direct.BeamDirectPipelineRunConfiguration;
import org.apache.hop.beam.transforms.debezium.BeamDebeziumInputMeta;
import org.apache.hop.beam.transforms.elasticsearch.BeamElasticsearchInputMeta;
import org.apache.hop.beam.transforms.mqtt.BeamMqttInputMeta;
import org.apache.hop.beam.transforms.mqtt.BeamMqttOutputMeta;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transform.stream.Stream;
import org.apache.hop.pipeline.transform.stream.StreamIcon;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Converts real Hop graphs. The production converter passes null predecessors and a null input
 * collection to every source handler, so a direct handler call with {@code List.of()} does not
 * exercise this contract.
 */
class BeamSourceConverterTest {
  private static MemoryMetadataProvider metadataProvider;

  @BeforeAll
  static void init() throws Exception {
    HopEnvironment.init();
    metadataProvider = new MemoryMetadataProvider();
    BeamDirectPipelineRunConfiguration direct = new BeamDirectPipelineRunConfiguration();
    direct.setUserAgent("Hop");
    direct.setTempLocation(System.getProperty("java.io.tmpdir"));
    metadataProvider
        .getSerializer(PipelineRunConfiguration.class)
        .save(
            new PipelineRunConfiguration(
                "direct", "", null, Collections.emptyList(), direct, null, false));
  }

  @Test
  void mqttSourceConvertsAndSourceToSinkBuilds() throws Exception {
    Pipeline alone = convert(pipeline("mqtt-source", mqttInput("read")));
    assertTrue(hasOutputCoder(alone, HopRowCoder.class));
    assertTrue(hasClassName(alone, "MqttIO"));

    PipelineMeta connected = new PipelineMeta();
    connected.setName("mqtt-copy");
    TransformMeta source = mqttInput("read");
    TransformMeta sink = mqttOutput("write");
    connected.addTransform(source);
    connected.addTransform(sink);
    connected.addPipelineHop(new PipelineHopMeta(source, sink));
    Pipeline graph = convert(connected);
    assertTrue(hasClassName(graph, "MqttIO"));
  }

  @Test
  void debeziumAndElasticsearchSourcesConvertWithoutPredecessors() throws Exception {
    Pipeline debezium = convert(pipeline("debezium-source", debezium("cdc")));
    assertTrue(hasOutputCoder(debezium, HopRowCoder.class));
    Pipeline elastic = convert(pipeline("elastic-source", elasticsearch("search")));
    assertTrue(hasOutputCoder(elastic, HopRowCoder.class));
  }

  @Test
  void enabledMainAndErrorHopsAreRejectedBeforeTheSourceExpands() {
    for (String kind : List.of("mqtt", "debezium", "elastic")) {
      Exception mainHop =
          assertThrows(Exception.class, () -> convert(incoming(source(kind), false)));
      assertTrue(causeText(mainHop).contains("does not accept incoming rows"), causeText(mainHop));
      Exception errorHop =
          assertThrows(Exception.class, () -> convert(incoming(source(kind), true)));
      assertTrue(
          causeText(errorHop).contains("does not accept incoming rows"), causeText(errorHop));
    }
  }

  @Test
  void disabledIncomingHopDoesNotRejectTheSource() throws Exception {
    for (TransformMeta source :
        List.of(mqttInput("read"), debezium("cdc"), elasticsearch("search"))) {
      PipelineMeta pipeline = new PipelineMeta();
      pipeline.setName("disabled-" + source.getName());
      TransformMeta upstream = new TransformMeta("upstream", new DummyMeta());
      TransformMeta sink = mqttOutput("write", payloadField(source));
      pipeline.addTransform(upstream);
      pipeline.addTransform(source);
      pipeline.addTransform(sink);
      PipelineHopMeta disabled = new PipelineHopMeta(upstream, source);
      disabled.setEnabled(false);
      pipeline.addPipelineHop(disabled);
      pipeline.addPipelineHop(new PipelineHopMeta(source, sink));
      Pipeline graph = convert(pipeline);
      assertTrue(hasOutputCoder(graph, HopRowCoder.class));
      assertFalse(graph.getOptions().getJobName().isBlank());
    }
  }

  @Test
  void informationalHopIsNotARowInput() throws Exception {
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.setName("mqtt-info");
    TransformMeta lookup = mqttInput("lookup");
    InfoAwareMqttInput infoMeta = new InfoAwareMqttInput();
    infoMeta.setServerUri("tcp://127.0.0.1:1883");
    infoMeta.setTopic("sensors/#");
    TransformMeta source = new TransformMeta("BeamMqttInput", "read", infoMeta);
    infoMeta.setInfoTransform(lookup);
    pipeline.addTransform(lookup);
    pipeline.addTransform(source);
    pipeline.addPipelineHop(new PipelineHopMeta(lookup, source));
    Pipeline graph = convert(pipeline);
    assertTrue(hasOutputCoder(graph, HopRowCoder.class));
  }

  private static Pipeline convert(PipelineMeta pipelineMeta) throws Exception {
    return new HopPipelineMetaToBeamPipelineConverter(
            new Variables(), pipelineMeta, metadataProvider, "direct", List.of(), null)
        .createPipeline();
  }

  private static PipelineMeta pipeline(String name, TransformMeta source) {
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.setName(name);
    pipeline.addTransform(source);
    return pipeline;
  }

  private static PipelineMeta incoming(TransformMeta source, boolean errorHop) {
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.setName((errorHop ? "error-" : "main-") + source.getName());
    TransformMeta upstream = new TransformMeta("upstream", new DummyMeta());
    pipeline.addTransform(upstream);
    pipeline.addTransform(source);
    PipelineHopMeta hop = new PipelineHopMeta(upstream, source);
    hop.setErrorHop(errorHop);
    pipeline.addPipelineHop(hop);
    return pipeline;
  }

  private static TransformMeta mqttInput(String name) {
    BeamMqttInputMeta meta = new BeamMqttInputMeta();
    meta.setServerUri("tcp://127.0.0.1:1883");
    meta.setTopic("sensors/#");
    return new TransformMeta("BeamMqttInput", name, meta);
  }

  private static TransformMeta source(String kind) {
    return switch (kind) {
      case "mqtt" -> mqttInput("read");
      case "debezium" -> debezium("cdc");
      default -> elasticsearch("search");
    };
  }

  private static String payloadField(TransformMeta source) {
    if (source.getTransform() instanceof BeamDebeziumInputMeta) {
      return "event";
    }
    if (source.getTransform() instanceof BeamElasticsearchInputMeta) {
      return "json";
    }
    return "message";
  }

  private static TransformMeta mqttOutput(String name) {
    return mqttOutput(name, "message");
  }

  private static TransformMeta mqttOutput(String name, String payloadField) {
    BeamMqttOutputMeta meta = new BeamMqttOutputMeta();
    meta.setServerUri("tcp://127.0.0.1:1883");
    meta.setTopic("sensors/data");
    meta.setPayloadField(payloadField);
    return new TransformMeta("BeamMqttOutput", name, meta);
  }

  private static TransformMeta debezium(String name) {
    BeamDebeziumInputMeta meta = new BeamDebeziumInputMeta();
    meta.setUsername("cdc");
    return new TransformMeta("BeamDebeziumInput", name, meta);
  }

  private static TransformMeta elasticsearch(String name) {
    BeamElasticsearchInputMeta meta = new BeamElasticsearchInputMeta();
    meta.setHosts("http://127.0.0.1:9200");
    meta.setIndex("documents");
    return new TransformMeta("BeamElasticsearchInput", name, meta);
  }

  private static boolean hasClassName(Pipeline pipeline, String fragment) {
    boolean[] found = {false};
    pipeline.traverseTopologically(
        new Pipeline.PipelineVisitor.Defaults() {
          @Override
          public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
            if (node.getTransform() != null
                && node.getTransform().getClass().getName().contains(fragment)) {
              found[0] = true;
            }
            return CompositeBehavior.ENTER_TRANSFORM;
          }

          @Override
          public void visitPrimitiveTransform(TransformHierarchy.Node node) {
            if (node.getTransform() != null
                && node.getTransform().getClass().getName().contains(fragment)) {
              found[0] = true;
            }
          }
        });
    return found[0];
  }

  private static boolean hasOutputCoder(Pipeline pipeline, Class<?> coderType) {
    List<Boolean> found = new ArrayList<>();
    pipeline.traverseTopologically(
        new Pipeline.PipelineVisitor.Defaults() {
          @Override
          public void visitValue(PValue value, TransformHierarchy.Node producer) {
            if (value instanceof PCollection<?> collection
                && coderType.isInstance(collection.getCoder())) {
              found.add(true);
            }
          }
        });
    return !found.isEmpty();
  }

  private static String causeText(Throwable error) {
    StringBuilder text = new StringBuilder();
    for (Throwable current = error; current != null; current = current.getCause()) {
      if (current.getMessage() != null) {
        text.append(current.getMessage()).append('\n');
      }
    }
    return text.toString();
  }

  /** MQTT source whose only declared info stream is not a row input. */
  public static final class InfoAwareMqttInput extends BeamMqttInputMeta {
    private TransformMeta infoTransform;

    void setInfoTransform(TransformMeta infoTransform) {
      this.infoTransform = infoTransform;
    }

    @Override
    public TransformIOMeta getTransformIOMeta() {
      TransformIOMeta ioMeta = new TransformIOMeta(false, true, false, false, false, false);
      if (infoTransform != null) {
        ioMeta.addStream(
            new Stream(IStream.StreamType.INFO, infoTransform, "lookup", StreamIcon.INFO, null));
      }
      return ioMeta;
    }
  }
}
