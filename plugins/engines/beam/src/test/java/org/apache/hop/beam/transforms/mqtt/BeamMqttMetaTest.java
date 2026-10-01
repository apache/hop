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

import java.lang.reflect.Field;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class BeamMqttMetaTest {
  @BeforeAll
  static void init() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  void nativeEngineRuntimeRejectsExecution() {
    org.apache.hop.pipeline.PipelineMeta pipelineMeta = new org.apache.hop.pipeline.PipelineMeta();
    org.apache.hop.pipeline.Pipeline pipeline =
        new org.apache.hop.pipeline.engines.local.LocalPipelineEngine();
    BeamMqttInputMeta source = new BeamMqttInputMeta();
    BeamMqttOutputMeta sink = new BeamMqttOutputMeta();
    pipelineMeta.addTransform(
        new org.apache.hop.pipeline.transform.TransformMeta("BeamMqttInput", "source", source));
    pipelineMeta.addTransform(
        new org.apache.hop.pipeline.transform.TransformMeta("BeamMqttOutput", "sink", sink));
    assertThrows(
        org.apache.hop.core.exception.HopException.class,
        () ->
            new BeamMqttInput(
                    new org.apache.hop.pipeline.transform.TransformMeta(
                        "BeamMqttInput", "source", source),
                    source,
                    new BeamMqttInputData(),
                    0,
                    pipelineMeta,
                    pipeline)
                .processRow());
    assertThrows(
        org.apache.hop.core.exception.HopException.class,
        () ->
            new BeamMqttOutput(
                    new org.apache.hop.pipeline.transform.TransformMeta(
                        "BeamMqttOutput", "sink", sink),
                    sink,
                    new BeamMqttOutputData(),
                    0,
                    pipelineMeta,
                    pipeline)
                .processRow());
  }

  @Test
  void inputXmlRoundTripPreservesConfiguration() throws Exception {
    BeamMqttInputMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/beam-mqtt-input-transform.xml", BeamMqttInputMeta.class);
    assertEquals("ssl://localhost:8883", meta.getServerUri());
    assertEquals("sensors/#", meta.getTopic());
    assertEquals("hop-read", meta.getClientId());
    assertEquals("reader", meta.getUsername());
    assertEquals("${MQTT_PASSWORD}", meta.getPassword());
    assertEquals("payload", meta.getPayloadField());
    assertEquals("Binary", meta.getPayloadType());
    assertEquals("12", meta.getMaxNumRecords());
    assertEquals("30", meta.getMaxReadTime());
  }

  @Test
  void outputXmlRoundTripEncryptsPassword() throws Exception {
    BeamMqttOutputMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/beam-mqtt-output-transform.xml", BeamMqttOutputMeta.class);
    assertEquals("tcp://localhost:1883", meta.getServerUri());
    assertEquals("sensors/data", meta.getTopic());
    assertEquals("hop-write", meta.getClientId());
    assertEquals("writer", meta.getUsername());
    assertEquals("test-secret", meta.getPassword());
    assertFalse(meta.getXml().contains("test-secret"));
    assertEquals("body", meta.getPayloadField());
    assertEquals("String", meta.getPayloadType());
    assertTrue(meta.isRetained());
  }

  @Test
  void inputDeclaresOneResolvedPayloadFieldAndSourceRole() throws Exception {
    BeamMqttInputMeta meta = new BeamMqttInputMeta();
    Variables vars = new Variables();
    vars.setVariable("FIELD", "body");
    meta.setPayloadField("${FIELD}");
    RowMeta row = new RowMeta();
    meta.getFields(row, "mqtt", null, null, vars, null);
    assertEquals(1, row.size());
    assertEquals("body", row.getValueMeta(0).getName());
    assertEquals(IValueMeta.TYPE_STRING, row.getValueMeta(0).getType());
    assertEquals("mqtt", row.getValueMeta(0).getOrigin());
    assertTrue(meta.canStartWithoutInput());
    assertFalse(meta.consumesMainInput());
    assertTrue(meta.isInput());
    assertFalse(meta.isOutput());
    meta.setPayloadType("Binary");
    row.clear();
    meta.getFields(row, "mqtt", null, null, vars, null);
    assertEquals(IValueMeta.TYPE_BINARY, row.getValueMeta(0).getType());
  }

  @Test
  void outputIsTerminalSink() throws Exception {
    BeamMqttOutputMeta meta = new BeamMqttOutputMeta();
    assertFalse(meta.isInput());
    assertTrue(meta.isOutput());
    RowMeta row = new RowMeta();
    row.addValueMeta(new org.apache.hop.core.row.value.ValueMetaString("body"));
    meta.getFields(row, "mqtt", null, null, new Variables(), null);
    assertEquals(0, row.size());
  }

  @Test
  void everyPersistedOptionHasGroupedLocalizedWidget() {
    for (Class<?> type : new Class<?>[] {BeamMqttInputMeta.class, BeamMqttOutputMeta.class}) {
      for (Field field : type.getDeclaredFields()) {
        HopMetadataProperty property = field.getAnnotation(HopMetadataProperty.class);
        if (property == null) continue;
        GuiWidgetElement gui = field.getAnnotation(GuiWidgetElement.class);
        assertNotNull(gui, field.getName());
        assertEquals(GuiWidgetGroupType.TABS, gui.groupType());
        assertFalse(gui.group().isEmpty());
        assertTrue(gui.label().startsWith("i18n::"));
        assertTrue(gui.toolTip().startsWith("i18n::"));
        if (field.getName().equals("password")) {
          assertTrue(property.password());
          assertTrue(gui.password());
        }
      }
    }
  }
}
