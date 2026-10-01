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

package org.apache.hop.beam.transforms.splunk;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.transform.BeamSplunkOutputTransform;
import org.apache.hop.beam.engines.direct.BeamDirectPipelineEngine;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class BeamSplunkOutputMetaTest {
  @BeforeAll
  static void init() throws Exception {
    BeamHop.init();
  }

  @Test
  void fixtureRoundTripsOptionsAndKeepsTheTokenVariable() throws Exception {
    BeamSplunkOutputMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/beam-splunk-output-transform.xml", BeamSplunkOutputMeta.class);
    assertEquals("${SPLUNK_URL}", meta.getHecUrl());
    assertEquals("${SPLUNK_HEC_TOKEN}", meta.getToken());
    assertEquals("${SPLUNK_BATCH}", meta.getBatchCount());
    assertFalse(meta.isDisableCertificateValidation());
    assertTrue(meta.isEnableGzip());
    assertEquals("body", meta.getEventField());
    assertEquals("main", meta.getIndex());
    assertEquals("hop", meta.getSource());
    assertEquals("json", meta.getSourceType());
    assertEquals("worker", meta.getHost());
  }

  @Test
  void serializesLiteralTokensEncrypted() throws Exception {
    var meta = new BeamSplunkOutputMeta();
    meta.setToken("hec-secret");
    String xml = meta.getXml();
    assertFalse(xml.contains("hec-secret"));
    assertTrue(xml.contains(Encr.encryptPasswordIfNotUsingVariables("hec-secret")));
    meta.setToken("${SPLUNK_HEC_TOKEN}");
    assertTrue(meta.getXml().contains("${SPLUNK_HEC_TOKEN}"));
  }

  @Test
  void resolvesVariablesWhenTheTransformIsBuilt() throws Exception {
    var variables = new Variables();
    variables.setVariable("SPLUNK_URL", "http://127.0.0.1:8088");
    variables.setVariable("SPLUNK_HEC_TOKEN", "from-variable");
    variables.setVariable("SPLUNK_BATCH", "4");
    variables.setVariable("SPLUNK_INDEX", "ops");
    var meta = configured(variables);
    meta.setHecUrl("${SPLUNK_URL}");
    meta.setToken("${SPLUNK_HEC_TOKEN}");
    meta.setBatchCount("${SPLUNK_BATCH}");
    meta.setIndex("${SPLUNK_INDEX}");
    BeamSplunkOutputTransform transform = meta.buildOutputTransform(variables, "splunk", row());
    var write = transform.splunkWrite();
    assertNotNull(write);
    assertEquals(4, writeBatch(transform));
  }

  @Test
  void rejectsAMissingTokenABadUrlAndAMissingField() {
    var meta = new BeamSplunkOutputMeta();
    meta.setHecUrl("http://127.0.0.1:8088");
    meta.setEventField("body");
    assertThrows(
        HopException.class, () -> meta.buildOutputTransform(new Variables(), "splunk", row()));
    meta.setToken("secret");
    meta.setHecUrl("http://127.0.0.1:8088/services/collector");
    HopException badUrl =
        assertThrows(
            HopException.class, () -> meta.buildOutputTransform(new Variables(), "splunk", row()));
    assertFalse(badUrl.getMessage().contains("secret"));
    meta.setHecUrl("http://127.0.0.1:8088");
    meta.setEventField("missing");
    HopException missing =
        assertThrows(
            HopException.class, () -> meta.buildOutputTransform(new Variables(), "splunk", row()));
    assertTrue(missing.getMessage().contains("missing"));
    assertFalse(missing.getMessage().contains("secret"));
  }

  @Test
  void missingInputIsAnErrorAndDoesNotClearValidationSuccess() throws Exception {
    var meta = configured(new Variables());
    var transform = new TransformMeta("BeamSplunkOutput", "splunk", meta);
    var remarks = new ArrayList<ICheckResult>();
    meta.check(
        remarks,
        new PipelineMeta(),
        transform,
        null,
        new String[0],
        null,
        null,
        new Variables(),
        null);
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, remarks.get(0).getType());
    assertThrows(
        HopException.class,
        () ->
            meta.handleTransform(
                null,
                new Variables(),
                null,
                null,
                null,
                null,
                new PipelineMeta(),
                transform,
                null,
                null,
                row(),
                List.of(),
                null,
                null));
  }

  @Test
  void sinkProducesNoDownstreamFieldsAndIsABeamHandler() throws Exception {
    var meta = new BeamSplunkOutputMeta();
    var row = row();
    meta.getFields(row, "splunk", null, null, new Variables(), null);
    assertEquals(0, row.size());
    assertTrue(meta.isOutput());
    assertFalse(meta.isInput());
    assertInstanceOf(IBeamPipelineTransformHandler.class, meta);
    var plugin = PluginRegistry.getInstance().getPlugin(TransformPluginType.class, meta);
    assertNotNull(plugin);
    assertTrue(new BeamDirectPipelineEngine().supports(plugin).isSupported());
    assertEquals(
        0,
        meta.getClass()
            .getAnnotation(org.apache.hop.core.annotations.Transform.class)
            .excludedEngines()
            .length);
  }

  private static int writeBatch(BeamSplunkOutputTransform transform) throws Exception {
    var field = BeamSplunkOutputTransform.class.getDeclaredField("batchCount");
    field.setAccessible(true);
    return (Integer) field.get(transform);
  }

  private static BeamSplunkOutputMeta configured(Variables variables) {
    var meta = new BeamSplunkOutputMeta();
    meta.setHecUrl("http://127.0.0.1:8088");
    meta.setToken("secret");
    meta.setEventField("body");
    return meta;
  }

  private static RowMeta row() {
    var row = new RowMeta();
    row.addValueMeta(new ValueMetaString("body"));
    return row;
  }
}
