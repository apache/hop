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

package org.apache.hop.beam.transforms.snowflake;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.coder.HopRowCoder;
import org.apache.hop.beam.engines.direct.BeamDirectPipelineEngine;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class BeamSnowflakeInputMetaTest {
  @BeforeAll
  static void init() throws Exception {
    BeamHop.init();
  }

  @Test
  void fixtureRoundTripsTheQueryAndKeepsSecretVariables() throws Exception {
    BeamSnowflakeInputMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/beam-snowflake-input-transform.xml", BeamSnowflakeInputMeta.class);
    assertEquals("${SNOW_SERVER}", meta.getServerName());
    assertEquals("${SNOW_USER}", meta.getUsername());
    assertEquals("${SNOW_PASSWORD}", meta.getPassword());
    assertEquals("ANALYTICS", meta.getDatabase());
    assertEquals("443", meta.getPort());
    assertEquals("gs://hop-stage/in/", meta.getStagingBucket());
    assertEquals("HOP_INT", meta.getStorageIntegration());
    assertEquals("\"", meta.getQuotationMark());
    assertEquals("select id from CUSTOMERS", meta.getQuery());
    assertTrue(meta.getTableName() == null || meta.getTableName().isEmpty());
    assertEquals(1, meta.getFields().size());
    assertEquals("id", meta.getFields().get(0).getName());
    assertEquals("Integer", meta.getFields().get(0).getType());
  }

  @Test
  void serializesLiteralSecretsEncrypted() throws Exception {
    var meta = new BeamSnowflakeInputMeta();
    meta.setPassword("snow-secret");
    meta.setPrivateKey("pem-secret");
    meta.setPrivateKeyPassphrase("phrase-secret");
    String xml = meta.getXml();
    assertFalse(xml.contains("snow-secret"));
    assertFalse(xml.contains("pem-secret"));
    assertFalse(xml.contains("phrase-secret"));
    assertTrue(xml.contains(Encr.encryptPasswordIfNotUsingVariables("snow-secret")));
    assertTrue(xml.contains(Encr.encryptPasswordIfNotUsingVariables("pem-secret")));
    assertTrue(xml.contains(Encr.encryptPasswordIfNotUsingVariables("phrase-secret")));
    meta.setPassword("${SNOW_PASSWORD}");
    assertTrue(meta.getXml().contains("${SNOW_PASSWORD}"));
  }

  @Test
  void resolvesTheServerFromAVariableAndOmitsThePasswordForAKeyPair() throws Exception {
    var variables = new Variables();
    variables.setVariable("SNOW_SERVER", "acct.snowflakecomputing.com");
    var meta = configured();
    meta.setServerName("${SNOW_SERVER}");
    meta.setPrivateKey("pem-secret");
    meta.setPrivateKeyPassphrase("phrase-secret");
    meta.setPassword("not-sent");
    var dataSource = meta.spec(variables).getDataSource();
    assertEquals("acct.snowflakecomputing.com", value(dataSource.getServerName()));
    assertEquals("hop", value(dataSource.getUsername()));
    assertEquals("pem-secret", value(dataSource.getRawPrivateKey()));
    assertEquals("phrase-secret", value(dataSource.getPrivateKeyPassphrase()));
    assertTrue(dataSource.getPassword() == null || dataSource.getPassword().get() == null);
    assertEquals(443, dataSource.getPortNumber());
    assertEquals("ANALYTICS", value(dataSource.getDatabase()));
  }

  @Test
  void passwordAuthSetsThePasswordAndLeavesTheKeyEmpty() throws Exception {
    var dataSource = configured().spec(new Variables()).getDataSource();
    assertEquals("pw", value(dataSource.getPassword()));
    assertTrue(
        dataSource.getRawPrivateKey() == null || dataSource.getRawPrivateKey().get() == null);
  }

  @Test
  void rejectsBadConnectionSettingsWithoutEchoingSecrets() {
    var meta = configured();
    meta.setServerName("secret-host.example");
    meta.setPassword("secret-password");
    HopException server = assertThrows(HopException.class, () -> meta.spec(new Variables()));
    assertFalse(server.getMessage().contains("secret-host"));
    assertFalse(server.getMessage().contains("secret-password"));
    meta.setServerName("acct.snowflakecomputing.com");
    meta.setStagingBucket("gs://secret-bucket/in");
    HopException bucket = assertThrows(HopException.class, () -> meta.spec(new Variables()));
    assertFalse(bucket.getMessage().contains("secret-bucket"));
    meta.setStagingBucket("gs://hop-stage/in/");
    meta.setQuery("select secret-query");
    HopException both = assertThrows(HopException.class, () -> meta.spec(new Variables()));
    assertTrue(
        both.getMessage().contains("Snowflake input needs a table or a query, and not both"));
    assertFalse(both.getMessage().contains("secret-query"));
    meta.setTableName(null);
    meta.setQuery(null);
    meta.setPassword(null);
    meta.setPrivateKey(null);
    HopException auth = assertThrows(HopException.class, () -> meta.spec(new Variables()));
    assertTrue(auth.getMessage().contains("Snowflake authentication is required"));
  }

  @Test
  void sourceRejectsIncomingRowsBeforeItBuildsTheRead() {
    var meta = new BeamSnowflakeInputMeta();
    var upstream = new TransformMeta();
    upstream.setName("upstream");
    HopException incoming =
        assertThrows(
            HopException.class, () -> handle(meta, Pipeline.create(), List.of(upstream), null));
    assertTrue(
        incoming
            .getMessage()
            .contains("Beam Snowflake input is a source and does not accept incoming rows"));
    Pipeline pipeline = Pipeline.create();
    PCollection<HopRow> rows =
        pipeline.apply(Create.of(new HopRow(new Object[] {1L})).withCoder(new HopRowCoder()));
    HopException collection =
        assertThrows(HopException.class, () -> handle(meta, pipeline, List.of(), rows));
    assertTrue(
        collection
            .getMessage()
            .contains("Beam Snowflake input is a source and does not accept incoming rows"));
  }

  @Test
  void readGraphIsAppliedWithoutRunningThePipeline() throws Exception {
    var meta = configured();
    meta.getFields().add(new SnowflakeField("id", "Integer"));
    Pipeline pipeline = Pipeline.create();
    PCollection<HopRow> rows = pipeline.apply(meta.buildRead(new Variables(), "read"));
    assertNotNull(rows);
    assertEquals(PCollection.IsBounded.BOUNDED, rows.isBounded());
    assertInstanceOf(HopRowCoder.class, rows.getCoder());
  }

  @Test
  void getFieldsReplacesTheRowAndRejectsADuplicateName() throws Exception {
    var meta = configured();
    meta.getFields().add(new SnowflakeField("amount", "Number"));
    var row = new RowMeta();
    row.addValueMeta(new ValueMetaString("old"));
    meta.getFields(row, "read", null, null, new Variables(), null);
    assertEquals(1, row.size());
    assertEquals("amount", row.getValueMeta(0).getName());
    assertEquals(IValueMeta.TYPE_NUMBER, row.getValueMeta(0).getType());
    meta.getFields().add(new SnowflakeField("amount", "Integer"));
    HopException duplicate =
        assertThrows(HopException.class, () -> meta.outputRowMeta(new Variables(), "read"));
    assertTrue(duplicate.getMessage().contains("Snowflake field name is duplicated"));
    assertFalse(duplicate.getMessage().contains("amount"));
    meta.setFields(new ArrayList<>());
    assertThrows(
        HopTransformException.class,
        () -> meta.getFields(row, "read", null, null, new Variables(), null));
  }

  @Test
  void checkReportsAnIncomingHopAndAValidSource() throws Exception {
    var meta = configured();
    meta.getFields().add(new SnowflakeField("id", "Integer"));
    var transform = new TransformMeta("BeamSnowflakeInput", "read", meta);
    var remarks = new ArrayList<ICheckResult>();
    meta.check(
        remarks,
        new PipelineMeta(),
        transform,
        null,
        new String[] {"upstream"},
        null,
        null,
        new Variables(),
        null);
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, remarks.get(0).getType());
    remarks.clear();
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
    assertEquals(ICheckResult.TYPE_RESULT_OK, remarks.get(0).getType());
    assertEquals("Snowflake configuration is valid", remarks.get(0).getText());
  }

  @Test
  void discoveredPluginIsABeamSource() {
    var meta = new BeamSnowflakeInputMeta();
    assertTrue(meta.isInput());
    assertFalse(meta.isOutput());
    assertFalse(meta.consumesMainInput());
    assertTrue(meta.canStartWithoutInput());
    assertInstanceOf(IBeamPipelineTransformHandler.class, meta);
    var plugin = PluginRegistry.getInstance().getPlugin(TransformPluginType.class, meta);
    assertNotNull(plugin);
    assertTrue(new BeamDirectPipelineEngine().supports(plugin).isSupported());
    Transform annotation = meta.getClass().getAnnotation(Transform.class);
    assertArrayEquals(new String[] {"Beam*"}, annotation.supportedEngines());
    assertEquals(0, annotation.excludedEngines().length);
  }

  private static void handle(
      BeamSnowflakeInputMeta meta,
      Pipeline pipeline,
      List<TransformMeta> previous,
      PCollection<HopRow> input)
      throws HopException {
    meta.handleTransform(
        null,
        new Variables(),
        null,
        null,
        null,
        null,
        new PipelineMeta(),
        new TransformMeta("BeamSnowflakeInput", "read", meta),
        new HashMap<>(),
        pipeline,
        new RowMeta(),
        previous,
        input,
        null);
  }

  private static BeamSnowflakeInputMeta configured() {
    var meta = new BeamSnowflakeInputMeta();
    meta.setServerName("acct.snowflakecomputing.com");
    meta.setUsername("hop");
    meta.setPassword("pw");
    meta.setDatabase("ANALYTICS");
    meta.setWarehouse("LOAD_WH");
    meta.setSchema("PUBLIC");
    meta.setRole("HOP_ROLE");
    meta.setPort("443");
    meta.setStagingBucket("gs://hop-stage/in/");
    meta.setStorageIntegration("HOP_INT");
    meta.setTableName("CUSTOMERS");
    return meta;
  }

  private static String value(ValueProvider<String> provider) {
    return provider == null ? null : provider.get();
  }
}
