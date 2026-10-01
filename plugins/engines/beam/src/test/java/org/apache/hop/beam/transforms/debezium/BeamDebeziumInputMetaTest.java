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

package org.apache.hop.beam.transforms.debezium;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.junit.jupiter.api.Test;

class BeamDebeziumInputMetaTest {
  @org.junit.jupiter.api.BeforeAll
  static void init() throws Exception {
    org.apache.hop.core.HopEnvironment.init();
    org.apache.hop.core.plugins.PluginRegistry.init();
  }

  @Test
  void xmlRoundTripResolvesConnectionPropertiesAndEncryptsPassword() throws Exception {
    BeamDebeziumInputMeta meta =
        org.apache.hop.pipeline.transform.TransformSerializationTestUtil.testSerialization(
            "/beam-debezium-input-transform.xml", BeamDebeziumInputMeta.class);
    Variables variables = new Variables();
    variables.setVariable("CDC_HOST", "db.example.test");
    variables.setVariable("CDC_DB", "inventory");
    variables.setVariable("CDC_PASSWORD", "test-only-password");
    var config = meta.buildConnectorConfiguration(variables).getConfigurationMap();
    assertEquals(
        "io.debezium.connector.postgresql.PostgresConnector", config.get("connector.class"));
    assertEquals("db.example.test", config.get("database.hostname"));
    assertEquals("5432", config.get("database.port"));
    assertEquals("cdc_user", config.get("database.user"));
    assertEquals("test-only-password", config.get("database.password"));
    assertEquals("inventory", config.get("database.dbname"));
    assertEquals("pgoutput", config.get("plugin.name"));
    assertEquals("25", meta.getMaxRecords());
    assertEquals("3000", meta.getMaxTimeMs());
    assertEquals("100", meta.getPollingTimeoutMs());
    meta.setPassword("test-only-literal");
    assertFalse(meta.getXml().contains("test-only-literal"));
    assertTrue(meta.getXml().contains("Encrypted"));
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "hostname, ''",
    "port, 0",
    "port, abc",
    "port, 65536",
    "username, ''",
    "jsonField, ''",
    "maxRecords, -1",
    "maxRecords, 2147483648",
    "maxTimeMs, 0",
    "pollingTimeoutMs, 0",
    "connectorProperties, []",
    "connectorProperties, {broken}",
    "connectorProperties, {\"x\":null}",
    "connectorProperties, {\"x\":{}}",
    "connectorProperties, {\"database.password\":\"secret\"}"
  })
  void rejectsInvalidResolvedSourceConfiguration(String property, String value) throws Exception {
    BeamDebeziumInputMeta meta = validMeta();
    String setter = "set" + Character.toUpperCase(property.charAt(0)) + property.substring(1);
    meta.getClass().getMethod(setter, String.class).invoke(meta, value);
    assertThrows(
        org.apache.hop.core.exception.HopException.class,
        () -> meta.buildConnectorConfiguration(new Variables()),
        property);
  }

  @Test
  void guiPipelineCheckReportsMissingUsernameAndIncomingHops() {
    BeamDebeziumInputMeta meta = new BeamDebeziumInputMeta();
    java.util.List<org.apache.hop.core.ICheckResult> remarks = new java.util.ArrayList<>();
    meta.check(
        remarks,
        new org.apache.hop.pipeline.PipelineMeta(),
        new org.apache.hop.pipeline.transform.TransformMeta("BeamDebeziumInput", "CDC", meta),
        new RowMeta(),
        new String[] {"previous"},
        new String[0],
        null,
        new Variables(),
        null);
    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == org.apache.hop.core.ICheckResult.TYPE_RESULT_ERROR
                        && r.getText().contains("Username")));
    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == org.apache.hop.core.ICheckResult.TYPE_RESULT_ERROR
                        && r.getText().contains("incoming")));
  }

  private static BeamDebeziumInputMeta validMeta() {
    BeamDebeziumInputMeta meta = new BeamDebeziumInputMeta();
    meta.setUsername("cdc");
    return meta;
  }

  @Test
  void distributedPostgresConnectorAndDriverInstantiateWithoutOptionalJars() throws Exception {
    org.apache.kafka.connect.source.SourceConnector connector =
        new io.debezium.connector.postgresql.PostgresConnector();
    assertNotNull(connector.config());
    // Debezium declares Kafka Connect transforms as provided, but its schema utilities need them.
    assertNotNull(
        io.debezium.data.SchemaUtil.copySchemaBasics(
            org.apache.kafka.connect.data.Schema.STRING_SCHEMA));
    assertNotNull(connector.taskClass().getConstructor().newInstance());
    assertNotNull(Class.forName("org.postgresql.Driver").getConstructor().newInstance());
  }

  @Test
  void sourcePluginIsLimitedToBeamEngines() {
    var annotation =
        BeamDebeziumInputMeta.class.getAnnotation(org.apache.hop.core.annotations.Transform.class);
    assertArrayEquals(
        new String[] {
          "BeamDirectPipelineEngine", "BeamFlinkPipelineEngine", "BeamDataFlowPipelineEngine"
        },
        annotation.supportedEngines());
    assertEquals(0, annotation.excludedEngines().length);
  }

  @Test
  void registeredPluginRejectsSparkAndLocalWhileKeepingTheOtherBeamRunners() {
    var plugin =
        org.apache.hop.core.plugins.PluginRegistry.getInstance()
            .findPluginWithId(
                org.apache.hop.core.plugins.TransformPluginType.class, "BeamDebeziumInput");
    assertNotNull(plugin, "Debezium must be a registered transform plugin");
    var direct = new org.apache.hop.beam.engines.direct.BeamDirectPipelineEngine();
    var flink = new org.apache.hop.beam.engines.flink.BeamFlinkPipelineEngine();
    var dataflow = new org.apache.hop.beam.engines.dataflow.BeamDataFlowPipelineEngine();
    var spark = new org.apache.hop.beam.engines.spark.BeamSparkPipelineEngine();
    assertTrue(
        org.apache.hop.core.plugins.EngineCompatibilityResolver.resolve(
                plugin, "BeamSparkPipelineEngine", spark::supports)
            .isUnsupported());
    assertTrue(
        org.apache.hop.core.plugins.EngineCompatibilityResolver.resolve(
                plugin, "Local", direct::supports)
            .isUnsupported());
    assertTrue(
        org.apache.hop.core.plugins.EngineCompatibilityResolver.resolve(
                plugin, "BeamDirectPipelineEngine", direct::supports)
            .isSupported());
    assertTrue(
        org.apache.hop.core.plugins.EngineCompatibilityResolver.resolve(
                plugin, "BeamFlinkPipelineEngine", flink::supports)
            .isSupported());
    assertTrue(
        org.apache.hop.core.plugins.EngineCompatibilityResolver.resolve(
                plugin, "BeamDataFlowPipelineEngine", dataflow::supports)
            .isSupported());
  }

  @Test
  void sourcePluginExposesOneJsonStringWithoutIncomingFields() throws Exception {
    Class<?> type =
        assertDoesNotThrow(
            () -> Class.forName("org.apache.hop.beam.transforms.debezium.BeamDebeziumInputMeta"));
    ITransformMeta meta = (ITransformMeta) type.getConstructor().newInstance();
    meta.setDefault();
    assertInstanceOf(IBeamPipelineTransformHandler.class, meta);
    assertTrue(meta.canStartWithoutInput());
    assertFalse(meta.consumesMainInput());
    RowMeta row = new RowMeta();
    meta.getFields(row, "cdc", null, null, new Variables(), null);
    assertEquals(1, row.size());
    assertEquals("event", row.getValueMeta(0).getName());
    assertEquals(IValueMeta.TYPE_STRING, row.getValueMeta(0).getType());
    assertEquals("cdc", row.getValueMeta(0).getOrigin());
  }
}
