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

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeTableSchema;
import org.apache.beam.sdk.io.snowflake.enums.CreateDisposition;
import org.apache.beam.sdk.io.snowflake.enums.WriteDisposition;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.coder.HopRowCoder;
import org.apache.hop.beam.core.transform.BeamSnowflakeOutputTransform;
import org.apache.hop.beam.engines.direct.BeamDirectPipelineEngine;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class BeamSnowflakeOutputMetaTest {
  @BeforeAll
  static void init() throws Exception {
    BeamHop.init();
  }

  @Test
  void fixtureRoundTripsDefaultsAndKeepsSecretVariables() throws Exception {
    BeamSnowflakeOutputMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/beam-snowflake-output-transform.xml", BeamSnowflakeOutputMeta.class);
    assertEquals("${SNOW_SERVER}", meta.getServerName());
    assertEquals("${SNOW_PASSWORD}", meta.getPassword());
    assertEquals("CUSTOMERS", meta.getTableName());
    assertEquals("APPEND", meta.getWriteDisposition());
    assertEquals("CREATE_NEVER", meta.getCreateDisposition());
    assertTrue(meta.getQuotationMark() == null || meta.getQuotationMark().isEmpty());
  }

  @Test
  void serializesLiteralSecretsEncrypted() throws Exception {
    var meta = new BeamSnowflakeOutputMeta();
    meta.setPassword("snow-secret");
    meta.setPrivateKey("pem-secret");
    String xml = meta.getXml();
    assertFalse(xml.contains("snow-secret"));
    assertFalse(xml.contains("pem-secret"));
    assertTrue(xml.contains(Encr.encryptPasswordIfNotUsingVariables("snow-secret")));
    meta.setPassword("${SNOW_PASSWORD}");
    assertTrue(meta.getXml().contains("${SNOW_PASSWORD}"));
  }

  @Test
  void defaultsToAppendAndDoesNotSendASchemaUntilCreateIsRequested() throws Exception {
    var meta = configured();
    BeamSnowflakeOutputTransform transform = meta.buildWrite(new Variables(), "write", row());
    var write = transform.snowflakeWrite();
    assertEquals(WriteDisposition.APPEND, call(write, "getWriteDisposition"));
    assertEquals(CreateDisposition.CREATE_NEVER, call(write, "getCreateDisposition"));
    assertNull(call(write, "getTableSchema"));
    meta.setCreateDisposition("CREATE_IF_NEEDED");
    meta.setWriteDisposition("TRUNCATE");
    var creating = meta.buildWrite(new Variables(), "write", row()).snowflakeWrite();
    assertEquals(WriteDisposition.TRUNCATE, call(creating, "getWriteDisposition"));
    assertEquals(CreateDisposition.CREATE_IF_NEEDED, call(creating, "getCreateDisposition"));
    String sql = ((SnowflakeTableSchema) call(creating, "getTableSchema")).sql();
    assertTrue(sql.contains("id NUMBER(38,0)"));
    assertTrue(sql.contains("amount FLOAT"));
  }

  @Test
  void rejectsAQueryAnUnboundedCollectionAndABadDispositionWithoutEchoingIt() throws Exception {
    HopException query =
        assertThrows(
            HopException.class,
            () ->
                SnowflakeSpec.build(
                    new Variables(),
                    "acct.snowflakecomputing.com",
                    "hop",
                    "secret-password",
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    "gs://hop-stage/out/",
                    "HOP_INT",
                    null,
                    "CUSTOMERS",
                    "select secret-query",
                    false));
    assertTrue(query.getMessage().contains("Snowflake output does not take a query"));
    assertFalse(query.getMessage().contains("secret-query"));
    assertFalse(query.getMessage().contains("secret-password"));
    HopException unbounded =
        assertThrows(
            HopException.class,
            () -> SnowflakeSpec.rejectUnbounded(PCollection.IsBounded.UNBOUNDED));
    assertTrue(unbounded.getMessage().contains("Snowpipe"));
    assertDoesNotThrow(() -> SnowflakeSpec.rejectUnbounded(PCollection.IsBounded.BOUNDED));
    var meta = configured();
    meta.setWriteDisposition("secret-mode");
    HopException disposition =
        assertThrows(HopException.class, () -> meta.buildWrite(new Variables(), "write", row()));
    assertTrue(
        disposition
            .getMessage()
            .contains("Snowflake write disposition must be APPEND, TRUNCATE or EMPTY"));
    assertFalse(disposition.getMessage().contains("secret-mode"));
    meta.setWriteDisposition("APPEND");
    meta.setCreateDisposition("secret-create");
    HopException create =
        assertThrows(HopException.class, () -> meta.buildWrite(new Variables(), "write", row()));
    assertFalse(create.getMessage().contains("secret-create"));
  }

  @Test
  void sinkRequiresExactlyOneIncomingTransform() {
    var meta = configured();
    HopException none =
        assertThrows(
            HopException.class, () -> handle(meta, Pipeline.create(), List.of(), null, row()));
    assertTrue(
        none.getMessage()
            .contains("Beam Snowflake output requires exactly one incoming transform"));
    Pipeline pipeline = Pipeline.create();
    PCollection<HopRow> rows =
        pipeline.apply(Create.of(new HopRow(new Object[] {1L})).withCoder(new HopRowCoder()));
    var first = new TransformMeta();
    first.setName("first");
    var second = new TransformMeta();
    second.setName("second");
    HopException two =
        assertThrows(
            HopException.class, () -> handle(meta, pipeline, List.of(first, second), rows, row()));
    assertTrue(
        two.getMessage().contains("Beam Snowflake output requires exactly one incoming transform"));
  }

  @Test
  void writeGraphIsAppliedWithoutRunningThePipeline() throws Exception {
    var meta = configured();
    Pipeline pipeline = Pipeline.create();
    PCollection<HopRow> rows =
        pipeline.apply(Create.of(new HopRow(new Object[] {1L})).withCoder(new HopRowCoder()));
    rows.apply(meta.buildWrite(new Variables(), "write", row()));
    var upstream = new TransformMeta();
    upstream.setName("upstream");
    assertDoesNotThrow(() -> handle(meta, pipeline, List.of(upstream), rows, row()));
  }

  @Test
  void sinkProducesNoDownstreamFieldsAndIsABeamHandler() throws Exception {
    var meta = configured();
    var row = row();
    meta.getFields(row, "write", null, null, new Variables(), null);
    assertEquals(0, row.size());
    assertTrue(meta.isOutput());
    assertFalse(meta.isInput());
    assertInstanceOf(IBeamPipelineTransformHandler.class, meta);
    var plugin = PluginRegistry.getInstance().getPlugin(TransformPluginType.class, meta);
    assertNotNull(plugin);
    assertTrue(new BeamDirectPipelineEngine().supports(plugin).isSupported());
    Transform annotation = meta.getClass().getAnnotation(Transform.class);
    assertArrayEquals(new String[] {"Beam*"}, annotation.supportedEngines());
    assertEquals(0, annotation.excludedEngines().length);
    var transform = new TransformMeta("BeamSnowflakeOutput", "write", meta);
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
    remarks.clear();
    meta.check(
        remarks,
        new PipelineMeta(),
        transform,
        row(),
        new String[] {"upstream"},
        null,
        null,
        new Variables(),
        null);
    assertEquals(ICheckResult.TYPE_RESULT_OK, remarks.get(0).getType());
    assertEquals("Snowflake configuration is valid", remarks.get(0).getText());
  }

  private static void handle(
      BeamSnowflakeOutputMeta meta,
      Pipeline pipeline,
      List<TransformMeta> previous,
      PCollection<HopRow> input,
      IRowMeta row)
      throws HopException {
    meta.handleTransform(
        null,
        new Variables(),
        null,
        null,
        null,
        null,
        new PipelineMeta(),
        new TransformMeta("BeamSnowflakeOutput", "write", meta),
        new HashMap<>(),
        pipeline,
        row,
        previous,
        input,
        null);
  }

  private static Object call(Object target, String name) throws Exception {
    for (Class<?> type = target.getClass(); type != null; type = type.getSuperclass()) {
      try {
        Method method = type.getDeclaredMethod(name);
        method.setAccessible(true);
        return method.invoke(target);
      } catch (NoSuchMethodException ignored) {
        // The generated SnowflakeIO type may inherit the accessor.
      }
    }
    throw new NoSuchMethodException(name);
  }

  private static BeamSnowflakeOutputMeta configured() {
    var meta = new BeamSnowflakeOutputMeta();
    meta.setServerName("acct.snowflakecomputing.com");
    meta.setUsername("hop");
    meta.setPassword("pw");
    meta.setStagingBucket("gs://hop-stage/out/");
    meta.setStorageIntegration("HOP_INT");
    meta.setTableName("CUSTOMERS");
    return meta;
  }

  private static RowMeta row() {
    var row = new RowMeta();
    row.addValueMeta(new ValueMetaInteger("id"));
    row.addValueMeta(new ValueMetaNumber("amount"));
    row.addValueMeta(new ValueMetaString("note"));
    return row;
  }
}
