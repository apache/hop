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

package org.apache.hop.pipeline.transforms.combinationlookup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CombinationLookupMetaTest {
  private static final Class<?> PKG = CombinationLookupMeta.class;
  private static final String CONNECTION_NAME = "h2";
  private static final String TRANSFORM_NAME = "Combination lookup/update";

  @BeforeEach
  void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  void testSerialization() throws Exception {
    TransformSerializationTestUtil.testSerialization(
        "/combination-lookup-transform.xml", CombinationLookupMeta.class);
  }

  /**
   * CombinationLookupDialog.create() wraps dialog contents in a new TransformMeta that never gets a
   * parent pipeline. getSqlStatements must use the PipelineMeta argument, not the parent chain.
   */
  @Test
  void getSqlStatementsDoesNotNpeWhenParentPipelineIsNull() throws Exception {
    CombinationLookupMeta info = dialogStyleMeta();
    TransformMeta transformMeta = new TransformMeta(TRANSFORM_NAME, info);
    assertNull(transformMeta.getParentPipelineMeta());

    PipelineMeta pipelineMeta = pipelineWithoutConnection();
    RowMeta prev = incomingRow();

    SqlStatement sql =
        info.getSqlStatements(
            new Variables(), pipelineMeta, transformMeta, prev, pipelineMeta.getMetadataProvider());

    assertNotNull(sql);
    assertTrue(sql.hasError());
    assertEquals(
        BaseMessages.getString(PKG, "CombinationLookupMeta.ReturnValue.NotConnectionDefined"),
        sql.getError());
  }

  @Test
  void getSqlStatementsUsesPipelineMetaToResolveConnection() throws Exception {
    CombinationLookupMeta info = dialogStyleMeta();
    info.setTableName("");
    TransformMeta transformMeta = new TransformMeta(TRANSFORM_NAME, info);
    assertNull(transformMeta.getParentPipelineMeta());

    PipelineMeta pipelineMeta = pipelineWithConnection();

    SqlStatement sql =
        info.getSqlStatements(
            new Variables(),
            pipelineMeta,
            transformMeta,
            incomingRow(),
            pipelineMeta.getMetadataProvider());

    assertNotNull(sql);
    assertNotNull(sql.getDatabase());
    assertEquals(CONNECTION_NAME, sql.getDatabase().getName());
    assertTrue(sql.hasError());
    assertEquals(
        BaseMessages.getString(PKG, "CombinationLookupMeta.ReturnValue.NotTableDefined"),
        sql.getError());
  }

  @Test
  void getSqlStatementsReportsMissingInputFieldsWithoutNpe() throws Exception {
    CombinationLookupMeta info = dialogStyleMeta();
    TransformMeta transformMeta = new TransformMeta(TRANSFORM_NAME, info);
    PipelineMeta pipelineMeta = pipelineWithConnection();

    SqlStatement sql =
        info.getSqlStatements(
            new Variables(),
            pipelineMeta,
            transformMeta,
            new RowMeta(),
            pipelineMeta.getMetadataProvider());

    assertTrue(sql.hasError());
    assertEquals(
        BaseMessages.getString(PKG, "CombinationLookupMeta.ReturnValue.NotReceivingField"),
        sql.getError());
  }

  @Test
  void analyseImpactDoesNotNpeWhenParentPipelineIsNull() throws Exception {
    CombinationLookupMeta info = dialogStyleMeta();
    TransformMeta transformMeta = new TransformMeta(TRANSFORM_NAME, info);
    assertNull(transformMeta.getParentPipelineMeta());

    PipelineMeta pipelineMeta = pipelineWithConnection();
    pipelineMeta.setName("issue-8283");
    List<DatabaseImpact> impact = new ArrayList<>();

    info.analyseImpact(
        new Variables(),
        impact,
        pipelineMeta,
        transformMeta,
        incomingRow(),
        new String[] {"Data grid"},
        new String[0],
        null,
        pipelineMeta.getMetadataProvider());

    assertEquals(1, impact.size());
    DatabaseImpact first = impact.get(0);
    assertEquals(DatabaseImpact.TYPE_IMPACT_READ_WRITE, first.getType());
    assertEquals("issue-8283", first.getPipelineName());
    assertEquals(TRANSFORM_NAME, first.getTransformName());
    assertEquals("junk_customer", first.getTable());
    assertEquals("customer_code", first.getField());
  }

  @Test
  void analyseImpactSkipsWorkWhenConnectionIsMissing() throws Exception {
    CombinationLookupMeta info = dialogStyleMeta();
    TransformMeta transformMeta = new TransformMeta(TRANSFORM_NAME, info);
    PipelineMeta pipelineMeta = pipelineWithoutConnection();
    List<DatabaseImpact> impact = new ArrayList<>();

    info.analyseImpact(
        new Variables(),
        impact,
        pipelineMeta,
        transformMeta,
        incomingRow(),
        new String[0],
        new String[0],
        null,
        pipelineMeta.getMetadataProvider());

    assertTrue(impact.isEmpty());
  }

  @Test
  void analyseImpactIncludesHashFieldWhenEnabled() throws Exception {
    CombinationLookupMeta info = dialogStyleMeta();
    info.setUseHash(true);
    info.setHashField("hashcode");
    TransformMeta transformMeta = new TransformMeta(TRANSFORM_NAME, info);
    PipelineMeta pipelineMeta = pipelineWithConnection();
    List<DatabaseImpact> impact = new ArrayList<>();

    info.analyseImpact(
        new Variables(),
        impact,
        pipelineMeta,
        transformMeta,
        incomingRow(),
        new String[0],
        new String[0],
        null,
        pipelineMeta.getMetadataProvider());

    assertEquals(2, impact.size());
    assertEquals("hashcode", impact.get(1).getField());
  }

  private static CombinationLookupMeta dialogStyleMeta() {
    CombinationLookupMeta info = new CombinationLookupMeta();
    info.setConnectionName(CONNECTION_NAME);
    info.setTableName("junk_customer");
    info.getFields().getKeyFields().add(new KeyField("customer_code", "customer_code"));
    info.getFields().getReturnFields().setTechnicalKeyField("customer_tk");
    info.getFields()
        .getReturnFields()
        .setTechKeyCreation(CombinationLookupMeta.CREATION_METHOD_TABLEMAX);
    return info;
  }

  private static PipelineMeta pipelineWithoutConnection() {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setMetadataProvider(metadataProvider);
    return pipelineMeta;
  }

  private static PipelineMeta pipelineWithConnection() throws Exception {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider
        .getSerializer(DatabaseMeta.class)
        .save(new DatabaseMeta(CONNECTION_NAME, "NONE", "Native", "", "", "", "", ""));
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setMetadataProvider(metadataProvider);
    return pipelineMeta;
  }

  private static RowMeta incomingRow() {
    RowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("customer_code"));
    return prev;
  }
}
