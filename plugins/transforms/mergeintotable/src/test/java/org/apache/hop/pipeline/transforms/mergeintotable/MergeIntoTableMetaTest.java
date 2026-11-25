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

package org.apache.hop.pipeline.transforms.mergeintotable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.lineage.LineageMetadataWalker;
import org.apache.hop.lineage.LineageMetadataWalker.ColumnMapping;
import org.apache.hop.lineage.LineageMetadataWalker.Declaration;
import org.apache.hop.lineage.model.RelationalIoOperation;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Node;

class MergeIntoTableMetaTest {

  @BeforeAll
  static void initHop() throws Exception {
    HopClientEnvironment.init();
  }

  private static MergeIntoTableMeta load(String body) throws Exception {
    Node node =
        XmlHandler.loadXmlString("<transform>" + body + "</transform>").getDocumentElement();
    MergeIntoTableMeta meta = new MergeIntoTableMeta();
    meta.loadXml(node, new MemoryMetadataProvider());
    return meta;
  }

  private static MergeIntoTableMeta sample() {
    MergeIntoTableMeta meta = new MergeIntoTableMeta();
    meta.setConnection("warehouse");
    meta.setSchemaName("staging");
    meta.setTableName("orders");
    meta.setCommitSize("250");
    meta.setUseBatchUpdate(true);
    meta.setUpdateExisting(false);
    meta.setUpdateOnlyChanged(true);
    meta.setUpsertForm(MergeUpsertForm.ON_CONFLICT);
    meta.getKeyFields().add(new MergeKeyField("id", "order_id"));
    meta.getKeyFields().add(new MergeKeyField("line", "line_nr"));
    meta.getValueFields().add(new MergeValueField("amount", "total", true));
    meta.getValueFields().add(new MergeValueField("created", "created_at", false));
    return meta;
  }

  @Test
  void aNewTransformBatchesAndUpdates() {
    MergeIntoTableMeta meta = new MergeIntoTableMeta();
    meta.setDefault();

    assertEquals("1000", meta.getCommitSize());
    assertEquals(1000, meta.getCommitSize(new Variables()));
    assertTrue(meta.isUseBatchUpdate());
    assertTrue(meta.isUpdateExisting());
    assertFalse(meta.isUpdateOnlyChanged());
    assertTrue(meta.supportsErrorHandling());
    assertEquals(MergeUpsertForm.AUTOMATIC, meta.getUpsertForm());
    assertNull(meta.getRequestedUpsertForm());
  }

  @Test
  void aLoadedTransformWithoutOptionsWritesRowByRowAndUpdates() throws Exception {
    MergeIntoTableMeta meta = load("");

    assertFalse(meta.isUseBatchUpdate());
    assertTrue(meta.isUpdateExisting());
    assertFalse(meta.isUpdateOnlyChanged());
    assertEquals(0, meta.getCommitSize(new Variables()));
    assertTrue(meta.getKeyFields().isEmpty());
    assertTrue(meta.getValueFields().isEmpty());
    assertEquals(MergeUpsertForm.AUTOMATIC, meta.getUpsertForm());
  }

  @Test
  void blankOrVariableCommitSize() throws Exception {
    assertEquals(0, load("<commit/>").getCommitSize(new Variables()));

    MergeIntoTableMeta meta = new MergeIntoTableMeta();
    meta.setCommitSize("${SIZE}");
    Variables variables = new Variables();
    variables.setVariable("SIZE", "42");
    assertEquals(42, meta.getCommitSize(variables));
  }

  @Test
  void xmlRoundTrip() throws Exception {
    MergeIntoTableMeta meta = sample();

    MergeIntoTableMeta copy = load(meta.getXml());

    assertEquals("warehouse", copy.getConnection());
    assertEquals("staging", copy.getSchemaName());
    assertEquals("orders", copy.getTableName());
    assertEquals("250", copy.getCommitSize());
    assertTrue(copy.isUseBatchUpdate());
    assertFalse(copy.isUpdateExisting());
    assertTrue(copy.isUpdateOnlyChanged());
    assertEquals(MergeUpsertForm.ON_CONFLICT, copy.getUpsertForm());
    assertTrue(meta.getXml().contains("<upsert_form>ON_CONFLICT</upsert_form>"), meta.getXml());
    assertEquals(2, copy.getKeyFields().size());
    assertEquals("line", copy.getKeyFields().get(1).getStreamField());
    assertEquals("line_nr", copy.getKeyFields().get(1).getTableColumn());
    assertEquals(2, copy.getValueFields().size());
    assertEquals("amount", copy.getValueFields().get(0).getStreamField());
    assertEquals("total", copy.getValueFields().get(0).getTableColumn());
    assertTrue(copy.getValueFields().get(0).isUpdate());
    assertFalse(copy.getValueFields().get(1).isUpdate());
    assertEquals(meta.getXml(), copy.getXml());
  }

  @Test
  void cloneCopiesTheFieldLists() throws Exception {
    MergeIntoTableMeta meta = sample();

    MergeIntoTableMeta clone = meta.clone();
    clone.getKeyFields().get(0).setTableColumn("changed");
    clone.getValueFields().clear();

    assertNotSame(meta.getKeyFields(), clone.getKeyFields());
    assertEquals("order_id", meta.getKeyFields().get(0).getTableColumn());
    assertEquals(2, meta.getValueFields().size());
    assertEquals(meta.getXml(), sample().getXml());
  }

  @Test
  void declaresAWriteToTheTargetTable() {
    Declaration declaration = LineageMetadataWalker.read(sample());

    assertEquals(RelationalIoOperation.WRITE, declaration.operation());
    assertEquals("warehouse", declaration.connectionName());
    assertEquals("staging", declaration.schemaName());
    assertEquals("orders", declaration.tableName());
    assertTrue(declaration.isUsable());
    assertEquals(
        Set.of(
            new ColumnMapping("order_id", "id"),
            new ColumnMapping("line_nr", "line"),
            new ColumnMapping("total", "amount"),
            new ColumnMapping("created_at", "created")),
        Set.copyOf(declaration.columns()));
  }
}
