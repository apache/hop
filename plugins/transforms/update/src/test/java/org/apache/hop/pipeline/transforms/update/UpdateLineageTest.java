/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hop.pipeline.transforms.update;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import org.apache.hop.lineage.LineageMetadataWalker;
import org.apache.hop.lineage.LineageMetadataWalker.ColumnMapping;
import org.apache.hop.lineage.LineageMetadataWalker.Declaration;
import org.apache.hop.lineage.model.RelationalIoOperation;
import org.junit.jupiter.api.Test;

/**
 * Checks that the relational lineage Update declares on its metadata matches what it does to the
 * database — the target table, and each stream field mapped to the column it updates rather than
 * the reverse, which would produce a lineage graph that is wrong but not broken.
 */
class UpdateLineageTest {

  private static UpdateMeta meta() {
    UpdateMeta meta = new UpdateMeta();
    meta.setConnection("warehouse");

    UpdateLookupField lookup = new UpdateLookupField();
    lookup.setSchemaName("staging");
    lookup.setTableName("orders");
    lookup.setLookupKeys(List.of(new UpdateKeyField("id", "order_id", "=")));
    lookup.setUpdateFields(List.of(new UpdateField("total", "amount")));

    meta.setLookupField(lookup);
    return meta;
  }

  @Test
  void declaresAWriteToTheTargetTable() {
    Declaration declaration = LineageMetadataWalker.read(meta());

    assertNotNull(declaration, "Update should declare relational lineage");
    assertEquals(RelationalIoOperation.WRITE, declaration.operation());
    assertEquals("warehouse", declaration.connectionName());
    assertEquals("staging", declaration.schemaName());
    assertEquals("orders", declaration.tableName());
    assertTrue(declaration.isUsable());
  }

  @Test
  void mapsStreamFieldsToTableColumnsAndNotTheOtherWayRound() {
    Declaration declaration = LineageMetadataWalker.read(meta());

    assertEquals(
        Set.of(new ColumnMapping("order_id", "id"), new ColumnMapping("total", "amount")),
        Set.copyOf(declaration.columns()));
  }
}
