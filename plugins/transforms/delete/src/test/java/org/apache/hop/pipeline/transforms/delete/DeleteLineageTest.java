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

package org.apache.hop.pipeline.transforms.delete;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.lineage.LineageMetadataWalker;
import org.apache.hop.lineage.LineageMetadataWalker.ColumnMapping;
import org.apache.hop.lineage.LineageMetadataWalker.Declaration;
import org.apache.hop.lineage.model.RelationalIoOperation;
import org.junit.jupiter.api.Test;

/**
 * Checks that Delete declares a DELETE against the right table.
 *
 * <p>The operation is the point here rather than the columns: a delete removes rows, so it affects
 * the table without writing any column, and the emitter reports it without column lineage.
 * Declaring it as a WRITE instead would put phantom columns on the dataset.
 */
class DeleteLineageTest {

  private static DeleteMeta meta() {
    DeleteMeta meta = new DeleteMeta();
    meta.setConnection("warehouse");
    // DeleteKeyField takes (keyLookup, keyCondition, keyStream, keyStream2): table column
    // "order_id" is matched against stream field "id".
    meta.setLookup(
        new DeleteLookupField(
            "staging", "orders", List.of(new DeleteKeyField("order_id", "=", "id", null))));
    return meta;
  }

  @Test
  void declaresADeleteAgainstTheTargetTable() {
    Declaration declaration = LineageMetadataWalker.read(meta());

    assertNotNull(declaration, "Delete should declare relational lineage");
    assertEquals(RelationalIoOperation.DELETE, declaration.operation());
    assertEquals("warehouse", declaration.connectionName());
    assertEquals("staging", declaration.schemaName());
    assertEquals("orders", declaration.tableName());
    assertTrue(declaration.isUsable());
  }

  // The emitter reports no columns for a delete, but the key mapping is still annotated, so guard
  // its direction here rather than let a reversed pair sit unnoticed until deletes report columns.
  @Test
  void matchesTheTableColumnAgainstTheStreamField() {
    assertEquals(
        List.of(new ColumnMapping("order_id", "id")), LineageMetadataWalker.read(meta()).columns());
  }
}
