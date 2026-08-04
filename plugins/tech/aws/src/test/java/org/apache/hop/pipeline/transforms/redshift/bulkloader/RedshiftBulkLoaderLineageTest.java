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

package org.apache.hop.pipeline.transforms.redshift.bulkloader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.lineage.LineageMetadataWalker;
import org.apache.hop.lineage.LineageMetadataWalker.ColumnMapping;
import org.apache.hop.lineage.LineageMetadataWalker.Declaration;
import org.apache.hop.lineage.model.RelationalIoOperation;
import org.junit.jupiter.api.Test;

/** Checks the relational lineage the Redshift bulk loader declares on its metadata. */
class RedshiftBulkLoaderLineageTest {

  private static RedshiftBulkLoaderMeta meta(boolean truncate) {
    RedshiftBulkLoaderMeta meta = new RedshiftBulkLoaderMeta();
    meta.setConnection("warehouse");
    meta.setSchemaName("staging");
    meta.setTablename("orders");
    meta.setTruncateTable(truncate);
    // The constructor takes (fieldDatabase, fieldStream).
    meta.setFields(List.of(new RedshiftBulkLoaderField("order_id", "id")));
    return meta;
  }

  @Test
  void declaresAWriteToTheTargetTable() {
    Declaration declaration = LineageMetadataWalker.read(meta(false));

    assertNotNull(declaration, "RedshiftBulkLoader should declare relational lineage");
    assertEquals(RelationalIoOperation.WRITE, declaration.operation());
    assertEquals("warehouse", declaration.connectionName());
    assertEquals("staging", declaration.schemaName());
    assertEquals("orders", declaration.tableName());
    assertTrue(declaration.isUsable());
  }

  @Test
  void mapsTheStreamFieldToTheTableColumnAndNotTheOtherWayRound() {
    assertEquals(
        List.of(new ColumnMapping("order_id", "id")),
        LineageMetadataWalker.read(meta(false)).columns());
  }

  @Test
  void truncatingMakesTheWriteAnOverwrite() {
    assertFalse(LineageMetadataWalker.read(meta(false)).overwrite());
    assertTrue(LineageMetadataWalker.read(meta(true)).overwrite());
  }
}
