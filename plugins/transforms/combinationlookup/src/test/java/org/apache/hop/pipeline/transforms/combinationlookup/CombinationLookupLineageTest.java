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

package org.apache.hop.pipeline.transforms.combinationlookup;

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
 * Checks that the relational lineage Combination Lookup/Update declares on its metadata matches
 * what it actually does to the database.
 *
 * <p>The direction of the key mapping is the point. A {@link KeyField} holds both a stream field
 * and a dimension column, and the transform quotes {@code getLookup()} as the database identifier —
 * so {@code lookup} is the column and {@code name} is the stream field. Annotating them the other
 * way round still compiles and still produces column lineage; it just produces it backwards, which
 * is invisible until someone reads the lineage graph. Hence this test.
 */
class CombinationLookupLineageTest {

  private static CombinationLookupMeta metaWithKey(String streamField, String dimensionColumn) {
    CombinationLookupMeta meta = new CombinationLookupMeta();
    meta.setConnectionName("warehouse");
    meta.setSchemaName("staging");
    meta.setTableName("dim_orders");
    meta.getFields().getKeyFields().add(new KeyField(streamField, dimensionColumn));
    return meta;
  }

  @Test
  void declaresAWriteToTheDimensionTable() {
    Declaration declaration = LineageMetadataWalker.read(metaWithKey("id", "order_id"));

    assertNotNull(declaration, "Combination Lookup should declare relational lineage");
    assertEquals(RelationalIoOperation.WRITE, declaration.operation());
    assertEquals("warehouse", declaration.connectionName());
    assertEquals("staging", declaration.schemaName());
    assertEquals("dim_orders", declaration.tableName());
    assertTrue(declaration.isUsable());
  }

  @Test
  void mapsTheStreamFieldToTheDimensionColumnAndNotTheOtherWayRound() {
    Declaration declaration = LineageMetadataWalker.read(metaWithKey("id", "order_id"));

    assertEquals(List.of(new ColumnMapping("order_id", "id")), declaration.columns());
  }
}
