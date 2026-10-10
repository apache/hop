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

package org.apache.hop.spark.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.lakehouse.LakeField;
import org.apache.hop.lakehouse.LakeFormats;
import org.apache.hop.lakehouse.transforms.LakeTableInputMeta;
import org.apache.hop.lakehouse.transforms.LakeTableMergeMeta;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class LakeSessionPlanCopyTest {

  @BeforeAll
  static void init() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void copiesEverySettingOfTheLiveMeta() throws Exception {
    LakeTableInputMeta live = new LakeTableInputMeta();
    live.setFormat(LakeFormats.FORMAT_ICEBERG);
    live.setIdentifierMode(LakeTableInputMeta.MODE_TABLE);
    live.setTableIdentifier("lake.sales.orders");
    live.setCatalogMetadataName("lake");
    live.setTimeTravelType(LakeTableInputMeta.TIME_TRAVEL_VERSION);
    live.setTimeTravelVersion("42");
    live.getFields().add(new LakeField("id", "Integer"));

    LakeTableInputMeta copy = new LakeTableInputMeta();
    LakeSessionPlan.copyFromLive(copy, live, new MemoryMetadataProvider());

    assertEquals(live.getXml(), copy.getXml());
    assertEquals("lake.sales.orders", copy.getTableIdentifier());
    assertEquals("id", copy.getFields().get(0).getName());
  }

  @Test
  void refusesAnotherTransformType() {
    assertThrows(
        HopException.class,
        () ->
            LakeSessionPlan.copyFromLive(
                new LakeTableInputMeta(), new LakeTableMergeMeta(), new MemoryMetadataProvider()));
  }
}
