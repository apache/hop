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

package org.apache.hop.beam.transforms.partition;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Issue #2040: metadata for the Beam partition transform. */
class BeamPartitionMetaTest {

  @BeforeAll
  static void initHopEnvironment() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  void theDefaultsAreASinglePartition() {
    // The zero-value has to be usable, so that a pipeline that was saved without a partitioning
    // mode still builds: one partition means "do not partition".
    //
    BeamPartitionMeta meta = new BeamPartitionMeta();

    assertEquals(
        org.apache.hop.beam.core.BeamDefaults.PARTITION_TYPE_SINGLE, meta.getPartitionType());
  }

  @Test
  void thePartitionTypeRoundTrips() {
    BeamPartitionMeta meta = new BeamPartitionMeta();
    meta.setPartitionType(org.apache.hop.beam.core.BeamDefaults.PARTITION_TYPE_KEY);
    meta.setKeyField("customerId");
    meta.setNumPartitions("8");

    assertEquals(org.apache.hop.beam.core.BeamDefaults.PARTITION_TYPE_KEY, meta.getPartitionType());
    assertEquals("customerId", meta.getKeyField());
    assertEquals("8", meta.getNumPartitions());
  }

  @Test
  void setDefaultClearsEverything() {
    BeamPartitionMeta meta = new BeamPartitionMeta();
    meta.setPartitionType(org.apache.hop.beam.core.BeamDefaults.PARTITION_TYPE_KEY);
    meta.setKeyField("customerId");
    meta.setNumPartitions("8");

    meta.setDefault();

    assertEquals(
        org.apache.hop.beam.core.BeamDefaults.PARTITION_TYPE_SINGLE, meta.getPartitionType());
    assertEquals("", meta.getKeyField());
    assertEquals("1", meta.getNumPartitions());
  }

  @Test
  void partitioningDoesNotChangeTheRowLayout() throws Exception {
    // Partitioning only decides which worker gets a row; it never adds or removes columns.
    //
    BeamPartitionMeta meta = new BeamPartitionMeta();
    meta.setPartitionType(org.apache.hop.beam.core.BeamDefaults.PARTITION_TYPE_KEY);
    meta.setKeyField("customerId");

    org.apache.hop.core.row.IRowMeta rowMeta = new org.apache.hop.core.row.RowMeta();
    rowMeta.addValueMeta(new org.apache.hop.core.row.value.ValueMetaInteger("id"));
    rowMeta.addValueMeta(new org.apache.hop.core.row.value.ValueMetaInteger("customerId"));

    // getFields writes into rowMeta in place, so compare against the size before the call.
    //
    int sizeBefore = rowMeta.size();
    meta.getFields(
        rowMeta, "Partition", null, null, new org.apache.hop.core.variables.Variables(), null);

    assertEquals(sizeBefore, rowMeta.size(), "partitioning must not add or drop columns");
  }
}
