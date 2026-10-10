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

package org.apache.hop.beam.core.partition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Issue #2040: partitioning support for Beam pipelines. */
class BeamPartitionFnsTest {

  @BeforeAll
  static void initHopEnvironment() throws Exception {
    // The row meta is round-tripped through JSON, and JsonRowMeta.fromJson resolves the value metas
    // through ValueMetaFactory, which needs the plugin registry.
    //
    HopEnvironment.init();
    PluginRegistry.init();
  }

  /** Row meta with two Integer fields: id and customerId. */
  private static String twoIntegerFieldsMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaInteger("customerId"));
    return org.apache.hop.core.row.JsonRowMeta.toJson(rowMeta);
  }

  private static HopRow rowOf(Number id, Number customerId) {
    // HopRow's no-arg constructor asserts against a null row, so build it with the values.
    // In Hop, ValueMetaInteger values are represented by Long in row arrays.
    Long idVal = id == null ? null : id.longValue();
    Long custVal = customerId == null ? null : customerId.longValue();
    return new HopRow(new Object[] {idVal, custVal});
  }

  @Test
  void theSinglePartitionFunctionAlwaysPutsEverythingInPartitionZero() {
    SinglePartitionFn fn = new SinglePartitionFn();
    HopRow row = rowOf(1, 100);

    // Regardless of how many partitions there are, every row lands in the first one. That is what
    // makes the single-partition mode useful: it forces the following steps to run sequentially.
    //
    for (int numPartitions = 1; numPartitions <= 5; numPartitions++) {
      assertEquals(0, fn.partitionFor(row, numPartitions));
    }
  }

  private static HopRow rowOfString(Integer id, String customerName) {
    return new HopRow(new Object[] {id, customerName});
  }

  @Test
  void theKeyedPartitionFunctionGivesTheSameKeyTheSamePartition() {
    KeyedPartitionFn fn = new KeyedPartitionFn("partition", twoIntegerFieldsMeta(), 1);

    int first = fn.partitionFor(rowOf(1, 500), 8);
    int second = fn.partitionFor(rowOf(2, 500), 8);

    // Rows sharing a key must share a partition, otherwise the partitioning would group nothing
    // and the option would silently do nothing.
    //
    assertEquals(first, second, "the same key must land in the same partition");
    assertTrue(first >= 0 && first < 8, "partition out of range: " + first);
  }

  @Test
  void differentKeysAreSpreadOverThePartitions() {
    KeyedPartitionFn fn = new KeyedPartitionFn("partition", twoIntegerFieldsMeta(), 1);

    java.util.Set<Integer> partitions = new java.util.HashSet<>();
    for (int customerId = 0; customerId < 100; customerId++) {
      partitions.add(fn.partitionFor(rowOf(customerId, customerId), 8));
    }

    assertEquals(
        8, partitions.size(), "expected the keys to use all 8 partitions, got: " + partitions);
  }

  @Test
  void theKeyedPartitionFunctionDealsWithANullKey() {
    KeyedPartitionFn fn = new KeyedPartitionFn("partition", twoIntegerFieldsMeta(), 1);

    // A null key must not take the pipeline down; all such rows share one partition.
    //
    HopRow nullKey = rowOf(1, null);

    int partition = fn.partitionFor(nullKey, 4);
    assertEquals(0, partition);
  }

  @Test
  void theKeyedPartitionFunctionSurvivesAZeroPartitionCount() {
    KeyedPartitionFn fn = new KeyedPartitionFn("partition", twoIntegerFieldsMeta(), 1);

    // Beam always passes a positive count, but a zero would make the modulo throw. Guard it rather
    // than let an ArithmeticException surface from inside the runner.
    //
    assertEquals(0, fn.partitionFor(rowOf(1, 1), 0));
  }

  @Test
  void thePartitionFunctionsAreSerializable() {
    // Beam ships the PartitionFn to the workers, so it has to survive serialization.
    //
    KeyedPartitionFn keyed = new KeyedPartitionFn("partition", twoIntegerFieldsMeta(), 1);
    KeyedPartitionFn copy = org.apache.commons.lang3.SerializationUtils.clone(keyed);
    assertEquals(
        keyed.partitionFor(rowOf(1, 77), 8),
        copy.partitionFor(rowOf(1, 77), 8),
        "a deserialized function has to partition identically");

    org.apache.commons.lang3.SerializationUtils.clone(new SinglePartitionFn());
  }

  @Test
  void thePartitionFunctionsImplementTheBeamInterface() {
    // Compile-time check that both are usable as Beam partition functions.
    //
    Partition.PartitionFn<HopRow> single = new SinglePartitionFn();
    assertEquals(0, single.partitionFor(rowOf(1, 1), 2));

    Partition.PartitionFn<HopRow> keyed =
        new KeyedPartitionFn("partition", twoIntegerFieldsMeta(), 1);
    assertTrue(keyed.partitionFor(rowOf(1, 1), 2) >= 0);
  }

  @Test
  void aStringKeyFieldWorksToo() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("customerName"));
    KeyedPartitionFn fn =
        new KeyedPartitionFn("partition", org.apache.hop.core.row.JsonRowMeta.toJson(rowMeta), 1);

    HopRow first = rowOfString(1, "Alice");
    HopRow second = rowOfString(2, "Alice");

    assertEquals(
        fn.partitionFor(first, 4),
        fn.partitionFor(second, 4),
        "the same name must land in the same partition");
  }

  @Test
  void binaryKeyFieldHashesIdenticalByteArraysToTheSamePartition() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaBinary("payload"));
    KeyedPartitionFn fn =
        new KeyedPartitionFn("partition", org.apache.hop.core.row.JsonRowMeta.toJson(rowMeta), 1);

    HopRow first = new HopRow(new Object[] {1, new byte[] {1, 2, 3, 4}});
    HopRow second = new HopRow(new Object[] {2, new byte[] {1, 2, 3, 4}});

    assertEquals(
        fn.partitionFor(first, 8),
        fn.partitionFor(second, 8),
        "distinct byte[] instances with identical contents must land in the same partition");
  }

  @Test
  void bigDecimalKeyFieldIgnoresScaleDifferencesInPartitioning() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaBigNumber("amount"));
    KeyedPartitionFn fn =
        new KeyedPartitionFn("partition", org.apache.hop.core.row.JsonRowMeta.toJson(rowMeta), 1);

    HopRow first = new HopRow(new Object[] {1, new BigDecimal("10.0")});
    HopRow second = new HopRow(new Object[] {2, new BigDecimal("10.00")});

    assertEquals(
        fn.partitionFor(first, 8),
        fn.partitionFor(second, 8),
        "BigDecimals representing equal numbers with different scales must land in the same partition");
  }
}
