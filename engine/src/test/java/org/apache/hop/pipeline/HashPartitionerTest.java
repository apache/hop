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

package org.apache.hop.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.partition.PartitionSchema;
import org.apache.hop.pipeline.transform.TransformPartitioningMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class HashPartitionerTest {

  private static TransformPartitioningMeta partitioning(String method, PartitionSchema schema)
      throws HopException {
    return new TransformPartitioningMeta(method, schema);
  }

  private static PartitionSchema static3() {
    return new PartitionSchema("static3", List.of("P1", "P2", "P3"));
  }

  private static IRowMeta rowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("customer"));
    rowMeta.addValueMeta(new ValueMetaInteger("day"));
    rowMeta.addValueMeta(new ValueMetaString("other"));
    return rowMeta;
  }

  @Test
  void thePartitioningFieldsSurviveThePipelineXml() throws Exception {
    TransformPartitioningMeta partitioning = partitioning(HashPartitioner.ID, static3());
    HashPartitioner hash = (HashPartitioner) partitioning.getPartitioner();
    hash.getFields().add(new HashPartitionField("customer"));
    hash.getFields().add(new HashPartitionField("day"));

    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(PartitionSchema.class).save(static3());
    TransformPartitioningMeta loaded =
        new TransformPartitioningMeta(
            XmlHandler.loadXmlString(partitioning.getXml(), "partitioning"), metadataProvider);

    HashPartitioner loadedHash = assertInstanceOf(HashPartitioner.class, loaded.getPartitioner());
    assertEquals(List.of("customer", "day"), loadedHash.getFieldNames());
    assertEquals(List.of("customer", "day"), hash.clone().getFieldNames());
  }

  @Test
  void rowsWithTheSameKeyGoToTheSamePartition() throws Exception {
    TransformPartitioningMeta partitioning = partitioning(HashPartitioner.ID, static3());
    HashPartitioner hash = (HashPartitioner) partitioning.getPartitioner();
    hash.getFields().add(new HashPartitionField("customer"));
    hash.getFields().add(new HashPartitionField("day"));
    hash.setMeta(partitioning);

    Map<String, Integer> partitionOfKey = new HashMap<>();
    int[] rowsPerPartition = new int[3];
    for (int i = 0; i < 300; i++) {
      Object[] row = {"c" + (i % 7), (long) (i % 5), "x" + i};
      int partition = hash.getPartition(new Variables(), rowMeta(), row);
      rowsPerPartition[partition]++;
      Integer earlier = partitionOfKey.putIfAbsent(row[0] + "/" + row[1], partition);
      if (earlier != null) {
        assertEquals(earlier, partition, "key " + row[0] + "/" + row[1]);
      }
    }
    // 35 keys over 3 partitions: every partition gets some
    for (int rows : rowsPerPartition) {
      assertTrue(rows > 0);
    }
  }

  @Test
  void aMissingFieldIsAnError() throws Exception {
    TransformPartitioningMeta partitioning = partitioning(HashPartitioner.ID, static3());
    HashPartitioner hash = (HashPartitioner) partitioning.getPartitioner();
    hash.getFields().add(new HashPartitionField("nope"));
    hash.setMeta(partitioning);

    assertThrows(
        HopException.class,
        () -> hash.getPartition(new Variables(), rowMeta(), new Object[] {"a", 1L, "b"}));
  }

  @Test
  void rebalanceHandsTheRowsOutInTurn() throws Exception {
    TransformPartitioningMeta partitioning = partitioning(RebalancePartitioner.ID, static3());
    IPartitioner rebalance = partitioning.getPartitioner();
    rebalance.setMeta(partitioning);

    int[] rowsPerPartition = new int[3];
    for (int i = 0; i < 9; i++) {
      // The same row each time: the values do not matter
      rowsPerPartition[rebalance.getPartition(new Variables(), rowMeta(), new Object[3])]++;
    }
    assertEquals(
        List.of(3, 3, 3), List.of(rowsPerPartition[0], rowsPerPartition[1], rowsPerPartition[2]));
    assertNull(rebalance.getDialogClassName());
  }
}
