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

package org.apache.hop.beam.core.partition;

import java.io.Serial;
import java.util.Objects;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.JsonRowMeta;

/**
 * Issue #2040: assigns a row to a partition based on the value of a key field.
 *
 * <p>Rows sharing a key always land in the same partition, which is what makes the following
 * per-key steps actually group anything.
 *
 * <p>The value meta is carried as JSON rather than as an {@link IValueMeta} because the partition
 * function is serialized to the workers, and an {@link IValueMeta} does not survive that; the JSON
 * form does, and the meta is rebuilt lazily on first use.
 */
public class KeyedPartitionFn implements Partition.PartitionFn<HopRow> {
  @Serial private static final long serialVersionUID = 95100000000000002L;

  private final String transformName;
  private final String rowMetaJson;
  private final int keyIndex;

  private transient IValueMeta keyValueMeta;

  public KeyedPartitionFn(String transformName, String rowMetaJson, int keyIndex) {
    this.transformName = transformName;
    this.rowMetaJson = rowMetaJson;
    this.keyIndex = keyIndex;
  }

  @Override
  public int partitionFor(HopRow elem, int numPartitions) {
    try {
      if (keyValueMeta == null) {
        IRowMeta rowMeta = JsonRowMeta.fromJson(rowMetaJson);
        keyValueMeta = rowMeta.getValueMeta(keyIndex);
      }

      Object key = elem.getRow()[keyIndex];

      // A null key still has to land somewhere: all such rows share one partition rather than
      // failing the pipeline.
      //
      if (numPartitions < 1) {
        return 0;
      }
      if (key == null) {
        return 0;
      }

      // Math.abs is not used: it returns a negative number for Integer.MIN_VALUE, which would
      // produce an invalid partition index.
      //
      int hash = Objects.hashCode(key);
      return Math.floorMod(hash, numPartitions);

    } catch (RuntimeException e) {
      throw new RuntimeException(
          "Unable to determine the partition for transform '" + transformName + "'", e);
    }
  }
}
