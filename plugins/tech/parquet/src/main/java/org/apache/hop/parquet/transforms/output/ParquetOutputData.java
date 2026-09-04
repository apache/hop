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

package org.apache.hop.parquet.transforms.output;

import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.io.CountingOutputStream;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.MessageType;

@SuppressWarnings("java:S1104")
public class ParquetOutputData extends BaseTransformData implements ITransformData {
  public List<Integer> sourceFieldIndexes;
  public List<ParquetField> outputFields;
  public Configuration conf;
  public ParquetProperties props;
  public String filename;
  public OutputStream outputStream;
  public CountingOutputStream countingStream;
  public ParquetOutputFile outputFile;
  public ParquetWriter<RowMetaAndData> writer;
  public int split = 0;
  public long splitRowCount;
  public long maxSplitSizeRows;
  public int rowGroupSize;
  public int pageSize;
  public int dictionaryPageSize;
  public Schema avroSchema;

  /** The Parquet schema, built once and shared by every partition file. */
  public MessageType messageType;

  /** Indexes of the input fields the output is partitioned by, in the configured order. */
  public List<Integer> partitionFieldIndexes;

  /**
   * The writer per partition folder, keyed by the relative {@code name=value/...} path. Access
   * order is maintained so the least recently written partition can be closed first when {@link
   * #maxOpenPartitions} is reached.
   */
  public Map<String, PartitionWriter> partitionWriters;

  /** Partition folders already emptied during this run, for the overwrite-partitions mode. */
  public Set<String> clearedPartitions;

  /** Whether the base folder has been emptied yet, for the overwrite-all mode. */
  public boolean baseFolderCleared;

  /** How many partitions may be written to at the same time. */
  public int maxOpenPartitions;

  /** Number of partition files opened so far, used to keep their names unique. */
  public int partitionFileNr;

  /** Short token unique to this run, so a later run does not overwrite this one's files. */
  public String runToken;

  public ParquetOutputData() {
    super();
  }

  /** Everything needed to write, measure and close one partition folder's current file. */
  public static class PartitionWriter {
    public String filename;
    public OutputStream outputStream;
    public CountingOutputStream countingStream;
    public ParquetOutputFile outputFile;
    public ParquetWriter<RowMetaAndData> writer;
    public long rowCount;

    public PartitionWriter(
        String filename,
        OutputStream outputStream,
        CountingOutputStream countingStream,
        ParquetOutputFile outputFile,
        ParquetWriter<RowMetaAndData> writer) {
      this.filename = filename;
      this.outputStream = outputStream;
      this.countingStream = countingStream;
      this.outputFile = outputFile;
      this.writer = writer;
    }
  }

  /**
   * Creates the partition writer map with access-order iteration for least-recently-used eviction.
   */
  public Map<String, PartitionWriter> newPartitionWriterMap() {
    return new LinkedHashMap<>(16, 0.75f, true);
  }
}
