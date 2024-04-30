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

import java.util.List;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hop.core.RowMetaAndData;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;

public class ParquetWriterBuilder
    extends ParquetWriter.Builder<RowMetaAndData, ParquetWriterBuilder> {

  private final MessageType messageType;
  private final Schema avroSchema;
  private final List<Integer> sourceFieldIndexes;
  private final List<ParquetField> fields;

  protected ParquetWriterBuilder(
      MessageType messageType,
      Schema avroSchema,
      OutputFile path,
      List<Integer> sourceFieldIndexes,
      List<ParquetField> fields) {
    super(path);
    this.messageType = messageType;
    this.avroSchema = avroSchema;
    this.sourceFieldIndexes = sourceFieldIndexes;
    this.fields = fields;
  }

  @Override
  protected ParquetWriterBuilder self() {
    return this;
  }

  @Override
  protected WriteSupport<RowMetaAndData> getWriteSupport(Configuration conf) {
    return new ParquetWriteSupport(messageType, avroSchema, sourceFieldIndexes, fields);
  }
}
