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

package org.apache.hop.parquet.transforms.input;

import java.io.OutputStream;
import java.util.List;
import java.util.function.Consumer;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.parquet.transforms.output.ParquetOutputFile;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

/**
 * Writes Parquet files with an arbitrary schema for the input tests, through Hop VFS so the same
 * helper serves local files and in-memory {@code ram://} files.
 */
final class ParquetTestFiles {

  private ParquetTestFiles() {}

  /** Writes one file with the given schema; each row consumer fills one group. */
  @SafeVarargs
  static void write(String filename, MessageType schema, Consumer<Group>... rows) throws Exception {
    SimpleGroupFactory factory = new SimpleGroupFactory(schema);
    try (OutputStream outputStream = HopVfs.getOutputStream(filename, false);
        ParquetWriter<Group> writer =
            ExampleParquetWriter.builder(new ParquetOutputFile(outputStream))
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withWriteMode(ParquetFileWriter.Mode.CREATE)
                .build()) {
      for (Consumer<Group> row : List.of(rows)) {
        Group group = factory.newGroup();
        row.accept(group);
        writer.write(group);
      }
    }
  }
}
