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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.io.CountingOutputStream;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;

/** Unit test for {@link ParquetFileStats} */
class ParquetFileStatsTest {

  private static final int ROWS = 20000;

  /** Writes rows through the transform's own builder and returns the file with its statistics. */
  private record Written(byte[] bytes, ParquetFileStats stats) {}

  private static Written write(int rowGroupSize) throws Exception {
    Schema avroSchema =
        SchemaBuilder.record("ApacheHopParquetSchema")
            .fields()
            .requiredLong("id")
            .requiredString("name")
            .endRecord();
    MessageType messageType = new AvroSchemaConverter().convert(avroSchema);
    List<Integer> indexes = List.of(0, 1);
    List<ParquetField> fields =
        List.of(new ParquetField("id", "id"), new ParquetField("name", "name"));

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    CountingOutputStream counting = new CountingOutputStream(bytes);
    ParquetWriter<RowMetaAndData> writer =
        new ParquetWriterBuilder(messageType, new ParquetOutputFile(counting), indexes, fields)
            .withPageSize(8192)
            .withDictionaryPageSize(ParquetProperties.DEFAULT_DICTIONARY_PAGE_SIZE)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .withRowGroupSize(rowGroupSize)
            .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
            .withWriteMode(ParquetFileWriter.Mode.CREATE)
            .build();
    for (int i = 0; i < ROWS; i++) {
      // Distinct, long-ish strings so the buffered size grows with every row.
      String name = "row-" + i + "-" + Long.toHexString(i * 0x9E3779B97F4A7C15L);
      writer.write(new RowMetaAndData(rowMeta, new Object[] {(long) i, name}));
    }
    writer.close();
    return new Written(
        bytes.toByteArray(), ParquetFileStats.of(writer.getFooter(), counting.getCount()));
  }

  /** The footer length a reader finds: the little-endian int just before the trailing magic. */
  private static int footerLengthFromTrailer(byte[] file) {
    return ByteBuffer.wrap(file, file.length - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
  }

  @Test
  void testFooterSizeMatchesTrailer() throws Exception {
    Written written = write(ParquetWriter.DEFAULT_BLOCK_SIZE);

    assertEquals(ROWS, written.stats().rows());
    assertEquals(1, written.stats().rowGroups());
    assertEquals(footerLengthFromTrailer(written.bytes()), written.stats().footerBytes());
  }

  @Test
  void testTinyRowGroupsBloatTheFooter() throws Exception {
    // The value Hop 2.3 and 2.4 wrote into every pipeline: a row count used as a byte size.
    Written tiny = write(ParquetProperties.DEFAULT_PAGE_ROW_COUNT_LIMIT);
    Written normal = write(ParquetWriter.DEFAULT_BLOCK_SIZE);

    assertEquals(footerLengthFromTrailer(tiny.bytes()), tiny.stats().footerBytes());
    assertTrue(
        tiny.stats().rowGroups() > 10, "expected many row groups, got " + tiny.stats().rowGroups());
    assertTrue(
        tiny.stats().footerBytes() > 10 * normal.stats().footerBytes(),
        "expected a much larger footer: " + tiny.stats() + " vs " + normal.stats());
  }

  @Test
  void testToStringIsReadable() {
    assertEquals(
        "1,234 rows in 5 row group(s), footer 6,789 bytes",
        new ParquetFileStats(1234, 5, 6789).toString());
  }
}
