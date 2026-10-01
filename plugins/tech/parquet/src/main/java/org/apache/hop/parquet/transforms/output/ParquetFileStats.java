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

import java.util.Locale;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;

/**
 * What a finished Parquet file looks like from the outside: how many rows and row groups it holds
 * and how large its footer is. The footer grows with every row group (one column chunk entry with
 * statistics per column per row group), and readers such as Dremio refuse footers over 16 MiB, so
 * this is what to look at when a file is rejected downstream.
 *
 * @param rows number of rows in the file
 * @param rowGroups number of row groups in the file
 * @param footerBytes size of the serialized file metadata, the same number a reader finds in the
 *     four bytes before the trailing magic
 */
record ParquetFileStats(long rows, int rowGroups, long footerBytes) {

  /** The {@code PAR1} magic that opens the file. */
  private static final int HEADER_BYTES = 4;

  /** The footer's length field plus the trailing {@code PAR1} magic. */
  private static final int TRAILER_BYTES = 8;

  /**
   * Derives the statistics of a file from the footer the writer produced and the number of bytes it
   * wrote in total. The footer starts right after the last row group, column index, offset index or
   * bloom filter, whichever comes last, and ends at the trailer.
   */
  static ParquetFileStats of(ParquetMetadata footer, long fileLength) {
    long rows = 0;
    long dataEnd = HEADER_BYTES;
    for (BlockMetaData block : footer.getBlocks()) {
      rows += block.getRowCount();
      dataEnd = Math.max(dataEnd, block.getStartingPos() + block.getCompressedSize());
      for (ColumnChunkMetaData column : block.getColumns()) {
        dataEnd = Math.max(dataEnd, end(column.getColumnIndexReference()));
        dataEnd = Math.max(dataEnd, end(column.getOffsetIndexReference()));
        if (column.getBloomFilterOffset() >= 0 && column.getBloomFilterLength() > 0) {
          dataEnd =
              Math.max(dataEnd, column.getBloomFilterOffset() + column.getBloomFilterLength());
        }
      }
    }
    long footerBytes = Math.max(0, fileLength - TRAILER_BYTES - dataEnd);
    return new ParquetFileStats(rows, footer.getBlocks().size(), footerBytes);
  }

  private static long end(IndexReference reference) {
    return reference == null ? 0 : reference.getOffset() + reference.getLength();
  }

  @Override
  public String toString() {
    return String.format(
        Locale.ROOT,
        "%,d rows in %,d row group(s), footer %,d bytes",
        rows,
        rowGroups,
        footerBytes);
  }
}
