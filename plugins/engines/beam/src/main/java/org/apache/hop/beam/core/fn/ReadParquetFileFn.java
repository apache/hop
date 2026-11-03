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

package org.apache.hop.beam.core.fn;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/**
 * Reads the Parquet file named by one incoming field and appends the configured columns.
 *
 * <p>{@code ParquetIO.readFiles} returns records without the path, so the source row would be lost.
 * This reads the file on the worker that holds the row. The path is opened with Beam {@link
 * FileSystems}, so a local path or a {@code file://}, {@code gs://}, {@code s3://} or {@code
 * hdfs://} URI works. The Avro projection is the same one {@code ParquetIO.read(schema)} would use,
 * and each value is turned into the Hop type of the column.
 */
@RequiredArgsConstructor
public class ReadParquetFileFn extends DoFn<HopRow, HopRow> {
  private final String transformName;
  private final String rowMetaJson;
  private final String filenameField;
  private final String schemaJson;
  private final List<ParquetColumnMapping> columns;

  private transient IRowMeta rowMeta;
  private transient int filenameIndex;
  private transient int inputSize;
  private transient Schema schema;
  private transient Counter readCounter;
  private transient Counter writtenCounter;
  private transient Counter errorCounter;

  @Setup
  public void setup() {
    readCounter = Metrics.counter(Pipeline.METRIC_NAME_READ, transformName);
    writtenCounter = Metrics.counter(Pipeline.METRIC_NAME_WRITTEN, transformName);
    errorCounter = Metrics.counter(Pipeline.METRIC_NAME_ERROR, transformName);
    try {
      BeamHop.init();
      rowMeta = JsonRowMeta.fromJson(rowMetaJson);
      filenameIndex = rowMeta.indexOfValue(filenameField);
      if (filenameIndex < 0) {
        throw new HopRuntimeException("Filename field '" + filenameField + "' was not found");
      }
      inputSize = rowMeta.size();
      schema = new Schema.Parser().parse(schemaJson);
      Metrics.counter(Pipeline.METRIC_NAME_INIT, transformName).inc();
    } catch (Exception e) {
      errorCounter.inc();
      throw new HopRuntimeException("Unable to initialize Parquet input " + transformName, e);
    }
  }

  @ProcessElement
  public void process(@Element HopRow row, OutputReceiver<HopRow> output) {
    String filename = null;
    try {
      filename = rowMeta.getString(row.getRow(), filenameIndex);
      if (filename == null || filename.isEmpty()) {
        return;
      }
      Configuration configuration = new Configuration(false);
      AvroReadSupport.setRequestedProjection(configuration, schema);
      try (ParquetReader<GenericRecord> reader = open(filename, configuration)) {
        GenericRecord record;
        while ((record = reader.read()) != null) {
          Object[] copy = Arrays.copyOf(row.getRow(), inputSize + columns.size());
          for (int i = 0; i < columns.size(); i++) {
            copy[inputSize + i] =
                hopValue(record.get(columns.get(i).getColumnName()), columns.get(i));
          }
          output.output(new HopRow(copy));
          readCounter.inc();
          writtenCounter.inc();
        }
      }
    } catch (Exception e) {
      errorCounter.inc();
      String where = filename == null ? "" : " '" + filename + "'";
      throw new HopRuntimeException(
          "Unable to read a Parquet file" + where + " for transform " + transformName, e);
    }
  }

  private static ParquetReader<GenericRecord> open(String filename, Configuration configuration)
      throws IOException {
    MatchResult.Metadata metadata = FileSystems.matchSingleFileSpec(filename);
    if (!metadata.isReadSeekEfficient()) {
      throw new IOException("Parquet input cannot seek in " + filename);
    }
    ReadableByteChannel opened = FileSystems.open(metadata.resourceId());
    if (!(opened instanceof SeekableByteChannel channel)) {
      opened.close();
      throw new IOException("Parquet input cannot seek in " + filename);
    }
    try {
      return AvroParquetReader.<GenericRecord>builder(
              new SeekableChannelInputFile(filename, channel))
          .withConf(configuration)
          .build();
    } catch (Exception e) {
      channel.close();
      if (e instanceof IOException ioException) {
        throw ioException;
      }
      throw new IOException("Unable to open Parquet file " + filename, e);
    }
  }

  private Object hopValue(Object value, ParquetColumnMapping column) {
    if (value == null) {
      return null;
    }
    Schema.Field field = schema.getField(column.getColumnName());
    if (field == null) {
      throw new HopRuntimeException(
          "Parquet column '" + column.getColumnName() + "' is not in the schema");
    }
    LogicalType logicalType = nonNull(field.schema()).getLogicalType();
    return switch (column.getHopType()) {
      case IValueMeta.TYPE_STRING -> value.toString();
      case IValueMeta.TYPE_INTEGER -> longValue(value);
      case IValueMeta.TYPE_NUMBER -> doubleValue(value);
      case IValueMeta.TYPE_BOOLEAN -> booleanValue(value);
      case IValueMeta.TYPE_DATE -> dateValue(value, logicalType);
      case IValueMeta.TYPE_TIMESTAMP -> timestampValue(value, logicalType);
      case IValueMeta.TYPE_BIGNUMBER -> bigNumberValue(value, logicalType);
      case IValueMeta.TYPE_BINARY -> bytesOf(value);
      default -> value;
    };
  }

  private static long longValue(Object value) {
    if (value instanceof Number number) {
      return number.longValue();
    }
    throw new HopRuntimeException("Parquet integer value is " + value.getClass().getName());
  }

  private static double doubleValue(Object value) {
    if (value instanceof Number number) {
      return number.doubleValue();
    }
    throw new HopRuntimeException("Parquet number value is " + value.getClass().getName());
  }

  private static boolean booleanValue(Object value) {
    if (value instanceof Boolean bool) {
      return bool;
    }
    throw new HopRuntimeException("Parquet boolean value is " + value.getClass().getName());
  }

  private static Date dateValue(Object value, LogicalType logicalType) {
    if (value instanceof java.sql.Timestamp timestamp) {
      return new Date(timestamp.getTime());
    }
    if (value instanceof Date date) {
      return date;
    }
    if (value instanceof Instant instant) {
      return Date.from(instant);
    }
    long raw = longValue(value);
    if (logicalType instanceof LogicalTypes.TimestampMicros) {
      return new Date(Math.floorDiv(raw, 1_000L));
    }
    return new Date(raw);
  }

  private static java.sql.Timestamp timestampValue(Object value, LogicalType logicalType) {
    if (value instanceof java.sql.Timestamp timestamp) {
      return timestamp;
    }
    if (value instanceof Instant instant) {
      return java.sql.Timestamp.from(instant);
    }
    if (value instanceof Date date) {
      return new java.sql.Timestamp(date.getTime());
    }
    long raw = longValue(value);
    if (logicalType instanceof LogicalTypes.TimestampMillis) {
      return new java.sql.Timestamp(raw);
    }
    long seconds = Math.floorDiv(raw, 1_000_000L);
    int nanos = (int) (Math.floorMod(raw, 1_000_000L) * 1_000L);
    return java.sql.Timestamp.from(Instant.ofEpochSecond(seconds, nanos));
  }

  private static BigDecimal bigNumberValue(Object value, LogicalType logicalType) {
    if (value instanceof BigDecimal decimal) {
      return decimal;
    }
    if (value instanceof CharSequence text) {
      return new BigDecimal(text.toString());
    }
    int scale = 0;
    if (logicalType instanceof LogicalTypes.Decimal decimal) {
      scale = decimal.getScale();
    }
    return new BigDecimal(new BigInteger(bytesOf(value)), scale);
  }

  private static byte[] bytesOf(Object value) {
    if (value instanceof byte[] bytes) {
      return bytes;
    }
    if (value instanceof ByteBuffer buffer) {
      ByteBuffer duplicate = buffer.duplicate();
      byte[] bytes = new byte[duplicate.remaining()];
      duplicate.get(bytes);
      return bytes;
    }
    throw new HopRuntimeException("Parquet binary value is " + value.getClass().getName());
  }

  private static Schema nonNull(Schema type) {
    if (type.getType() != Schema.Type.UNION) {
      return type;
    }
    for (Schema candidate : type.getTypes()) {
      if (candidate.getType() != Schema.Type.NULL) {
        return candidate;
      }
    }
    return type;
  }

  /**
   * One seekable Beam channel as the Parquet {@link InputFile} {@code ParquetIO} builds around
   * {@code ReadableFile.openSeekable()}.
   */
  private static final class SeekableChannelInputFile implements InputFile {
    private final String filename;
    private final SeekableByteChannel channel;

    private SeekableChannelInputFile(String filename, SeekableByteChannel channel) {
      this.filename = filename;
      this.channel = channel;
    }

    @Override
    public long getLength() throws IOException {
      return channel.size();
    }

    @Override
    public SeekableInputStream newStream() {
      return new DelegatingSeekableInputStream(Channels.newInputStream(channel)) {
        @Override
        public long getPos() throws IOException {
          return channel.position();
        }

        @Override
        public void seek(long newPos) throws IOException {
          channel.position(newPos);
        }
      };
    }

    @Override
    public String toString() {
      return filename;
    }
  }
}
