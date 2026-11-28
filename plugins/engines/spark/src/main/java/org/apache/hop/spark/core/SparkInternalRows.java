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

package org.apache.hop.spark.core;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Date;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.CatalystTypeConverters;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;
import scala.Function1;

/**
 * Rows between Spark and a Hop transform as Catalyst {@link InternalRow}s instead of Spark's
 * external {@link Row}: the partition is read from {@code queryExecution().toRdd()} and the output
 * becomes a DataFrame through {@code internalCreateDataFrame}. That skips the external-row encoder
 * on both sides; above all a string is decoded once, straight from the bytes Spark holds, instead
 * of being copied first. Values convert exactly as through {@link HopSparkRowConverter.RowCodec}
 * and the row encoder.
 */
public final class SparkInternalRows {

  /**
   * 1900-01-01T00:00:00Z. Spark rebases timestamps between its calendar and the one of {@code
   * java.sql.Timestamp} for old dates (the Julian cutover, and local mean time in some zones); from
   * here on the rebase is a no-op and micros = millis * 1000.
   */
  static final long NO_REBASE_FROM_MILLIS = -2208988800000L;

  private SparkInternalRows() {}

  /** Whether the Catalyst row path can be used: classic (not Spark Connect) Datasets. */
  public static boolean supported(Dataset<Row> dataset, SparkSession spark) {
    return dataset instanceof org.apache.spark.sql.classic.Dataset
        && spark instanceof org.apache.spark.sql.classic.SparkSession;
  }

  /** The Dataset's rows as Catalyst rows. A row object can be reused: read it before the next. */
  public static RDD<InternalRow> toRdd(Dataset<Row> dataset) {
    return ((org.apache.spark.sql.classic.Dataset<Row>) dataset).queryExecution().toRdd();
  }

  /** A DataFrame over Catalyst rows that match {@code schema}. */
  public static Dataset<Row> createDataFrame(
      SparkSession spark, RDD<InternalRow> rows, StructType schema) {
    return ((org.apache.spark.sql.classic.SparkSession) spark)
        .internalCreateDataFrame(rows, schema, false);
  }

  /** {@code utf8.toString()}, without the intermediate copy when the bytes are on the heap. */
  static String decode(UTF8String utf8) {
    if (utf8.getBaseObject() instanceof byte[] bytes) {
      int offset = (int) (utf8.getBaseOffset() - Platform.BYTE_ARRAY_OFFSET);
      return new String(bytes, offset, utf8.numBytes(), StandardCharsets.UTF_8);
    }
    return utf8.toString();
  }

  /** Converts Catalyst rows of one schema to Hop rows of one row layout, and back. */
  public static final class Codec {
    private static final int OTHER = 0;
    private static final int LONG = 1;
    private static final int DOUBLE = 2;
    private static final int STRING = 3;
    private static final int BOOLEAN = 4;
    private static final int TIMESTAMP = 5;

    private final IValueMeta[] metas;
    private final int[] hopTypes;
    private final DataType[] sparkTypes;
    private final int[] kinds;
    private final Function1<Object, Object>[] toScala;
    private final Function1<Object, Object>[] toCatalyst;

    @SuppressWarnings("unchecked")
    private Codec(IRowMeta rowMeta, StructType schema) {
      int n = rowMeta.size();
      metas = new IValueMeta[n];
      hopTypes = new int[n];
      sparkTypes = new DataType[n];
      kinds = new int[n];
      toScala = new Function1[n];
      toCatalyst = new Function1[n];
      for (int i = 0; i < n; i++) {
        metas[i] = rowMeta.getValueMeta(i);
        hopTypes[i] = metas[i].getType();
        DataType t = i < schema.size() ? schema.fields()[i].dataType() : DataTypes.NullType;
        sparkTypes[i] = t;
        if (t.equals(DataTypes.LongType)) {
          kinds[i] = LONG;
        } else if (t.equals(DataTypes.DoubleType)) {
          kinds[i] = DOUBLE;
        } else if (t.equals(DataTypes.StringType)) {
          kinds[i] = STRING;
        } else if (t.equals(DataTypes.BooleanType)) {
          kinds[i] = BOOLEAN;
        } else if (t.equals(DataTypes.TimestampType)) {
          kinds[i] = TIMESTAMP;
        } else {
          kinds[i] = OTHER;
        }
        toScala[i] = CatalystTypeConverters.createToScalaConverter(t);
        toCatalyst[i] = CatalystTypeConverters.createToCatalystConverter(t);
      }
    }

    /** For rows of {@code schema} read as {@code rowMeta}. */
    public static Codec of(IRowMeta rowMeta, StructType schema) {
      return new Codec(rowMeta, schema);
    }

    /** For rows written from {@code rowMeta}, in its Spark form. */
    public static Codec of(IRowMeta rowMeta) throws HopException {
      return new Codec(rowMeta, HopSparkRowConverter.toStructType(rowMeta));
    }

    /** A Catalyst row as a Hop row: what {@code RowCodec.toHop} gives for the external row. */
    public Object[] toHop(InternalRow row) throws HopException {
      Object[] hopRow = new Object[metas.length];
      int available = Math.min(metas.length, row.numFields());
      for (int i = 0; i < available; i++) {
        if (row.isNullAt(i)) {
          continue;
        }
        int hopType = hopTypes[i];
        hopRow[i] =
            switch (kinds[i]) {
              case LONG ->
                  hopType == IValueMeta.TYPE_INTEGER
                      ? (Object) row.getLong(i)
                      : HopSparkRowConverter.toHopValue(metas[i], row.getLong(i));
              case DOUBLE ->
                  hopType == IValueMeta.TYPE_NUMBER
                      ? (Object) row.getDouble(i)
                      : HopSparkRowConverter.toHopValue(metas[i], row.getDouble(i));
              case STRING -> {
                String s = decode(row.getUTF8String(i));
                yield hopType == IValueMeta.TYPE_STRING
                    ? s
                    : HopSparkRowConverter.toHopValue(metas[i], s);
              }
              case BOOLEAN ->
                  hopType == IValueMeta.TYPE_BOOLEAN
                      ? (Object) row.getBoolean(i)
                      : HopSparkRowConverter.toHopValue(metas[i], row.getBoolean(i));
              case TIMESTAMP -> {
                long micros = row.getLong(i);
                // The external value is a java.sql.Timestamp; RowCodec makes a Date of its millis
                yield hopType == IValueMeta.TYPE_DATE && micros >= NO_REBASE_FROM_MILLIS * 1000L
                    ? new Date(Math.floorDiv(micros, 1000L))
                    : HopSparkRowConverter.toHopValue(metas[i], toScala[i].apply(micros));
              }
              default ->
                  HopSparkRowConverter.toHopValue(
                      metas[i], toScala[i].apply(row.get(i, sparkTypes[i])));
            };
      }
      return hopRow;
    }

    /** A Hop row as a Catalyst row: what the row encoder makes of {@code RowCodec.toSpark}. */
    public InternalRow toInternal(Object[] hopRow) throws HopException {
      return new GenericInternalRow(catalystValues(hopRow, 0));
    }

    /** As {@link #toInternal}, after a leading target-stream tag column. */
    public InternalRow toTaggedInternal(String target, Object[] hopRow) throws HopException {
      Object[] values = catalystValues(hopRow, 1);
      values[0] = UTF8String.fromString(target != null ? target : HopSparkUtil.MAIN_TARGET_TAG);
      return new GenericInternalRow(values);
    }

    private Object[] catalystValues(Object[] hopRow, int offset) throws HopException {
      Object[] values = new Object[metas.length + offset];
      int available = hopRow == null ? 0 : Math.min(metas.length, hopRow.length);
      for (int i = 0; i < available; i++) {
        Object h = hopRow[i];
        if (h == null) {
          continue;
        }
        // The common cases straight from the Hop value: what RowCodec.toSpark and the row encoder
        // would make of it
        Object direct = direct(i, h);
        if (direct != null) {
          values[i + offset] = direct;
          continue;
        }
        Object v = HopSparkRowConverter.toSparkValue(metas[i], h);
        if (v == null) {
          continue;
        }
        values[i + offset] =
            switch (kinds[i]) {
              case LONG, DOUBLE, BOOLEAN -> v;
              case STRING -> UTF8String.fromString((String) v);
              case TIMESTAMP -> {
                Timestamp ts = (Timestamp) v;
                long millis = ts.getTime();
                yield millis >= NO_REBASE_FROM_MILLIS && ts.getNanos() % 1_000_000 == 0
                    ? (Object) (millis * 1000L)
                    : toCatalyst[i].apply(v);
              }
              default -> toCatalyst[i].apply(v);
            };
      }
      return values;
    }

    /** The Catalyst value of a plain Hop value of the field's own type, or null: convert. */
    private Object direct(int i, Object h) {
      if (metas[i].getStorageType() != IValueMeta.STORAGE_TYPE_NORMAL) {
        return null;
      }
      return switch (kinds[i]) {
        case LONG -> hopTypes[i] == IValueMeta.TYPE_INTEGER && h instanceof Long ? h : null;
        case DOUBLE -> hopTypes[i] == IValueMeta.TYPE_NUMBER && h instanceof Double ? h : null;
        case STRING ->
            hopTypes[i] == IValueMeta.TYPE_STRING && h instanceof String s
                ? UTF8String.fromString(s)
                : null;
        case BOOLEAN -> hopTypes[i] == IValueMeta.TYPE_BOOLEAN && h instanceof Boolean ? h : null;
        case TIMESTAMP -> {
          // new Timestamp(date.getTime()), then micros: only for a plain Date after 1900
          if (hopTypes[i] == IValueMeta.TYPE_DATE && h.getClass() == Date.class) {
            long millis = ((Date) h).getTime();
            yield millis >= NO_REBASE_FROM_MILLIS ? (Object) (millis * 1000L) : null;
          }
          yield null;
        }
        default -> null;
      };
    }
  }
}
