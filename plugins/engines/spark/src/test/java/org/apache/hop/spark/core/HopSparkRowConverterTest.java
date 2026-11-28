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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Date;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

class HopSparkRowConverterTest {

  @Test
  void roundTripBasicTypesIncludingNulls() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("s"));
    rowMeta.addValueMeta(new ValueMetaInteger("i"));
    rowMeta.addValueMeta(new ValueMetaNumber("n"));
    rowMeta.addValueMeta(new ValueMetaBoolean("b"));
    rowMeta.addValueMeta(new ValueMetaDate("d"));
    rowMeta.addValueMeta(new ValueMetaBigNumber("bn"));
    rowMeta.addValueMeta(new ValueMetaBinary("bin"));

    Date now = new Date(1_700_000_000_000L);
    Object[] hopRow = {
      "hello",
      42L,
      3.5d,
      true,
      now,
      new BigDecimal("123.45"),
      "bytes".getBytes(StandardCharsets.UTF_8)
    };

    StructType schema = HopSparkRowConverter.toStructType(rowMeta);
    assertEquals(7, schema.fields().length);
    assertEquals(DataTypes.StringType, schema.apply("s").dataType());
    assertEquals(DataTypes.LongType, schema.apply("i").dataType());
    assertEquals(DataTypes.DoubleType, schema.apply("n").dataType());
    assertEquals(DataTypes.BooleanType, schema.apply("b").dataType());
    assertEquals(DataTypes.TimestampType, schema.apply("d").dataType());
    assertTrue(HopSparkRowConverter.isDecimal(schema.apply("bn").dataType()));
    assertEquals(DataTypes.BinaryType, schema.apply("bin").dataType());

    Row sparkRow = HopSparkRowConverter.toSparkRow(rowMeta, hopRow);
    Object[] back = HopSparkRowConverter.toHopRow(rowMeta, sparkRow);

    assertEquals("hello", back[0]);
    assertEquals(42L, back[1]);
    assertEquals(3.5d, (Double) back[2], 0.0001);
    assertEquals(true, back[3]);
    assertEquals(now.getTime(), ((Date) back[4]).getTime());
    assertEquals(0, new BigDecimal("123.45").compareTo((BigDecimal) back[5]));
    assertArrayEquals("bytes".getBytes(StandardCharsets.UTF_8), (byte[]) back[6]);
  }

  @Test
  void toHopRowPadsWhenSparkRowIsShorterThanMeta() throws Exception {
    IRowMeta wide = new RowMeta();
    wide.addValueMeta(new ValueMetaString("s"));
    wide.addValueMeta(new ValueMetaInteger("i"));
    wide.addValueMeta(new ValueMetaString("extra"));
    Object[] padded =
        HopSparkRowConverter.toHopRow(wide, org.apache.spark.sql.RowFactory.create("only"));
    assertEquals("only", padded[0]);
    assertNull(padded[1]);
    assertNull(padded[2]);
  }

  @Test
  void nullValuesSurviveRoundTrip() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("s"));
    rowMeta.addValueMeta(new ValueMetaInteger("i"));

    Row sparkRow = HopSparkRowConverter.toSparkRow(rowMeta, new Object[] {null, null});
    Object[] back = HopSparkRowConverter.toHopRow(rowMeta, sparkRow);
    assertNull(back[0]);
    assertNull(back[1]);
  }

  /** ValueMetaTimestamp casts to java.sql.Timestamp: Spark gives one back, with its micros. */
  @Test
  void aTimestampStaysATimestampWithItsMicroseconds() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaTimestamp("moment"));
    rowMeta.addValueMeta(new ValueMetaDate("day"));
    Timestamp moment = new Timestamp(1_700_000_000_123L);
    moment.setNanos(123_456_789);

    Row sparkRow =
        HopSparkRowConverter.toSparkRow(
            rowMeta, new Object[] {moment, new Date(1_700_000_000_000L)});
    assertEquals(123_456_789, ((Timestamp) sparkRow.get(0)).getNanos());
    Object[] back = HopSparkRowConverter.toHopRow(rowMeta, sparkRow);
    assertEquals(Timestamp.class, back[0].getClass());
    assertEquals(123_456_789, ((Timestamp) back[0]).getNanos());
    assertEquals(moment, ((ValueMetaTimestamp) rowMeta.getValueMeta(0)).getTimestamp(back[0]));
    assertEquals(Date.class, back[1].getClass());

    // A plain Date in a Timestamp field, as some transforms write one
    Row fromDate =
        HopSparkRowConverter.toSparkRow(rowMeta, new Object[] {new Date(1_700_000_000_000L), null});
    assertEquals(
        new Timestamp(1_700_000_000_000L), HopSparkRowConverter.toHopRow(rowMeta, fromDate)[0]);
  }

  /** A value type of a plugin, such as Neo4j's Graph: a value it reads from its own text. */
  static final class PointValueMeta extends ValueMetaBase {
    static final int TYPE_POINT = 9901;

    PointValueMeta(String name) {
      super(name, TYPE_POINT);
    }

    @Override
    public String getString(Object object) {
      long[] point = (long[]) object;
      return point[0] + "," + point[1];
    }

    @Override
    public Object convertData(IValueMeta meta2, Object data2) throws HopValueException {
      String[] parts = meta2.getString(data2).split(",");
      return new long[] {Long.parseLong(parts[0]), Long.parseLong(parts[1])};
    }
  }

  @Test
  void aPluginValueTypeTravelsAsTextAndComesBack() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new PointValueMeta("point"));

    StructType schema = HopSparkRowConverter.toStructType(rowMeta);
    assertEquals(DataTypes.StringType, schema.fields()[0].dataType());

    Row sparkRow = HopSparkRowConverter.toSparkRow(rowMeta, new Object[] {new long[] {3, 4}});
    assertEquals("3,4", sparkRow.getString(0));

    Object[] back = HopSparkRowConverter.toHopRow(rowMeta, sparkRow);
    assertArrayEquals(new long[] {3, 4}, (long[]) back[0]);
    // The per-partition codec and a fused chain's hand-off give the same value
    HopSparkRowConverter.RowCodec codec = HopSparkRowConverter.RowCodec.of(rowMeta);
    assertArrayEquals(
        new long[] {3, 4},
        (long[]) codec.toHop(codec.toSpark(new Object[] {new long[] {3, 4}}))[0]);
    assertArrayEquals(
        new long[] {3, 4}, (long[]) codec.handOff(new Object[] {new long[] {3, 4}})[0]);
  }
}
