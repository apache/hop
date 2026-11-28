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

package org.apache.hop.spark.core;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.Test;

class HopSparkRowConverterLazyTest {

  /** CSV input with lazy conversion: the value is the raw bytes, the storage metadata a String. */
  private static RowMeta lazyRowMeta() {
    ValueMetaInteger id = new ValueMetaInteger("id");
    id.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
    id.setStorageMetadata(new ValueMetaString("id"));
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(id);
    rowMeta.addValueMeta(new ValueMetaString("name"));
    return rowMeta;
  }

  @Test
  void lazyValuesLeaveThePartitionInNormalStorage() throws Exception {
    Object[] row = {"42".getBytes(StandardCharsets.UTF_8), "x"};
    Object[] normal = HopSparkRowConverter.toNormalStorage(lazyRowMeta(), row);
    assertEquals(42L, normal[0]);
    assertEquals("x", normal[1]);
    // The original row keeps its bytes: it can still travel on in the transform's pipeline
    assertArrayEquals("42".getBytes(StandardCharsets.UTF_8), (byte[]) row[0]);
  }

  @Test
  void aRowInNormalStorageIsNotCopied() throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    Object[] row = {7L};
    assertSame(row, HopSparkRowConverter.toNormalStorage(rowMeta, row));
  }

  @Test
  void aValueOfAnotherJavaTypeThanItsFieldIsConverted() throws Exception {
    // YAML input, XML join ... write a String into a field they declare as a number or a date
    assertEquals(1.5d, HopSparkRowConverter.toSparkValue(new ValueMetaNumber("n"), "1.5"));
    assertEquals(42L, HopSparkRowConverter.toSparkValue(new ValueMetaInteger("i"), "42"));
    ValueMetaDate date = new ValueMetaDate("d");
    date.setConversionMask("yyyy-MM-dd");
    Object converted = HopSparkRowConverter.toSparkValue(date, "2020-02-28");
    assertEquals(Timestamp.class, converted.getClass());
  }
}
