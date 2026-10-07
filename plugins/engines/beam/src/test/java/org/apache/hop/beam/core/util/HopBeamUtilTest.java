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

package org.apache.hop.beam.core.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.Test;

class HopBeamUtilTest {

  /** Mimics a CSV File Input field with lazy conversion enabled. */
  private static IValueMeta lazy(IValueMeta valueMeta) {
    valueMeta.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
    ValueMetaString storageMetadata = new ValueMetaString(valueMeta.getName());
    storageMetadata.setConversionMask(valueMeta.getConversionMask());
    storageMetadata.setTrimType(valueMeta.getTrimType());
    valueMeta.setStorageMetadata(storageMetadata);
    return valueMeta;
  }

  private static byte[] bytes(String string) {
    return string.getBytes(StandardCharsets.UTF_8);
  }

  @Test
  void testNormalStorageRowIsPassedAsIs() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaBinary("data"));

    byte[] binary = {0, 1, 2, (byte) 255};
    Object[] row = {"Hop", 42L, binary};

    Object[] result = HopBeamUtil.toNormalStorage(rowMeta, row);

    // Real binary data is normal storage: nothing is copied or converted
    assertSame(row, result);
    assertSame(binary, result[2]);
  }

  @Test
  void testLazyValuesAreConvertedToNormalStorage() throws Exception {
    ValueMetaInteger idMeta = new ValueMetaInteger("id");
    idMeta.setConversionMask("#");
    idMeta.setTrimType(IValueMeta.TRIM_TYPE_BOTH);

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(lazy(idMeta));
    rowMeta.addValueMeta(lazy(new ValueMetaString("name")));
    rowMeta.addValueMeta(lazy(new ValueMetaBinary("lazyBinary")));
    rowMeta.addValueMeta(new ValueMetaBinary("realBinary"));
    rowMeta.addValueMeta(lazy(new ValueMetaString("empty")));

    byte[] realBinary = {0, 1, 2, (byte) 255};
    Object[] row = {bytes(" 123"), bytes("Hop"), bytes("abc"), realBinary, null, "extra"};

    Object[] result = HopBeamUtil.toNormalStorage(rowMeta, row);

    assertEquals(123L, result[0]);
    assertEquals("Hop", result[1]);
    assertInstanceOf(byte[].class, result[2]);
    assertArrayEquals(bytes("abc"), (byte[]) result[2]);
    assertSame(realBinary, result[3]);
    assertNull(result[4]);
    // Over-allocated slots beyond the row metadata are kept
    assertEquals(6, result.length);
    assertEquals("extra", result[5]);

    // The original row can still be in use by the local transform: it's not modified
    assertNotSame(row, result);
    assertArrayEquals(bytes(" 123"), (byte[]) row[0]);
    assertArrayEquals(bytes("Hop"), (byte[]) row[1]);
  }

  @Test
  void testConversionErrorNamesTheField() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(lazy(new ValueMetaInteger("quantity")));

    HopTransformException exception =
        assertThrows(
            HopTransformException.class,
            () -> HopBeamUtil.toNormalStorage(rowMeta, new Object[] {bytes("not a number")}));
    assertTrue(exception.getMessage().contains("'quantity'"));
  }
}
