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
package org.apache.hop.core.row.value;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Locale;
import java.util.Random;
import java.util.TimeZone;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class ValueMetaBaseSerializationTest {

  @Test
  void restoresMetaData_storageTypeNormal() throws Exception {
    ValueMetaBase vmb = createTestObject(IValueMeta.STORAGE_TYPE_NORMAL);

    checkRestoring(vmb);
  }

  @Test
  void restoresMetaData_storageTypeIndexed() throws Exception {
    ValueMetaBase vmb = createTestObject(IValueMeta.STORAGE_TYPE_INDEXED);
    vmb.setIndex(new Object[] {"qwerty", "asdfg"});

    checkRestoring(vmb);
  }

  @Test
  void restoresMetaData_storageTypeBinaryString() throws Exception {
    ValueMetaBase vmb = createTestObject(IValueMeta.STORAGE_TYPE_BINARY_STRING);
    vmb.setStorageMetadata(new ValueMetaBase("storageMetadataInstance", IValueMeta.TYPE_STRING));

    checkRestoring(vmb);
  }

  private static ValueMetaBase createTestObject(int storageType) {
    ValueMetaBase vmb = new ValueMetaBase("test", IValueMeta.TYPE_STRING);
    vmb.setStorageType(storageType);
    vmb.setLength(10, 5);
    vmb.setOrigin("origin");
    vmb.setComments("comments");
    vmb.setConversionMask("conversionMask");
    vmb.setDecimalSymbol("decimalSymbol");
    vmb.setGroupingSymbol("groupingSymbol");
    vmb.setCurrencySymbol("currencySymbol");
    vmb.setTrimType(IValueMeta.TRIM_TYPE_BOTH);
    vmb.setCaseInsensitive(true);
    vmb.setSortedDescending(true);
    vmb.setOutputPaddingEnabled(true);
    vmb.setDateFormatLenient(true);
    vmb.setLenientStringToNumber(true);
    vmb.setDateFormatLocale(Locale.JAPAN);
    vmb.setCollatorDisabled(false);
    vmb.setCollatorLocale(Locale.JAPANESE);
    vmb.setCollatorStrength(1);

    String[] zones = TimeZone.getAvailableIDs();
    vmb.setDateFormatTimeZone(TimeZone.getTimeZone(zones[new Random().nextInt(zones.length)]));

    return vmb;
  }

  private static void checkRestoring(ValueMetaBase initial) throws Exception {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    initial.writeMeta(new DataOutputStream(os));

    DataInputStream dataInputStream =
        new DataInputStream(new ByteArrayInputStream(os.toByteArray()));
    // an awkward hack, since readMetaData() expects object's type to have been read
    int restoredType = dataInputStream.readInt();
    assertEquals(initial.getType(), restoredType, "type");

    ValueMetaBase restored = new ValueMetaBase(initial.getName(), restoredType);
    restored.readMetaData(dataInputStream);

    assertMetaDataAreEqual(initial, restored);
  }

  private static void assertMetaDataAreEqual(IValueMeta expected, IValueMeta actual) {
    assertEquals(expected.getStorageType(), actual.getStorageType(), "storageType");

    if (expected.getIndex() == null) {
      assertNull(actual.getIndex(), "index");
    } else {
      assertEquals(expected.getIndex().length, actual.getIndex().length, "index.length");
      for (int i = 0; i < expected.getIndex().length; i++) {
        assertEquals(expected.getIndex()[i], actual.getIndex()[i], "index[" + i + "]");
      }
    }

    if (expected.getStorageMetadata() == null) {
      assertNull(actual.getStorageMetadata(), "storageMetadata");
    } else {
      assertMetaDataAreEqual(expected.getStorageMetadata(), actual.getStorageMetadata());
    }

    assertEquals(expected.getName(), actual.getName(), "name");
    assertEquals(expected.getLength(), actual.getLength(), "length");
    assertEquals(expected.getOrigin(), actual.getOrigin(), "origin");
    assertEquals(expected.getComments(), actual.getComments(), "comments");
    assertEquals(expected.getConversionMask(), actual.getConversionMask(), "conversionMask");
    assertEquals(expected.getDecimalSymbol(), actual.getDecimalSymbol(), "decimalSymbol");
    assertEquals(expected.getGroupingSymbol(), actual.getGroupingSymbol(), "groupingSymbol");
    assertEquals(expected.getCurrencySymbol(), actual.getCurrencySymbol(), "currencySymbol");
    assertEquals(expected.getTrimType(), actual.getTrimType(), "trimType");
    assertEquals(expected.isCaseInsensitive(), actual.isCaseInsensitive(), "caseInsensitive");
    assertEquals(expected.isSortedDescending(), actual.isSortedDescending(), "sortedDescending");
    assertEquals(
        expected.isOutputPaddingEnabled(), actual.isOutputPaddingEnabled(), "outputPaddingEnabled");
    assertEquals(expected.isDateFormatLenient(), actual.isDateFormatLenient(), "dateFormatLenient");
    assertEquals(expected.getDateFormatLocale(), actual.getDateFormatLocale(), "dateFormatLocale");
    assertEquals(
        expected.getDateFormatTimeZone(), actual.getDateFormatTimeZone(), "dateFormatTimeZone");
    assertEquals(
        expected.isLenientStringToNumber(),
        actual.isLenientStringToNumber(),
        "lenientStringToNumber");
    assertEquals(expected.isCollatorDisabled(), actual.isCollatorDisabled(), "collatorDisabled");
    assertEquals(expected.getCollatorLocale(), actual.getCollatorLocale(), "collatorLocale");
    assertEquals(expected.getCollatorStrength(), actual.getCollatorStrength(), "collatorStrength");
  }
}
