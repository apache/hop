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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.Locale;
import java.util.TimeZone;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** {@link ValueMetaFactory#createParseMeta(IValueMeta)} and its row variant. */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class ValueMetaFactoryParseMetaTest {

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  private static IValueMeta number(String mask, String decimal, String grouping) {
    IValueMeta meta = new ValueMetaNumber("amount");
    meta.setConversionMask(mask);
    meta.setDecimalSymbol(decimal);
    meta.setGroupingSymbol(grouping);
    return meta;
  }

  @Test
  void parseMetaCarriesTheFormatOfTheTarget() throws Exception {
    ValueMetaDate target = new ValueMetaDate("d");
    target.setConversionMask("dd/MM/yyyy");
    target.setDateFormatLocale(Locale.FRANCE);
    target.setDateFormatTimeZone(TimeZone.getTimeZone("Asia/Tokyo"));
    target.setDateFormatLenient(true);
    target.setLenientStringToNumber(true);
    target.setTrimType(IValueMeta.TRIM_TYPE_BOTH);
    target.setStringEncoding("UTF-16");
    target.setDecimalSymbol(",");
    target.setGroupingSymbol(".");
    target.setCurrencySymbol("€");

    IValueMeta parseMeta = ValueMetaFactory.createParseMeta(target);

    assertEquals(IValueMeta.TYPE_STRING, parseMeta.getType());
    assertEquals("d", parseMeta.getName());
    assertEquals("dd/MM/yyyy", parseMeta.getConversionMask());
    assertEquals(Locale.FRANCE, parseMeta.getDateFormatLocale());
    assertEquals("Asia/Tokyo", parseMeta.getDateFormatTimeZone().getID());
    assertTrue(parseMeta.isDateFormatLenient());
    assertTrue(parseMeta.isLenientStringToNumber());
    assertEquals(IValueMeta.TRIM_TYPE_BOTH, parseMeta.getTrimType());
    assertEquals("UTF-16", parseMeta.getStringEncoding());
    assertEquals(",", parseMeta.getDecimalSymbol());
    assertEquals(".", parseMeta.getGroupingSymbol());
    assertEquals("€", parseMeta.getCurrencySymbol());
    assertEquals(IValueMeta.STORAGE_TYPE_NORMAL, parseMeta.getStorageType());
    assertEquals(-1, parseMeta.getLength());
  }

  @Test
  void aFieldParsesTheCurrencyTextItWrites() throws Exception {
    IValueMeta amount = number("¤#,##0.00", ".", ",");
    amount.setCurrencySymbol("€");
    String text = amount.getString(1234.5);

    Object value = amount.convertData(ValueMetaFactory.createParseMeta(amount), text);

    assertEquals("€1,234.50", text);
    assertEquals(1234.5, value);
  }

  @Test
  void parsingIntoALazyFieldWorks() throws Exception {
    IValueMeta lazy = new ValueMetaInteger("id");
    lazy.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
    lazy.setStorageMetadata(new ValueMetaString("id"));

    IValueMeta parseMeta = ValueMetaFactory.createParseMeta(lazy);

    assertEquals(IValueMeta.STORAGE_TYPE_NORMAL, parseMeta.getStorageType());
    assertNull(parseMeta.getStorageMetadata());
    assertEquals(42L, lazy.convertData(parseMeta, "42"));
  }

  @Test
  void theTargetIsNotChanged() throws Exception {
    IValueMeta amount = number("#,##0.00", ",", ".");
    amount.setLength(9, 2);

    ValueMetaFactory.createParseMeta(amount).setConversionMask("0");

    assertEquals("#,##0.00", amount.getConversionMask());
    assertEquals(9, amount.getLength());
    assertEquals(IValueMeta.TYPE_NUMBER, amount.getType());
  }

  /** For every value type the helper parses exactly what the cloneValueMeta recipe parsed. */
  @Test
  void parsesTheSameAsTheCloneRecipe() throws Exception {
    Object[][] cases = {
      {number("#,##0.00", ",", "."), "1.234,56"},
      {number(null, null, null), "12.5"},
      {number("", "", ""), "7"},
      {withMask(new ValueMetaInteger("i"), "#,##0"), "1,234"},
      {new ValueMetaInteger("i"), "-42"},
      {withMask(new ValueMetaBigNumber("b"), "#,##0.000"), "1,234.567"},
      {new ValueMetaBigNumber("b"), "12345678901234567890.123"},
      {withMask(new ValueMetaDate("d"), "yyyy-MM-dd"), "2024-05-17"},
      {new ValueMetaDate("d"), "2024/05/17 10:11:12.123"},
      {new ValueMetaTimestamp("t"), "2024/05/17 10:11:12.123456789"},
      {new ValueMetaBoolean("bool"), "Y"},
      {withTrim(new ValueMetaString("s"), IValueMeta.TRIM_TYPE_BOTH), "  padded  "},
    };
    for (Object[] c : cases) {
      IValueMeta target = (IValueMeta) c[0];
      String text = (String) c[1];

      Object viaClone =
          target.convertData(ValueMetaFactory.cloneValueMeta(target, IValueMeta.TYPE_STRING), text);
      Object viaHelper = target.convertData(ValueMetaFactory.createParseMeta(target), text);

      if (viaClone instanceof BigDecimal big) {
        assertEquals(0, big.compareTo((BigDecimal) viaHelper), target + " / " + text);
      } else {
        assertEquals(viaClone, viaHelper, target + " / " + text);
      }
    }
  }

  @Test
  void rowVariantKeepsTheFieldsInOrder() throws Exception {
    IRowMeta target = new RowMeta();
    target.addValueMeta(number("#,##0.00", ",", "."));
    target.addValueMeta(new ValueMetaDate("when"));
    IValueMeta lazy = new ValueMetaString("lazy");
    lazy.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
    lazy.setStorageMetadata(new ValueMetaString("lazy"));
    target.addValueMeta(lazy);

    IRowMeta parseRowMeta = ValueMetaFactory.createParseRowMeta(target);

    assertEquals(3, parseRowMeta.size());
    assertEquals("amount", parseRowMeta.getValueMeta(0).getName());
    assertEquals("when", parseRowMeta.getValueMeta(1).getName());
    assertEquals("lazy", parseRowMeta.getValueMeta(2).getName());
    for (IValueMeta parseMeta : parseRowMeta.getValueMetaList()) {
      assertEquals(IValueMeta.TYPE_STRING, parseMeta.getType());
      assertEquals(IValueMeta.STORAGE_TYPE_NORMAL, parseMeta.getStorageType());
    }
    assertEquals(",", parseRowMeta.getValueMeta(0).getDecimalSymbol());
    assertFalse(parseRowMeta.getValueMeta(2).isStorageBinaryString());
  }

  private static IValueMeta withMask(IValueMeta meta, String mask) {
    meta.setConversionMask(mask);
    return meta;
  }

  private static IValueMeta withTrim(IValueMeta meta, int trimType) {
    meta.setTrimType(trimType);
    return meta;
  }
}
