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

package org.apache.hop.core.row;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.text.Collator;
import java.time.Instant;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Row metadata written as JSON (Beam engine, execution information) comes back as the fields that
 * were written, as it does from the binary format.
 */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class JsonRowMetaTest {

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void theFieldsKeepTheirTypeFormatAndOrigin() {
    IRowMeta rowMeta = new RowMeta();
    ValueMetaNumber price = new ValueMetaNumber("price", 9, 2);
    price.setConversionMask("0.00");
    price.setOrigin("read orders");
    price.setComments("unit price, excluding VAT");
    rowMeta.addValueMeta(price);
    rowMeta.addValueMeta(new ValueMetaString("name"));

    IRowMeta copy = JsonRowMeta.fromJson(JsonRowMeta.toJson(rowMeta));

    assertEquals(2, copy.size());
    IValueMeta copiedPrice = copy.getValueMeta(0);
    assertEquals("price", copiedPrice.getName());
    assertEquals(IValueMeta.TYPE_NUMBER, copiedPrice.getType());
    assertEquals(9, copiedPrice.getLength());
    assertEquals(2, copiedPrice.getPrecision());
    assertEquals("0.00", copiedPrice.getConversionMask());
    assertEquals("read orders", copiedPrice.getOrigin());
    assertEquals("unit price, excluding VAT", copiedPrice.getComments());
    assertNull(copy.getValueMeta(1).getOrigin());
  }

  /** Symbols, rounding, trimming, date settings and comparing: how the field is converted. */
  @Test
  void theFieldsKeepHowTheyAreWrittenReadAndCompared() {
    IRowMeta rowMeta = settingsOtherThanTheDefaults();

    IRowMeta copy = JsonRowMeta.fromJson(JsonRowMeta.toJson(rowMeta));

    assertSettingsOtherThanTheDefaults(copy);
    // Not part of the binary format, so checked here only
    assertEquals("half_up", copy.getValueMeta(0).getRoundingType());
    assertEquals("UTF-16", copy.getValueMeta(1).getStringEncoding());
  }

  /** The reference: the binary format already keeps all of these. */
  @Test
  void theBinaryFormatKeepsTheSameSettings() throws Exception {
    IRowMeta rowMeta = settingsOtherThanTheDefaults();

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      rowMeta.writeMeta(out);
    }
    IRowMeta copy;
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      copy = new RowMeta(in);
    }

    assertSettingsOtherThanTheDefaults(copy);
  }

  /**
   * Written on one machine (the Beam launcher), read on another (a worker): a field nobody
   * configured formats with the reader's regional settings, not the writer's.
   */
  @Test
  void aFieldNobodyConfiguredUsesTheRegionalSettingsOfTheReader() throws Exception {
    Locale originalLocale = Locale.getDefault();
    Locale originalFormat = Locale.getDefault(Locale.Category.FORMAT);
    TimeZone originalTimeZone = TimeZone.getDefault();
    try {
      setRegionalSettings(Locale.forLanguageTag("nl-BE"), "Europe/Brussels");
      IRowMeta rowMeta = new RowMeta();
      rowMeta.addValueMeta(new ValueMetaNumber("amount"));
      ValueMetaDate moment = new ValueMetaDate("moment");
      moment.setConversionMask("yyyy-MM-dd HH:mm");
      rowMeta.addValueMeta(moment);
      // Set explicitly, equal to the default of the reader: still written
      ValueMetaNumber price = new ValueMetaNumber("price");
      price.setDecimalSymbol(".");
      rowMeta.addValueMeta(price);
      String json = JsonRowMeta.toJson(rowMeta);

      setRegionalSettings(Locale.US, "UTC");
      IRowMeta copy = JsonRowMeta.fromJson(json);

      assertEquals("1.5", copy.getValueMeta(0).getString(1.5));
      Date tenUtc = Date.from(Instant.parse("2025-11-19T10:00:00Z"));
      assertEquals("2025-11-19 10:00", copy.getValueMeta(1).getString(tenUtc));
      assertEquals(".", copy.getValueMeta(2).getDecimalSymbol());
    } finally {
      Locale.setDefault(originalLocale);
      Locale.setDefault(Locale.Category.FORMAT, originalFormat);
      TimeZone.setDefault(originalTimeZone);
    }
  }

  private static void setRegionalSettings(Locale locale, String timeZone) {
    Locale.setDefault(locale);
    Locale.setDefault(Locale.Category.FORMAT, locale);
    TimeZone.setDefault(TimeZone.getTimeZone(timeZone));
  }

  /** JSON written before these settings were stored loads with the defaults, as before. */
  @Test
  void olderJsonStillLoadsWithTheDefaults() {
    String json =
        "{\"values\":[{\"name\":\"id\",\"type\":5,\"length\":-1,\"precision\":-1,"
            + "\"conversionMask\":null}]}";

    IValueMeta id = JsonRowMeta.fromJson(json).getValueMeta(0);
    IValueMeta defaults = new ValueMetaString("defaults");

    assertEquals("id", id.getName());
    assertEquals(IValueMeta.TYPE_INTEGER, id.getType());
    assertNull(id.getOrigin());
    assertNull(id.getComments());
    assertEquals(defaults.getTrimType(), id.getTrimType());
    assertEquals(defaults.isCaseInsensitive(), id.isCaseInsensitive());
    assertEquals(defaults.isSortedDescending(), id.isSortedDescending());
    assertEquals(defaults.isDateFormatLenient(), id.isDateFormatLenient());
    assertEquals(defaults.isLenientStringToNumber(), id.isLenientStringToNumber());
    assertEquals(defaults.isCollatorDisabled(), id.isCollatorDisabled());
    assertEquals(defaults.isOutputPaddingEnabled(), id.isOutputPaddingEnabled());
  }

  private static IRowMeta settingsOtherThanTheDefaults() {
    IRowMeta rowMeta = new RowMeta();

    ValueMetaNumber price = new ValueMetaNumber("price");
    price.setConversionMask("#,##0.00");
    price.setDecimalSymbol(",");
    price.setGroupingSymbol(".");
    price.setCurrencySymbol("EUR");
    price.setRoundingType("half_up");
    price.setOutputPaddingEnabled(true);
    rowMeta.addValueMeta(price);

    ValueMetaString name = new ValueMetaString("name");
    name.setTrimType(IValueMeta.TRIM_TYPE_BOTH);
    name.setCaseInsensitive(true);
    name.setSortedDescending(true);
    name.setStringEncoding("UTF-16");
    name.setCollatorDisabled(false);
    name.setCollatorLocale(Locale.forLanguageTag("fr-FR"));
    name.setCollatorStrength(Collator.SECONDARY);
    rowMeta.addValueMeta(name);

    ValueMetaDate day = new ValueMetaDate("day");
    day.setDateFormatLenient(true);
    day.setDateFormatLocale(Locale.forLanguageTag("nl-BE"));
    day.setDateFormatTimeZone(TimeZone.getTimeZone("Asia/Tokyo"));
    day.setLenientStringToNumber(true);
    rowMeta.addValueMeta(day);

    return rowMeta;
  }

  private static void assertSettingsOtherThanTheDefaults(IRowMeta copy) {
    IValueMeta price = copy.getValueMeta(0);
    assertEquals(",", price.getDecimalSymbol());
    assertEquals(".", price.getGroupingSymbol());
    assertEquals("EUR", price.getCurrencySymbol());
    assertTrue(price.isOutputPaddingEnabled());

    IValueMeta name = copy.getValueMeta(1);
    assertEquals(IValueMeta.TRIM_TYPE_BOTH, name.getTrimType());
    assertTrue(name.isCaseInsensitive());
    assertTrue(name.isSortedDescending());
    assertFalse(name.isCollatorDisabled());
    assertEquals(Locale.forLanguageTag("fr-FR"), name.getCollatorLocale());
    assertEquals(Collator.SECONDARY, name.getCollatorStrength());

    IValueMeta day = copy.getValueMeta(2);
    assertTrue(day.isDateFormatLenient());
    assertEquals(Locale.forLanguageTag("nl-BE"), day.getDateFormatLocale());
    assertEquals("Asia/Tokyo", day.getDateFormatTimeZone().getID());
    assertTrue(day.isLenientStringToNumber());
  }
}
