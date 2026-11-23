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

import static org.apache.hop.core.row.RowMetaAssert.assertAbsent;
import static org.apache.hop.core.row.RowMetaAssert.assertConsistent;
import static org.apache.hop.core.row.RowMetaAssert.assertNames;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Locale;
import java.util.stream.Stream;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Pins that {@link RowMeta} matches names with {@link String#equalsIgnoreCase(String)}, also for
 * non-ASCII names, for lookups and for the duplicate check on add. Some of the pairs below are
 * equal ignoring case although {@link String#toLowerCase()} turns them into different strings.
 */
class RowMetaCaseInsensitiveNameTest {

  private static IValueMeta string(String name) {
    return new ValueMetaString(name);
  }

  private static RowMeta rowOf(String... names) {
    RowMeta rowMeta = new RowMeta();
    for (String name : names) {
      rowMeta.addValueMeta(string(name));
    }
    return rowMeta;
  }

  /** Pairs of names that {@link String#equalsIgnoreCase(String)} considers equal. */
  static Stream<Arguments> namesEqualIgnoringCase() {
    return Stream.of(
        Arguments.of("Name", "NAME"),
        Arguments.of("ÄRGER", "ärger"),
        Arguments.of("Ölpreis", "ÖLPREIS"),
        Arguments.of("ΟΔΟΣ", "οδος"),
        Arguments.of("日本", "日本"),
        Arguments.of("Ǆ", "ǅ"),
        // Deseret capital and small long I, outside the basic multilingual plane
        Arguments.of("\uD801\uDC00x", "\uD801\uDC28X"),
        // Kelvin sign
        Arguments.of("KELVIN\u212A", "kelvink"),
        // equal ignoring case, but toLowerCase() gives different strings:
        // Greek capital sigma against a lower-case medial sigma at the end of the word
        Arguments.of("ΟΔΟΣ", "οδοσ"),
        // Turkish dotless i
        Arguments.of("ADI", "adı"),
        // Turkish dotted capital I
        Arguments.of("İL", "il"),
        Arguments.of("il", "İL"),
        // micro sign against Greek small mu
        Arguments.of("\u00B5g", "\u03BCg"),
        // Latin small long s
        Arguments.of("ſum", "SUM"));
  }

  /** Pairs of names that are not equal ignoring case, although they look alike. */
  static Stream<Arguments> namesNotEqualIgnoringCase() {
    return Stream.of(
        Arguments.of("Straße", "STRASSE"), Arguments.of("ﬁle", "FILE"), Arguments.of("a", "á"));
  }

  @ParameterizedTest(name = "{0} / {1}")
  @MethodSource("namesEqualIgnoringCase")
  void aNameIsFoundByAnyNameEqualToItIgnoringCase(String existing, String other) {
    assertTrue(existing.equalsIgnoreCase(other), "test precondition");
    RowMeta rowMeta = rowOf("x", existing, "y");
    assertEquals(1, rowMeta.indexOfValue(other));
    assertSame(rowMeta.getValueMeta(1), rowMeta.searchValueMeta(other));
    assertTrue(rowMeta.exists(string(other)));
  }

  @ParameterizedTest(name = "{0} / {1}")
  @MethodSource("namesEqualIgnoringCase")
  void appendingANameEqualIgnoringCaseRenamesIt(String existing, String other) {
    RowMeta rowMeta = rowOf("x", existing);
    rowMeta.addValueMeta(string(other));
    assertNames(rowMeta, "x", existing, other + "_1");
  }

  @ParameterizedTest(name = "{0} / {1}")
  @MethodSource("namesEqualIgnoringCase")
  void insertingANameEqualIgnoringCaseRenamesIt(String existing, String other) {
    RowMeta rowMeta = rowOf("x", existing);
    rowMeta.addValueMeta(0, string(other));
    assertNames(rowMeta, other + "_1", "x", existing);
  }

  @ParameterizedTest(name = "{0} / {1}")
  @MethodSource("namesEqualIgnoringCase")
  void mergingANameEqualIgnoringCaseRenamesIt(String existing, String other) {
    RowMeta rowMeta = rowOf("x", existing);
    RowMeta toMerge = rowOf(other);
    rowMeta.mergeRowMeta(toMerge, "merged");
    assertNames(rowMeta, "x", existing, other + "_1");
    assertEquals("merged", rowMeta.getValueMeta(2).getOrigin());
  }

  @ParameterizedTest(name = "{0} / {1}")
  @MethodSource("namesEqualIgnoringCase")
  void replacingWithANameEqualIgnoringCaseElsewhereRenamesIt(String existing, String other) {
    RowMeta rowMeta = rowOf("x", existing);
    rowMeta.setValueMeta(0, string(other));
    assertNames(rowMeta, other + "_1", existing);
  }

  @ParameterizedTest(name = "{0} / {1}")
  @MethodSource("namesEqualIgnoringCase")
  void removingByANameEqualIgnoringCase(String existing, String other) throws Exception {
    RowMeta rowMeta = rowOf("x", existing, "y");
    rowMeta.removeValueMeta(other);
    assertConsistent(rowMeta, "x", "y");
  }

  @ParameterizedTest(name = "{0} / {1}")
  @MethodSource("namesNotEqualIgnoringCase")
  void namesThatAreNotEqualIgnoringCaseAreDistinct(String existing, String other) {
    assertFalse(existing.equalsIgnoreCase(other), "test precondition");
    RowMeta rowMeta = rowOf("x", existing);
    assertEquals(-1, rowMeta.indexOfValue(other));

    rowMeta.addValueMeta(string(other));
    rowMeta.addValueMeta(0, string(other + "!"));
    assertNames(rowMeta, other + "!", "x", existing, other);
    assertEquals(2, rowMeta.indexOfValue(existing));
    assertEquals(3, rowMeta.indexOfValue(other));
    assertEquals(2, rowMeta.indexOfValue(existing));
  }

  /**
   * Surprising but pinned: a cached index is checked against the lower-cased lookup name, not the
   * name that was looked up. The lower case of a dotted capital I is an i followed by a combining
   * dot above, which is equal ignoring case to a field spelled that way although the dotted capital
   * I itself is not. Once that field is cached, looking up the dotted capital I finds it.
   */
  @Test
  void aCachedIndexIsCheckedAgainstTheLowerCasedLookupName() {
    String dottedCapitalI = "İ";
    String iWithCombiningDot = "i\u0307";
    assertFalse(dottedCapitalI.equalsIgnoreCase(iWithCombiningDot), "test precondition");
    assertEquals(iWithCombiningDot, dottedCapitalI.toLowerCase(Locale.ROOT), "test precondition");

    RowMeta rowMeta = rowOf(dottedCapitalI, iWithCombiningDot);
    // both are kept as they are: they are not equal ignoring case
    assertNames(rowMeta, dottedCapitalI, iWithCombiningDot);
    assertEquals(1, rowMeta.indexOfValue(iWithCombiningDot));
    // the cache maps the shared lower case to index 1, and that check passes
    assertEquals(1, rowMeta.indexOfValue(dottedCapitalI));
    assertSame(rowMeta.getValueMeta(1), rowMeta.searchValueMeta(dottedCapitalI));
  }

  @Test
  void nonAsciiNamesThroughAMixedSequence() throws Exception {
    RowMeta rowMeta = rowOf("Größe", "ÄRGER", "ΟΔΟΣ", "日本", "çay", "Ølpris", "\uD801\uDC00x");
    assertConsistent(rowMeta, "Größe", "ÄRGER", "ΟΔΟΣ", "日本", "çay", "Ølpris", "\uD801\uDC00x");

    rowMeta.addValueMeta(0, string("ärger"));
    assertConsistent(
        rowMeta, "ärger_1", "Größe", "ÄRGER", "ΟΔΟΣ", "日本", "çay", "Ølpris", "\uD801\uDC00x");

    rowMeta.removeValueMeta("GRÖßE");
    rowMeta.removeValueMeta(3);
    assertConsistent(rowMeta, "ärger_1", "ÄRGER", "ΟΔΟΣ", "çay", "Ølpris", "\uD801\uDC00x");
    assertAbsent(rowMeta, "Größe", "日本");

    rowMeta.getValueMeta(3).setName("ÇAY2");
    rowMeta.addValueMeta(string("çay2"));
    rowMeta.addValueMeta(string("ÇAY"));
    rowMeta.addValueMeta(string("ølpris"));
    assertConsistent(
        rowMeta,
        "ärger_1",
        "ÄRGER",
        "ΟΔΟΣ",
        "ÇAY2",
        "Ølpris",
        "\uD801\uDC00x",
        "çay2_1",
        "ÇAY",
        "ølpris_1");
  }

  /**
   * In a Turkish default locale {@code "ID".toLowerCase()} is {@code "ıd"} with a dotless i, while
   * {@code "ID".equalsIgnoreCase("id")} is still true.
   */
  @Test
  void namesWithAnUpperCaseIInATurkishDefaultLocale() throws Exception {
    Locale defaultLocale = Locale.getDefault();
    Locale displayLocale = Locale.getDefault(Locale.Category.DISPLAY);
    Locale formatLocale = Locale.getDefault(Locale.Category.FORMAT);
    try {
      Locale.setDefault(Locale.forLanguageTag("tr-TR"));
      assertEquals("ıd", "ID".toLowerCase(), "test precondition");

      RowMeta rowMeta = rowOf("ID", "field");
      assertEquals(0, rowMeta.indexOfValue("id"));
      assertEquals(0, rowMeta.indexOfValue("Id"));
      assertEquals(1, rowMeta.indexOfValue("FIELD"));

      RowMeta duplicates = rowOf("ID", "field");
      duplicates.addValueMeta(string("id"));
      duplicates.addValueMeta(string("FIELD"));
      duplicates.addValueMeta(0, string("Id"));
      assertNames(duplicates, "Id_2", "ID", "field", "id_1", "FIELD_1");

      RowMeta lowerFirst = rowOf("id", "INFO");
      lowerFirst.addValueMeta(string("ID"));
      lowerFirst.addValueMeta(string("info"));
      assertNames(lowerFirst, "id", "INFO", "ID_1", "info_1");

      RowMeta removal = rowOf("ID", "x");
      removal.removeValueMeta("id");
      assertNames(removal, "x");
    } finally {
      Locale.setDefault(defaultLocale);
      Locale.setDefault(Locale.Category.DISPLAY, displayLocale);
      Locale.setDefault(Locale.Category.FORMAT, formatLocale);
    }
  }
}
