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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.hop.core.util.Utils;

/**
 * Assertions shared by the RowMeta behaviour tests. They only use the public {@link IRowMeta} API
 * so they hold for any implementation of the name cache and the lock-free snapshot.
 */
final class RowMetaAssert {

  private RowMetaAssert() {}

  /**
   * Checks the fields of the row by position: size, names, the value meta list and the bounds of
   * {@link IRowMeta#getValueMeta(int)}. None of these consult the name cache, so this can be called
   * between mutations without changing what the cache knows.
   *
   * @param expected the expected names; null stands for a value meta without a name
   */
  static void assertNames(IRowMeta rowMeta, String... expected) {
    assertEquals(expected.length, rowMeta.size(), "size()");
    assertEquals(expected.length == 0, rowMeta.isEmpty(), "isEmpty()");

    String[] expectedFieldNames = new String[expected.length];
    for (int i = 0; i < expected.length; i++) {
      expectedFieldNames[i] = expected[i] == null ? "" : expected[i];
    }
    assertArrayEquals(expectedFieldNames, rowMeta.getFieldNames(), "getFieldNames()");

    List<IValueMeta> list = rowMeta.getValueMetaList();
    assertEquals(expected.length, list.size(), "getValueMetaList().size()");
    for (int i = 0; i < expected.length; i++) {
      IValueMeta valueMeta = rowMeta.getValueMeta(i);
      assertNotNull(valueMeta, "getValueMeta(" + i + ")");
      assertEquals(expected[i], valueMeta.getName(), "getValueMeta(" + i + ").getName()");
      assertSame(valueMeta, list.get(i), "getValueMetaList().get(" + i + ")");
    }
    assertNull(rowMeta.getValueMeta(expected.length), "getValueMeta(size())");
    assertNull(rowMeta.getValueMeta(expected.length + 1), "getValueMeta(size() + 1)");
    assertNull(rowMeta.getValueMeta(-1), "getValueMeta(-1)");
  }

  /**
   * Checks that every named field is found at its own index by its name, in upper and lower case
   * too, through {@link IRowMeta#indexOfValue(String)}, {@link IRowMeta#searchValueMeta(String)}
   * and {@link IRowMeta#exists(IValueMeta)}. The names in the row must be unique ignoring case.
   */
  static void assertLookups(IRowMeta rowMeta) {
    int size = rowMeta.size();
    Set<String> seen = new LinkedHashSet<>();
    for (int i = 0; i < size; i++) {
      IValueMeta valueMeta = rowMeta.getValueMeta(i);
      String name = valueMeta.getName();
      if (Utils.isEmpty(name)) {
        continue;
      }
      assertTrue(
          seen.add(name.toUpperCase(Locale.ROOT).toLowerCase(Locale.ROOT)),
          "test precondition: names must be unique ignoring case, '" + name + "' is not");
      for (String variant : variants(name)) {
        assertEquals(i, rowMeta.indexOfValue(variant), "indexOfValue(\"" + variant + "\")");
        assertSame(
            valueMeta, rowMeta.searchValueMeta(variant), "searchValueMeta(\"" + variant + "\")");
      }
      assertTrue(rowMeta.exists(valueMeta), "exists(" + name + ")");
    }
  }

  /** {@link #assertNames} followed by {@link #assertLookups}. */
  static void assertConsistent(IRowMeta rowMeta, String... expected) {
    assertNames(rowMeta, expected);
    assertLookups(rowMeta);
  }

  /** None of the names is in the row, in any case. */
  static void assertAbsent(IRowMeta rowMeta, String... names) {
    for (String name : names) {
      for (String variant : variants(name)) {
        assertEquals(-1, rowMeta.indexOfValue(variant), "indexOfValue(\"" + variant + "\")");
        assertNull(rowMeta.searchValueMeta(variant), "searchValueMeta(\"" + variant + "\")");
      }
    }
  }

  /** The name as given, upper-cased and lower-cased, where those still match it ignoring case. */
  static Set<String> variants(String name) {
    Set<String> variants = new LinkedHashSet<>();
    variants.add(name);
    String upper = name.toUpperCase(Locale.ROOT);
    if (upper.equalsIgnoreCase(name)) {
      variants.add(upper);
    }
    String lower = name.toLowerCase(Locale.ROOT);
    if (lower.equalsIgnoreCase(name)) {
      variants.add(lower);
    }
    assertFalse(variants.isEmpty());
    return variants;
  }
}
