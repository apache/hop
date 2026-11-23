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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

/**
 * The public methods of {@link RowMeta} that read its fields by position without the lock -
 * getters, comparisons, hashing, equality, serialization - after rows grown one append at a time,
 * so that every row length up to a few dozen is covered.
 */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class RowMetaSnapshotReadersTest {

  private static final int MAX_FIELDS = 70;

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  /** Field i is a String, an Integer or a Number, in turn. */
  private static IValueMeta field(int i) {
    return switch (i % 3) {
      case 0 -> new ValueMetaString("s" + i);
      case 1 -> new ValueMetaInteger("i" + i);
      default -> new ValueMetaNumber("n" + i);
    };
  }

  private static Object value(int i) {
    return switch (i % 3) {
      case 0 -> "v" + i;
      case 1 -> (long) i;
      default -> i + 0.5;
    };
  }

  /** A value that sorts after {@link #value(int)} */
  private static Object largerValue(int i) {
    return switch (i % 3) {
      case 0 -> "v" + i + "z";
      case 1 -> (long) i + 1;
      default -> i + 1.5;
    };
  }

  private static String name(int i) {
    return field(i).getName();
  }

  /** A row of n values, over-allocated like the engine does, with junk after the last field. */
  private static Object[] row(int n, Object junk) {
    Object[] row = RowDataUtil.allocateRowData(n);
    for (int i = 0; i < n; i++) {
      row[i] = value(i);
    }
    for (int i = n; i < row.length; i++) {
      row[i] = junk;
    }
    return row;
  }

  private static int[] allIndexes(int n) {
    int[] indexes = new int[n];
    for (int i = 0; i < n; i++) {
      indexes[i] = i;
    }
    return indexes;
  }

  @Test
  void everyReaderAgreesWithTheFieldsAfterEachAppend() throws Exception {
    RowMeta rowMeta = new RowMeta();
    List<IValueMeta> appended = new ArrayList<>();

    for (int n = 0; n <= MAX_FIELDS; n++) {
      if (n > 0) {
        IValueMeta valueMeta = field(n - 1);
        rowMeta.addValueMeta(valueMeta);
        appended.add(valueMeta);
      }
      String context = "after " + n + " appends";
      assertEquals(n, rowMeta.size(), context);

      Object[] row = row(n, "junk");
      Object[] same = row(n, 12345L);
      int[] all = allIndexes(n);

      // getters by index and by name
      for (int i = 0; i < n; i++) {
        switch (i % 3) {
          case 0 -> {
            assertEquals("v" + i, rowMeta.getString(row, i), context);
            assertEquals("v" + i, rowMeta.getString(row, name(i), "default"), context);
          }
          case 1 -> {
            assertEquals(Long.valueOf(i), rowMeta.getInteger(row, i), context);
            assertEquals(Long.valueOf(i), rowMeta.getInteger(row, name(i), -1L), context);
          }
          default -> assertEquals(Double.valueOf(i + 0.5), rowMeta.getNumber(row, i), context);
        }
        assertFalse(rowMeta.isNull(row, i), context);
      }
      assertEquals("default", rowMeta.getString(row, "missing", "default"), context);
      assertEquals(Long.valueOf(-1L), rowMeta.getInteger(row, "missing", -1L), context);

      // comparisons only look at the fields the row metadata describes, not the junk after them
      assertEquals(0, rowMeta.compare(row, same), context);
      assertEquals(0, rowMeta.compare(row, same, all), context);
      assertTrue(rowMeta.equals(row, same, all), context);
      assertEquals(0, rowMeta.compare(row, same, all, all), context);
      assertEquals(0, rowMeta.compare(row, rowMeta, same, all, all), context);
      assertEquals(Arrays.deepHashCode(Arrays.copyOf(row, n)), rowMeta.hashCode(row), context);
      assertEquals(rowMeta.hashCode(row), rowMeta.hashCode(same), context);

      if (n > 0) {
        int last = n - 1;
        Object[] larger = row(n, "junk");
        larger[last] = largerValue(last);
        int[] lastOnly = {last};
        assertTrue(rowMeta.compare(row, larger) < 0, context);
        assertTrue(rowMeta.compare(larger, row) > 0, context);
        assertTrue(rowMeta.compare(row, larger, lastOnly) < 0, context);
        assertFalse(rowMeta.equals(row, larger, lastOnly), context);
        assertTrue(rowMeta.equals(row, larger, Arrays.copyOf(all, last)), context);
        assertTrue(rowMeta.compare(row, larger, lastOnly, lastOnly) < 0, context);
        assertTrue(rowMeta.compare(larger, rowMeta.clone(), row, lastOnly, lastOnly) > 0, context);
        assertNotEquals(rowMeta.hashCode(row), rowMeta.hashCode(larger), context);
      }

      // string forms
      StringBuilder expectedData = new StringBuilder();
      StringBuilder expectedToString = new StringBuilder();
      for (int i = 0; i < n; i++) {
        if (i > 0) {
          expectedData.append(", ");
          expectedToString.append(", ");
        }
        expectedData.append('[').append(rowMeta.getString(row, i)).append(']');
        expectedToString.append('[').append(appended.get(i)).append(']');
      }
      assertEquals(expectedData.toString(), rowMeta.getString(row), context);
      assertEquals(expectedToString.toString(), rowMeta.toString(), context);
      assertEquals(n, rowMeta.getFieldNamesAndTypes(8).length, context);

      // equality with a row metadata holding the same value metas in an exact-size list
      RowMeta fromList = new RowMeta();
      fromList.setValueMetaList(new ArrayList<>(appended));
      assertTrue(rowMeta.equals(fromList), context);
      assertTrue(fromList.equals(rowMeta), context);
      assertEquals(fromList.hashCode(), rowMeta.hashCode(), context);
      assertEquals(rowMeta.clone(), rowMeta, context);

      // cloned rows
      Object[] cloned = rowMeta.cloneRow(row);
      assertArrayEquals(row, cloned, context);

      // XML round trips
      String metaXml = rowMeta.getMetaXml();
      assertEquals(
          n, metaXml.split("<" + ValueMetaBase.XML_META_TAG + ">", -1).length - 1, context);
      Document metaDocument = XmlHandler.loadXmlString(metaXml);
      RowMeta fromXml = new RowMeta(XmlHandler.getSubNode(metaDocument, RowMeta.XML_META_TAG));
      assertArrayEquals(rowMeta.getFieldNames(), fromXml.getFieldNames(), context);

      String dataXml = rowMeta.getDataXml(row);
      Document dataDocument = XmlHandler.loadXmlString(dataXml);
      Node dataNode = XmlHandler.getSubNode(dataDocument, RowMeta.XML_DATA_TAG);
      Object[] fromDataXml = rowMeta.getRow(dataNode);
      assertArrayEquals(Arrays.copyOf(row, n), Arrays.copyOf(fromDataXml, n), context);

      // binary round trips
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (DataOutputStream out = new DataOutputStream(bytes)) {
        rowMeta.writeMeta(out);
        rowMeta.writeData(out, row);
      }
      try (DataInputStream in =
          new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
        RowMeta read = new RowMeta(in);
        assertArrayEquals(rowMeta.getFieldNames(), read.getFieldNames(), context);
        Object[] readRow = read.readData(in);
        assertEquals(n, readRow.length, context);
        assertArrayEquals(Arrays.copyOf(row, n), readRow, context);
      }
    }
  }

  @Test
  void stringFormsOfASmallRow() throws Exception {
    RowMeta rowMeta = new RowMeta();
    for (int i = 0; i < 3; i++) {
      rowMeta.addValueMeta(field(i));
    }
    Object[] row = row(3, "junk");
    assertEquals("[v0], [1], [2.5]", rowMeta.getString(row));
    assertEquals("[s0 String], [i1 Integer], [n2 Number]", rowMeta.toString());
    assertArrayEquals(
        new String[] {"s0   (String)", "i1   (Integer)", "n2   (Number)"},
        rowMeta.getFieldNamesAndTypes(2));
  }

  /**
   * A field number at or past the end of the row metadata, with a row long enough to hold it, as
   * the engine's over-allocated rows are.
   */
  @Test
  void comparingOnAFieldNumberPastTheEndThrowsArrayIndexOutOfBounds() {
    RowMeta rowMeta = new RowMeta();
    for (int i = 0; i < 3; i++) {
      rowMeta.addValueMeta(field(i));
    }
    Object[] row = row(3, "junk");
    Object[] other = row(3, "junk");
    int[] pastTheEnd = {3};
    int[] first = {0};

    ArrayIndexOutOfBoundsException compare =
        assertThrows(
            ArrayIndexOutOfBoundsException.class, () -> rowMeta.compare(row, other, pastTheEnd));
    assertEquals("Index 3 out of bounds for length 3", compare.getMessage());

    ArrayIndexOutOfBoundsException equals =
        assertThrows(
            ArrayIndexOutOfBoundsException.class, () -> rowMeta.equals(row, other, pastTheEnd));
    assertEquals("Index 3 out of bounds for length 3", equals.getMessage());

    ArrayIndexOutOfBoundsException compareTwoKeyLists =
        assertThrows(
            ArrayIndexOutOfBoundsException.class,
            () -> rowMeta.compare(row, other, pastTheEnd, first));
    assertEquals("Index 3 out of bounds for length 3", compareTwoKeyLists.getMessage());

    ArrayIndexOutOfBoundsException compareTwoMetas =
        assertThrows(
            ArrayIndexOutOfBoundsException.class,
            () -> rowMeta.compare(row, rowMeta, other, pastTheEnd, first));
    assertEquals("Index 3 out of bounds for length 3", compareTwoMetas.getMessage());
  }

  /** Surprising but pinned: the converted-values hash walks the whole row, past the metadata. */
  @Test
  void convertedValuesHashCodeOfAnOverAllocatedRowThrows() throws Exception {
    RowMeta rowMeta = new RowMeta();
    for (int i = 0; i < 3; i++) {
      rowMeta.addValueMeta(field(i));
    }
    Object[] row = row(3, null);
    assertThrows(NullPointerException.class, () -> rowMeta.convertedValuesHashCode(row));
    assertEquals(
        rowMeta.convertedValuesHashCode(Arrays.copyOf(row, 3)),
        rowMeta.convertedValuesHashCode(new Object[] {"x", 1L, 1.0}));
  }
}
