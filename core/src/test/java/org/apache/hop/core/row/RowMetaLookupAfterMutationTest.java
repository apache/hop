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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.Test;

/**
 * Pins what every name and index lookup of {@link RowMeta} answers after each kind of mutation, and
 * after a long mixed sequence of them.
 */
class RowMetaLookupAfterMutationTest {

  private static IValueMeta string(String name) {
    return new ValueMetaString(name);
  }

  private static IValueMeta integer(String name) {
    return new ValueMetaInteger(name);
  }

  private static RowMeta rowOf(String... names) {
    RowMeta rowMeta = new RowMeta();
    for (String name : names) {
      rowMeta.addValueMeta(string(name));
    }
    return rowMeta;
  }

  @Test
  void lookupsAfterEachAppend() {
    RowMeta rowMeta = new RowMeta();
    assertConsistent(rowMeta);
    assertAbsent(rowMeta, "a");

    IValueMeta a = string("a");
    rowMeta.addValueMeta(a);
    assertConsistent(rowMeta, "a");
    assertSame(a, rowMeta.getValueMeta(0));
    assertAbsent(rowMeta, "b", "c");

    IValueMeta b = integer("b");
    rowMeta.addValueMeta(b);
    assertConsistent(rowMeta, "a", "b");
    assertSame(b, rowMeta.getValueMeta(1));
    assertAbsent(rowMeta, "c");

    IValueMeta c = new ValueMetaDate("c");
    rowMeta.addValueMeta(c);
    assertConsistent(rowMeta, "a", "b", "c");
    assertSame(c, rowMeta.getValueMeta(2));
    assertAbsent(rowMeta, "d", "ab", "");
  }

  @Test
  void lookupsAfterInsertAtTheStartTheMiddleAndTheEnd() {
    RowMeta rowMeta = rowOf("a", "b", "c");

    IValueMeta x = string("x");
    rowMeta.addValueMeta(0, x);
    assertConsistent(rowMeta, "x", "a", "b", "c");
    assertSame(x, rowMeta.getValueMeta(0));

    IValueMeta y = string("y");
    rowMeta.addValueMeta(2, y);
    assertConsistent(rowMeta, "x", "a", "y", "b", "c");
    assertSame(y, rowMeta.getValueMeta(2));

    IValueMeta z = string("z");
    rowMeta.addValueMeta(rowMeta.size(), z);
    assertConsistent(rowMeta, "x", "a", "y", "b", "c", "z");
    assertSame(z, rowMeta.getValueMeta(5));
  }

  @Test
  void insertWithoutLookupsInBetweenKeepsEveryIndexRight() {
    RowMeta rowMeta = rowOf("a", "b", "c");
    // no lookups between the inserts: whatever the cache holds must be shifted correctly
    rowMeta.addValueMeta(1, string("x"));
    rowMeta.addValueMeta(0, string("y"));
    rowMeta.addValueMeta(4, string("z"));
    rowMeta.addValueMeta(2, string("w"));
    assertConsistent(rowMeta, "y", "a", "w", "x", "b", "z", "c");
  }

  @Test
  void lookupsAfterReplace() {
    RowMeta rowMeta = rowOf("a", "b", "c");

    IValueMeta x = string("x");
    rowMeta.setValueMeta(1, x);
    assertConsistent(rowMeta, "a", "x", "c");
    assertSame(x, rowMeta.getValueMeta(1));
    assertAbsent(rowMeta, "b");

    // a replacement with the name of the field it replaces, in another case, is not renamed
    IValueMeta upperX = string("X");
    rowMeta.setValueMeta(1, upperX);
    assertConsistent(rowMeta, "a", "X", "c");
    assertSame(upperX, rowMeta.getValueMeta(1));

    // a replacement with the name of another field is renamed
    IValueMeta upperA = string("A");
    rowMeta.setValueMeta(2, upperA);
    assertConsistent(rowMeta, "a", "X", "A_1");
    assertNotSame(upperA, rowMeta.getValueMeta(2));
    assertEquals("A", upperA.getName());
    assertAbsent(rowMeta, "c");
  }

  @Test
  void lookupsAfterRemoveByIndex() {
    RowMeta rowMeta = rowOf("a", "b", "c", "d", "e");

    rowMeta.removeValueMeta(0);
    assertConsistent(rowMeta, "b", "c", "d", "e");
    assertAbsent(rowMeta, "a");

    rowMeta.removeValueMeta(1);
    assertConsistent(rowMeta, "b", "d", "e");
    assertAbsent(rowMeta, "c");

    rowMeta.removeValueMeta(2);
    assertConsistent(rowMeta, "b", "d");
    assertAbsent(rowMeta, "e");

    rowMeta.removeValueMeta(0);
    rowMeta.removeValueMeta(0);
    assertConsistent(rowMeta);
    assertAbsent(rowMeta, "b", "d");
  }

  @Test
  void lookupsAfterRemoveByNameInAnyCase() throws Exception {
    RowMeta rowMeta = rowOf("a", "Bb", "c");

    rowMeta.removeValueMeta("bB");
    assertConsistent(rowMeta, "a", "c");
    assertAbsent(rowMeta, "bb");

    rowMeta.removeValueMeta("A");
    assertConsistent(rowMeta, "c");

    rowMeta.removeValueMeta("c");
    assertConsistent(rowMeta);
  }

  @Test
  void removeByUnknownNameThrowsAndKeepsTheRow() {
    RowMeta rowMeta = rowOf("a", "b");

    HopValueException e =
        assertThrows(HopValueException.class, () -> rowMeta.removeValueMeta("missing"));
    assertEquals(
        Const.CR
            + "Unable to find value metadata with name 'missing', so I can't delete it."
            + Const.CR,
        e.getMessage());

    HopValueException nullName =
        assertThrows(HopValueException.class, () -> rowMeta.removeValueMeta((String) null));
    assertEquals(
        Const.CR
            + "Unable to find value metadata with name 'null', so I can't delete it."
            + Const.CR,
        nullName.getMessage());

    assertConsistent(rowMeta, "a", "b");
  }

  @Test
  void outOfRangeMutationsThrowAndKeepTheRow() {
    RowMeta rowMeta = rowOf("a", "b", "c");

    IndexOutOfBoundsException remove =
        assertThrows(IndexOutOfBoundsException.class, () -> rowMeta.removeValueMeta(3));
    assertEquals("Index 3 out of bounds for length 3", remove.getMessage());
    IndexOutOfBoundsException removeNegative =
        assertThrows(IndexOutOfBoundsException.class, () -> rowMeta.removeValueMeta(-1));
    assertEquals("Index -1 out of bounds for length 3", removeNegative.getMessage());

    IndexOutOfBoundsException insert =
        assertThrows(IndexOutOfBoundsException.class, () -> rowMeta.addValueMeta(4, string("x")));
    assertEquals("Index: 4, Size: 3", insert.getMessage());
    // a duplicate name is renamed before the insert fails; the row must not change either way
    IndexOutOfBoundsException insertDuplicate =
        assertThrows(IndexOutOfBoundsException.class, () -> rowMeta.addValueMeta(-1, string("a")));
    assertEquals("Index: -1, Size: 3", insertDuplicate.getMessage());

    IndexOutOfBoundsException replace =
        assertThrows(IndexOutOfBoundsException.class, () -> rowMeta.setValueMeta(3, string("x")));
    assertEquals("Index 3 out of bounds for length 3", replace.getMessage());

    assertConsistent(rowMeta, "a", "b", "c");
    assertAbsent(rowMeta, "x", "a_1");

    // the row still accepts mutations afterwards
    rowMeta.addValueMeta(string("x"));
    rowMeta.addValueMeta(string("a"));
    assertConsistent(rowMeta, "a", "b", "c", "x", "a_1");
  }

  @Test
  void nullValueMetasAreIgnored() {
    RowMeta rowMeta = rowOf("a", "b");
    rowMeta.addValueMeta(null);
    rowMeta.addValueMeta(0, null);
    rowMeta.setValueMeta(0, null);
    // even out of range: a null value meta is ignored before the index is looked at
    rowMeta.addValueMeta(10, null);
    rowMeta.setValueMeta(10, null);
    assertConsistent(rowMeta, "a", "b");
    assertFalse(rowMeta.exists(null));
  }

  @Test
  void lookupsAfterSetValueMetaList() {
    RowMeta rowMeta = rowOf("a", "b", "c");

    List<IValueMeta> list = new ArrayList<>();
    IValueMeta x = string("x");
    IValueMeta y = integer("Y");
    list.add(x);
    list.add(y);
    rowMeta.setValueMetaList(list);
    assertConsistent(rowMeta, "x", "Y");
    assertSame(x, rowMeta.getValueMeta(0));
    assertSame(y, rowMeta.getValueMeta(1));
    assertAbsent(rowMeta, "a", "b", "c");

    // appends and inserts after a wholesale replacement
    rowMeta.addValueMeta(string("a"));
    rowMeta.addValueMeta(0, string("b"));
    rowMeta.addValueMeta(string("y"));
    assertConsistent(rowMeta, "b", "x", "Y", "a", "y_1");

    rowMeta.setValueMetaList(new ArrayList<>());
    assertConsistent(rowMeta);
    assertAbsent(rowMeta, "x", "y", "a", "b");
    rowMeta.addValueMeta(string("x"));
    assertConsistent(rowMeta, "x");
  }

  @Test
  void lookupsAfterClear() {
    RowMeta rowMeta = rowOf("a", "b", "c");
    rowMeta.clear();
    assertConsistent(rowMeta);
    assertAbsent(rowMeta, "a", "b", "c");

    // the old names are free again
    rowMeta.addValueMeta(string("c"));
    rowMeta.addValueMeta(string("A"));
    rowMeta.addValueMeta(string("b"));
    assertConsistent(rowMeta, "c", "A", "b");

    rowMeta.clear();
    rowMeta.clear();
    assertConsistent(rowMeta);
  }

  @Test
  void lookupsAfterAddRowMeta() {
    RowMeta rowMeta = rowOf("a", "b");
    IValueMeta c = string("c");
    IValueMeta upperA = integer("A");
    IValueMeta d = string("d");
    RowMeta other = new RowMeta();
    other.addValueMeta(c);
    other.addValueMeta(upperA);
    other.addValueMeta(d);

    rowMeta.addRowMeta(other);
    assertConsistent(rowMeta, "a", "b", "c", "A_1", "d");
    assertSame(c, rowMeta.getValueMeta(2));
    assertNotSame(upperA, rowMeta.getValueMeta(3));
    assertEquals(IValueMeta.TYPE_INTEGER, rowMeta.getValueMeta(3).getType());
    assertSame(d, rowMeta.getValueMeta(4));
    // the source row is not changed
    assertConsistent(other, "c", "A", "d");

    rowMeta.addRowMeta(new RowMeta());
    assertConsistent(rowMeta, "a", "b", "c", "A_1", "d");
  }

  @Test
  void lookupsAfterMergeRowMetaWithoutOrigin() {
    RowMeta rowMeta = rowOf("a", "b");
    IValueMeta c = string("c");
    c.setOrigin("source");
    IValueMeta upperB = string("B");
    upperB.setOrigin("source");
    RowMeta other = new RowMeta();
    other.addValueMeta(c);
    other.addValueMeta(upperB);

    rowMeta.mergeRowMeta(other);
    assertConsistent(rowMeta, "a", "b", "c", "B_1");
    assertSame(c, rowMeta.getValueMeta(2));
    assertNotSame(upperB, rowMeta.getValueMeta(3));
    assertEquals("source", rowMeta.getValueMeta(3).getOrigin());
    assertEquals("B", upperB.getName());
    assertConsistent(other, "c", "B");
  }

  @Test
  void lookupsAfterMergeRowMetaWithOrigin() {
    RowMeta rowMeta = rowOf("a", "b");
    IValueMeta c = string("c");
    c.setOrigin("source");
    IValueMeta a = string("a");
    a.setOrigin("source");
    IValueMeta upperB = string("B");
    upperB.setOrigin("source");
    RowMeta other = new RowMeta();
    other.addValueMeta(c);
    other.addValueMeta(a);
    other.addValueMeta(upperB);

    rowMeta.mergeRowMeta(other, "merger");
    assertConsistent(rowMeta, "a", "b", "c", "a_1", "B_1");
    // fields that are added unchanged keep their object and origin
    assertSame(c, rowMeta.getValueMeta(2));
    assertEquals("source", c.getOrigin());
    // renamed fields are copies with the new origin
    assertEquals("merger", rowMeta.getValueMeta(3).getOrigin());
    assertEquals("merger", rowMeta.getValueMeta(4).getOrigin());
    assertEquals("source", a.getOrigin());
    assertEquals("a", a.getName());

    // merging the same row again renames every field
    rowMeta.mergeRowMeta(other, "again");
    assertConsistent(rowMeta, "a", "b", "c", "a_1", "B_1", "c_1", "a_2", "B_2");
    assertEquals("again", rowMeta.getValueMeta(5).getOrigin());
  }

  @Test
  void lookupsOnACloneAndIndependenceOfTheOriginal() {
    RowMeta original = new RowMeta();
    original.addValueMeta(string("a"));
    original.addValueMeta(integer("b"));
    original.addValueMeta(new ValueMetaNumber("c"));
    original.addValueMeta(string("A"));
    assertConsistent(original, "a", "b", "c", "A_1");

    RowMeta clone = original.clone();
    assertConsistent(clone, "a", "b", "c", "A_1");
    for (int i = 0; i < clone.size(); i++) {
      assertNotSame(original.getValueMeta(i), clone.getValueMeta(i));
      assertEquals(original.getValueMeta(i).getType(), clone.getValueMeta(i).getType());
    }

    clone.addValueMeta(string("d"));
    clone.removeValueMeta(0);
    clone.addValueMeta(0, string("a"));
    assertConsistent(clone, "a", "b", "c", "A_1", "d");
    assertConsistent(original, "a", "b", "c", "A_1");
    assertAbsent(original, "d");

    original.addValueMeta(string("e"));
    original.removeValueMeta(1);
    assertConsistent(original, "a", "c", "A_1", "e");
    assertConsistent(clone, "a", "b", "c", "A_1", "d");
    assertAbsent(clone, "e");

    // a duplicate in the clone is found from the cache state the clone copied
    clone.addValueMeta(string("C"));
    assertConsistent(clone, "a", "b", "c", "A_1", "d", "C_1");
  }

  @Test
  void cloneOfAnEmptyRowAndOfARowWithoutLookups() {
    RowMeta empty = new RowMeta().clone();
    assertConsistent(empty);
    empty.addValueMeta(string("a"));
    empty.addValueMeta(string("a"));
    assertConsistent(empty, "a", "a_1");

    RowMeta afterRemove = rowOf("a", "b", "c");
    afterRemove.removeValueMeta(1);
    RowMeta clone = afterRemove.clone();
    // no lookups yet: the clone must find the remaining fields as duplicates
    clone.addValueMeta(string("C"));
    clone.addValueMeta(string("b"));
    assertConsistent(clone, "a", "c", "C_1", "b");
  }

  /** Every kind of mutation in turn, with lookups checked after each. */
  @Test
  void longMixedSequence() throws Exception {
    RowMeta rowMeta = new RowMeta();

    rowMeta.addValueMeta(string("a"));
    rowMeta.addValueMeta(string("b"));
    assertConsistent(rowMeta, "a", "b");

    rowMeta.addValueMeta(string("A"));
    assertConsistent(rowMeta, "a", "b", "A_1");

    rowMeta.addValueMeta(0, string("c"));
    assertConsistent(rowMeta, "c", "a", "b", "A_1");

    rowMeta.addValueMeta(2, string("B"));
    assertConsistent(rowMeta, "c", "a", "B_1", "b", "A_1");

    rowMeta.setValueMeta(0, string("d"));
    assertConsistent(rowMeta, "d", "a", "B_1", "b", "A_1");
    assertAbsent(rowMeta, "c");

    rowMeta.removeValueMeta("a");
    assertConsistent(rowMeta, "d", "B_1", "b", "A_1");

    rowMeta.addValueMeta(string("a"));
    assertConsistent(rowMeta, "d", "B_1", "b", "A_1", "a");

    // a_1 is taken (as A_1, ignoring case), so the next free suffix is used
    rowMeta.addValueMeta(string("a"));
    assertConsistent(rowMeta, "d", "B_1", "b", "A_1", "a", "a_2");

    rowMeta.removeValueMeta(1);
    assertConsistent(rowMeta, "d", "b", "A_1", "a", "a_2");

    rowMeta.addValueMeta(string("b_1"));
    assertConsistent(rowMeta, "d", "b", "A_1", "a", "a_2", "b_1");

    rowMeta.addValueMeta(string("B"));
    assertConsistent(rowMeta, "d", "b", "A_1", "a", "a_2", "b_1", "B_2");

    RowMeta source = rowOf("e", "D", "a_2");
    rowMeta.addRowMeta(source);
    assertConsistent(rowMeta, "d", "b", "A_1", "a", "a_2", "b_1", "B_2", "e", "D_1", "a_2_1");

    RowMeta toMerge = rowOf("f", "E");
    rowMeta.mergeRowMeta(toMerge, "merge");
    assertConsistent(
        rowMeta, "d", "b", "A_1", "a", "a_2", "b_1", "B_2", "e", "D_1", "a_2_1", "f", "E_1");
    assertEquals("merge", rowMeta.getValueMeta(11).getOrigin());
    assertNull(rowMeta.getValueMeta(10).getOrigin());

    RowMeta clone = rowMeta.clone();
    assertConsistent(
        clone, "d", "b", "A_1", "a", "a_2", "b_1", "B_2", "e", "D_1", "a_2_1", "f", "E_1");

    clone.removeValueMeta("A_1");
    assertConsistent(clone, "d", "b", "a", "a_2", "b_1", "B_2", "e", "D_1", "a_2_1", "f", "E_1");

    // b, b_1 and B_2 are taken
    clone.setValueMeta(0, string("b"));
    assertConsistent(clone, "b_3", "b", "a", "a_2", "b_1", "B_2", "e", "D_1", "a_2_1", "f", "E_1");

    clone.addValueMeta(clone.size(), string("e"));
    assertConsistent(
        clone, "b_3", "b", "a", "a_2", "b_1", "B_2", "e", "D_1", "a_2_1", "f", "E_1", "e_2");

    // the original did not follow the clone
    assertConsistent(
        rowMeta, "d", "b", "A_1", "a", "a_2", "b_1", "B_2", "e", "D_1", "a_2_1", "f", "E_1");

    clone.clear();
    assertConsistent(clone);

    clone.addValueMeta(string("a"));
    assertConsistent(clone, "a");

    List<IValueMeta> list = new ArrayList<>();
    list.add(string("x"));
    list.add(string("y"));
    clone.setValueMetaList(list);
    assertConsistent(clone, "x", "y");

    clone.addValueMeta(string("X"));
    assertConsistent(clone, "x", "y", "X_1");
  }
}
