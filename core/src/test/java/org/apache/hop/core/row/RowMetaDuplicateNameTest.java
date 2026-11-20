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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.Test;

/**
 * Pins how {@link RowMeta} treats a field whose name is already in the row, ignoring case: adds,
 * inserts, replaces and merges rename a copy to {@code name_1}, {@code name_2}, ..., while {@link
 * RowMeta#setValueMetaList(List)} keeps duplicates as they are.
 */
class RowMetaDuplicateNameTest {

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

  private static RowMeta listRowOf(String... names) {
    RowMeta rowMeta = new RowMeta();
    List<IValueMeta> list = new ArrayList<>();
    for (String name : names) {
      list.add(string(name));
    }
    rowMeta.setValueMetaList(list);
    return rowMeta;
  }

  @Test
  void appendingADuplicateAddsARenamedCopyWithTheFirstFreeSuffix() {
    RowMeta rowMeta = new RowMeta();
    IValueMeta first = new ValueMetaInteger("a");
    first.setOrigin("origin");
    rowMeta.addValueMeta(first);
    IValueMeta second = new ValueMetaInteger("a");
    second.setOrigin("origin");
    rowMeta.addValueMeta(second);
    IValueMeta third = new ValueMetaInteger("a");
    rowMeta.addValueMeta(third);
    rowMeta.addValueMeta(third);

    assertConsistent(rowMeta, "a", "a_1", "a_2", "a_3");
    assertSame(first, rowMeta.getValueMeta(0));
    assertNotSame(second, rowMeta.getValueMeta(1));
    assertNotSame(third, rowMeta.getValueMeta(2));
    assertNotSame(third, rowMeta.getValueMeta(3));
    // the value metas that were passed in keep their name
    assertEquals("a", second.getName());
    assertEquals("a", third.getName());
    // a renamed copy keeps everything but the name
    assertEquals(IValueMeta.TYPE_INTEGER, rowMeta.getValueMeta(1).getType());
    assertEquals("origin", rowMeta.getValueMeta(1).getOrigin());
    assertNull(rowMeta.getValueMeta(2).getOrigin());
  }

  @Test
  void duplicateDetectionIgnoresCase() {
    RowMeta rowMeta = rowOf("Name", "NAME", "name", "nAmE");
    // name_1 is taken by NAME_1 and name_2 by name_2, ignoring case
    assertConsistent(rowMeta, "Name", "NAME_1", "name_2", "nAmE_3");
  }

  @Test
  void theSuffixSkipsNamesThatAreTakenInAnyCase() {
    RowMeta rowMeta = rowOf("a", "a_1", "A_3");
    rowMeta.addValueMeta(string("a"));
    assertConsistent(rowMeta, "a", "a_1", "A_3", "a_2");
    rowMeta.addValueMeta(string("A"));
    assertConsistent(rowMeta, "a", "a_1", "A_3", "a_2", "A_4");
  }

  @Test
  void aRenamedNameCanBeRenamedAgain() {
    RowMeta rowMeta = rowOf("a_1", "a_1", "a_1_1", "a_1");
    assertConsistent(rowMeta, "a_1", "a_1_1", "a_1_1_1", "a_1_2");
  }

  @Test
  void theSameValueMetaAddedTwiceIsCopiedTheSecondTime() {
    RowMeta rowMeta = new RowMeta();
    IValueMeta a = string("a");
    rowMeta.addValueMeta(a);
    rowMeta.addValueMeta(a);
    rowMeta.addValueMeta(0, a);
    assertConsistent(rowMeta, "a_2", "a", "a_1");
    assertSame(a, rowMeta.getValueMeta(1));
    assertEquals("a", a.getName());
  }

  @Test
  void insertingADuplicateInsertsARenamedCopy() {
    RowMeta rowMeta = rowOf("a", "b");
    rowMeta.addValueMeta(0, string("B"));
    assertConsistent(rowMeta, "B_1", "a", "b");
    rowMeta.addValueMeta(1, string("A"));
    assertConsistent(rowMeta, "B_1", "A_1", "a", "b");
    rowMeta.addValueMeta(4, string("a"));
    assertConsistent(rowMeta, "B_1", "A_1", "a", "b", "a_2");
    rowMeta.addValueMeta(2, string("b_1"));
    assertConsistent(rowMeta, "B_1", "A_1", "b_1_1", "a", "b", "a_2");
  }

  @Test
  void insertingADuplicateWhoseFirstSuffixIsTaken() {
    RowMeta rowMeta = rowOf("a", "a_1");
    rowMeta.addValueMeta(0, string("a"));
    assertConsistent(rowMeta, "a_2", "a", "a_1");
  }

  @Test
  void replacingWithANameThatIsElsewhereInTheRowRenames() {
    RowMeta rowMeta = rowOf("a", "b", "c");

    rowMeta.setValueMeta(0, string("C"));
    assertConsistent(rowMeta, "C_1", "b", "c");

    // the name of the field being replaced is not a duplicate
    IValueMeta upperB = string("B");
    rowMeta.setValueMeta(1, upperB);
    assertConsistent(rowMeta, "C_1", "B", "c");
    assertSame(upperB, rowMeta.getValueMeta(1));

    // c_1 is C_1 ignoring case
    rowMeta.setValueMeta(2, string("c_1"));
    assertConsistent(rowMeta, "C_1", "B", "c_1_1");

    // C_1_1 is taken by c_1_1 ignoring case
    rowMeta.setValueMeta(1, string("C_1"));
    assertConsistent(rowMeta, "C_1", "C_1_2", "c_1_1");
  }

  @Test
  void theReplacedFieldStillBlocksItsNameAsASuffix() {
    // a_1 is the field being replaced, yet it still counts as taken when the new one is renamed
    RowMeta rowMeta = rowOf("a", "a_1");
    rowMeta.setValueMeta(1, string("a"));
    assertConsistent(rowMeta, "a", "a_2");
    assertAbsent(rowMeta, "a_1");
  }

  @Test
  void setValueMetaListKeepsDuplicatesAndTheLastOneIsFoundByName() {
    RowMeta rowMeta = listRowOf("a", "b", "a", "A");
    assertNames(rowMeta, "a", "b", "a", "A");
    // upstream maps each name as the list is set, so the last of equal names wins
    assertEquals(3, rowMeta.indexOfValue("a"));
    assertEquals(3, rowMeta.indexOfValue("A"));
    assertSame(rowMeta.getValueMeta(3), rowMeta.searchValueMeta("a"));
    assertEquals(1, rowMeta.indexOfValue("b"));
    assertTrue(rowMeta.exists(rowMeta.getValueMeta(0)));

    rowMeta.addValueMeta(string("a"));
    rowMeta.addValueMeta(string("A"));
    rowMeta.addValueMeta(string("B"));
    assertNames(rowMeta, "a", "b", "a", "A", "a_1", "A_2", "B_1");
    assertEquals(4, rowMeta.indexOfValue("a_1"));
    assertEquals(5, rowMeta.indexOfValue("a_2"));
    assertEquals(6, rowMeta.indexOfValue("b_1"));
  }

  @Test
  void removingADuplicatedNameRemovesTheOneThatIsFoundAndThenFindsTheOther() throws Exception {
    RowMeta rowMeta = listRowOf("a", "b", "a");
    assertEquals(2, rowMeta.indexOfValue("a"));

    rowMeta.removeValueMeta("a");
    assertConsistent(rowMeta, "a", "b");
    assertSame(rowMeta.getValueMeta(0), rowMeta.searchValueMeta("A"));

    rowMeta.removeValueMeta("A");
    assertConsistent(rowMeta, "b");
  }

  @Test
  void replacingTheMappedOccurrenceOfANameHeldTwiceStillDetectsTheOther() {
    // setValueMetaList maps "a" to index 2; replacing index 2 drops that mapping
    RowMeta rowMeta = listRowOf("a", "b", "a");
    rowMeta.setValueMeta(2, string("x"));
    // no lookup in between: the duplicate check itself must find the "a" at index 0
    rowMeta.addValueMeta(string("a"));
    assertConsistent(rowMeta, "a", "b", "x", "a_1");
  }

  @Test
  void replacingTheOtherOccurrenceOfANameHeldTwiceStillDetectsTheMappedOne() {
    // setValueMetaList maps "a" to index 2; replacing index 0 removes "a" from the mapping too
    RowMeta rowMeta = listRowOf("a", "b", "a");
    rowMeta.setValueMeta(0, string("x"));
    rowMeta.addValueMeta(string("A"));
    assertConsistent(rowMeta, "x", "b", "a", "A_1");
  }

  @Test
  void replacingAnOccurrenceOfANameHeldTwiceThenInsertingTheName() {
    RowMeta rowMeta = listRowOf("a", "b", "a");
    rowMeta.setValueMeta(2, string("x"));
    rowMeta.addValueMeta(0, string("a"));
    assertConsistent(rowMeta, "a_1", "a", "b", "x");
  }

  @Test
  void replacingAnOccurrenceOfANameHeldTwiceThenMerging() {
    RowMeta rowMeta = listRowOf("a", "b", "a");
    rowMeta.setValueMeta(0, string("x"));
    rowMeta.mergeRowMeta(rowOf("a", "c"), "merged");
    assertConsistent(rowMeta, "x", "b", "a", "a_1", "c");
    assertEquals("merged", rowMeta.getValueMeta(3).getOrigin());
  }

  @Test
  void replacingWithACaseVariantOfANameHeldTwice() {
    RowMeta rowMeta = listRowOf("a", "b", "a");
    // index 2 holds the mapped "a"; the replacement is the same name in upper case
    rowMeta.setValueMeta(2, string("A"));
    assertNames(rowMeta, "a", "b", "A");
    assertEquals(2, rowMeta.indexOfValue("a"));
    rowMeta.addValueMeta(string("a"));
    assertNames(rowMeta, "a", "b", "A", "a_1");
  }

  @Test
  void insertingIntoARowWithDuplicatesShiftsTheMappedOccurrence() {
    RowMeta rowMeta = listRowOf("a", "a");
    rowMeta.addValueMeta(0, string("a"));
    assertNames(rowMeta, "a_1", "a", "a");
    assertEquals(2, rowMeta.indexOfValue("a"));
    assertEquals(0, rowMeta.indexOfValue("A_1"));
  }

  @Test
  void addRowMetaWithDuplicatesInsideTheSource() {
    RowMeta source = listRowOf("a", "a", "b", "A");

    RowMeta empty = new RowMeta();
    empty.addRowMeta(source);
    assertConsistent(empty, "a", "a_1", "b", "A_2");
    assertSame(source.getValueMeta(0), empty.getValueMeta(0));
    assertSame(source.getValueMeta(2), empty.getValueMeta(2));

    RowMeta rowMeta = rowOf("b");
    rowMeta.addRowMeta(source);
    assertConsistent(rowMeta, "b", "a", "a_1", "b_1", "A_2");
  }

  @Test
  void mergeRowMetaWithDuplicatesInsideTheSource() {
    RowMeta source = listRowOf("a", "a", "b", "A");

    RowMeta empty = new RowMeta();
    empty.mergeRowMeta(source);
    assertConsistent(empty, "a", "a_1", "b", "A_2");
    assertSame(source.getValueMeta(0), empty.getValueMeta(0));

    RowMeta rowMeta = rowOf("a");
    rowMeta.mergeRowMeta(source, "merged");
    assertConsistent(rowMeta, "a", "a_1", "a_2", "b", "A_3");
    assertEquals("merged", rowMeta.getValueMeta(1).getOrigin());
    assertEquals("merged", rowMeta.getValueMeta(2).getOrigin());
    assertNull(rowMeta.getValueMeta(3).getOrigin());
    assertEquals("merged", rowMeta.getValueMeta(4).getOrigin());
    assertSame(source.getValueMeta(2), rowMeta.getValueMeta(3));
  }

  @Test
  void valueMetasWithoutANameAreNeverRenamedOrFound() {
    RowMeta rowMeta = new RowMeta();
    IValueMeta unnamed = string(null);
    rowMeta.addValueMeta(unnamed);
    rowMeta.addValueMeta(unnamed);
    rowMeta.addValueMeta(string(null));
    assertNames(rowMeta, null, null, null);
    assertSame(unnamed, rowMeta.getValueMeta(0));
    assertSame(unnamed, rowMeta.getValueMeta(1));
    assertEquals(-1, rowMeta.indexOfValue(null));
    assertEquals(-1, rowMeta.indexOfValue(""));
    assertNull(rowMeta.searchValueMeta(null));
    assertNull(rowMeta.searchValueMeta(""));
    assertFalse(rowMeta.exists(unnamed));

    rowMeta.addValueMeta(string("a"));
    rowMeta.addValueMeta(1, string(null));
    rowMeta.addValueMeta(string("a"));
    assertNames(rowMeta, null, null, null, null, "a", "a_1");
    assertEquals(4, rowMeta.indexOfValue("a"));
    assertEquals(5, rowMeta.indexOfValue("A_1"));
  }

  @Test
  void valueMetasWithAnEmptyNameAreKeptButFoundByTheEmptyName() {
    RowMeta rowMeta = new RowMeta();
    IValueMeta empty = string("");
    rowMeta.addValueMeta(string(null));
    rowMeta.addValueMeta(empty);
    rowMeta.addValueMeta(string(""));
    assertNames(rowMeta, null, "", "");
    assertEquals(1, rowMeta.indexOfValue(""));
    assertSame(empty, rowMeta.searchValueMeta(""));
    assertTrue(rowMeta.exists(empty));
    assertEquals(-1, rowMeta.indexOfValue(null));
  }

  @Test
  void insertingAnUnnamedValueMetaKeepsTheOtherIndexesRight() {
    RowMeta rowMeta = rowOf("a", "b", "c");
    rowMeta.addValueMeta(0, string(null));
    rowMeta.addValueMeta(2, string(""));
    assertNames(rowMeta, null, "a", "", "b", "c");
    // no lookups yet: the duplicate checks must find the shifted fields
    rowMeta.addValueMeta(string("c"));
    rowMeta.addValueMeta(0, string("A"));
    rowMeta.addValueMeta(string("b"));
    assertNames(rowMeta, "A_1", null, "a", "", "b", "c", "c_1", "b_1");
    assertEquals(2, rowMeta.indexOfValue("a"));
    assertEquals(4, rowMeta.indexOfValue("B"));
    assertEquals(5, rowMeta.indexOfValue("c"));
    assertEquals(6, rowMeta.indexOfValue("c_1"));
    assertEquals(0, rowMeta.indexOfValue("a_1"));
  }

  @Test
  void replacingWithAnUnnamedValueMetaFreesTheName() {
    RowMeta rowMeta = rowOf("a", "b", "c");
    rowMeta.setValueMeta(1, string(null));
    assertNames(rowMeta, "a", null, "c");
    assertAbsent(rowMeta, "b");
    rowMeta.addValueMeta(string("b"));
    rowMeta.addValueMeta(string("C"));
    assertNames(rowMeta, "a", null, "c", "b", "C_1");
    assertEquals(3, rowMeta.indexOfValue("b"));
  }

  @Test
  void mergeRenamesAnEmptyNameButNotAMissingOne() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(string(""));
    rowMeta.addValueMeta(string(null));

    RowMeta other = new RowMeta();
    other.setValueMetaList(new ArrayList<>(Arrays.asList(string(""), string(null))));
    rowMeta.mergeRowMeta(other, "merged");

    // "" is found by searchValueMeta("") and renamed to "_1"; a null name is never found
    assertNames(rowMeta, "", null, "_1", null);
    assertEquals("merged", rowMeta.getValueMeta(2).getOrigin());
    assertSame(other.getValueMeta(1), rowMeta.getValueMeta(3));
    assertEquals(2, rowMeta.indexOfValue("_1"));
  }
}
