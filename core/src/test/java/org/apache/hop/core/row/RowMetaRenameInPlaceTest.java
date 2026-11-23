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
import static org.apache.hop.core.row.RowMetaAssert.assertLookups;
import static org.apache.hop.core.row.RowMetaAssert.assertNames;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Pins what {@link RowMeta} answers after a value meta in it is renamed in place, through {@code
 * getValueMeta(i).setName(...)}, without telling the row. Lookups and the duplicate check on add
 * must both see the new name. Where a test says "no lookup in between" the duplicate check is the
 * first thing that reads names after the rename.
 */
class RowMetaRenameInPlaceTest {

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

  private static RowMeta single(IValueMeta valueMeta) {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(valueMeta);
    return rowMeta;
  }

  /** The ways a field can be added to a row that check for a duplicate name. */
  enum Adder {
    APPEND {
      @Override
      void add(RowMeta rowMeta, IValueMeta valueMeta) {
        rowMeta.addValueMeta(valueMeta);
      }
    },
    INSERT_FIRST {
      @Override
      void add(RowMeta rowMeta, IValueMeta valueMeta) {
        rowMeta.addValueMeta(0, valueMeta);
      }

      @Override
      int position(int sizeBefore) {
        return 0;
      }
    },
    INSERT_MIDDLE {
      @Override
      void add(RowMeta rowMeta, IValueMeta valueMeta) {
        rowMeta.addValueMeta(1, valueMeta);
      }

      @Override
      int position(int sizeBefore) {
        return 1;
      }
    },
    ADD_ROW_META {
      @Override
      void add(RowMeta rowMeta, IValueMeta valueMeta) {
        rowMeta.addRowMeta(single(valueMeta));
      }
    },
    MERGE {
      @Override
      void add(RowMeta rowMeta, IValueMeta valueMeta) {
        rowMeta.mergeRowMeta(single(valueMeta));
      }
    },
    MERGE_WITH_ORIGIN {
      @Override
      void add(RowMeta rowMeta, IValueMeta valueMeta) {
        rowMeta.mergeRowMeta(single(valueMeta), "merged");
      }
    };

    abstract void add(RowMeta rowMeta, IValueMeta valueMeta);

    int position(int sizeBefore) {
      return sizeBefore;
    }

    String addAndGetName(RowMeta rowMeta, String name) {
      int sizeBefore = rowMeta.size();
      add(rowMeta, string(name));
      assertEquals(sizeBefore + 1, rowMeta.size());
      return rowMeta.getValueMeta(position(sizeBefore)).getName();
    }
  }

  @Test
  void aRenamedFieldIsFoundByItsNewNameAndNotByItsOldName() {
    RowMeta rowMeta = rowOf("a", "b", "c");
    IValueMeta b = rowMeta.getValueMeta(1);
    // look the old name up first, so the cache holds it
    assertEquals(1, rowMeta.indexOfValue("b"));

    b.setName("x");
    assertConsistent(rowMeta, "a", "x", "c");
    assertSame(b, rowMeta.searchValueMeta("X"));
    assertAbsent(rowMeta, "b");
  }

  @ParameterizedTest
  @EnumSource(Adder.class)
  void theDuplicateCheckSeesTheNewNameWithNoLookupInBetween(Adder adder) {
    RowMeta rowMeta = rowOf("a", "b", "c");
    rowMeta.getValueMeta(1).setName("x");

    assertEquals("X_1", adder.addAndGetName(rowMeta, "X"));
    // the old name is free
    assertEquals("b", adder.addAndGetName(rowMeta, "b"));
    assertLookups(rowMeta);
    assertAbsent(rowMeta, "b_1", "x_2");
  }

  @ParameterizedTest
  @EnumSource(Adder.class)
  void theDuplicateCheckSeesACaseOnlyRename(Adder adder) {
    RowMeta rowMeta = rowOf("a", "b", "c");
    rowMeta.getValueMeta(1).setName("B");

    assertEquals("b_1", adder.addAndGetName(rowMeta, "b"));
    assertLookups(rowMeta);
    assertEquals(rowMeta.indexOfValue("B"), rowMeta.indexOfValue("b"));
  }

  @ParameterizedTest
  @EnumSource(Adder.class)
  void theRenameSuffixSkipsANameGivenInPlace(Adder adder) {
    RowMeta rowMeta = rowOf("a", "b");
    // b now holds the name a duplicate of "a" would get first
    rowMeta.getValueMeta(1).setName("a_1");

    assertEquals("a_2", adder.addAndGetName(rowMeta, "a"));
    assertEquals("A_1_1", adder.addAndGetName(rowMeta, "A_1"));
    assertLookups(rowMeta);
  }

  @ParameterizedTest
  @EnumSource(Adder.class)
  void theDuplicateCheckSeesARenameAfterTheCacheWasCleared(Adder adder) throws Exception {
    RowMeta rowMeta = rowOf("a", "b", "c", "d");
    rowMeta.removeValueMeta("a");
    // the next add rebuilds whatever it needs, then the rename comes after that
    assertEquals("e", adder.addAndGetName(rowMeta, "e"));
    rowMeta.getValueMeta(rowMeta.indexOfValue("c")).setName("x");

    assertEquals("X_1", adder.addAndGetName(rowMeta, "X"));
    assertEquals("c", adder.addAndGetName(rowMeta, "c"));
    assertLookups(rowMeta);
  }

  @Test
  void replaceSeesTheNewName() {
    RowMeta rowMeta = rowOf("a", "b", "c");
    rowMeta.getValueMeta(1).setName("x");

    rowMeta.setValueMeta(2, string("X"));
    assertConsistent(rowMeta, "a", "x", "X_1");

    rowMeta.getValueMeta(2).setName("x_2");
    rowMeta.setValueMeta(0, string("x"));
    assertConsistent(rowMeta, "x_1", "x", "x_2");
  }

  @Test
  void aCaseOnlyRenameIsFoundInEveryCase() {
    RowMeta rowMeta = rowOf("a", "b", "c");
    assertEquals(1, rowMeta.indexOfValue("b"));
    rowMeta.getValueMeta(1).setName("B");
    assertConsistent(rowMeta, "a", "B", "c");
  }

  @Test
  void renamingToTheNameOfAnotherField() {
    RowMeta rowMeta = rowOf("a", "b", "c");
    IValueMeta c = rowMeta.getValueMeta(2);
    c.setName("A");

    assertNames(rowMeta, "a", "b", "A");
    // the field that had the name first is still the one that is found
    assertEquals(0, rowMeta.indexOfValue("a"));
    assertEquals(0, rowMeta.indexOfValue("A"));
    assertAbsent(rowMeta, "c");

    rowMeta.addValueMeta(string("a"));
    rowMeta.addValueMeta(string("c"));
    assertNames(rowMeta, "a", "b", "A", "a_1", "c");
    assertEquals(3, rowMeta.indexOfValue("A_1"));
    assertEquals(4, rowMeta.indexOfValue("c"));
  }

  @Test
  void renamingToTheNameOfAnotherFieldWithNoLookupInBetween() {
    RowMeta rowMeta = rowOf("a", "b", "c");
    rowMeta.getValueMeta(0).setName("C");
    rowMeta.addValueMeta(string("c"));
    rowMeta.addValueMeta(string("a"));
    assertNames(rowMeta, "C", "b", "c", "c_1", "a");
  }

  @Test
  void renamingBack() {
    RowMeta rowMeta = rowOf("a", "b", "c");
    IValueMeta b = rowMeta.getValueMeta(1);
    b.setName("x");
    assertConsistent(rowMeta, "a", "x", "c");
    b.setName("b");
    assertConsistent(rowMeta, "a", "b", "c");
    assertAbsent(rowMeta, "x");

    rowMeta.addValueMeta(string("x"));
    rowMeta.addValueMeta(string("B"));
    assertConsistent(rowMeta, "a", "b", "c", "x", "B_1");
  }

  @Test
  void renamingAwayAndBackWithNoLookupInBetween() {
    RowMeta rowMeta = rowOf("a", "b", "c");
    IValueMeta b = rowMeta.getValueMeta(1);
    b.setName("x");
    rowMeta.addValueMeta(string("y"));
    b.setName("b");
    rowMeta.addValueMeta(string("x"));
    rowMeta.addValueMeta(string("B"));
    rowMeta.addValueMeta(string("Y"));
    assertConsistent(rowMeta, "a", "b", "c", "y", "x", "B_1", "Y_1");
  }

  @Test
  void aValueMetaSharedByTwoRowsAndRenamedThroughOne() {
    IValueMeta shared = string("s");
    RowMeta first = new RowMeta();
    first.addValueMeta(string("a"));
    first.addValueMeta(shared);
    RowMeta second = new RowMeta();
    second.addValueMeta(shared);
    second.addValueMeta(string("z"));

    first.getValueMeta(1).setName("t");

    // no lookup on the second row before its duplicate checks
    second.addValueMeta(string("T"));
    second.addValueMeta(string("s"));
    assertConsistent(second, "t", "z", "T_1", "s");
    assertSame(shared, second.getValueMeta(0));

    first.addValueMeta(string("s"));
    first.addValueMeta(string("t"));
    assertConsistent(first, "a", "t", "s", "t_1");
    assertSame(shared, first.getValueMeta(1));
  }

  @Test
  void aRenameInACloneDoesNotAffectTheOriginal() {
    RowMeta original = rowOf("a", "b", "c");
    RowMeta clone = original.clone();

    clone.getValueMeta(1).setName("x");

    assertConsistent(original, "a", "b", "c");
    assertAbsent(original, "x");
    original.addValueMeta(string("x"));
    original.addValueMeta(string("b"));
    assertConsistent(original, "a", "b", "c", "x", "b_1");

    clone.addValueMeta(string("x"));
    clone.addValueMeta(string("b"));
    assertConsistent(clone, "a", "x", "c", "x_1", "b");
  }

  @Test
  void aRenameInTheOriginalDoesNotAffectAClone() {
    RowMeta original = rowOf("a", "b", "c");
    RowMeta clone = original.clone();

    original.getValueMeta(1).setName("x");

    clone.addValueMeta(string("x"));
    clone.addValueMeta(string("b"));
    assertConsistent(clone, "a", "b", "c", "x", "b_1");

    original.addValueMeta(string("x"));
    original.addValueMeta(string("b"));
    assertConsistent(original, "a", "x", "c", "x_1", "b");
  }

  @Test
  void aCloneTakenAfterARenameSeesTheNewName() {
    RowMeta original = rowOf("a", "b", "c");
    original.getValueMeta(1).setName("x");
    RowMeta clone = original.clone();

    clone.addValueMeta(string("X"));
    clone.addValueMeta(string("b"));
    assertConsistent(clone, "a", "x", "c", "X_1", "b");
  }

  @Test
  void renamesAfterSetValueMetaList() {
    RowMeta rowMeta = new RowMeta();
    List<IValueMeta> list = new ArrayList<>();
    list.add(string("a"));
    list.add(string("b"));
    rowMeta.setValueMetaList(list);

    rowMeta.getValueMeta(1).setName("x");
    rowMeta.addValueMeta(string("x"));
    rowMeta.addValueMeta(string("b"));
    assertConsistent(rowMeta, "a", "x", "x_1", "b");
  }

  @Test
  void aRotationOfNamesBetweenAdds() {
    RowMeta rowMeta = rowOf("a", "b", "c", "d");
    rowMeta.getValueMeta(0).setName("tmp");
    rowMeta.getValueMeta(1).setName("a");
    rowMeta.getValueMeta(2).setName("b");
    rowMeta.getValueMeta(0).setName("c");

    rowMeta.addValueMeta(string("A"));
    rowMeta.addValueMeta(string("c"));
    rowMeta.addValueMeta(string("tmp"));
    assertConsistent(rowMeta, "c", "a", "b", "d", "A_1", "c_1", "tmp");
  }

  @Test
  void swappingTwoNamesThenAddingDuplicates() {
    RowMeta rowMeta = rowOf("x", "y");
    rowMeta.getValueMeta(0).setName("y");
    rowMeta.getValueMeta(1).setName("x");

    rowMeta.addValueMeta(string("x"));
    rowMeta.addValueMeta(string("y"));
    assertNames(rowMeta, "y", "x", "x_1", "y_1");
    assertConsistent(rowMeta, "y", "x", "x_1", "y_1");
  }

  @Test
  void renamingARenamedCopyFreesItsName() {
    RowMeta rowMeta = rowOf("a", "a");
    assertNames(rowMeta, "a", "a_1");
    rowMeta.getValueMeta(1).setName("q");

    rowMeta.addValueMeta(string("a"));
    rowMeta.addValueMeta(string("q"));
    assertConsistent(rowMeta, "a", "q", "a_1", "q_1");
  }

  @Test
  void anUnnamedValueMetaNamedAfterItWasAdded() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(string("a"));
    IValueMeta unnamed = string(null);
    rowMeta.addValueMeta(unnamed);
    IValueMeta empty = string("");
    rowMeta.addValueMeta(empty);

    unnamed.setName("x");
    empty.setName("y");
    rowMeta.addValueMeta(string("X"));
    rowMeta.addValueMeta(0, string("y"));
    assertConsistent(rowMeta, "y_1", "a", "x", "y", "X_1");
  }

  @Test
  void anUnnamedValueMetaNamedAfterTheCacheWasRebuilt() throws Exception {
    RowMeta rowMeta = rowOf("a", "b");
    IValueMeta unnamed = string(null);
    rowMeta.addValueMeta(unnamed);
    rowMeta.removeValueMeta("b");
    rowMeta.addValueMeta(string("q"));

    unnamed.setName("x");
    rowMeta.addValueMeta(string("x"));
    assertConsistent(rowMeta, "a", "x", "q", "x_1");
  }

  @Test
  void renamedToNoNameAndThenNamedAgain() {
    RowMeta rowMeta = rowOf("a", "b");
    IValueMeta b = rowMeta.getValueMeta(1);
    b.setName(null);
    rowMeta.addValueMeta(string("c"));
    // giving a name to a value meta that has none
    b.setName("x");
    rowMeta.addValueMeta(string("x"));
    rowMeta.addValueMeta(string("b"));
    assertConsistent(rowMeta, "a", "x", "c", "x_1", "b");
  }

  @Test
  void aRenameInAnUnrelatedRowChangesNothing() {
    RowMeta rowMeta = rowOf("a", "b");
    RowMeta unrelated = rowOf("a", "q");
    unrelated.getValueMeta(1).setName("b");

    rowMeta.addValueMeta(string("a"));
    rowMeta.addValueMeta(string("q"));
    assertConsistent(rowMeta, "a", "b", "a_1", "q");
  }

  /**
   * A value meta that does not extend {@link org.apache.hop.core.row.value.ValueMetaBase}: renaming
   * it in place goes through its own setName.
   */
  private static IValueMeta customValueMeta(String initialName) {
    String[] name = {initialName};
    IValueMeta valueMeta = mock(IValueMeta.class);
    when(valueMeta.getName()).thenAnswer(invocation -> name[0]);
    doAnswer(
            invocation -> {
              name[0] = invocation.getArgument(0);
              return null;
            })
        .when(valueMeta)
        .setName(any());
    when(valueMeta.clone()).thenAnswer(invocation -> customValueMeta(name[0]));
    return valueMeta;
  }

  @Test
  void aCustomValueMetaRenamedInPlace() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(string("a"));
    IValueMeta custom = customValueMeta("m");
    rowMeta.addValueMeta(custom);

    custom.setName("n");
    rowMeta.addValueMeta(string("N"));
    rowMeta.addValueMeta(string("m"));
    assertConsistent(rowMeta, "a", "n", "N_1", "m");
    assertSame(custom, rowMeta.getValueMeta(1));

    // a duplicate of the custom value meta is renamed through its own clone
    rowMeta.addValueMeta(customValueMeta("A"));
    assertConsistent(rowMeta, "a", "n", "N_1", "m", "A_1");
  }
}
