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

import static org.apache.hop.core.row.RowMetaAssert.assertConsistent;
import static org.apache.hop.core.row.RowMetaAssert.assertNames;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.Test;

/**
 * The package-private {@link RowMeta#valueMetaList}, and the list handed to {@link
 * RowMeta#setValueMetaList(List)} which the row keeps using, can be changed without going through
 * the row. These tests pin what the public API answers afterwards: the next mutation through the
 * row picks up the list as it is, and lookups by name scan it.
 */
class RowMetaDirectListMutationTest {

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

  @Test
  void aDirectAppendIsFoundByNameButCountedOnlyAfterTheNextAppend() {
    RowMeta rowMeta = rowOf("a", "b");
    IValueMeta c = string("c");
    rowMeta.valueMetaList.add(c);

    // size and index access still show the row as it was published by the last mutation
    assertEquals(2, rowMeta.size());
    assertNull(rowMeta.getValueMeta(2));
    assertArrayEquals(new String[] {"a", "b"}, rowMeta.getFieldNames());
    // the list and name lookups read the list itself
    assertEquals(3, rowMeta.getValueMetaList().size());
    assertEquals(2, rowMeta.indexOfValue("c"));
    assertSame(c, rowMeta.searchValueMeta("C"));

    IValueMeta d = string("d");
    rowMeta.addValueMeta(d);
    assertConsistent(rowMeta, "a", "b", "c", "d");
    assertSame(c, rowMeta.getValueMeta(2));
    assertSame(d, rowMeta.getValueMeta(3));
  }

  @Test
  void aDuplicateOfADirectlyAppendedFieldIsRenamedOnAppend() {
    RowMeta rowMeta = rowOf("a", "b");
    rowMeta.valueMetaList.add(string("c"));
    // no lookup in between: the duplicate check must see the field that bypassed the row
    rowMeta.addValueMeta(string("C"));
    assertConsistent(rowMeta, "a", "b", "c", "C_1");
  }

  @Test
  void aDuplicateOfADirectlyAppendedFieldIsRenamedOnInsert() {
    RowMeta rowMeta = rowOf("a", "b");
    rowMeta.valueMetaList.add(string("c"));
    rowMeta.addValueMeta(0, string("c"));
    assertConsistent(rowMeta, "c_1", "a", "b", "c");
  }

  @Test
  void aDuplicateOfADirectlyAppendedFieldIsRenamedOnMerge() {
    RowMeta rowMeta = rowOf("a", "b");
    rowMeta.valueMetaList.add(string("c"));
    rowMeta.mergeRowMeta(rowOf("c", "d"), "merged");
    assertConsistent(rowMeta, "a", "b", "c", "c_1", "d");
    assertEquals("merged", rowMeta.getValueMeta(3).getOrigin());
  }

  @Test
  void aCloneOfARowWithADirectAppendHoldsTheFieldAndDetectsItsDuplicate() {
    RowMeta rowMeta = rowOf("a", "b");
    rowMeta.valueMetaList.add(string("c"));
    RowMeta clone = rowMeta.clone();
    assertNames(clone, "a", "b", "c");
    clone.addValueMeta(string("C"));
    assertConsistent(clone, "a", "b", "c", "C_1");
  }

  @Test
  void aDirectReplaceIsPublishedByTheNextAppend() {
    RowMeta rowMeta = rowOf("a", "b", "c");
    IValueMeta x = string("x");
    rowMeta.valueMetaList.set(1, x);
    // still the published row
    assertEquals("b", rowMeta.getValueMeta(1).getName());

    rowMeta.addValueMeta(string("d"));
    assertConsistent(rowMeta, "a", "x", "c", "d");
    assertSame(x, rowMeta.getValueMeta(1));
  }

  @Test
  void aDirectRemoveAndAppendIsPublishedByTheNextAppend() {
    RowMeta rowMeta = rowOf("a", "b", "c");
    rowMeta.valueMetaList.remove(0);
    rowMeta.valueMetaList.add(string("z"));
    // same size as before, so only the contents differ from the published row
    assertEquals(3, rowMeta.size());

    rowMeta.addValueMeta(string("d"));
    assertConsistent(rowMeta, "b", "c", "z", "d");
  }

  @Test
  void aDirectRemovalIsPublishedByEveryOtherMutation() throws Exception {
    RowMeta insert = rowOf("a", "b", "c", "d");
    insert.valueMetaList.remove(3);
    insert.addValueMeta(0, string("x"));
    assertConsistent(insert, "x", "a", "b", "c");

    // the removed field is the last one, so no cached index points past the end of the list
    RowMeta replace = rowOf("a", "b", "c", "d");
    replace.valueMetaList.remove(3);
    replace.setValueMeta(0, string("x"));
    assertConsistent(replace, "x", "b", "c");

    RowMeta removeByIndex = rowOf("a", "b", "c", "d");
    removeByIndex.valueMetaList.remove(0);
    removeByIndex.removeValueMeta(0);
    assertConsistent(removeByIndex, "c", "d");

    RowMeta removeByName = rowOf("a", "b", "c", "d");
    removeByName.valueMetaList.remove(0);
    removeByName.removeValueMeta("c");
    assertConsistent(removeByName, "b", "d");

    RowMeta clear = rowOf("a", "b");
    clear.valueMetaList.remove(0);
    clear.clear();
    assertConsistent(clear);
  }

  @Test
  void aDirectClearIsPublishedByTheNextAppend() {
    RowMeta rowMeta = rowOf("a", "b");
    rowMeta.valueMetaList.clear();
    assertEquals(2, rowMeta.size());
    assertEquals("a", rowMeta.getValueMeta(0).getName());
    assertEquals(0, rowMeta.getValueMetaList().size());

    rowMeta.addValueMeta(string("z"));
    assertConsistent(rowMeta, "z");
  }

  /** Surprising but pinned: the name cache still points at the removed position. */
  @Test
  void lookingUpTheNameOfADirectlyRemovedLastFieldThrows() {
    RowMeta rowMeta = rowOf("a", "b", "c");
    rowMeta.valueMetaList.remove(2);
    IndexOutOfBoundsException e =
        assertThrows(IndexOutOfBoundsException.class, () -> rowMeta.indexOfValue("c"));
    assertEquals("Index 2 out of bounds for length 2", e.getMessage());
    assertEquals(1, rowMeta.indexOfValue("b"));
  }

  @Test
  void setValueMetaListKeepsUsingTheGivenList() {
    List<IValueMeta> list = new ArrayList<>();
    list.add(string("a"));
    list.add(string("b"));
    RowMeta rowMeta = new RowMeta();
    rowMeta.setValueMetaList(list);

    IValueMeta c = string("c");
    list.add(c);
    assertEquals(2, rowMeta.size());
    assertEquals(3, rowMeta.getValueMetaList().size());
    assertEquals(2, rowMeta.indexOfValue("c"));

    rowMeta.addValueMeta(string("d"));
    assertConsistent(rowMeta, "a", "b", "c", "d");
    // the row appended to the caller's list
    assertEquals(4, list.size());
    assertEquals("d", list.get(3).getName());
  }

  @Test
  void aDuplicateOfAFieldAddedToTheGivenListIsRenamed() {
    List<IValueMeta> list = new ArrayList<>();
    list.add(string("a"));
    list.add(string("b"));
    RowMeta rowMeta = new RowMeta();
    rowMeta.setValueMetaList(list);

    list.add(string("c"));
    rowMeta.addValueMeta(string("c"));
    assertConsistent(rowMeta, "a", "b", "c", "c_1");
  }

  @Test
  void aFieldReplacedInTheGivenListIsPublishedByTheNextAppend() {
    List<IValueMeta> list = new ArrayList<>();
    list.add(string("a"));
    list.add(string("b"));
    RowMeta rowMeta = new RowMeta();
    rowMeta.setValueMetaList(list);

    list.set(0, string("x"));
    rowMeta.addValueMeta(string("y"));
    assertConsistent(rowMeta, "x", "b", "y");
  }
}
