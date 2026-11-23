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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.Test;

/** Rows with a few thousand fields: every lookup must stay right through removes and inserts. */
class RowMetaWideRowTest {

  private static final int WIDTH = 3000;

  private static String fieldName(int i) {
    return String.format(Locale.ROOT, "field_%04d", i);
  }

  private static IValueMeta field(int i) {
    return i % 2 == 0 ? new ValueMetaString(fieldName(i)) : new ValueMetaInteger(fieldName(i));
  }

  private static void assertRow(RowMeta rowMeta, List<String> expected) {
    assertConsistent(rowMeta, expected.toArray(new String[0]));
  }

  @Test
  void everyLookupInAWideRowThroughRemovesAndInserts() throws Exception {
    RowMeta rowMeta = new RowMeta();
    List<String> expected = new ArrayList<>();
    List<IValueMeta> added = new ArrayList<>();
    for (int i = 0; i < WIDTH; i++) {
      IValueMeta valueMeta = field(i);
      rowMeta.addValueMeta(valueMeta);
      added.add(valueMeta);
      expected.add(fieldName(i));
    }
    assertRow(rowMeta, expected);
    for (int i = 0; i < WIDTH; i++) {
      assertSame(added.get(i), rowMeta.getValueMeta(i));
    }

    // removes in the middle, by index and by name in another case, and at both ends
    rowMeta.removeValueMeta(1500);
    expected.remove(1500);
    rowMeta.removeValueMeta(fieldName(10).toUpperCase(Locale.ROOT));
    expected.remove(10);
    rowMeta.removeValueMeta(rowMeta.size() - 1);
    expected.remove(expected.size() - 1);
    rowMeta.removeValueMeta(0);
    expected.remove(0);
    assertRow(rowMeta, expected);
    assertAbsent(rowMeta, fieldName(0), fieldName(10), fieldName(1500), fieldName(WIDTH - 1));
    assertEquals(1498, rowMeta.indexOfValue(fieldName(1501)));

    // inserts in the middle, with no lookups in between
    int[] positions = {1, 700, 1499, 2500, 2};
    for (int n = 0; n < positions.length; n++) {
      String name = "inserted_" + n;
      rowMeta.addValueMeta(positions[n], new ValueMetaString(name));
      expected.add(positions[n], name);
    }
    assertRow(rowMeta, expected);

    // the removed names are free again
    rowMeta.addValueMeta(new ValueMetaString(fieldName(1500)));
    expected.add(fieldName(1500));
    rowMeta.addValueMeta(0, new ValueMetaString(fieldName(10)));
    expected.add(0, fieldName(10));
    assertRow(rowMeta, expected);
  }

  @Test
  void duplicatesOfFieldsInAWideRow() {
    RowMeta rowMeta = new RowMeta();
    List<String> expected = new ArrayList<>();
    for (int i = 0; i < WIDTH; i++) {
      rowMeta.addValueMeta(field(i));
      expected.add(fieldName(i));
    }

    // a duplicate of every 100th field, in upper case, with no lookups in between
    for (int i = 0; i < WIDTH; i += 100) {
      String upper = fieldName(i).toUpperCase(Locale.ROOT);
      rowMeta.addValueMeta(new ValueMetaString(upper));
      expected.add(upper + "_1");
    }
    // and of every 100th field again, inserted in the middle
    for (int i = 50; i < WIDTH; i += 100) {
      rowMeta.addValueMeta(WIDTH / 2, new ValueMetaString(fieldName(i)));
      expected.add(WIDTH / 2, fieldName(i) + "_1");
    }
    assertRow(rowMeta, expected);

    // many duplicates of one name
    for (int n = 0; n < 500; n++) {
      rowMeta.addValueMeta(new ValueMetaString("dup"));
      expected.add(n == 0 ? "dup" : "dup_" + n);
    }
    assertRow(rowMeta, expected);
  }

  @Test
  void aWideRowFromAListThenAppendsRenamesAndAClone() throws Exception {
    List<IValueMeta> list = new ArrayList<>();
    List<String> expected = new ArrayList<>();
    for (int i = 0; i < WIDTH; i++) {
      list.add(field(i));
      expected.add(fieldName(i));
    }
    RowMeta rowMeta = new RowMeta();
    rowMeta.setValueMetaList(list);
    assertNames(rowMeta, expected.toArray(new String[0]));

    // renames in place across the row, then duplicates of the new and the old names
    for (int i = 5; i < WIDTH; i += 250) {
      rowMeta.getValueMeta(i).setName("renamed_" + i);
      expected.set(i, "renamed_" + i);
    }
    for (int i = 5; i < WIDTH; i += 250) {
      rowMeta.addValueMeta(new ValueMetaString("RENAMED_" + i));
      expected.add("RENAMED_" + i + "_1");
      rowMeta.addValueMeta(new ValueMetaString(fieldName(i)));
      expected.add(fieldName(i));
    }
    assertRow(rowMeta, expected);

    RowMeta clone = rowMeta.clone();
    assertRow(clone, expected);
    for (int i = 0; i < clone.size(); i++) {
      assertNotSame(rowMeta.getValueMeta(i), clone.getValueMeta(i));
    }

    clone.removeValueMeta(WIDTH / 2);
    List<String> cloneExpected = new ArrayList<>(expected);
    cloneExpected.remove(WIDTH / 2);
    clone.addValueMeta(new ValueMetaString(fieldName(WIDTH / 2 + 1)));
    cloneExpected.add(fieldName(WIDTH / 2 + 1) + "_1");
    assertRow(clone, cloneExpected);
    assertRow(rowMeta, expected);
  }
}
