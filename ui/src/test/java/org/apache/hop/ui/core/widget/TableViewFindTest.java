/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.core.widget;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hop.ui.core.widget.TableViewFind.Hit;
import org.apache.hop.ui.core.widget.TableViewFind.Result;
import org.apache.hop.ui.core.widget.TableViewFind.Status;
import org.junit.jupiter.api.Test;

class TableViewFindTest {

  private static final String[][] GRID = {
    {"id", "Ada"},
    {"id", "Grace"},
    {"code", "ada lovelace"},
  };

  private static final int[] LEFT_TO_RIGHT = {0, 1};

  private static final boolean[] BOTH = {true, true};

  @Test
  void findFirstMatchesSubstringIgnoringCase() {
    Result result = find(GRID, LEFT_TO_RIGHT, BOTH, "ada", false, false, 0, -1, true);

    assertEquals(Status.FOUND, result.status());
    assertEquals(new Hit(0, 1), result.hit());
  }

  @Test
  void caseSensitiveSearchSkipsADifferentCase() {
    Result result = find(GRID, LEFT_TO_RIGHT, BOTH, "ada", true, false, 0, -1, true);

    assertEquals(new Hit(2, 1), result.hit());
  }

  @Test
  void regularExpressionMatchesInsideTheCell() {
    Result result = find(GRID, LEFT_TO_RIGHT, BOTH, "ad.", false, true, 0, -1, true);

    assertEquals(new Hit(0, 1), result.hit());
  }

  @Test
  void regularExpressionCanSpanALineBreak() {
    String[][] rows = {{"a\nb"}};
    Result result = find(rows, new int[] {0}, new boolean[] {true}, "a.b", true, true, 0, -1, true);

    assertEquals(new Hit(0, 0), result.hit());
  }

  @Test
  void regularExpressionHonorsCase() {
    Result insensitive = find(GRID, LEFT_TO_RIGHT, BOTH, "grace", false, true, 0, -1, true);
    Result sensitive = find(GRID, LEFT_TO_RIGHT, BOTH, "grace", true, true, 0, -1, true);

    assertEquals(new Hit(1, 1), insensitive.hit());
    assertEquals(Status.NOT_FOUND, sensitive.status());
    assertNull(sensitive.hit());
  }

  @Test
  void invalidRegularExpressionDoesNotMatch() {
    Result result = find(GRID, LEFT_TO_RIGHT, BOTH, "[", true, true, 0, -1, true);

    assertEquals(Status.INVALID_REGEX, result.status());
    assertNull(result.hit());
  }

  @Test
  void emptyQueryDoesNotScan() {
    Result result = find(GRID, LEFT_TO_RIGHT, BOTH, "", false, false, 0, -1, true);

    assertEquals(Status.EMPTY_QUERY, result.status());
  }

  @Test
  void uncheckedColumnIsSkipped() {
    Result hidden =
        find(GRID, LEFT_TO_RIGHT, new boolean[] {true, false}, "Ada", true, false, 0, -1, true);
    Result visible =
        find(GRID, LEFT_TO_RIGHT, new boolean[] {false, true}, "Ada", true, false, 0, -1, true);

    assertEquals(Status.NOT_FOUND, hidden.status());
    assertEquals(new Hit(0, 1), visible.hit());
  }

  @Test
  void visualOrderDecidesWhichColumnMatchesFirst() {
    String[][] rows = {{"alpha-id", "id-beta"}};
    Result rightFirst = find(rows, new int[] {1, 0}, BOTH, "id", true, false, 0, -1, true);
    Result leftFirst = find(rows, LEFT_TO_RIGHT, BOTH, "id", true, false, 0, -1, true);

    assertEquals(new Hit(0, 1), rightFirst.hit());
    assertEquals(new Hit(0, 0), leftFirst.hit());
  }

  @Test
  void findNextIncludesTheStartCellThenMovesPastIt() {
    Result first = find(GRID, LEFT_TO_RIGHT, BOTH, "id", true, false, 0, 0, true);
    Result next =
        find(
            GRID,
            LEFT_TO_RIGHT,
            BOTH,
            "id",
            true,
            false,
            first.hit().row(),
            first.hit().dataColumn(),
            false);

    assertEquals(new Hit(0, 0), first.hit());
    assertEquals(new Hit(1, 0), next.hit());
  }

  @Test
  void searchDoesNotWrap() {
    Result result = find(GRID, LEFT_TO_RIGHT, BOTH, "id", true, false, 2, 0, false);

    assertEquals(Status.NOT_FOUND, result.status());
  }

  @Test
  void nullCellDoesNotMatchAndDoesNotFail() {
    String[][] rows = {{null, "needle"}};
    Result result = find(rows, LEFT_TO_RIGHT, BOTH, "needle", true, false, 0, -1, true);

    assertEquals(new Hit(0, 1), result.hit());
  }

  @Test
  void missingStartColumnContinuesOnTheNextRow() {
    Result result = find(GRID, new int[] {0}, BOTH, "code", true, false, 0, 1, false);

    assertEquals(new Hit(2, 0), result.hit());
  }

  private static Result find(
      String[][] rows,
      int[] visualDataColumns,
      boolean[] included,
      String query,
      boolean caseSensitive,
      boolean regex,
      int startRow,
      int startDataColumn,
      boolean inclusive) {
    return TableViewFind.find(
        rows,
        visualDataColumns,
        included,
        query,
        caseSensitive,
        regex,
        startRow,
        startDataColumn,
        inclusive);
  }
}
