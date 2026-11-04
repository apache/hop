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

import org.junit.jupiter.api.Test;

/**
 * Header hit testing without a display. Edges are the painted column bounds in visual order; index
 * 0 is the row-number column.
 */
class TableViewColumnHitTest {

  /** Row number, first data column, a gap, then the second data column. */
  private static final int[] INDEXES = {0, 1, 3};

  private static final int[] LEFTS = {0, 40, 110};

  private static final int[] RIGHTS = {40, 100, 200};

  @Test
  void rowNumberColumnResolvesToMinusOne() {
    assertEquals(-1, TableView.hitTestDataColumn(INDEXES, LEFTS, RIGHTS, 0));
    assertEquals(-1, TableView.hitTestDataColumn(INDEXES, LEFTS, RIGHTS, 39));
  }

  @Test
  void gapBetweenCellsBelongsToTheColumnOnTheLeft() {
    assertEquals(0, TableView.hitTestDataColumn(INDEXES, LEFTS, RIGHTS, 40));
    assertEquals(0, TableView.hitTestDataColumn(INDEXES, LEFTS, RIGHTS, 99));
    // The painted cells leave 100..109 empty. That gap still belongs to the column on the left.
    assertEquals(0, TableView.hitTestDataColumn(INDEXES, LEFTS, RIGHTS, 100));
    assertEquals(0, TableView.hitTestDataColumn(INDEXES, LEFTS, RIGHTS, 109));
    assertEquals(2, TableView.hitTestDataColumn(INDEXES, LEFTS, RIGHTS, 110));
    assertEquals(2, TableView.hitTestDataColumn(INDEXES, LEFTS, RIGHTS, 199));
  }

  @Test
  void zeroWidthColumnIsSkipped() {
    int[] indexes = {0, 1, 2, 3};
    int[] lefts = {0, 40, 100, 110};
    int[] rights = {40, 100, 100, 200};

    assertEquals(0, TableView.hitTestDataColumn(indexes, lefts, rights, 100));
    assertEquals(2, TableView.hitTestDataColumn(indexes, lefts, rights, 110));
  }

  @Test
  void clickOutsideThePaintedColumnsMisses() {
    assertEquals(-1, TableView.hitTestDataColumn(INDEXES, LEFTS, RIGHTS, -1));
    assertEquals(-1, TableView.hitTestDataColumn(INDEXES, LEFTS, RIGHTS, 200));
  }

  @Test
  void visualOrderIsTrustedWithoutSorting() {
    // Creation order is row, A, B. On screen the row number is followed by B, then A.
    int[] indexes = {0, 2, 1};
    int[] lefts = {0, 30, 80};
    int[] rights = {30, 80, 140};

    assertEquals(1, TableView.hitTestDataColumn(indexes, lefts, rights, 50));
    assertEquals(0, TableView.hitTestDataColumn(indexes, lefts, rights, 100));
  }

  @Test
  void emptyGridWalksWidthsInVisualOrderAndSkipsHiddenColumns() {
    int[] order = {0, 2, 1, 3};
    int[] widths = {40, 50, 0, 90};

    assertEquals(-1, TableView.hitTestDataColumnFromWidths(order, widths, 0, 0));
    assertEquals(-1, TableView.hitTestDataColumnFromWidths(order, widths, 39, 0));
    // Column 2 is hidden, so the next visible column (index 1) starts at 40.
    assertEquals(0, TableView.hitTestDataColumnFromWidths(order, widths, 40, 0));
    assertEquals(0, TableView.hitTestDataColumnFromWidths(order, widths, 89, 0));
    assertEquals(2, TableView.hitTestDataColumnFromWidths(order, widths, 90, 0));
    assertEquals(2, TableView.hitTestDataColumnFromWidths(order, widths, 179, 0));
    assertEquals(-1, TableView.hitTestDataColumnFromWidths(order, widths, 180, 0));
  }

  @Test
  void emptyGridOnWindowsAddsTheScrollConvertedToPoints() {
    int[] order = {0, 1, 2};
    int[] widths = {40, 100, 80};
    int contentWidth = 220;
    // 200% scaling: the scrollbar speaks device pixels, the columns speak points.
    int selection = 150;
    int maximum = 440;

    assertEquals(75, TableView.emptyGridScrollOffset(0, selection, maximum, contentWidth));
    // Without the offset the click lands on the row-number column, left of the pointer.
    assertEquals(-1, TableView.hitTestDataColumnFromWidths(order, widths, 10, 0));
    assertEquals(0, TableView.hitTestDataColumnFromWidths(order, widths, 10, 75));
    assertEquals(1, TableView.hitTestDataColumnFromWidths(order, widths, 70, 75));
  }

  @Test
  void scrolledGtkAndCocoaKeepContentCoordinates() {
    int contentWidth = 220;
    assertEquals(0, TableView.emptyGridScrollOffset(-75, 150, 440, contentWidth));
    assertEquals(0, TableView.emptyGridScrollOffset(75, 150, 440, contentWidth));
    assertEquals(0, TableView.emptyGridScrollOffset(0, 0, 440, contentWidth));
  }

  @Test
  void hopWebUsesTheSelectionWhenTheScrollbarMaximumIsUnset() {
    int[] order = {0, 1, 2};
    int[] widths = {40, 100, 80};
    // RAP never publishes a device-pixel maximum. The selection is already the scroll in pixels.
    int scroll = TableView.emptyGridScrollOffset(0, 80, 0, 220);

    assertEquals(80, scroll);
    assertEquals(0, TableView.hitTestDataColumnFromWidths(order, widths, 10, scroll));
    assertEquals(0, TableView.emptyGridScrollOffset(0, 0, 0, 220));
    assertEquals(0, TableView.emptyGridScrollOffset(0, 80, 0, 0));
  }
}
