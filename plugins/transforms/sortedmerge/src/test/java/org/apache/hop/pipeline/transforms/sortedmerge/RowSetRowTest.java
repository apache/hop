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

package org.apache.hop.pipeline.transforms.sortedmerge;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.row.IRowMeta;
import org.junit.jupiter.api.Test;

/** Unit test for {@link RowSetRow} */
class RowSetRowTest {

  @Test
  void constructorStoresRowSetMetaAndData() {
    IRowSet rowSet = mock(IRowSet.class);
    IRowMeta rowMeta = mock(IRowMeta.class);
    Object[] rowData = new Object[] {"a", 1};

    RowSetRow row = new RowSetRow(rowSet, rowMeta, rowData);

    assertSame(rowSet, row.getRowSet());
    assertSame(rowMeta, row.getRowMeta());
    assertArrayEquals(rowData, row.getRowData());
  }

  @Test
  void settersReplaceStoredValues() {
    RowSetRow row = new RowSetRow(mock(IRowSet.class), mock(IRowMeta.class), new Object[] {"old"});

    IRowSet rowSet = mock(IRowSet.class);
    IRowMeta rowMeta = mock(IRowMeta.class);
    Object[] rowData = new Object[] {"new"};
    row.setRowSet(rowSet);
    row.setRowMeta(rowMeta);
    row.setRowData(rowData);

    assertSame(rowSet, row.getRowSet());
    assertSame(rowMeta, row.getRowMeta());
    assertArrayEquals(rowData, row.getRowData());
  }
}
