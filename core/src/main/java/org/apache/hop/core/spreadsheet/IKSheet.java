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

package org.apache.hop.core.spreadsheet;

/**
 * The interface that is needed by ExcelInput to handle a single numeric data sheet in a workbook.
 *
 * @author matt
 */
public interface IKSheet {
  /**
   * Get one row of cells in the sheets
   *
   * @param rownr The row number to get
   * @return the cells of the row
   * @throws ArrayIndexOutOfBoundsException in case you try to read beyond the last row. (never returns null as such)
   */
  IKCell[] getRow( int rownr );

  /**
   * @return The name of the cell
   */
  String getName();

  /**
   * @return The number of rows in the sheet
   */
  int getRows();

  /**
   * Get a cell in the sheet by specification of its coordinates.
   *
   * @param colnr The column number
   * @param rownr The row number
   * @return The cell at the specified coordinates.
   */
  IKCell getCell( int colnr, int rownr );
}
