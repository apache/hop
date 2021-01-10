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

public interface IKWorkbook extends AutoCloseable {
  /**
   * Get a sheet with a certain name in a workbook.
   *
   * @param sheetName The name of the sheet.
   * @return The sheet or null if the sheet was not found.
   */
  IKSheet getSheet( String sheetName );

  /**
   * @return The array of sheet names in the workbook
   */
  String[] getSheetNames();

  /**
   * Close the workbook file
   */
  void close();

  /**
   * @return The number of sheets in the workbook
   */
  int getNumberOfSheets();

  /**
   * Get a sheet in the workbook by index
   *
   * @param sheetNr The sheet number to get
   * @return The selected sheet
   */
  IKSheet getSheet( int sheetNr );

  /**
   * Get a sheet name in the workbook by index
   *
   * @param sheetNr The sheet number to get
   * @return The selected sheet's name
   */
  String getSheetName( int sheetNr );
}
