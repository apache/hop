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

package org.apache.hop.pipeline.transforms.excelwriter.ods;

import org.odftoolkit.odfdom.doc.OdfSpreadsheetDocument;
import org.odftoolkit.odfdom.doc.table.OdfTable;

/** Holds ODF-specific workbook state for the Excel Writer transform. */
public class OdsWorkbookHandle {

  private final OdfSpreadsheetDocument document;
  private OdfTable table;

  public OdsWorkbookHandle(OdfSpreadsheetDocument document, OdfTable table) {
    this.document = document;
    this.table = table;
  }

  public OdfSpreadsheetDocument getDocument() {
    return document;
  }

  public OdfTable getTable() {
    return table;
  }

  public void setTable(OdfTable table) {
    this.table = table;
  }
}
