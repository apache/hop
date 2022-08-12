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

package org.apache.hop.pipeline.transforms.excelwriter;

import org.apache.commons.vfs2.FileObject;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

public class ExcelWriterWorkbookDefinition {

  private FileObject file;
  private String fileName;
  private Workbook workbook;
  private Sheet sheet;
  private int posX;
  private int posY;
  private int datalines;
  private int splitNr;
  private CellStyle[] cellStyleCache;
  private CellStyle[] cellLinkStyleCache;

  public ExcelWriterWorkbookDefinition(
      String fileName, FileObject file, Workbook workbook, Sheet sheet, int posX, int posY) {
    this.fileName = fileName;
    this.file = file;
    this.workbook = workbook;
    this.sheet = sheet;
    this.posX = posX;
    this.posY = posY;
    this.datalines = 0;
    this.splitNr = 0;
  }

  public FileObject getFile() {
    return file;
  }

  public void setFile(FileObject file) {
    this.file = file;
  }

  public Workbook getWorkbook() {
    return workbook;
  }

  public void setWorkbook(Workbook workbook) {
    this.workbook = workbook;
  }

  public Sheet getSheet() {
    return sheet;
  }

  public void setSheet(Sheet sheet) {
    this.sheet = sheet;
  }

  public int getPosX() {
    return posX;
  }

  public void setPosX(int posX) {
    this.posX = posX;
  }

  public int getPosY() {
    return posY;
  }

  public void setPosY(int posY) {
    this.posY = posY;
  }

  public int getDatalines() {
    return datalines;
  }

  public void setDatalines(int datalines) {
    this.datalines = datalines;
  }

  public int getSplitNr() {
    return splitNr;
  }

  public void setSplitNr(int splitNr) {
    this.splitNr = splitNr;
  }

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public void incrementY() {
    this.posY++;
  }

  public void incrementX() {
    this.posX++;
  }

  public void clearStyleCache(int nrFields) {
    cellStyleCache = new CellStyle[nrFields];
    cellLinkStyleCache = new CellStyle[nrFields];
  }

  public void cacheStyle(int fieldNr, CellStyle style) {
    cellStyleCache[fieldNr] = style;
  }

  public void cacheLinkStyle(int fieldNr, CellStyle style) {
    cellLinkStyleCache[fieldNr] = style;
  }

  public CellStyle getCachedStyle(int fieldNr) {
    return cellStyleCache[fieldNr];
  }

  public CellStyle getCachedLinkStyle(int fieldNr) {
    return cellLinkStyleCache[fieldNr];
  }

  public CellStyle[] getCellStyleCache() {
    return cellStyleCache;
  }

  public void setCellStyleCache(CellStyle[] cellStyleCache) {
    this.cellStyleCache = cellStyleCache;
  }

  public CellStyle[] getCellLinkStyleCache() {
    return cellLinkStyleCache;
  }

  public void setCellLinkStyleCache(CellStyle[] cellLinkStyleCache) {
    this.cellLinkStyleCache = cellLinkStyleCache;
  }
}
