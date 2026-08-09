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

package org.apache.hop.pipeline.transforms.excelinput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.spreadsheet.IKSheet;
import org.apache.hop.core.spreadsheet.IKWorkbook;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.poi.hssf.record.crypto.Biff8EncryptionKey;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.crypt.EncryptionInfo;
import org.apache.poi.poifs.crypt.EncryptionMode;
import org.apache.poi.poifs.crypt.Encryptor;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests reading workbooks that are protected with a password. */
class PasswordProtectedWorkbookTest {

  private static final String PASSWORD = "s3cr3t";
  private static final String SHEET_NAME = "Data";
  private static final String CELL_VALUE = "protected-content";

  @TempDir private File tempDir;

  private IVariables variables;

  @BeforeEach
  void setUp() {
    variables = new Variables();
  }

  @Test
  void testEncryptedXlsxWithPoi() throws Exception {
    String filename = createEncryptedXlsx();

    assertCellValue(SpreadSheetType.POI, filename, PASSWORD);
  }

  @Test
  void testEncryptedXlsxWithSaxPoi() throws Exception {
    String filename = createEncryptedXlsx();

    assertCellValue(SpreadSheetType.SAX_POI, filename, PASSWORD);
  }

  @Test
  void testEncryptedXlsWithPoi() throws Exception {
    String filename = createEncryptedXls();

    assertCellValue(SpreadSheetType.POI, filename, PASSWORD);
  }

  @Test
  void testEncryptedXlsxWithWrongPassword() throws Exception {
    String filename = createEncryptedXlsx();

    HopException poiException =
        assertThrows(
            HopException.class,
            () ->
                WorkbookFactory.getWorkbook(
                    SpreadSheetType.POI, filename, null, "not-the-password", variables));
    assertTrue(poiException.getMessage().contains("Password incorrect"), poiException.getMessage());

    HopException staxException =
        assertThrows(
            HopException.class,
            () ->
                WorkbookFactory.getWorkbook(
                    SpreadSheetType.SAX_POI, filename, null, "not-the-password", variables));
    assertTrue(
        staxException.getMessage().contains("password provided is not correct"),
        staxException.getMessage());
    assertTrue(staxException.getMessage().contains(filename), staxException.getMessage());
  }

  @Test
  void testEncryptedXlsxWithoutPassword() throws Exception {
    String filename = createEncryptedXlsx();

    assertThrows(
        HopException.class,
        () -> WorkbookFactory.getWorkbook(SpreadSheetType.POI, filename, null, variables));
    assertThrows(
        HopException.class,
        () -> WorkbookFactory.getWorkbook(SpreadSheetType.SAX_POI, filename, null, variables));
  }

  /** A plain workbook keeps working, both engines take the regular (streaming) code path. */
  @Test
  void testPlainWorkbookWithoutPassword() throws Exception {
    String filename = createPlainXlsx();

    assertCellValue(SpreadSheetType.POI, filename, null);
    assertCellValue(SpreadSheetType.SAX_POI, filename, null);
    assertCellValue(SpreadSheetType.POI, filename, "");
    assertCellValue(SpreadSheetType.SAX_POI, filename, "");
  }

  /**
   * A password on a workbook that isn't encrypted is harmless, which matters when a single
   * transform reads a folder holding both protected and plain files.
   */
  @Test
  void testPlainWorkbookWithPassword() throws Exception {
    String filename = createPlainXlsx();

    assertCellValue(SpreadSheetType.POI, filename, PASSWORD);
    assertCellValue(SpreadSheetType.SAX_POI, filename, PASSWORD);
  }

  /** The ODF library can't decrypt, so we say so instead of silently ignoring the password. */
  @Test
  void testPasswordIsRejectedForOds() {
    String filename = new File(tempDir, "does-not-matter.ods").getPath();

    HopException e =
        assertThrows(
            HopException.class,
            () ->
                WorkbookFactory.getWorkbook(
                    SpreadSheetType.ODS, filename, null, PASSWORD, variables));
    assertTrue(e.getMessage().contains("not supported"), e.getMessage());
  }

  private void assertCellValue(SpreadSheetType type, String filename, String password)
      throws HopException {
    IKWorkbook workbook = WorkbookFactory.getWorkbook(type, filename, null, password, variables);
    try {
      IKSheet sheet = workbook.getSheet(SHEET_NAME);
      assertEquals(CELL_VALUE, sheet.getRow(0)[0].getValue());
    } finally {
      workbook.close();
    }
  }

  private String createPlainXlsx() throws IOException {
    File file = new File(tempDir, "plain.xlsx");
    try (Workbook workbook = new XSSFWorkbook();
        OutputStream outputStream = new FileOutputStream(file)) {
      fill(workbook);
      workbook.write(outputStream);
    }
    return file.getPath();
  }

  private String createEncryptedXlsx() throws Exception {
    File plainFile = new File(createPlainXlsx());
    File file = new File(tempDir, "encrypted.xlsx");

    try (POIFSFileSystem fs = new POIFSFileSystem()) {
      Encryptor encryptor = new EncryptionInfo(EncryptionMode.agile).getEncryptor();
      encryptor.confirmPassword(PASSWORD);
      try (OutputStream outputStream = encryptor.getDataStream(fs);
          InputStream inputStream = Files.newInputStream(plainFile.toPath())) {
        inputStream.transferTo(outputStream);
      }
      try (OutputStream outputStream = new FileOutputStream(file)) {
        fs.writeFilesystem(outputStream);
      }
    }
    return file.getPath();
  }

  private String createEncryptedXls() throws IOException {
    File file = new File(tempDir, "encrypted.xls");
    Biff8EncryptionKey.setCurrentUserPassword(PASSWORD);
    try (Workbook workbook = new HSSFWorkbook();
        OutputStream outputStream = new FileOutputStream(file)) {
      fill(workbook);
      workbook.write(outputStream);
    } finally {
      Biff8EncryptionKey.setCurrentUserPassword(null);
    }
    return file.getPath();
  }

  private void fill(Workbook workbook) {
    Sheet sheet = workbook.createSheet(SHEET_NAME);
    sheet.createRow(0).createCell(0).setCellValue(CELL_VALUE);
  }
}
