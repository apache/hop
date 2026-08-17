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

import java.io.InputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.spreadsheet.IKWorkbook;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.transforms.excelinput.ods.OdfWorkbook;
import org.apache.hop.pipeline.transforms.excelinput.poi.PoiWorkbook;
import org.apache.hop.pipeline.transforms.excelinput.staxpoi.StaxPoiWorkbook;

public class WorkbookFactory {

  private WorkbookFactory() {
    throw new IllegalStateException("Utility class");
  }

  public static IKWorkbook getWorkbook(
      SpreadSheetType type, String filename, String encoding, IVariables variables)
      throws HopException {
    return getWorkbook(type, filename, encoding, null, variables);
  }

  /**
   * Open a workbook, optionally protected with a password.
   *
   * @param password the password of an encrypted workbook, null or empty for a plain workbook. Only
   *     the POI based engines support this.
   */
  public static IKWorkbook getWorkbook(
      SpreadSheetType type, String filename, String encoding, String password, IVariables variables)
      throws HopException {
    checkPasswordSupported(type, password);
    return switch (type) {
      case POI ->
          new PoiWorkbook(
              filename, encoding, password,
              variables); // encoding is not used, perhaps detected automatically?
      case SAX_POI -> new StaxPoiWorkbook(filename, encoding, password, variables);
      case ODS ->
          new OdfWorkbook(
              filename, encoding,
              variables); // encoding is not used, perhaps detected automatically?
      default ->
          throw new HopException(
              "Sorry, spreadsheet type " + type.getDescription() + " is not yet supported");
    };
  }

  public static IKWorkbook getWorkbook(
      SpreadSheetType type, InputStream inputStream, String encoding) throws HopException {
    return getWorkbook(type, inputStream, encoding, null);
  }

  /**
   * Open a workbook from a stream, optionally protected with a password.
   *
   * @param password the password of an encrypted workbook, null or empty for a plain workbook. Only
   *     the POI based engines support this.
   */
  public static IKWorkbook getWorkbook(
      SpreadSheetType type, InputStream inputStream, String encoding, String password)
      throws HopException {
    checkPasswordSupported(type, password);
    return switch (type) {
      case POI ->
          new PoiWorkbook(
              inputStream,
              encoding,
              password); // encoding is not used, perhaps detected automatically?
      case SAX_POI -> new StaxPoiWorkbook(inputStream, encoding, password);
      case ODS ->
          new OdfWorkbook(
              inputStream, encoding); // encoding is not used, perhaps detected automatically?
      default ->
          throw new HopException(
              "Sorry, spreadsheet type " + type.getDescription() + " is not yet supported");
    };
  }

  /**
   * Only the POI based engines can decrypt a workbook. The ODF library we use has no notion of
   * encrypted documents, so we fail early with a clear message instead of handing the password over
   * to a reader that silently ignores it.
   */
  private static void checkPasswordSupported(SpreadSheetType type, String password)
      throws HopException {
    if (StringUtils.isNotEmpty(password)
        && type != SpreadSheetType.POI
        && type != SpreadSheetType.SAX_POI) {
      throw new HopException(
          "Reading password protected files is not supported by spreadsheet type "
              + type.getDescription());
    }
  }
}
