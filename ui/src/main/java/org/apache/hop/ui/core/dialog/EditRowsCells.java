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

package org.apache.hop.ui.core.dialog;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.widget.NullPlaceholder;

/** Turns one edited data-grid cell into the field's native value. */
public final class EditRowsCells {

  private static final Class<?> PKG = EditRowsDialog.class;

  private EditRowsCells() {}

  /**
   * The value of one cell.
   *
   * <p>A cell is null only while it is still the blue {@code <null>} placeholder. Text typed over
   * that placeholder is parsed, even when the null color has not been cleared yet. Surrounding
   * whitespace is trimmed on fields that are not strings, so it does not fail a format mask that
   * has to consume the whole string. A string keeps its padding.
   *
   * @param valueMeta the field the text is parsed into
   * @param parseMeta the string metadata that carries the field's mask and symbols
   * @param cellText the text in the cell
   * @param foregroundIsNullColor true when the cell foreground is the null color
   * @param rowNumber the 1-based row, used when conversion fails
   * @return the native value, or null when the cell is empty or still the null placeholder
   * @throws HopException when the text does not match the field's format mask. The message names
   *     the row, the field, the value and the mask.
   */
  public static Object convertCell(
      IValueMeta valueMeta,
      IValueMeta parseMeta,
      String cellText,
      boolean foregroundIsNullColor,
      int rowNumber)
      throws HopException {
    if (NullPlaceholder.isPlaceholder(cellText, foregroundIsNullColor)) {
      return null;
    }
    if (parseMeta != null && parseMeta.isNull(cellText)) {
      cellText = null;
    }
    try {
      // Strings keep their padding. Golden data compares them without trimming.
      int trimType = valueMeta.isString() ? IValueMeta.TRIM_TYPE_NONE : IValueMeta.TRIM_TYPE_BOTH;
      return valueMeta.convertDataFromString(cellText, parseMeta, null, null, trimType);
    } catch (HopException e) {
      throw new HopException(failureMessage(rowNumber, valueMeta, cellText), e);
    }
  }

  /**
   * The text for the error dialog. A conversion failure names the row, field, value and mask
   * without the cause chain {@link HopException#getMessage()} appends.
   *
   * @param e the failure from reading the grid
   * @return the dialog message
   */
  public static String errorDialogMessage(Exception e) {
    if (e instanceof HopException hop) {
      String message = hop.getSuperMessage();
      if (StringUtils.isNotEmpty(message)) {
        return message;
      }
    }
    if (e != null && StringUtils.isNotEmpty(e.getMessage())) {
      return e.getMessage();
    }
    return BaseMessages.getString(PKG, "EditRowsDialog.ErrorConvertingData");
  }

  /**
   * @param rowNumber the 1-based row
   * @param valueMeta the field that rejected the text
   * @param cellText the text that was rejected
   * @return the message naming the row, field, value and format mask
   */
  public static String failureMessage(int rowNumber, IValueMeta valueMeta, String cellText) {
    String mask = valueMeta.getConversionMask();
    if (StringUtils.isEmpty(mask)) {
      mask = StringUtils.defaultString(valueMeta.getFormatMask());
    }
    return BaseMessages.getString(
        PKG,
        "EditRowsDialog.Error.ValueDoesNotMatchMask",
        Integer.toString(rowNumber),
        StringUtils.defaultString(valueMeta.getName()),
        cellText == null ? "" : cellText,
        mask);
  }
}
