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

package org.apache.hop.ui.hopgui.file.pipeline.debug;

import org.apache.hop.core.Const;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.i18n.BaseMessages;

/** One field of an {@link IValueMeta}, in the same order as the output-fields dialog. */
public final class DebugFieldRows {

  private static final Class<?> PKG = DebugFieldRows.class;

  public static String[] headers() {
    return new String[] {
      text("DebugFieldRows.Column.FieldName"),
      text("DebugFieldRows.Column.Type"),
      text("DebugFieldRows.Column.ConversionMask"),
      text("DebugFieldRows.Column.Length"),
      text("DebugFieldRows.Column.Precision"),
      text("DebugFieldRows.Column.Origin"),
      text("DebugFieldRows.Column.StorageType"),
      text("DebugFieldRows.Column.Currency"),
      text("DebugFieldRows.Column.Decimal"),
      text("DebugFieldRows.Column.Group"),
      text("DebugFieldRows.Column.TrimType"),
      text("DebugFieldRows.Column.Comments")
    };
  }

  private static String text(String key) {
    return BaseMessages.getString(PKG, key);
  }

  private DebugFieldRows() {}

  public static String[] cells(IValueMeta value) {
    return new String[] {
      Const.NVL(value.getName(), ""),
      Const.NVL(value.getTypeDesc(), ""),
      Const.NVL(value.getConversionMask(), ""),
      value.getLength() < 0 ? "-" : Integer.toString(value.getLength()),
      value.getPrecision() < 0 ? "-" : Integer.toString(value.getPrecision()),
      Const.NVL(value.getOrigin(), ""),
      ValueMetaBase.getStorageTypeCode(value.getStorageType()),
      Const.NVL(value.getCurrencySymbol(), ""),
      Const.NVL(value.getDecimalSymbol(), ""),
      Const.NVL(value.getGroupingSymbol(), ""),
      ValueMetaBase.getTrimTypeDesc(value.getTrimType()),
      Const.NVL(value.getComments(), "")
    };
  }
}
