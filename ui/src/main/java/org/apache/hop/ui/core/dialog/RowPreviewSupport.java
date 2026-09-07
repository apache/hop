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

package org.apache.hop.ui.core.dialog;

import java.util.Objects;
import org.apache.commons.codec.binary.Hex;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.widgets.TableItem;

/** Shared row-preview formatting used by {@link ShowRowsDialog} and the Database results panel. */
public final class RowPreviewSupport {

  private static final Class<?> PKG = ShowRowsDialog.class;

  private RowPreviewSupport() {}

  public static boolean avoidBinaryHexPreview() {
    return Const.toBoolean(
        HopConfig.readStringVariable(Const.HOP_BINARY_FIELDS_AVOID_HEX_PREVIEW, "false"));
  }

  public static String formatCell(IValueMeta valueMeta, Object value) throws HopValueException {
    return formatCell(valueMeta, value, avoidBinaryHexPreview());
  }

  static String formatCell(IValueMeta valueMeta, Object value, boolean avoidHex)
      throws HopValueException {
    if (valueMeta == null) {
      return null;
    }
    if (valueMeta.isBinary()) {
      byte[] bytes = valueMeta.getBinary(value);
      if (bytes == null) {
        return null;
      }
      String display = avoidHex ? valueMeta.getString(bytes) : Hex.encodeHexString(bytes);
      if (display != null && display.length() > PreviewRowsDialog.MAX_BINARY_STRING_PREVIEW_SIZE) {
        return display.substring(0, PreviewRowsDialog.MAX_BINARY_STRING_PREVIEW_SIZE);
      }
      return display;
    }
    return valueMeta.getString(value);
  }

  public static String formatColumnMetaTooltip(IValueMeta valueMeta) {
    if (valueMeta == null) {
      return null;
    }
    StringBuilder tip = new StringBuilder();
    tip.append(
        BaseMessages.getString(
            PKG, "ShowRowsDialog.CellTooltip.Name", Const.NVL(valueMeta.getName(), "")));
    tip.append(Const.CR);
    tip.append(
        BaseMessages.getString(
            PKG, "ShowRowsDialog.CellTooltip.Type", Const.NVL(valueMeta.getTypeDesc(), "")));
    if (valueMeta.getLength() > 0) {
      tip.append(Const.CR);
      tip.append(
          BaseMessages.getString(
              PKG, "ShowRowsDialog.CellTooltip.Length", Integer.toString(valueMeta.getLength())));
    }
    if (valueMeta.getPrecision() > 0) {
      tip.append(Const.CR);
      tip.append(
          BaseMessages.getString(
              PKG,
              "ShowRowsDialog.CellTooltip.Precision",
              Integer.toString(valueMeta.getPrecision())));
    }
    if (!Utils.isEmpty(valueMeta.getOrigin())) {
      tip.append(Const.CR);
      tip.append(
          BaseMessages.getString(PKG, "ShowRowsDialog.CellTooltip.Origin", valueMeta.getOrigin()));
    }
    return tip.toString();
  }

  public static void applyColumnMeta(ColumnInfo column, IValueMeta valueMeta) {
    if (column == null || valueMeta == null) {
      return;
    }
    column.setToolTip(formatColumnMetaTooltip(valueMeta));
    column.setValueMeta(valueMeta);
    column.setImage(GuiResource.getInstance().getImage(valueMeta));
    column.setReadOnly(true);
  }

  public static void installCellTooltips(TableView tableView, IRowMeta rowMeta) {
    if (tableView == null || tableView.isDisposed() || rowMeta == null) {
      return;
    }
    tableView.table.addListener(
        SWT.MouseMove,
        event -> {
          int dataColumn = dataColumnAt(tableView, rowMeta, new Point(event.x, event.y));
          String tip =
              dataColumn < 0 ? null : formatColumnMetaTooltip(rowMeta.getValueMeta(dataColumn));
          if (!Objects.equals(tip, tableView.table.getToolTipText())) {
            tableView.table.setToolTipText(tip);
          }
        });
  }

  static int dataColumnAt(TableView tableView, IRowMeta rowMeta, Point point) {
    if (tableView == null || tableView.isDisposed() || rowMeta == null || point == null) {
      return -1;
    }
    TableItem item = tableView.table.getItem(point);
    if (item == null) {
      return -1;
    }
    for (int i = 1; i < tableView.table.getColumnCount(); i++) {
      if (item.getBounds(i).contains(point)) {
        int dataColumn = i - 1;
        return dataColumn < rowMeta.size() ? dataColumn : -1;
      }
    }
    return -1;
  }
}
