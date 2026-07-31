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

package org.apache.hop.naming.gui;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.naming.engine.NamingEngine;
import org.apache.hop.naming.metadata.NamingScheme;
import org.apache.hop.naming.metadata.NamingSchemeType;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

/**
 * TableView toolbar action that applies a {@link NamingScheme} of type Hop field names to a chosen
 * column (issue #2683).
 */
@GuiPlugin
public class TableViewNamingToolbarButton {

  private static final Class<?> PKG = TableViewNamingToolbarButton.class;

  public static final String ID_TOOLBAR_APPLY_NAMING =
      "tableview-toolbar-30100-apply-naming-scheme";

  @GuiToolbarElement(
      root = TableView.ID_TOOLBAR,
      id = ID_TOOLBAR_APPLY_NAMING,
      toolTip = "i18n::Naming.Toolbar.Apply.ToolTip",
      separator = true,
      image = "naming.svg")
  public static void applyNamingScheme(TableView tableView) {
    if (tableView == null || tableView.isDisposed() || tableView.isReadonly()) {
      return;
    }

    Shell shell = tableView.getShell();
    ColumnInfo[] columns = tableView.getColumns();
    if (columns == null || columns.length == 0) {
      return;
    }

    try {
      // 1. Pick column (default: first data column)
      //
      String[] columnNames = new String[columns.length];
      for (int i = 0; i < columns.length; i++) {
        columnNames[i] = columns[i].getName();
      }
      EnterSelectionDialog columnDialog =
          new EnterSelectionDialog(
              shell,
              columnNames,
              BaseMessages.getString(PKG, "Naming.ColumnSelection.Title"),
              BaseMessages.getString(PKG, "Naming.ColumnSelection.Message"));
      columnDialog.setSelectedNrs(new int[] {0});
      if (columnDialog.open() == null) {
        return;
      }
      int columnIndex = columnDialog.getSelectionNr();
      if (columnIndex < 0 || columnIndex >= columns.length) {
        return;
      }
      // TableItem column 0 is "#"; data columns start at 1
      int tableColNr = columnIndex + 1;

      // 2. Pick Naming Scheme (Hop field names)
      //
      IHopMetadataSerializer<NamingScheme> serializer =
          HopGui.getInstance().getMetadataProvider().getSerializer(NamingScheme.class);
      List<NamingScheme> allSchemes = serializer.loadAll();
      List<NamingScheme> fieldSchemes = new ArrayList<>();
      for (NamingScheme scheme : allSchemes) {
        NamingSchemeType type = NamingSchemeType.fromCode(scheme.getType());
        if (type == NamingSchemeType.HOP_FIELD) {
          fieldSchemes.add(scheme);
        }
      }
      if (fieldSchemes.isEmpty()) {
        MessageBox box = new MessageBox(shell, SWT.ICON_INFORMATION | SWT.OK);
        box.setText(BaseMessages.getString(PKG, "Naming.NoSchemes.Title"));
        box.setMessage(BaseMessages.getString(PKG, "Naming.NoSchemes.Message"));
        box.open();
        return;
      }

      String[] schemeNames = new String[fieldSchemes.size()];
      for (int i = 0; i < fieldSchemes.size(); i++) {
        schemeNames[i] = fieldSchemes.get(i).getName();
      }
      EnterSelectionDialog schemeDialog =
          new EnterSelectionDialog(
              shell,
              schemeNames,
              BaseMessages.getString(PKG, "Naming.SchemeSelection.Title"),
              BaseMessages.getString(PKG, "Naming.SchemeSelection.Message"));
      if (schemeDialog.open() == null) {
        return;
      }
      int schemeIndex = schemeDialog.getSelectionNr();
      if (schemeIndex < 0 || schemeIndex >= fieldSchemes.size()) {
        return;
      }
      NamingScheme scheme = fieldSchemes.get(schemeIndex);

      // 3. Apply to non-empty rows
      //
      List<TableItem> items = tableView.getNonEmptyItems();
      if (items.isEmpty()) {
        return;
      }

      int[] rowIndices = new int[items.size()];
      String[] newValues = new String[items.size()];
      for (int i = 0; i < items.size(); i++) {
        TableItem item = items.get(i);
        rowIndices[i] = tableView.getTable().indexOf(item);
        String current = item.getText(tableColNr);
        if (StringUtils.isEmpty(current) || "<null>".equals(current)) {
          newValues[i] = current;
        } else {
          newValues[i] = NamingEngine.apply(scheme, current);
        }
      }

      tableView.applyColumnValues(tableColNr, rowIndices, newValues);
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "Naming.Error.Title"),
          BaseMessages.getString(PKG, "Naming.Error.Message"),
          e);
    }
  }
}
