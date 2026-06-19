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
package org.apache.hop.pipeline.transforms.vcard;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.eclipse.swt.widgets.TableItem;

public final class VCardMappingDialogUtil {

  private static final Class<?> PKG = VCardMappingDialogUtil.class;

  private VCardMappingDialogUtil() {}

  public static ColumnInfo[] createOutputMappingColumns() {
    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "VCardMappingDialog.Column.VCardProperty"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              VCardPropertyType.getDescriptions(),
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "VCardMappingDialog.Column.StreamField"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {},
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "VCardMappingDialog.Column.ParameterTypes"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false)
        };
    columns[2].setToolTip(
        BaseMessages.getString(
            PKG,
            "VCardMappingDialog.Column.ParameterTypes.Tooltip",
            VCardMapper.emailTypeOptions(),
            VCardMapper.telephoneTypeOptions()));
    return columns;
  }

  public static ColumnInfo[] createMappingColumns() {
    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "VCardMappingDialog.Column.HopField"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {},
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "VCardMappingDialog.Column.Property"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              VCardPropertyType.getDescriptions(),
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "VCardMappingDialog.Column.ParameterTypes"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false)
        };
    columns[2].setToolTip(
        BaseMessages.getString(
            PKG,
            "VCardMappingDialog.Column.ParameterTypes.Tooltip",
            VCardMapper.emailTypeOptions(),
            VCardMapper.telephoneTypeOptions()));
    return columns;
  }

  public static void loadMappings(TableView table, List<VCardFieldMapping> mappings) {
    if (mappings == null) {
      return;
    }
    for (VCardFieldMapping mapping : mappings) {
      TableItem item = new TableItem(table.getTable(), 0);
      item.setText(1, mapping.getHopField() == null ? "" : mapping.getHopField());
      item.setText(2, mapping.getProperty() == null ? "" : mapping.getProperty().getDescription());
      item.setText(3, mapping.getParameterTypes() == null ? "" : mapping.getParameterTypes());
    }
    table.removeEmptyRows();
    table.setRowNums();
    table.optWidth(true);
  }

  public static List<VCardFieldMapping> saveMappings(TableView table) {
    List<VCardFieldMapping> mappings = new ArrayList<>();
    for (int i = 0; i < table.nrNonEmpty(); i++) {
      TableItem item = table.getNonEmpty(i);
      VCardFieldMapping mapping = new VCardFieldMapping();
      mapping.setHopField(item.getText(1));
      mapping.setProperty(VCardPropertyType.lookupDescription(item.getText(2)));
      mapping.setParameterTypes(item.getText(3));
      mappings.add(mapping);
    }
    return mappings;
  }

  public static void loadOutputMappings(TableView table, List<VCardFieldMapping> mappings) {
    if (mappings == null) {
      return;
    }
    for (VCardFieldMapping mapping : mappings) {
      TableItem item = new TableItem(table.getTable(), 0);
      item.setText(1, mapping.getProperty() == null ? "" : mapping.getProperty().getDescription());
      item.setText(2, mapping.getHopField() == null ? "" : mapping.getHopField());
      item.setText(3, mapping.getParameterTypes() == null ? "" : mapping.getParameterTypes());
    }
    table.removeEmptyRows();
    table.setRowNums();
    table.optWidth(true);
  }

  public static List<VCardFieldMapping> saveOutputMappings(TableView table) {
    List<VCardFieldMapping> mappings = new ArrayList<>();
    for (int i = 0; i < table.nrNonEmpty(); i++) {
      TableItem item = table.getNonEmpty(i);
      VCardFieldMapping mapping = new VCardFieldMapping();
      mapping.setProperty(VCardPropertyType.lookupDescription(item.getText(1)));
      mapping.setHopField(item.getText(2));
      mapping.setParameterTypes(item.getText(3));
      mappings.add(mapping);
    }
    return mappings;
  }
}
