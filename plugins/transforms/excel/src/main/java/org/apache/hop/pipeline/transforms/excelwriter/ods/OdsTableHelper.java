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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.List;
import org.apache.hop.core.util.Utils;
import org.odftoolkit.odfdom.doc.OdfSpreadsheetDocument;
import org.odftoolkit.odfdom.doc.table.OdfTable;
import org.odftoolkit.odfdom.doc.table.OdfTableColumn;
import org.odftoolkit.odfdom.dom.OdfDocumentNamespace;
import org.odftoolkit.odfdom.dom.OdfSettingsDom;
import org.odftoolkit.odfdom.dom.attribute.table.TableVisibilityAttribute;
import org.odftoolkit.odfdom.dom.element.config.ConfigConfigItemElement;
import org.odftoolkit.odfdom.dom.element.table.TableTableElement;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/** Sheet-level operations for ODS output (clone, reorder, active tab, auto-size, row insert). */
final class OdsTableHelper {

  private static final String TABLE_VISIBILITY = "table:visibility";
  private static final String ACTIVE_TABLE = "ActiveTable";
  private static final String SHA1_DIGEST_ALGORITHM = "http://www.w3.org/2000/09/xmldsig#sha1";

  private OdsTableHelper() {}

  static OdfTable cloneTable(OdfSpreadsheetDocument document, OdfTable sourceTable, String newName)
      throws Exception {
    TableTableElement sourceElement = sourceTable.getOdfElement();
    Node clonedNode = document.getContentDom().importNode(sourceElement, true);
    sourceElement.getParentNode().appendChild(clonedNode);
    OdfTable clonedTable = OdfTable.getInstance((TableTableElement) clonedNode);
    clonedTable.setTableName(newName);
    setTableVisible(clonedTable, true);
    return clonedTable;
  }

  static void setTableVisible(OdfTable table, boolean visible) {
    TableTableElement element = table.getOdfElement();
    if (visible) {
      element.removeAttributeNS(OdfDocumentNamespace.TABLE.getUri(), TABLE_VISIBILITY);
    } else {
      element.setAttributeNS(
          OdfDocumentNamespace.TABLE.getUri(),
          TABLE_VISIBILITY,
          TableVisibilityAttribute.Value.COLLAPSE.toString());
    }
  }

  static int getTableIndex(OdfSpreadsheetDocument document, String tableName) {
    List<OdfTable> tables = document.getTableList();
    for (int i = 0; i < tables.size(); i++) {
      if (tableName.equals(tables.get(i).getTableName())) {
        return i;
      }
    }
    return -1;
  }

  static void moveTableToIndex(OdfSpreadsheetDocument document, OdfTable table, int targetIndex) {
    List<OdfTable> tables = document.getTableList();
    if (targetIndex < 0 || targetIndex >= tables.size()) {
      return;
    }
    TableTableElement element = table.getOdfElement();
    Node parent = element.getParentNode();
    Node reference = tables.get(targetIndex).getOdfElement();
    if (reference == element) {
      return;
    }
    parent.removeChild(element);
    parent.insertBefore(element, reference);
  }

  static String getActiveTableName(OdfSpreadsheetDocument document) throws Exception {
    OdfSettingsDom settingsDom = document.getSettingsDom();
    if (settingsDom == null || settingsDom.getRootElement() == null) {
      return null;
    }
    return findConfigItemValue(settingsDom.getRootElement(), ACTIVE_TABLE);
  }

  static void setActiveTableName(OdfSpreadsheetDocument document, String tableName)
      throws Exception {
    if (Utils.isEmpty(tableName)) {
      return;
    }
    OdfSettingsDom settingsDom = document.getSettingsDom();
    if (settingsDom == null || settingsDom.getRootElement() == null) {
      return;
    }
    if (!updateConfigItemValue(settingsDom.getRootElement(), ACTIVE_TABLE, tableName)) {
      // Settings from minimal documents may not contain view settings yet; ignore quietly.
    }
  }

  static void shiftRowsDown(OdfTable table, int rowIndex) {
    if (rowIndex < 0) {
      return;
    }
    table.insertRowsBefore(rowIndex, 1);
  }

  static void autoSizeColumns(OdfTable table, int startColumn, int columnCount) {
    if (columnCount <= 0) {
      return;
    }
    for (int i = 0; i < columnCount; i++) {
      int columnIndex = startColumn + i;
      if (columnIndex < 0 || columnIndex >= table.getColumnCount()) {
        continue;
      }
      OdfTableColumn column = table.getColumnByIndex(columnIndex);
      if (column != null) {
        column.setUseOptimalWidth(true);
      }
    }
  }

  static void protectTable(OdfTable table, String password) throws Exception {
    TableTableElement element = table.getOdfElement();
    element.setTableProtectedAttribute(true);
    if (Utils.isEmpty(password)) {
      return;
    }
    MessageDigest digest = MessageDigest.getInstance("SHA-1");
    digest.update(password.getBytes(StandardCharsets.UTF_8));
    element.setTableProtectionKeyAttribute(Base64.getEncoder().encodeToString(digest.digest()));
    element.setTableProtectionKeyDigestAlgorithmAttribute(SHA1_DIGEST_ALGORITHM);
  }

  private static String findConfigItemValue(Node node, String itemName) {
    if (node instanceof ConfigConfigItemElement configItem
        && itemName.equals(configItem.getConfigNameAttribute())) {
      return configItem.getTextContent();
    }
    NodeList children = node.getChildNodes();
    for (int i = 0; i < children.getLength(); i++) {
      String value = findConfigItemValue(children.item(i), itemName);
      if (!Utils.isEmpty(value)) {
        return value;
      }
    }
    return null;
  }

  private static boolean updateConfigItemValue(Node node, String itemName, String value) {
    if (node instanceof ConfigConfigItemElement configItem
        && itemName.equals(configItem.getConfigNameAttribute())) {
      configItem.setTextContent(value);
      return true;
    }
    NodeList children = node.getChildNodes();
    for (int i = 0; i < children.getLength(); i++) {
      if (updateConfigItemValue(children.item(i), itemName, value)) {
        return true;
      }
    }
    return false;
  }
}
