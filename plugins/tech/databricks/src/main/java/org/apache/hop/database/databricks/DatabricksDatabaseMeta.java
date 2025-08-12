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
 *
 */

package org.apache.hop.database.databricks;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
@DatabaseMetaPlugin(
    type = "DATABRICKS",
    typeDescription = "Databricks",
    documentationUrl = "/database/databases/databricks.html")
@GuiPlugin(id = "GUI-DatabricksDatabaseMeta")
public class DatabricksDatabaseMeta extends BaseDatabaseMeta implements IDatabase {

  public static final Class<?> PKG = DatabricksDatabaseMeta.class;

  @GuiWidgetElement(
      id = "ucHttpPath",
      order = "10",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "httpPath")
  @HopMetadataProperty
  private String httpPath;

  @GuiWidgetElement(
      id = "ucCatalogName",
      order = "11",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "Catalog Name")
  @HopMetadataProperty
  private String catalogName;

  @Override
  public String getFieldDefinition(
      IValueMeta iValueMeta, String s, String s1, boolean b, boolean b1, boolean b2) {
    return "";
  }

  @Override
  public int[] getAccessTypeList() {
    return new int[0];
  }

  @Override
  public String getDriverClass() {
    return "com.databricks.client.jdbc.Driver";
  }

  @Override
  public String getURL(String hostname, String port, String databaseName)
      throws HopDatabaseException {
    String url = "jdbc:databricks://" + hostname + ":" + port + ";HttpPath=" + httpPath;
    if (!StringUtils.isEmpty(catalogName)) {
      url += ";ConnCatalog=" + catalogName;
    }
    return url;
  }

  @Override
  public String getAddColumnStatement(
      String tableName,
      IValueMeta v,
      String tk,
      boolean useAutoIncrement,
      String pk,
      boolean semicolon) {
    return "ALTER TABLE "
        + tableName
        + " ADD COLUMN "
        + getFieldDefinition(v, tk, pk, useAutoIncrement, true, false);
  }

  @Override
  public String getModifyColumnStatement(
      String tableName,
      IValueMeta v,
      String tk,
      boolean useAutoIncrement,
      String pk,
      boolean semicolon) {
    return "ALTER TABLE "
        + tableName
        + " ALTER COLUMN "
        + getFieldDefinition(v, tk, pk, useAutoIncrement, false, false);
  }

  @Override
  public boolean isSupportsBooleanDataType() {
    return true;
  }

  @Override
  public boolean isSupportsTimestampDataType() {
    return true;
  }

  @Override
  public boolean isSupportsOptionsInURL() {
    return true;
  }

  @Override
  public int getDefaultDatabasePort() {
    return 443;
  }

  @Override
  public String[] getReservedWords() {
    return new String[] {
      "ANTI",
      "CROSS",
      "EXCEPT",
      "FULL",
      "INNER",
      "INTERSECT",
      "JOIN",
      "LATERAL",
      "LEFT",
      "MINUS",
      "NATURAL",
      "ON",
      "RIGHT",
      "SEMI",
      "UNION",
      "USING"
    };
  }
}
