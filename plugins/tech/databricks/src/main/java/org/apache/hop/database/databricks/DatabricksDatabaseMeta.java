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

import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.DriverDownload;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.types.ColumnContext;
import org.apache.hop.core.database.types.DatabaseTypes;
import org.apache.hop.core.database.types.IDatabaseTypeRule;
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
      id = "port",
      ignored = true,
      type = GuiElementType.TEXT,
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID)
  @HopMetadataProperty
  private String port;

  @GuiWidgetElement(
      id = "databaseName",
      ignored = true,
      type = GuiElementType.TEXT,
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID)
  @HopMetadataProperty
  private String databaseName;

  // Constructor to set default values for ignored fields
  public DatabricksDatabaseMeta() {
    super();
    // Set default values for fields that are ignored in the UI
    // but still needed for URL construction
    this.port = "443"; // Default Databricks port
    this.databaseName = ""; // Not used for Databricks
  }

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

  /**
   * Databricks holds semi structured data in a VARIANT, so a JSON value gets a column of its own
   * rather than the text a dialect with no such type falls back to. VARIANT arrived in Databricks
   * Runtime 15.3 and in Databricks SQL 2024.30; an older cluster rejects it, and the column has to
   * be the STRING that {@link org.apache.hop.core.database.types.ColumnTypeFallback} would give.
   */
  private static final List<IDatabaseTypeRule> TYPE_RULES =
      DatabaseTypes.rules().write(IValueMeta.TYPE_JSON).as("VARIANT").build();

  @Override
  public List<IDatabaseTypeRule> getTypeRules() {
    return TYPE_RULES;
  }

  /** The widest precision a Databricks DECIMAL carries. */
  private static final int MAX_DECIMAL_PRECISION = 38;

  /** The scale Spark itself picks for a decimal whose size nobody stated. */
  private static final int DEFAULT_DECIMAL_SCALE = 18;

  /** The largest number of digits that still fits a BIGINT. */
  private static final int BIGINT_DIGITS = 20;

  @Override
  public String getFieldDefinition(
      IValueMeta v,
      String tk,
      String pk,
      boolean useAutoIncrement,
      boolean addFieldName,
      boolean addCr) {

    StringBuilder definition = new StringBuilder();
    if (addFieldName) {
      definition.append(v.getName()).append(' ');
    }

    definition.append(columnType(v, tk, pk, useAutoIncrement));

    if (addCr) {
      definition.append(Const.CR);
    }
    return definition.toString();
  }

  /** The Databricks type for a value, without the column name or the line break around it. */
  private String columnType(IValueMeta v, String tk, String pk, boolean useAutoIncrement) {
    String fieldName = v.getName();
    int type = v.getType();

    if ((type == IValueMeta.TYPE_INTEGER
            || type == IValueMeta.TYPE_NUMBER
            || type == IValueMeta.TYPE_BIGNUMBER)
        && (fieldName.equalsIgnoreCase(tk) || fieldName.equalsIgnoreCase(pk))) {
      // Delta has no AUTO_INCREMENT: a generated key is an identity column. Both constraints are
      // informational on Databricks, which records them but does not enforce them.
      return useAutoIncrement
          ? "BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL PRIMARY KEY"
          : "BIGINT NOT NULL PRIMARY KEY";
    }

    return switch (type) {
        // A Hop date carries a time of day, which the DATE column would drop.
      case IValueMeta.TYPE_DATE, IValueMeta.TYPE_TIMESTAMP -> "TIMESTAMP";
        // BOOLEAN is a Databricks type on every version, so unlike the dialects that grew one
        // late, this does not consult the connection's "supports boolean" option: there is no
        // Databricks that needs the single character column that option falls back to.
      case IValueMeta.TYPE_BOOLEAN -> "BOOLEAN";
        // A Databricks STRING is unbounded, so a length has nothing to say here. VARCHAR(n) is
        // accepted but stored as a STRING with a length check, which is not what was asked for.
      case IValueMeta.TYPE_STRING -> "STRING";
      case IValueMeta.TYPE_BINARY -> "BINARY";
      case IValueMeta.TYPE_INTEGER -> integerType(v.getLength());
      case IValueMeta.TYPE_NUMBER -> "DOUBLE";
      case IValueMeta.TYPE_BIGNUMBER -> decimalType(v.getLength(), v.getPrecision());
        // Anything else has already been swapped for something spellable by ColumnTypeFallback, so
        // this answers a caller that came straight here: the type Databricks can always hold.
      default -> "STRING";
    };
  }

  /** The integer column for a number of digits, widening as the digits do. */
  private String integerType(int length) {
    if (length <= 0) {
      // A Hop integer is a 64 bit long, so an unstated length is the whole of one rather than the
      // narrowest column, which is what makes an unsized integer overflow elsewhere: issue #4174.
      return "BIGINT";
    }
    if (length < 3) {
      return "TINYINT";
    }
    if (length < 5) {
      return "SMALLINT";
    }
    if (length < 10) {
      return "INT";
    }
    if (length < BIGINT_DIGITS) {
      return "BIGINT";
    }
    return "DECIMAL(" + Math.min(length, MAX_DECIMAL_PRECISION) + ")";
  }

  /** The decimal column for a stated size, clamped to what Databricks can carry. */
  private String decimalType(int length, int precision) {
    boolean sized = length > 0;
    int digits = sized ? Math.min(length, MAX_DECIMAL_PRECISION) : MAX_DECIMAL_PRECISION;
    int scale = precision > 0 ? Math.min(precision, digits) : (sized ? 0 : DEFAULT_DECIMAL_SCALE);
    return "DECIMAL(" + digits + "," + scale + ")";
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
  public DriverDownload getDriverDownload() {
    return DriverDownload.builder()
        .mavenCoordinate("com.databricks:databricks-jdbc")
        .defaultVersion("3.4.2")
        .licenseCategory("A")
        .licenseName("Apache-2.0")
        .licenseUrl("https://github.com/databricks/databricks-jdbc/blob/main/LICENSE")
        .vendor("Databricks")
        .vendorUrl("https://github.com/databricks/databricks-jdbc")
        .notes("Open source Databricks JDBC driver (uber jar, ~39 MB)")
        .build();
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
        + getColumnDefinition(
            v, tk, pk, useAutoIncrement, true, false, ColumnContext.Purpose.ADD_COLUMN);
  }

  @Override
  public String getModifyColumnStatement(
      String tableName,
      IValueMeta v,
      String tk,
      boolean useAutoIncrement,
      String pk,
      boolean semicolon) {
    // The column name belongs to the ALTER syntax rather than to the column definition, so the
    // definition is asked for without one.
    return "ALTER TABLE "
        + tableName
        + " ALTER COLUMN "
        + v.getName()
        + " TYPE "
        + getColumnDefinition(
            v, tk, pk, useAutoIncrement, false, false, ColumnContext.Purpose.MODIFY_COLUMN);
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

  /**
   * Returns a list of UI element IDs that should be excluded from the database editor. Only for
   * elements created directly in DatabaseMetaEditor (not @GuiWidgetElement). Databricks doesn't
   * need manual URL field.
   *
   * @return List of element IDs to exclude
   */
  @Override
  public List<String> getRemoveItems() {
    return Arrays.asList(
        BaseDatabaseMeta.ELEMENT_ID_MANUAL_URL // We construct the URL automatically
        );
  }

  @Override
  public boolean isRequiresName() {
    return false;
  }

  @Override
  public void addDefaultOptions() {
    setSupportsBooleanDataType(true);
    setSupportsTimestampDataType(true);
  }
}
