/*! ******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.databases.snowflake;

import org.apache.commons.lang.Validate;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.metadata.api.HopMetadataProperty;

import java.util.HashMap;
import java.util.Map;

/**
 * Contains Snowflake specific information through static final members
 *
 * https://docs.snowflake.net/manuals/sql-reference/info-schema.html
 *
 */
@DatabaseMetaPlugin(type = "SNOWFLAKE", typeDescription = "Snowflake")
@GuiPlugin(id = "GUI-SnowflakeDatabaseMeta")
public class SnowflakeDatabaseMeta extends BaseDatabaseMeta implements IDatabase {

  // TODO: Manage all attributes in plugin when HOP-67 is fixed
  @HopMetadataProperty
  @GuiWidgetElement(
      id = "warehouse",
      order = "02B",
      label = "i18n:org.apache.hop.ui.core.database:DatabaseDialog.label.Warehouse",
      type = GuiElementType.TEXT,
      variables = true,
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID)
  private String warehouse;

	public String getWarehouse() {
		return warehouse;
	}

	public void setWarehouse(String warehouse) {
		this.warehouse = warehouse;
	}

	@Override
	public int[] getAccessTypeList() {
		return new int[] { DatabaseMeta.TYPE_ACCESS_NATIVE};
	}

	@Override
	public int getDefaultDatabasePort() {
		if (getAccessType() == DatabaseMeta.TYPE_ACCESS_NATIVE) {
			return 443;
		}
		return -1;
	}

	@Override
	public Map<String, String> getDefaultOptions() {
		Map<String, String> defaultOptions = new HashMap<>();
		defaultOptions.put(getPluginId() + ".ssl", "on");

		return defaultOptions;
	}

	@Override
	public boolean isFetchSizeSupported() {
		return false;
	}

	@Override
	public String getDriverClass() {
		return "net.snowflake.client.jdbc.SnowflakeDriver";
	}

	@Override
	public String getURL(String hostName, String port, String databaseName) {

		Validate.notEmpty(hostName, "Host name is empty");

		String url = "jdbc:snowflake://" + hostName.toLowerCase();
		if (!url.endsWith(".snowflakecomputing.com")) {
			url = url + ".snowflakecomputing.com";
		}	

		if (!Utils.isEmpty(port)) {
			url = url + ":" + port;
		}
				
		boolean isFirstQueryParam = true;
		if (!Utils.isEmpty(warehouse)) {
			url = url + "/?warehouse=" + warehouse;
			isFirstQueryParam = false;
		}

		if (!Utils.isEmpty(databaseName)) {
			if (isFirstQueryParam)
				url = url + "/?db=" + databaseName;
			else
				url = url + "&db=" + databaseName;
		}

		return url;
	}

	@Override
	public String getAddColumnStatement( String tableName, IValueMeta v, String tk, boolean useAutoinc,
                                       String pk, boolean semicolon) {
		return "ALTER TABLE " + tableName + " ADD COLUMN " + getFieldDefinition(v, tk, pk, useAutoinc, true, false);
	}

	@Override
	public String getDropColumnStatement( String tableName, IValueMeta v, String tk, boolean useAutoinc,
                                        String pk, boolean semicolon) {
		return "ALTER TABLE " + tableName + " DROP COLUMN " + v.getName() + Const.CR;
	}

	@Override
	public String getSqlListOfSchemas() {
		return "SELECT SCHEMA_NAME AS \"name\" FROM " + getDatabaseName() + ".INFORMATION_SCHEMA.SCHEMATA";
	}

	@Override
	public String getSqlListOfProcedures() {
		return "SELECT PROCEDURE_NAME AS \"name\" FROM " + getDatabaseName() + ".INFORMATION_SCHEMA.PROCEDURES";
	}

	@Override
	public String getSqlListOfSequences() {
		return "SELECT SEQUENCE_NAME AS \"name\" FROM " + getDatabaseName() + ".INFORMATION_SCHEMA.SEQUENCES";
	}
	
    @Override
	public String getSqlNextSequenceValue( String sequenceName ) {
	    return "SELECT " + sequenceName + ".NEXTVAL FROM DUAL";
	}
		
	@Override
	public String getModifyColumnStatement( String tableName, IValueMeta v, String tk, boolean useAutoinc,
                                          String pk, boolean semicolon) {
		return "ALTER TABLE " + tableName + " MODIFY COLUMN " + getFieldDefinition(v, tk, pk, useAutoinc, true, false);
	}

	@Override
	public String getFieldDefinition( IValueMeta v, String surrogateKey, String primaryKey, boolean useAutoinc,
                                    boolean addFieldName, boolean addCr) {
		String fieldDefinitionDdl = "";

		String newline = addCr ? Const.CR : "";

		String fieldname = v.getName();
		int length = v.getLength();
		int precision = v.getPrecision();
		int type = v.getType();

		boolean isKeyField = fieldname.equalsIgnoreCase(surrogateKey) || fieldname.equalsIgnoreCase(primaryKey);

		if (addFieldName) {
			fieldDefinitionDdl += fieldname + " ";
		}
		if (isKeyField) {
			Validate.isTrue(type == IValueMeta.TYPE_NUMBER || type == IValueMeta.TYPE_INTEGER
					|| type == IValueMeta.TYPE_BIGNUMBER);
			return ddlForPrimaryKey(useAutoinc) + newline;
		}
		switch (type) {
		case IValueMeta.TYPE_TIMESTAMP:
		case IValueMeta.TYPE_DATE:
			// timestamp w/ local timezone
			fieldDefinitionDdl += "TIMESTAMP_LTZ";
			break;
		case IValueMeta.TYPE_BOOLEAN:
			fieldDefinitionDdl += ddlForBooleanValue();
			break;
		case IValueMeta.TYPE_NUMBER:
		case IValueMeta.TYPE_INTEGER:
		case IValueMeta.TYPE_BIGNUMBER:
			if (precision == 0) {
				fieldDefinitionDdl += ddlForIntegerValue(length);
			} else {
				fieldDefinitionDdl += ddlForFloatValue(length, precision);
			}
			break;
		case IValueMeta.TYPE_STRING:
			if (length <= 0) {
				fieldDefinitionDdl += "VARCHAR";
			} else {
				fieldDefinitionDdl += "VARCHAR(" + length + ")";
			}
			break;
		case IValueMeta.TYPE_BINARY:
			fieldDefinitionDdl += "VARIANT";
			break;
		default:
			fieldDefinitionDdl += " UNKNOWN";
			break;
		}
		return fieldDefinitionDdl + newline;
	}

	private String ddlForBooleanValue() {
		if (supportsBooleanDataType()) {
			return "BOOLEAN";
		} else {
			return "CHAR(1)";
		}
	}

	private String ddlForIntegerValue(int length) {
		if (length > 9) {
			if (length < 19) {
				// can hold signed values between -9223372036854775808 and 9223372036854775807
				// 18 significant digits
				return "BIGINT";
			} else {
				return "NUMBER(" + length + ", 0 )";
			}
		} else {
			return "INT";
		}
	}

	private String ddlForFloatValue(int length, int precision) {
		if (length > 15) {
			return "NUMBER(" + length + ", " + precision + ")";
		} else {
			return "FLOAT";
		}
	}

	private String ddlForPrimaryKey(boolean useAutoinc) {
		if (useAutoinc) {
			return "BIGINT AUTOINCREMENT NOT NULL PRIMARY KEY";
		} else {
			return "BIGINT NOT NULL PRIMARY KEY";
		}
	}

	@Override
	public String getLimitClause(int nrRows) {
		return " LIMIT " + nrRows;
	}

	/**
	 * Returns the minimal SQL to launch in order to determine the layout of the
	 * resultset for a given database table
	 *
	 * @param tableName The name of the table to determine the layout for
	 * @return The SQL to launch.
	 */
	@Override
	public String getSqlQueryFields(String tableName) {
		return "SELECT * FROM " + tableName + " LIMIT 0";
	}

	@Override
	public String getSqlTableExists(String tableName) {
		return getSqlQueryFields(tableName);
	}

	@Override
	public String getSqlColumnExists(String columnname, String tableName) {
		return getSqlQueryColumnFields(columnname, tableName);
	}

	public String getSqlQueryColumnFields(String columnname, String tableName) {
		return "SELECT " + columnname + " FROM " + tableName + " LIMIT 0";
	}

	@Override
	public int getNotFoundTK(boolean useAutoinc) {
		if (supportsAutoInc() && useAutoinc) {
			return 1;
		}
		return super.getNotFoundTK(useAutoinc);
	}

	/**
	 * @return The extra option separator in database URL for this platform (usually
	 *         this is semicolon ; )
	 */
	@Override
	public String getExtraOptionSeparator() {
		return "&";
	}

	/**
	 * @return This indicator separates the normal URL from the options
	 */
	@Override
	public String getExtraOptionIndicator() {
		return "&";
	}

	/**
	 * @return true if the database supports transactions.
	 */
	@Override
	public boolean supportsTransactions() {
		return false;
	}

	@Override
	public boolean supportsTimeStampToDateConversion() {
		return false; // The 3.6.9 driver _does_ support conversion, but errors when value is null.
	}

	/**
	 * @return true if the database supports bitmap indexes
	 */
	@Override
	public boolean supportsBitmapIndex() {
		return false;
	}

	/**
	 * @return true if the database supports views
	 */
	@Override
	public boolean supportsViews() {
		return true;
	}

	@Override
	public boolean supportsSequences() {
		return true;
	}
	
	@Override
	public boolean supportsSynonyms() {
		return false;
	}

	@Override
	public boolean supportsBooleanDataType() {
		return true;
	}
	
	@Override
	public boolean supportsErrorHandlingOnBatchUpdates() {
		return true;
	}

	@Override
	public String[] getReservedWords() {
		return new String[] { "ALL", "ALTER", "AND", "ANY", "AS", "ASC", "BETWEEN", "BY", "CASE", "CAST", "CHECK",
				"CLUSTER", "COLUMN", "CONNECT", "CREATE", "CROSS", "CURRENT", "DELETE", "DESC", "DISTINCT", "DROP",
				"ELSE", "EXCLUSIVE", "EXISTS", "FALSE", "FOR", "FROM", "FULL", "GRANT", "GROUP", "HAVING", "IDENTIFIED",
				"IMMEDIATE", "IN", "INCREMENT", "INNER", "INSERT", "INTERSECT", "INTO", "IS", "JOIN", "LATERAL", "LEFT",
				"LIKE", "LOCK", "LONG", "MAXEXTENTS", "MINUS", "MODIFY", "NATURAL", "NOT", "NULL", "OF", "ON", "OPTION",
				"OR", "ORDER", "REGEXP", "RENAME", "REVOKE", "RIGHT", "RLIKE", "ROW", "ROWS", "SELECT", "SET", "SOME",
				"START", "TABLE", "THEN", "TO", "TRIGGER", "TRUE", "UNION", "UNIQUE", "UPDATE", "USING", "VALUES",
				"WHEN", "WHENEVER", "WHERE", "WITH" };
	}

	@Override
	public String getExtraOptionsHelpText() {
		return "https://docs.snowflake.net/manuals/user-guide/jdbc-configure.html";
	}

	@Override
	public String getSqlInsertAutoIncUnknownDimensionRow(String schemaTable, String keyField, String versionField) {
		return "insert into " + schemaTable + "(" + keyField + ", " + versionField + ") values (1, 1)";
	}

	@Override
	public String quoteSqlString(String string) {
		string = string.replace("'", "\\\\'");
		string = string.replace("\\n", "\\\\n");
		string = string.replace("\\r", "\\\\r");
		return "'" + string + "'";
	}

	@Override
	public boolean releaseSavepoint() {
		return false;
	}

	@Override
	public boolean isRequiringTransactionsOnQueries() {
		return false;
	}

}
