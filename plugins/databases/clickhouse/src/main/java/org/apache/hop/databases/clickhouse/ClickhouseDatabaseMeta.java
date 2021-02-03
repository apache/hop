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

package org.apache.hop.databases.clickhouse;

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
 * Contains Clickhouse specific information through static final members
 *
 * https://clickhouse.tech/docs/en/sql-reference/
 *
 */
@DatabaseMetaPlugin(type = "CLICKHOUSE", typeDescription = "ClickHouse")
@GuiPlugin(id = "GUI-ClickhouseDatabaseMeta")
public class ClickhouseDatabaseMeta extends BaseDatabaseMeta implements IDatabase {

	// TODO: Manage all attributes in plugin when HOP-67 is fixed
	@Override
	public int[] getAccessTypeList() {
		return new int[] { DatabaseMeta.TYPE_ACCESS_NATIVE};
	}

	@Override
	public int getDefaultDatabasePort() {
		if (getAccessType() == DatabaseMeta.TYPE_ACCESS_NATIVE) {
			return 8123;
		}
		return -1;
	}

	@Override
	public String getDriverClass() {
		return "cc.blynk.clickhouse.ClickHouseDriver";
	}

	@Override
	public String getURL(String hostName, String port, String databaseName) {

		Validate.notEmpty(hostName, "Host name is empty");

		String url = "jdbc:clickhouse://" + hostName.toLowerCase();

		if (!Utils.isEmpty(port)) {
			url = url + ":" + port;
		}
				
		boolean isFirstQueryParam = true;
		if (!Utils.isEmpty(databaseName)) {
			if (isFirstQueryParam)
				url = url + "/" + databaseName;
			else
				url = url + databaseName;
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
			return ddlForPrimaryKey() + newline;
		}
		switch (type) {
		case IValueMeta.TYPE_TIMESTAMP:
			// timestamp w/ local timezone
			fieldDefinitionDdl += "DATETIME";
			break;
		case IValueMeta.TYPE_DATE:
			fieldDefinitionDdl += "DATE";
			break;
		case IValueMeta.TYPE_BOOLEAN:
			fieldDefinitionDdl += "UINT8";
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
			fieldDefinitionDdl += "STRING";
			break;
		case IValueMeta.TYPE_BINARY:
			fieldDefinitionDdl += "UNSUPPORTED";
			break;
		default:
			fieldDefinitionDdl += " UNKNOWN";
			break;
		}
		return fieldDefinitionDdl + newline;
	}

	private String ddlForIntegerValue(int length) {
		if (length > 9) {
			if (length < 19) {
				// can hold signed values between -9223372036854775808 and 9223372036854775807
				// 18 significant digits
				return "INT64";
			} else {
				// can hold signed values between -170141183460469231731687303715884105728 and 170141183460469231731687303715884105727
				// 36 significant digits
				return "INT128";
			}
		} else {
			return "INT32";
		}
	}

	private String ddlForFloatValue(int length, int precision) {
		if (length > 15) {
			return "DECIMAL(" + length + ", " + precision + ")";
		} else {
			return "FLOAT32";
		}
	}

	private String ddlForPrimaryKey() {
		return "UUID NOT NULL PRIMARY KEY";
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

	/**
	 * @return The extra option separator in database URL for this platform (usually
	 *         this is semicolon ; )
	 */
	@Override
	public String getExtraOptionSeparator() {
		return "&";
	}

	/**
	 * @return true if all fields should always be quoted in db
	 */
	public boolean isQuoteAllFields() {
		return false;
	}

	/**
	 * @return This indicator separates the normal URL from the options
	 */
	@Override
	public String getExtraOptionIndicator() {
		return "&";
	}


	/**
	 * @return true if the database supports schemas
	 */
	public boolean supportsSchemas() {
		return false;
	}

	/**
	 * @return true if the database supports transactions.
	 */
	@Override
	public boolean supportsTransactions() {
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
		return false;
	}
	
	@Override
	public boolean supportsSynonyms() {
		return true;
	}

	@Override
	public boolean supportsBooleanDataType() {
		return false;
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
		return "https://github.com/ClickHouse/clickhouse-jdbc";
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


	/**
	 * @return true if we need to supply the schema-name to getTables in order to get a correct list of items.
	 */
	public boolean useSchemaNameForTableList() {
		return false;
	}

	/**
	 * @return true if the database resultsets support getTimeStamp() to retrieve date-time. (Date)
	 */
	public boolean supportsTimeStampToDateConversion() {
		return false;
	}
}
