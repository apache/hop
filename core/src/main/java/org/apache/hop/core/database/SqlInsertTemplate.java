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

package org.apache.hop.core.database;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;

/**
 * Renders literal {@code INSERT INTO schema.table(col, ...) VALUES (...)} statements for rows of
 * one row layout, as {@link Database#getSqlOutput(String, String, IRowMeta, Object[], String)}
 * does, but with the quoted table and column names and the value formatters worked out once instead
 * of for every row.
 *
 * <p>Numbers and dates are written the same in every locale, see {@link SqlLiteralFormatter}.
 *
 * <p>Not thread-safe: the date formatters are reused across rows. Use one instance per transform
 * copy.
 */
public class SqlInsertTemplate {

  private static final String ORACLE_DATE_PATTERN = "yyyy/MM/dd HH:mm:ss";

  private final DatabaseMeta databaseMeta;
  private final IRowMeta fields;
  private final String dateFormat;
  private Boolean oracleVariant;

  /** "INSERT INTO schema.table(col1, col2) VALUES (" */
  private final String prefix;

  private SimpleDateFormat oracleDateFormatter;
  private SimpleDateFormat dateFormatter;

  /** Per field: the formatter of an Integer, Number, BigNumber or Date, or null. */
  private final SqlLiteralFormatter[] literals;

  private FastValueFormatter oracleDateFastFormatter;
  private FastValueFormatter dateFastFormatter;

  /**
   * @param databaseMeta the dialect that quotes names and string literals
   * @param variables resolves variables in the schema and table name
   * @param schemaName the schema, may be empty
   * @param tableName the table
   * @param fields the layout of the rows that will be rendered
   * @param dateFormat the pattern for date values, or empty for the dialect default
   * @throws HopDatabaseException when the names cannot be quoted
   */
  public SqlInsertTemplate(
      DatabaseMeta databaseMeta,
      IVariables variables,
      String schemaName,
      String tableName,
      IRowMeta fields,
      String dateFormat)
      throws HopDatabaseException {
    this.databaseMeta = databaseMeta;
    this.fields = fields;
    this.dateFormat = dateFormat;
    try {
      StringBuilder sb = new StringBuilder(128);
      sb.append("INSERT INTO ")
          .append(databaseMeta.getQuotedSchemaTableCombination(variables, schemaName, tableName))
          .append('(');
      for (int i = 0; i < fields.size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(databaseMeta.quoteField(fields.getValueMeta(i).getName()));
      }
      sb.append(") VALUES (");
      this.prefix = sb.toString();

      literals = new SqlLiteralFormatter[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        IValueMeta valueMeta = fields.getValueMeta(i);
        // A date in its own format is only written without a date format.
        if (valueMeta.getType() != IValueMeta.TYPE_DATE || Utils.isEmpty(dateFormat)) {
          literals[i] = SqlLiteralFormatter.forValueMeta(valueMeta);
        }
      }
    } catch (Exception e) {
      throw new HopDatabaseException(e);
    }
  }

  /**
   * @param r a row of the layout given to the constructor
   * @return the INSERT statement for the row, without a trailing semicolon
   * @throws HopDatabaseException when a value cannot be rendered
   */
  public String getSql(Object[] r) throws HopDatabaseException {
    StringBuilder sb = new StringBuilder(prefix.length() + 16 * fields.size());
    appendSql(sb, r);
    return sb.toString();
  }

  /**
   * Appends the INSERT statement for a row, without a trailing semicolon. On an exception the
   * builder may hold a partial statement.
   *
   * @param ins the builder to append to
   * @param r a row of the layout given to the constructor
   * @throws HopDatabaseException when a value cannot be rendered
   */
  public void appendSql(StringBuilder ins, Object[] r) throws HopDatabaseException {
    try {
      ins.append(prefix);

      for (int i = 0; i < fields.size(); i++) {
        IValueMeta valueMeta = fields.getValueMeta(i);
        Object valueData = r[i];

        if (i > 0) {
          ins.append(",");
        }

        if (valueMeta.isNull(valueData)) {
          ins.append("null");
          continue;
        }
        switch (valueMeta.getType()) {
          case IValueMeta.TYPE_BOOLEAN, IValueMeta.TYPE_STRING:
            // Have the database dialect do the quoting.
            // This also adds the single quotes around the string.
            ins.append(databaseMeta.quoteSqlString(valueMeta.getString(valueData)));
            break;
          case IValueMeta.TYPE_DATE:
            appendDate(ins, r, i);
            break;
          case IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_BIGNUMBER:
            literals[i].append(ins, valueData);
            break;
          default:
            ins.append(fields.getString(r, i));
            break;
        }
      }
      ins.append(')');
    } catch (Exception e) {
      throw new HopDatabaseException(e);
    }
  }

  private void appendDate(StringBuilder ins, Object[] r, int i) throws Exception {
    if (Utils.isEmpty(dateFormat)) {
      // Deprecated path. How a date literal is spelled in generated SQL is dialect syntax.
      if (oracleVariant == null) {
        oracleVariant = databaseMeta.getIDatabase().isOracleVariant();
      }
      if (oracleVariant) {
        Date date = fields.getDate(r, i);
        if (oracleDateFormatter == null) {
          oracleDateFormatter = SqlLiteralFormatter.newDateFormat(ORACLE_DATE_PATTERN);
          oracleDateFastFormatter = FastValueFormatter.forDateFormat(oracleDateFormatter);
        }
        ins.append("TO_DATE('");
        if (oracleDateFastFormatter == null || !oracleDateFastFormatter.append(ins, date)) {
          ins.append(oracleDateFormatter.format(date));
        }
        ins.append("', 'YYYY/MM/DD HH24:MI:SS')");
      } else {
        ins.append('\'');
        literals[i].append(ins, r[i]);
        ins.append('\'');
      }
    } else {
      Date date = fields.getDate(r, i);
      try {
        if (dateFormatter == null) {
          dateFormatter = SqlLiteralFormatter.newDateFormat(dateFormat);
          dateFastFormatter = FastValueFormatter.forDateFormat(dateFormatter);
        }
        ins.append('\'');
        if (dateFastFormatter == null || !dateFastFormatter.append(ins, date)) {
          ins.append(dateFormatter.format(date));
        }
        ins.append('\'');
      } catch (Exception e) {
        throw new HopDatabaseException("Error : ", e);
      }
    }
  }
}
