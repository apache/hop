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

package org.apache.hop.pipeline.transforms.dynamicsqlrow;

import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "DynamicSqlRow",
    image = "dynamicsqlrow.svg",
    name = "i18n::DynamicSQLRow.Name",
    description = "i18n::DynamicSQLRow.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Lookup",
    keywords = "i18n::DynamicSqlRowMeta.keyword",
    documentationUrl = "/pipeline/transforms/dynamicsqlrow.html",
    actionTransformTypes = {ActionTransformType.RDBMS})
public class DynamicSqlRowMeta extends BaseTransformMeta<DynamicSqlRow, DynamicSqlRowData> {
  private static final Class<?> PKG = DynamicSqlRowMeta.class;

  /** database connection */
  @HopMetadataProperty(
      key = "connection",
      injectionKeyDescription = "DynamicSQLRow.Injection.Connection",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String connection;

  private DatabaseMeta databaseMeta;

  /** SQL Statement */
  @HopMetadataProperty(
      key = "sql",
      injectionKeyDescription = "DynamicSQLRow.Injection.Sql",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_SQL)
  private String sql;

  @HopMetadataProperty(
      key = "sql_fieldname",
      injectionKeyDescription = "DynamicSQLRow.Injection.SqlFieldName",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_SQL)
  private String sqlFieldName;

  /** Number of rows to return (0=ALL) */
  @HopMetadataProperty(
      key = "rowlimit",
      injectionKeyDescription = "DynamicSQLRow.Injection.RowLimit")
  private int rowLimit;

  /**
   * false: don't return rows where nothing is found true: at least return one source row, the rest
   * is NULL
   */
  @HopMetadataProperty(
      key = "outer_join",
      injectionKeyDescription = "DynamicSQLRow.Injection.OuterJoin")
  private boolean outerJoin;

  @HopMetadataProperty(
      key = "replace_vars",
      injectionKeyDescription = "DynamicSQLRow.Injection.ReplaceVariables")
  private boolean replaceVariables;

  @HopMetadataProperty(
      key = "query_only_on_change",
      injectionKeyDescription = "DynamicSQLRow.Injection.QueryOnlyOnChange")
  private boolean queryOnlyOnChange;

  public DynamicSqlRowMeta() {
    super(); // allocate BaseTransformMeta
  }

  public DynamicSqlRowMeta(DynamicSqlRowMeta meta) {
    super();
    this.connection = meta.connection;
    this.sql = meta.sql;
    this.sqlFieldName = meta.sqlFieldName;
    this.replaceVariables = meta.replaceVariables;
    this.rowLimit = meta.rowLimit;
    this.connection = meta.connection;
    this.outerJoin = meta.outerJoin;
    this.queryOnlyOnChange = meta.queryOnlyOnChange;
  }

  public String getConnection() {
    return connection;
  }

  public void setConnection(String connection) {
    this.connection = connection;
  }

  /**
   * @return Returns the database.
   */
  public DatabaseMeta getDatabaseMeta() {
    return databaseMeta;
  }

  /**
   * @param database The database to set.
   */
  public void setDatabaseMeta(DatabaseMeta database) {
    this.databaseMeta = database;
  }

  /**
   * @return Returns the outerJoin.
   */
  public boolean isOuterJoin() {
    return outerJoin;
  }

  /**
   * @param outerJoin The outerJoin to set.
   */
  public void setOuterJoin(boolean outerJoin) {
    this.outerJoin = outerJoin;
  }

  /**
   * @return Returns the replacevars.
   */
  public boolean isReplaceVariables() {
    return replaceVariables;
  }

  public void setReplaceVariables(boolean replaceVariables) {
    this.replaceVariables = replaceVariables;
  }

  /**
   * @return Returns the queryonlyonchange.
   */
  public boolean isQueryOnlyOnChange() {
    return queryOnlyOnChange;
  }

  /**
   * @param queryonlyonchange The queryonlyonchange to set.
   */
  public void setQueryOnlyOnChange(boolean queryonlyonchange) {
    this.queryOnlyOnChange = queryonlyonchange;
  }

  /**
   * @return Returns the rowLimit.
   */
  public int getRowLimit() {
    return rowLimit;
  }

  /**
   * @param rowLimit The rowLimit to set.
   */
  public void setRowLimit(int rowLimit) {
    this.rowLimit = rowLimit;
  }

  /**
   * @return Returns the sql.
   */
  public String getSql() {
    return sql;
  }

  /**
   * @param sql The sql to set.
   */
  public void setSql(String sql) {
    this.sql = sql;
  }

  /**
   * @return Returns the sqlfieldname.
   */
  public String getSqlFieldName() {
    return sqlFieldName;
  }

  /**
   * @param sqlfieldname The sqlfieldname to set.
   */
  public void setSqlFieldName(String sqlfieldname) {
    this.sqlFieldName = sqlfieldname;
  }

  @Override
  public Object clone() {
    return new DynamicSqlRowMeta(this);
  }

  @Override
  public void setDefault() {
    this.connection = null;
    this.databaseMeta = null;
    this.rowLimit = 0;
    this.sql = "";
    this.outerJoin = false;
    this.replaceVariables = false;
    this.sqlFieldName = null;
    this.queryOnlyOnChange = false;
  }

  @Override
  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    if (databaseMeta == null) {
      return;
    }

    Database db = new Database(loggingObject, variables, databaseMeta);
    databases = new Database[] {db}; // Keep track of this one for cancelQuery

    // First try without connecting to the database... (can be S L O W)
    // See if it's in the cache...
    IRowMeta add = null;
    String realSql = sql;
    if (replaceVariables) {
      realSql = variables.resolve(realSql);
    }
    try {
      add = db.getQueryFields(realSql, false);
    } catch (HopDatabaseException dbe) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "DynamicSQLRowMeta.Exception.UnableToDetermineQueryFields")
              + Const.CR
              + sql,
          dbe);
    }

    if (add != null) { // Cache hit, just return it this...
      for (int i = 0; i < add.size(); i++) {
        IValueMeta v = add.getValueMeta(i);
        v.setOrigin(name);
      }
      row.addRowMeta(add);
    } else {
      // No cache hit, connect to the database, do it the hard way...
      try {
        db.connect();
        add = db.getQueryFields(realSql, false);
        for (int i = 0; i < add.size(); i++) {
          IValueMeta v = add.getValueMeta(i);
          v.setOrigin(name);
        }
        row.addRowMeta(add);
        db.disconnect();
      } catch (HopDatabaseException dbe) {
        throw new HopTransformException(
            BaseMessages.getString(PKG, "DynamicSQLRowMeta.Exception.ErrorObtainingFields"), dbe);
      }
    }
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    CheckResult cr;
    String errorMessage = "";

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "DynamicSQLRowMeta.CheckResult.ReceivingInfo"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "DynamicSQLRowMeta.CheckResult.NoInputReceived"),
              transformMeta);
      remarks.add(cr);
    }

    // Check for SQL field
    if (Utils.isEmpty(sqlFieldName)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "DynamicSQLRowMeta.CheckResult.SQLFieldNameMissing"),
              transformMeta);
      remarks.add(cr);
    } else {
      IValueMeta vfield = prev.searchValueMeta(sqlFieldName);
      if (vfield == null) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG, "DynamicSQLRowMeta.CheckResult.SQLFieldNotFound", sqlFieldName),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG,
                    "DynamicSQLRowMeta.CheckResult.SQLFieldFound",
                    sqlFieldName,
                    vfield.getOrigin()),
                transformMeta);
      }
      remarks.add(cr);
    }

    if (databaseMeta != null) {
      Database db = new Database(loggingObject, variables, databaseMeta);
      databases = new Database[] {db}; // Keep track of this one for cancelQuery

      try {
        db.connect();
        if (sql != null && sql.length() != 0) {

          errorMessage = "";

          IRowMeta r = db.getQueryFields(sql, true);
          if (r != null) {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(PKG, "DynamicSQLRowMeta.CheckResult.QueryOK"),
                    transformMeta);
            remarks.add(cr);
          } else {
            errorMessage =
                BaseMessages.getString(PKG, "DynamicSQLRowMeta.CheckResult.InvalidDBQuery");
            cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
            remarks.add(cr);
          }
        }
      } catch (HopException e) {
        errorMessage =
            BaseMessages.getString(PKG, "DynamicSQLRowMeta.CheckResult.ErrorOccurred")
                + e.getMessage();
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } finally {
        db.disconnect();
      }
    } else {
      errorMessage = BaseMessages.getString(PKG, "DynamicSQLRowMeta.CheckResult.InvalidConnection");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public void analyseImpact(
      IVariables variables,
      List<DatabaseImpact> impact,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    IRowMeta out = prev.clone();
    getFields(
        out,
        transformMeta.getName(),
        new IRowMeta[] {
          info,
        },
        null,
        variables,
        metadataProvider);
    if (out != null) {
      for (int i = 0; i < out.size(); i++) {
        IValueMeta outvalue = out.getValueMeta(i);
        DatabaseImpact di =
            new DatabaseImpact(
                DatabaseImpact.TYPE_IMPACT_READ,
                pipelineMeta.getName(),
                transformMeta.getName(),
                databaseMeta.getDatabaseName(),
                "",
                outvalue.getName(),
                outvalue.getName(),
                transformMeta.getName(),
                sql,
                BaseMessages.getString(PKG, "DynamicSQLRowMeta.DatabaseImpact.Title"));
        impact.add(di);
      }
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
