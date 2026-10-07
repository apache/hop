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
import lombok.Getter;
import lombok.Setter;
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

@Getter
@Setter
@Transform(
    id = "DynamicSqlRow",
    image = "dynamicsqlrow.svg",
    name = "i18n::DynamicSQLRow.Name",
    description = "i18n::DynamicSQLRow.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
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
    this.outerJoin = meta.outerJoin;
    this.queryOnlyOnChange = meta.queryOnlyOnChange;
  }

  @Override
  public void setDefault() {
    this.connection = null;
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

    // An empty template is legal until the transform runs: there are no template fields to add.
    String realSql = replaceVariables ? variables.resolve(sql) : sql;
    if (Utils.isEmpty(realSql)) {
      return;
    }

    DatabaseMeta databaseMeta = loadDatabaseMeta(variables, metadataProvider);
    if (databaseMeta == null) {
      return;
    }

    try (Database db = new Database(loggingObject, variables, databaseMeta)) {
      // Keep track of this one for cancelQuery
      databases = new Database[] {db};

      // First try without connecting to the database... (can be S L O W)
      // See if it's in the cache...
      IRowMeta add = null;
      try {
        add = db.getQueryFields(realSql, false);
      } catch (HopDatabaseException dbe) {
        throw new HopTransformException(
            BaseMessages.getString(PKG, "DynamicSQLRowMeta.Exception.UnableToDetermineQueryFields")
                + Const.CR
                + sql,
            dbe);
      }
      // Cache hit, just return it this...
      if (add != null) {
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
        } catch (HopDatabaseException dbe) {
          throw new HopTransformException(
              BaseMessages.getString(PKG, "DynamicSQLRowMeta.Exception.ErrorObtainingFields"), dbe);
        }
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

    DatabaseMeta databaseMeta;
    try {
      databaseMeta = loadDatabaseMeta(variables, metadataProvider);
    } catch (HopTransformException e) {
      errorMessage =
          BaseMessages.getString(PKG, "DynamicSQLRowMeta.CheckResult.ErrorOccurred")
              + e.getMessage();
      remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta));
      return;
    }

    if (databaseMeta != null) {
      try (Database db = new Database(loggingObject, variables, databaseMeta)) {
        // Keep track of this one for cancelQuery
        databases = new Database[] {db};
        db.connect();
        if (!Utils.isEmpty(sql)) {

          errorMessage = "";

          String realSql = replaceVariables ? variables.resolve(sql) : sql;
          IRowMeta r = db.getQueryFields(realSql, true);
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

    DatabaseMeta databaseMeta = loadDatabaseMeta(variables, metadataProvider);
    if (databaseMeta == null) {
      return;
    }
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

  /**
   * Looks up the connection by name every time it's needed. The resolved connection isn't kept on
   * the meta: a freshly loaded pipeline would otherwise report no template fields until the dialog
   * was opened.
   *
   * @return the connection, or null when no connection is set or its name can't be resolved yet
   * @throws HopTransformException when a resolved connection name isn't in the metadata store
   */
  private DatabaseMeta loadDatabaseMeta(IVariables variables, IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    if (Utils.isEmpty(connection)) {
      return null;
    }
    String realConnection = variables.resolve(connection);
    // A connection variable that is only set at runtime can't be resolved at design time.
    if (Utils.isEmpty(realConnection) || realConnection.contains("${")) {
      return null;
    }
    DatabaseMeta databaseMeta;
    try {
      databaseMeta = metadataProvider.getSerializer(DatabaseMeta.class).load(realConnection);
    } catch (HopException e) {
      throw new HopTransformException(
          BaseMessages.getString(
              PKG, "DynamicSQLRowMeta.Exception.ConnectionNotFound", realConnection),
          e);
    }
    if (databaseMeta == null) {
      throw new HopTransformException(
          BaseMessages.getString(
              PKG, "DynamicSQLRowMeta.Exception.ConnectionNotFound", realConnection));
    }
    return databaseMeta;
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
