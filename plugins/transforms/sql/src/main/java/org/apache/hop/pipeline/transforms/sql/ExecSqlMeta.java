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

package org.apache.hop.pipeline.transforms.sql;

import org.apache.hop.core.*;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;
import java.util.List;

/*
 * Contains meta-data to execute arbitrary SQL, optionally each row again.
 */

@Transform(
    id = "ExecSql",
    image = "sql.svg",
    name = "i18n::ExecSql.Name",
    description = "i18n::ExecSql.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Scripting",
    keywords = "i18n::ExecSqlMeta.keyword",
    documentationUrl = "/pipeline/transforms/execsql.html")
public class ExecSqlMeta extends BaseTransformMeta implements ITransformMeta<ExecSql, ExecSqlData> {
  private static final Class<?> PKG = ExecSqlMeta.class; // For Translator

  @HopMetadataProperty(
      key = "connection",
      injectionKeyDescription = "ExecSqlMeta.Injection.CONNECTIONNAME")
  private String connection;

  @HopMetadataProperty(injectionKeyDescription = "ExecSqlMeta.Injection.SQL", injectionKey = "SQL")
  private String sql;

  @HopMetadataProperty(
      key = "execute_each_row",
      injectionKeyDescription = "ExecSqlMeta.Injection.EXECUTE_FOR_EACH_ROW",
      injectionKey = "EXECUTE_FOR_EACH_ROW")
  private boolean executedEachInputRow;

  @HopMetadataProperty(
      key = "update_field",
      injectionKeyDescription = "ExecSqlMeta.Injection.UPDATE_STATS_FIELD",
      injectionKey = "UPDATE_STATS_FIELD")
  private String updateField;

  @HopMetadataProperty(
      key = "insert_field",
      injectionKeyDescription = "ExecSqlMeta.Injection.INSERT_STATS_FIELD",
      injectionKey = "INSERT_STATS_FIELD")
  private String insertField;

  @HopMetadataProperty(
      key = "delete_field",
      injectionKeyDescription = "ExecSqlMeta.Injection.DELETE_STATS_FIELD",
      injectionKey = "DELETE_STATS_FIELD")
  private String deleteField;

  @HopMetadataProperty(
      key = "read_field",
      injectionKeyDescription = "ExecSqlMeta.Injection.READ_STATS_FIELD",
      injectionKey = "READ_STATS_FIELD")
  private String readField;

  @HopMetadataProperty(
      key = "single_statement",
      injectionKeyDescription = "ExecSqlMeta.Injection.EXECUTE_AS_SINGLE_STATEMENT",
      injectionKey = "EXECUTE_AS_SINGLE_STATEMENT")
  private boolean singleStatement;

  @HopMetadataProperty(
      key = "replace_variables",
      injectionKeyDescription = "ExecSqlMeta.Injection.REPLACE_VARIABLES",
      injectionKey = "REPLACE_VARIABLES")
  private boolean replaceVariables;

  @HopMetadataProperty(
      injectionKeyDescription = "ExecSqlMeta.Injection.QUOTE_STRINGS",
      injectionKey = "QUOTE_STRINGS")
  private boolean quoteString;

  @HopMetadataProperty(
      key = "set_params",
      injectionKeyDescription = "ExecSqlMeta.Injection.BIND_PARAMETERS",
      injectionKey = "BIND_PARAMETERS")
  private boolean params;

  @HopMetadataProperty(
      key = "argument",
      groupKey = "arguments",
      injectionGroupKey = "PARAMETERS",
      injectionGroupDescription = "ExecSqlMeta.Injection.PARAMETERS",
      injectionKeyDescription = "ExecSqlMeta.Injection.PARAMETER_NAME",
      injectionKey = "PARAMETER_NAME")
  private List<String> arguments;

  public ExecSqlMeta() {
    super();
    arguments = new ArrayList<>();
  }

  /** @return Returns the true if we have to set params. */
  public boolean isParams() {
    return this.params;
  }

  /** @param value set true if we have to set params. */
  public void setParams(boolean value) {
    this.params = value;
  }

  /** @return Returns the sql. */
  public String getSql() {
    return sql;
  }

  /** @param sql The sql to set. */
  public void setSql(String sql) {
    this.sql = sql;
  }

  /** @return Returns the arguments. */
  public List<String> getArguments() {
    return arguments;
  }

  /** @param arguments The arguments to set. */
  public void setArguments(List<String> arguments) {
    this.arguments = arguments;
  }

  /** @return Returns the executedEachInputRow. */
  public boolean isExecutedEachInputRow() {
    return executedEachInputRow;
  }

  /** @param executedEachInputRow The executedEachInputRow to set. */
  public void setExecutedEachInputRow(boolean executedEachInputRow) {
    this.executedEachInputRow = executedEachInputRow;
  }

  /** @return Returns the deleteField. */
  public String getDeleteField() {
    return deleteField;
  }

  /** @param deleteField The deleteField to set. */
  public void setDeleteField(String deleteField) {
    this.deleteField = deleteField;
  }

  /** @return Returns the insertField. */
  public String getInsertField() {
    return insertField;
  }

  /** @param insertField The insertField to set. */
  public void setInsertField(String insertField) {
    this.insertField = insertField;
  }

  /** @return Returns the readField. */
  public String getReadField() {
    return readField;
  }

  /** @param readField The readField to set. */
  public void setReadField(String readField) {
    this.readField = readField;
  }

  /** @return Returns the updateField. */
  public String getUpdateField() {
    return updateField;
  }

  /** @param updateField The updateField to set. */
  public void setUpdateField(String updateField) {
    this.updateField = updateField;
  }

  @Override
  public Object clone() {
    ExecSqlMeta retval = (ExecSqlMeta) super.clone();
    return retval;
  }

  @Override
  public void setDefault() {
    sql = "";
    arguments = new ArrayList();
  }

  @Override
  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    RowMetaAndData add =
        ExecSql.getResultRow(
            new Result(), getUpdateField(), getInsertField(), getDeleteField(), getReadField());

    r.mergeRowMeta(add.getRowMeta());
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

    DatabaseMeta databaseMeta = null;

    try {
      databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connection));
    } catch (HopException e) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "ExecSqlMeta.CheckResult.DatabaseMetaError", variables.resolve(connection)),
              transformMeta);
      remarks.add(cr);
    }

    if (databaseMeta != null) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "ExecSqlMeta.CheckResult.ConnectionExists"),
              transformMeta);
      remarks.add(cr);

      Database db = new Database(loggingObject, variables, databaseMeta);
      databases = new Database[] {db}; // keep track of it for
      // cancelling purposes...

      try {
        db.connect();
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "ExecSqlMeta.CheckResult.DBConnectionOK"),
                transformMeta);
        remarks.add(cr);

        if (sql != null && sql.length() != 0) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(PKG, "ExecSqlMeta.CheckResult.SQLStatementEntered"),
                  transformMeta);
          remarks.add(cr);
        } else {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(PKG, "ExecSqlMeta.CheckResult.SQLStatementMissing"),
                  transformMeta);
          remarks.add(cr);
        }
      } catch (HopException e) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "ExecSqlMeta.CheckResult.ErrorOccurred")
                    + e.getMessage(),
                transformMeta);
        remarks.add(cr);
      } finally {
        db.disconnect();
      }
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ExecSqlMeta.CheckResult.ConnectionNeeded"),
              transformMeta);
      remarks.add(cr);
    }

    // If it's executed each row, make sure we have input
    if (executedEachInputRow) {
      if (input.length > 0) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "ExecSqlMeta.CheckResult.TransformReceivingInfoOK"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "ExecSqlMeta.CheckResult.NoInputReceivedError"),
                transformMeta);
        remarks.add(cr);
      }
    } else {
      if (input.length > 0) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "ExecSqlMeta.CheckResult.SQLOnlyExecutedOnce"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG, "ExecSqlMeta.CheckResult.InputReceivedOKForSQLOnlyExecuteOnce"),
                transformMeta);
        remarks.add(cr);
      }
    }
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      ExecSqlData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new ExecSql(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  @Override
  public ExecSqlData getTransformData() {
    return new ExecSqlData();
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
    try {
      DatabaseMeta databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connection));
      DatabaseImpact ii =
          new DatabaseImpact(
              DatabaseImpact.TYPE_IMPACT_READ_WRITE,
              pipelineMeta.getName(),
              transformMeta.getName(),
              databaseMeta.getDatabaseName(),
              BaseMessages.getString(PKG, "ExecSqlMeta.DatabaseMeta.Unknown.Label"),
              BaseMessages.getString(PKG, "ExecSqlMeta.DatabaseMeta.Unknown2.Label"),
              BaseMessages.getString(PKG, "ExecSqlMeta.DatabaseMeta.Unknown3.Label"),
              transformMeta.getName(),
              sql,
              BaseMessages.getString(PKG, "ExecSqlMeta.DatabaseMeta.Title"));
      impact.add(ii);

    } catch (HopException e) {
      throw new HopTransformException(
          "Unable to get databaseMeta for connection: " + Const.CR + variables.resolve(connection),
          e);
    }
  }

  /** @return Returns the variableReplacementActive. */
  public boolean isReplaceVariables() {
    return replaceVariables;
  }

  /** @param replaceVariables The variableReplacement to set. */
  public void setReplaceVariables(boolean replaceVariables) {
    this.replaceVariables = replaceVariables;
  }

  public boolean isQuoteString() {
    return quoteString;
  }

  public void setQuoteString(boolean quoteString) {
    this.quoteString = quoteString;
  }

  public String getConnection() {
    return connection;
  }

  public void setConnection(String connection) {
    this.connection = connection;
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  /** @return the singleStatement */
  public boolean isSingleStatement() {
    return singleStatement;
  }

  /** @param singleStatement the singleStatement to set */
  public void setSingleStatement(boolean singleStatement) {
    this.singleStatement = singleStatement;
  }
}
