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

package org.apache.hop.pipeline.transforms.execsqlrow;

import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "ExecSqlRow",
    image = "execsqlrow.svg",
    name = "i18n::ExecSqlRow.Name",
    description = "i18n::ExecSqlRow.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Scripting",
    keywords = "i18n::ExecSqlRowMeta.keyword",
    documentationUrl = "/pipeline/transforms/execsqlrow.html",
    actionTransformTypes = {ActionTransformType.RDBMS})
public class ExecSqlRowMeta extends BaseTransformMeta<ExecSqlRow, ExecSqlRowData> {
  private static final Class<?> PKG = ExecSqlRowMeta.class;

  private IHopMetadataProvider metadataProvider;

  @HopMetadataProperty(
      key = "sql_field",
      injectionKey = "SQL_FIELD_NAME",
      injectionKeyDescription = "ExecSqlRowMeta.Injection.SQL_FIELD_NAME",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_SQL)
  private String sqlField;

  @HopMetadataProperty(
      key = "update_field",
      injectionKey = "UPDATE_STATS",
      injectionKeyDescription = "ExecSqlRowMeta.Injection.UPDATE_STATS")
  private String updateField;

  @HopMetadataProperty(
      key = "insert_field",
      injectionKey = "INSERT_STATS",
      injectionKeyDescription = "ExecSqlRowMeta.Injection.INSERT_STATS")
  private String insertField;

  @HopMetadataProperty(
      key = "delete_field",
      injectionKey = "DELETE_STATS",
      injectionKeyDescription = "ExecSqlRowMeta.Injection.DELETE_STATS")
  private String deleteField;

  @HopMetadataProperty(
      key = "read_field",
      injectionKey = "READ_STATS",
      injectionKeyDescription = "ExecSqlRowMeta.Injection.READ_STATS")
  private String readField;

  /** Commit size for inserts/updates */
  @HopMetadataProperty(
      key = "commit",
      injectionKey = "COMMIT_SIZE",
      injectionKeyDescription = "ExecSqlRowMeta.Injection.COMMIT_SIZE")
  private int commitSize;

  @HopMetadataProperty(
      injectionKey = "READ_SQL_FROM_FILE",
      injectionKeyDescription = "ExecSqlRowMeta.Injection.READ_SQL_FROM_FILE",
      defaultBoolean = false)
  private boolean sqlFromfile;

  /** Send SQL as single statement */
  @HopMetadataProperty(
      injectionKey = "SEND_SINGLE_STATEMENT",
      injectionKeyDescription = "ExecSqlRowMeta.Injection.SEND_SINGLE_STATEMENT",
      defaultBoolean = false)
  private boolean sendOneStatement;

  @HopMetadataProperty(
      injectionKey = "CONNECTION_NAME",
      injectionKeyDescription = "ExecSqlRowMeta.Injection.CONNECTION_NAME",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String connection;

  public ExecSqlRowMeta() {
    super();
  }

  public String getSqlField() {
    return sqlField;
  }

  public void setSqlField(String sqlField) {
    this.sqlField = sqlField;
  }

  public String getConnection() {
    return connection;
  }

  public void setConnection(String connectionName) {
    this.connection = connectionName;
  }

  /**
   * @return Returns the sqlFromfile.
   */
  public boolean isSendOneStatement() {
    return sendOneStatement;
  }

  /**
   * @param sendOneStatement The sendOneStatement to set.
   */
  public void setSendOneStatement(boolean sendOneStatement) {
    this.sendOneStatement = sendOneStatement;
  }

  /**
   * @return Returns the sqlFromfile.
   */
  public boolean isSqlFromfile() {
    return sqlFromfile;
  }

  /**
   * @param sqlFromfile The sqlFromfile to set.
   */
  public void setSqlFromfile(boolean sqlFromfile) {
    this.sqlFromfile = sqlFromfile;
  }

  /**
   * @return Returns the sqlField.
   */
  public String getSqlFieldName() {
    return sqlField;
  }

  /**
   * @param sqlField The sqlField to sqlField.
   */
  public void setSqlFieldName(String sqlField) {
    this.sqlField = sqlField;
  }

  /**
   * @return Returns the commitSize.
   */
  public int getCommitSize() {
    return commitSize;
  }

  /**
   * @param commitSize The commitSize to set.
   */
  public void setCommitSize(int commitSize) {
    this.commitSize = commitSize;
  }

  /**
   * @return Returns the deleteField.
   */
  public String getDeleteField() {
    return deleteField;
  }

  /**
   * @param deleteField The deleteField to set.
   */
  public void setDeleteField(String deleteField) {
    this.deleteField = deleteField;
  }

  /**
   * @return Returns the insertField.
   */
  public String getInsertField() {
    return insertField;
  }

  /**
   * @param insertField The insertField to set.
   */
  public void setInsertField(String insertField) {
    this.insertField = insertField;
  }

  /**
   * @return Returns the readField.
   */
  public String getReadField() {
    return readField;
  }

  /**
   * @param readField The readField to set.
   */
  public void setReadField(String readField) {
    this.readField = readField;
  }

  /**
   * @return Returns the updateField.
   */
  public String getUpdateField() {
    return updateField;
  }

  /**
   * @param updateField The updateField to set.
   */
  public void setUpdateField(String updateField) {
    this.updateField = updateField;
  }

  @Override
  public Object clone() {
    return super.clone();
  }

  @Override
  public void setDefault() {
    sqlFromfile = false;
    commitSize = 1;
    connection = null;
    sqlField = null;
    sendOneStatement = true;
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
        ExecSqlRow.getResultRow(
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
    Database db = null;

    try {

      DatabaseMeta databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connection));

      if (databaseMeta != null) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "ExecSqlRowMeta.CheckResult.ConnectionExists"),
                transformMeta);
        remarks.add(cr);

        db = new Database(loggingObject, variables, databaseMeta);
        databases = new Database[] {db}; // keep track of it for cancelling purposes...

        db.connect();
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "ExecSqlRowMeta.CheckResult.DBConnectionOK"),
                transformMeta);
        remarks.add(cr);

        if (sqlField != null && sqlField.length() != 0) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(PKG, "ExecSqlRowMeta.CheckResult.SQLFieldNameEntered"),
                  transformMeta);
        } else {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(PKG, "ExecSqlRowMeta.CheckResult.SQLFieldNameMissing"),
                  transformMeta);
        }
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "ExecSqlRowMeta.CheckResult.ConnectionNeeded"),
                transformMeta);
        remarks.add(cr);
      }
    } catch (HopException e) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ExecSqlRowMeta.CheckResult.ErrorOccurred")
                  + e.getMessage(),
              transformMeta);
      remarks.add(cr);
    } finally {
      db.disconnect();
    }

    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "ExecSqlRowMeta.CheckResult.TransformReceivingInfoOK"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ExecSqlRowMeta.CheckResult.NoInputReceivedError"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  /**
   * Gets metadataProvider
   *
   * @return value of metadataProvider
   */
  public IHopMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  /**
   * @param metadataProvider The metadataProvider to set
   */
  public void setMetadataProvider(IHopMetadataProvider metadataProvider) {
    this.metadataProvider = metadataProvider;
  }
}
