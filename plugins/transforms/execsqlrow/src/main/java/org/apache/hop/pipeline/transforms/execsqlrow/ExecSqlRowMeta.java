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

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

@Transform(
    id = "ExecSqlRow",
    image = "execsqlrow.svg",
    name = "i18n::BaseTransform.TypeLongDesc.ExecSqlRow",
    description = "i18n::BaseTransform.TypeTooltipDesc.ExecSqlRow",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Scripting",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/execsqlrow.html")
@InjectionSupported(localizationPrefix = "ExecSqlRowMeta.Injection.", groups = "OUTPUT_FIELDS")
public class ExecSqlRowMeta extends BaseTransformMeta
    implements ITransformMeta<ExecSqlRow, ExecSqlRowData> {
  private static final Class<?> PKG = ExecSqlRowMeta.class; // For Translator

  private IHopMetadataProvider metadataProvider;

  private DatabaseMeta databaseMeta;

  @Injection(name = "SQL_FIELD_NAME")
  private String sqlField;

  @Injection(name = "UPDATE_STATS", group = "OUTPUT_FIELDS")
  private String updateField;

  @Injection(name = "INSERT_STATS", group = "OUTPUT_FIELDS")
  private String insertField;

  @Injection(name = "DELETE_STATS", group = "OUTPUT_FIELDS")
  private String deleteField;

  @Injection(name = "READ_STATS", group = "OUTPUT_FIELDS")
  private String readField;

  /** Commit size for inserts/updates */
  @Injection(name = "COMMIT_SIZE")
  private int commitSize;

  @Injection(name = "READ_SQL_FROM_FILE")
  private boolean sqlFromfile;

  /** Send SQL as single statement */
  @Injection(name = "SEND_SINGLE_STATEMENT")
  private boolean sendOneStatement;

  public ExecSqlRowMeta() {
    super();
  }

  @Injection(name = "CONNECTION_NAME")
  public void setConnection(String connectionName) {
    try {
      databaseMeta = DatabaseMeta.loadDatabase(metadataProvider, connectionName);
    } catch (Exception e) {
      throw new RuntimeException("Unable to load connection '" + connectionName + "'", e);
    }
  }

  /** @return Returns the sqlFromfile. */
  public boolean IsSendOneStatement() {
    return sendOneStatement;
  }

  /** @param sendOneStatement The sendOneStatement to set. */
  public void SetSendOneStatement(boolean sendOneStatement) {
    this.sendOneStatement = sendOneStatement;
  }

  /** @return Returns the sqlFromfile. */
  public boolean isSqlFromfile() {
    return sqlFromfile;
  }

  /** @param sqlFromfile The sqlFromfile to set. */
  public void setSqlFromfile(boolean sqlFromfile) {
    this.sqlFromfile = sqlFromfile;
  }

  /** @return Returns the database. */
  public DatabaseMeta getDatabaseMeta() {
    return databaseMeta;
  }

  /** @param database The database to set. */
  public void setDatabaseMeta(DatabaseMeta database) {
    this.databaseMeta = database;
  }

  /** @return Returns the sqlField. */
  public String getSqlFieldName() {
    return sqlField;
  }

  /** @param sqlField The sqlField to sqlField. */
  public void setSqlFieldName(String sqlField) {
    this.sqlField = sqlField;
  }

  /** @return Returns the commitSize. */
  public int getCommitSize() {
    return commitSize;
  }

  /** @param commitSize The commitSize to set. */
  public void setCommitSize(int commitSize) {
    this.commitSize = commitSize;
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

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode, metadataProvider);
  }

  public Object clone() {
    ExecSqlRowMeta retval = (ExecSqlRowMeta) super.clone();
    return retval;
  }

  private void readData(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    this.metadataProvider = metadataProvider;
    try {
      String csize;
      String con = XmlHandler.getTagValue(transformNode, "connection");
      databaseMeta = DatabaseMeta.loadDatabase(metadataProvider, con);
      csize = XmlHandler.getTagValue(transformNode, "commit");
      commitSize = Const.toInt(csize, 0);
      sqlField = XmlHandler.getTagValue(transformNode, "sql_field");

      insertField = XmlHandler.getTagValue(transformNode, "insert_field");
      updateField = XmlHandler.getTagValue(transformNode, "update_field");
      deleteField = XmlHandler.getTagValue(transformNode, "delete_field");
      readField = XmlHandler.getTagValue(transformNode, "read_field");
      sqlFromfile = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "sqlFromfile"));

      sendOneStatement =
          "Y"
              .equalsIgnoreCase(
                  Const.NVL(XmlHandler.getTagValue(transformNode, "sendOneStatement"), "Y"));
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "ExecSqlRowMeta.Exception.UnableToLoadTransformMetaFromXML"),
          e);
    }
  }

  public void setDefault() {
    sqlFromfile = false;
    commitSize = 1;
    databaseMeta = null;
    sqlField = null;
    sendOneStatement = true;
  }

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

  public String getXml() {
    StringBuilder retval = new StringBuilder(300);
    retval.append("    ").append(XmlHandler.addTagValue("commit", commitSize));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "connection", databaseMeta == null ? "" : databaseMeta.getName()));
    retval.append("    ").append(XmlHandler.addTagValue("sql_field", sqlField));

    retval.append("    ").append(XmlHandler.addTagValue("insert_field", insertField));
    retval.append("    ").append(XmlHandler.addTagValue("update_field", updateField));
    retval.append("    ").append(XmlHandler.addTagValue("delete_field", deleteField));
    retval.append("    ").append(XmlHandler.addTagValue("read_field", readField));
    retval.append("    ").append(XmlHandler.addTagValue("sqlFromfile", sqlFromfile));
    retval.append("    ").append(XmlHandler.addTagValue("sendOneStatement", sendOneStatement));
    return retval.toString();
  }

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

    if (databaseMeta != null) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "ExecSqlRowMeta.CheckResult.ConnectionExists"),
              transformMeta);
      remarks.add(cr);

      Database db = new Database(loggingObject, variables, databaseMeta );
      databases = new Database[] {db}; // keep track of it for cancelling purposes...

      try {
        db.connect();
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "ExecSqlRowMeta.CheckResult.DBConnectionOK"),
                transformMeta);
        remarks.add(cr);

        if (sqlField != null && sqlField.length() != 0) {
          cr =
              new CheckResult(
                  CheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(PKG, "ExecSqlRowMeta.CheckResult.SQLFieldNameEntered"),
                  transformMeta);
        } else {
          cr =
              new CheckResult(
                  CheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(PKG, "ExecSqlRowMeta.CheckResult.SQLFieldNameMissing"),
                  transformMeta);
        }
        remarks.add(cr);
      } catch (HopException e) {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "ExecSqlRowMeta.CheckResult.ErrorOccurred")
                    + e.getMessage(),
                transformMeta);
        remarks.add(cr);
      } finally {
        db.disconnect();
      }
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ExecSqlRowMeta.CheckResult.ConnectionNeeded"),
              transformMeta);
      remarks.add(cr);
    }

    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "ExecSqlRowMeta.CheckResult.TransformReceivingInfoOK"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ExecSqlRowMeta.CheckResult.NoInputReceivedError"),
              transformMeta);
      remarks.add(cr);
    }
  }

  public ExecSqlRow createTransform(
      TransformMeta transformMeta,
      ExecSqlRowData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new ExecSqlRow(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  public ExecSqlRowData getTransformData() {
    return new ExecSqlRowData();
  }

  public DatabaseMeta[] getUsedDatabaseConnections() {
    if (databaseMeta != null) {
      return new DatabaseMeta[] {databaseMeta};
    } else {
      return super.getUsedDatabaseConnections();
    }
  }

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

  /** @param metadataProvider The metadataProvider to set */
  public void setMetadataProvider(IHopMetadataProvider metadataProvider) {
    this.metadataProvider = metadataProvider;
  }
}
