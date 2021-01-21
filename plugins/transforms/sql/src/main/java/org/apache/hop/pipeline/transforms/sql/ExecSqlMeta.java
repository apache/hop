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
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

/*******************************************************************************
 * Contains meta-data to execute arbitrary SQL, optionally each row again.
 *
 * Created on 10-sep-2005
 */

@Transform(
    id = "ExecSql",
    image = "sql.svg",
    name = "i18n::ExecSql.Name",
    description = "i18n::ExecSql.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Scripting",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/sql.html")
@InjectionSupported(
    localizationPrefix = "ExecSqlMeta.Injection.",
    groups = {"PARAMETERS"})
public class ExecSqlMeta extends BaseTransformMeta implements ITransformMeta<ExecSql, ExecSqlData> {
  private static final Class<?> PKG = ExecSqlMeta.class; // For Translator

  private DatabaseMeta databaseMeta;

  @Injection(name = "SQL")
  private String sql;

  @Injection(name = "EXECUTE_FOR_EACH_ROW")
  private boolean executedEachInputRow;

  @Injection(name = "PARAMETER_NAME", group = "PARAMETERS")
  private String[] arguments;

  @Injection(name = "UPDATE_STATS_FIELD")
  private String updateField;

  @Injection(name = "INSERT_STATS_FIELD")
  private String insertField;

  @Injection(name = "DELETE_STATS_FIELD")
  private String deleteField;

  @Injection(name = "READ_STATS_FIELD")
  private String readField;

  @Injection(name = "EXECUTE_AS_SINGLE_STATEMENT")
  private boolean singleStatement;

  @Injection(name = "REPLACE_VARIABLES")
  private boolean replaceVariables;

  @Injection(name = "QUOTE_STRINGS")
  private boolean quoteString;

  @Injection(name = "BIND_PARAMETERS")
  private boolean setParams;

  @Injection(name = "CONNECTIONNAME")
  public void setConnection(String connectionName) {
    databaseMeta =
        DatabaseMeta.findDatabase(
            getParentTransformMeta().getParentPipelineMeta().getDatabases(), connectionName);
  }

  public ExecSqlMeta() {
    super();
  }

  /** @return Returns the true if we have to set params. */
  public boolean isParams() {
    return this.setParams;
  }

  /** @param value set true if we have to set params. */
  public void setParams(boolean value) {
    this.setParams = value;
  }

  /** @return Returns the database. */
  public DatabaseMeta getDatabaseMeta() {
    return databaseMeta;
  }

  /** @param database The database to set. */
  public void setDatabaseMeta(DatabaseMeta database) {
    this.databaseMeta = database;
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
  public String[] getArguments() {
    return arguments;
  }

  /** @param arguments The arguments to set. */
  public void setArguments(String[] arguments) {
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

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode, metadataProvider);
  }

  public Object clone() {
    ExecSqlMeta retval = (ExecSqlMeta) super.clone();
    int nrArgs = arguments.length;
    retval.allocate(nrArgs);
    System.arraycopy(arguments, 0, retval.arguments, 0, nrArgs);
    return retval;
  }

  public void allocate(int nrargs) {
    arguments = new String[nrargs];
  }

  private void readData(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      String con = XmlHandler.getTagValue(transformNode, "connection");
      databaseMeta = DatabaseMeta.loadDatabase(metadataProvider, con);
      String eachRow = XmlHandler.getTagValue(transformNode, "execute_each_row");
      executedEachInputRow = "Y".equalsIgnoreCase(eachRow);
      singleStatement =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "single_statement"));
      replaceVariables = "Y".equals(XmlHandler.getTagValue(transformNode, "replace_variables"));
      quoteString = "Y".equals(XmlHandler.getTagValue(transformNode, "quoteString"));
      setParams = "Y".equals(XmlHandler.getTagValue(transformNode, "set_params"));
      sql = XmlHandler.getTagValue(transformNode, "sql");

      insertField = XmlHandler.getTagValue(transformNode, "insert_field");
      updateField = XmlHandler.getTagValue(transformNode, "update_field");
      deleteField = XmlHandler.getTagValue(transformNode, "delete_field");
      readField = XmlHandler.getTagValue(transformNode, "read_field");

      Node argsnode = XmlHandler.getSubNode(transformNode, "arguments");
      int nrArguments = XmlHandler.countNodes(argsnode, "argument");
      allocate(nrArguments);
      for (int i = 0; i < nrArguments; i++) {
        Node argnode = XmlHandler.getSubNodeByNr(argsnode, "argument", i);
        arguments[i] = XmlHandler.getTagValue(argnode, "name");
      }
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "ExecSqlMeta.Exception.UnableToLoadTransformMetaFromXML"), e);
    }
  }

  public void setDefault() {
    databaseMeta = null;
    sql = "";
    arguments = new String[0];
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
        ExecSql.getResultRow(
            new Result(), getUpdateField(), getInsertField(), getDeleteField(), getReadField());

    r.mergeRowMeta(add.getRowMeta());
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder(300);

    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "connection", databaseMeta == null ? "" : databaseMeta.getName()));
    retval.append("    ").append(XmlHandler.addTagValue("execute_each_row", executedEachInputRow));
    retval.append("    ").append(XmlHandler.addTagValue("single_statement", singleStatement));
    retval.append("    ").append(XmlHandler.addTagValue("replace_variables", replaceVariables));
    retval.append("    ").append(XmlHandler.addTagValue("quoteString", quoteString));
    retval.append("    ").append(XmlHandler.addTagValue("sql", sql));
    retval.append("    ").append(XmlHandler.addTagValue("set_params", setParams));
    retval.append("    ").append(XmlHandler.addTagValue("insert_field", insertField));
    retval.append("    ").append(XmlHandler.addTagValue("update_field", updateField));
    retval.append("    ").append(XmlHandler.addTagValue("delete_field", deleteField));
    retval.append("    ").append(XmlHandler.addTagValue("read_field", readField));

    retval.append("    <arguments>").append(Const.CR);
    for (int i = 0; i < arguments.length; i++) {
      retval
          .append("       <argument>")
          .append(XmlHandler.addTagValue("name", arguments[i], false))
          .append("</argument>")
          .append(Const.CR);
    }
    retval.append("    </arguments>").append(Const.CR);

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
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "ExecSqlMeta.CheckResult.ConnectionExists"),
              transformMeta);
      remarks.add(cr);

      Database db = new Database(loggingObject, variables, databaseMeta );
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

  public ITransform createTransform(
      TransformMeta transformMeta,
      ExecSqlData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new ExecSql(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  public ExecSqlData getTransformData() {
    return new ExecSqlData();
  }

  public void analyseImpact(
      IVariables variables,
      List<DatabaseImpact> impact,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      RowMeta prev,
      String[] input,
      String[] output,
      RowMeta info)
      throws HopTransformException {
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
  }

  public DatabaseMeta[] getUsedDatabaseConnections() {
    if (databaseMeta != null) {
      return new DatabaseMeta[] {databaseMeta};
    } else {
      return super.getUsedDatabaseConnections();
    }
  }

  /** @return Returns the variableReplacementActive. */
  public boolean isReplaceVariables() {
    return replaceVariables;
  }

  /** @param variableReplacementActive The variableReplacementActive to set. */
  public void setVariableReplacementActive(boolean variableReplacementActive) {
    this.replaceVariables = variableReplacementActive;
  }

  public boolean isQuoteString() {
    return quoteString;
  }

  public void setQuoteString(boolean quoteString) {
    this.quoteString = quoteString;
  }

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
