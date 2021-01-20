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

package org.apache.hop.workflow.actions.truncatetables;

import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.AbstractFileValidator;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.action.validator.ValidatorContext;
import org.w3c.dom.Node;

import java.util.List;

/**
 * This defines a Truncate Tables action.
 *
 * @author Samatar
 * @since 22-07-2008
 */
@Action(
    id = "TRUNCATE_TABLES",
    name = "i18n::ActionTruncateTables.Name",
    description = "i18n::ActionTruncateTables.Description",
    image = "TruncateTables.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Utility",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/truncatetables.html")
public class ActionTruncateTables extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionTruncateTables.class; // For Translator

  private boolean argFromPrevious;

  private DatabaseMeta connection;

  private String[] tableNames;

  private String[] schemaNames;

  private int nrErrors = 0;
  private int nrSuccess = 0;
  private boolean continueProcess = true;

  public ActionTruncateTables(String name) {
    super(name, "");
    this.argFromPrevious = false;
    this.tableNames = null;
    this.schemaNames = null;
    this.connection = null;
  }

  public ActionTruncateTables() {
    this("");
  }

  public void allocate(int nrFields) {
    this.tableNames = new String[nrFields];
    this.schemaNames = new String[nrFields];
  }

  public Object clone() {
    ActionTruncateTables je = (ActionTruncateTables) super.clone();
    if (tableNames != null) {
      int nrFields = tableNames.length;
      je.allocate(nrFields);
      System.arraycopy(tableNames, 0, je.tableNames, 0, nrFields);
      System.arraycopy(schemaNames, 0, je.schemaNames, 0, nrFields);
    }
    return je;
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(200);

    retval.append(super.getXml());
    retval
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                "connection", this.connection == null ? null : this.connection.getName()));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("arg_from_previous", this.argFromPrevious));
    retval.append("      <fields>").append(Const.CR);
    if (tableNames != null) {
      for (int i = 0; i < this.tableNames.length; i++) {
        retval.append("        <field>").append(Const.CR);
        retval.append("          ").append(XmlHandler.addTagValue("name", this.tableNames[i]));
        retval
            .append("          ")
            .append(XmlHandler.addTagValue("schemaname", this.schemaNames[i]));
        retval.append("        </field>").append(Const.CR);
      }
    }
    retval.append("      </fields>").append(Const.CR);
    return retval.toString();
  }

  @Override
  public void loadXml(Node entrynode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    try {
      super.loadXml(entrynode);

      String dbname = XmlHandler.getTagValue(entrynode, "connection");
      this.connection = DatabaseMeta.loadDatabase(metadataProvider, dbname);
      this.argFromPrevious =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "arg_from_previous"));

      Node fields = XmlHandler.getSubNode(entrynode, "fields");

      // How many field arguments?
      int nrFields = XmlHandler.countNodes(fields, "field");
      allocate(nrFields);

      // Read them all...
      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);
        this.tableNames[i] = XmlHandler.getTagValue(fnode, "name");
        this.schemaNames[i] = XmlHandler.getTagValue(fnode, "schemaname");
      }
    } catch (HopException e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "ActionTruncateTables.UnableLoadXML"), e);
    }
  }

  public void setDatabase(DatabaseMeta database) {
    this.connection = database;
  }

  public DatabaseMeta getDatabase() {
    return this.connection;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return true;
  }

  private boolean truncateTables(String tableName, String schemaname, Database db) {
    boolean retval = false;
    try {
      // check if table exists!
      if (db.checkTableExists(schemaname, tableName)) {
        if (!Utils.isEmpty(schemaname)) {
          db.truncateTable(schemaname, tableName);
        } else {
          db.truncateTable(tableName);
        }

        if (log.isDetailed()) {
          logDetailed(
              BaseMessages.getString(PKG, "ActionTruncateTables.Log.TableTruncated", tableName));
        }

        retval = true;
      } else {
        logError(
            BaseMessages.getString(PKG, "ActionTruncateTables.Error.CanNotFindTable", tableName));
      }
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG, "ActionTruncateTables.Error.CanNotTruncateTables", tableName, e.toString()));
    }
    return retval;
  }

  @Override
  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;
    List<RowMetaAndData> rows = result.getRows();
    RowMetaAndData resultRow = null;

    result.setResult(true);
    nrErrors = 0;
    continueProcess = true;
    nrSuccess = 0;

    if (argFromPrevious) {
      if (log.isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG,
                "ActionTruncateTables.FoundPreviousRows",
                String.valueOf((rows != null ? rows.size() : 0))));
      }
      if (rows.isEmpty()) {
        return result;
      }
    }
    if (connection != null) {
      Database db = new Database(this, this, connection );
      try {
        db.connect();
        if (argFromPrevious && rows != null) { // Copy the input row to the (command line) arguments

          for (int iteration = 0;
              iteration < rows.size() && !parentWorkflow.isStopped() && continueProcess;
              iteration++) {
            resultRow = rows.get(iteration);

            // Get values from previous result
            String tableNamePrevious = resultRow.getString(0, null);
            String schemaNamePrevious = resultRow.getString(1, null);

            if (!Utils.isEmpty(tableNamePrevious)) {
              if (log.isDetailed()) {
                logDetailed(
                    BaseMessages.getString(
                        PKG,
                        "ActionTruncateTables.ProcessingRow",
                        tableNamePrevious,
                        schemaNamePrevious));
              }

              // let's truncate table
              if (truncateTables(tableNamePrevious, schemaNamePrevious, db)) {
                updateSuccess();
              } else {
                updateErrors();
              }
            } else {
              logError(BaseMessages.getString(PKG, "ActionTruncateTables.RowEmpty"));
            }
          }

        } else if (tableNames != null) {
          for (int i = 0;
              i < tableNames.length && !parentWorkflow.isStopped() && continueProcess;
              i++) {
            String realTablename = resolve(tableNames[i]);
            String realSchemaname = resolve(schemaNames[i]);
            if (!Utils.isEmpty(realTablename)) {
              if (log.isDetailed()) {
                logDetailed(
                    BaseMessages.getString(
                        PKG, "ActionTruncateTables.ProcessingArg", tableNames[i], schemaNames[i]));
              }

              // let's truncate table
              if (truncateTables(realTablename, realSchemaname, db)) {
                updateSuccess();
              } else {
                updateErrors();
              }
            } else {
              logError(
                  BaseMessages.getString(
                      PKG, "ActionTruncateTables.ArgEmpty", tableNames[i], schemaNames[i]));
            }
          }
        }
      } catch (Exception dbe) {
        result.setNrErrors(1);
        logError(
            BaseMessages.getString(
                PKG, "ActionTruncateTables.Error.RunningEntry", dbe.getMessage()));
      } finally {
        if (db != null) {
          db.disconnect();
        }
      }
    } else {
      result.setNrErrors(1);
      logError(BaseMessages.getString(PKG, "ActionTruncateTables.NoDbConnection"));
    }

    result.setNrErrors(nrErrors);
    result.setNrLinesDeleted(nrSuccess);
    result.setResult(nrErrors == 0);
    return result;
  }

  private void updateErrors() {
    nrErrors++;
    continueProcess = false;
  }

  private void updateSuccess() {
    nrSuccess++;
  }

  @Override
  public DatabaseMeta[] getUsedDatabaseConnections() {
    return new DatabaseMeta[] {
      connection,
    };
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    boolean res =
        ActionValidatorUtils.andValidator()
            .validate(
                this,
                "arguments",
                remarks,
                AndValidator.putValidators(ActionValidatorUtils.notNullValidator()));

    if (!res) {
      return;
    }

    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace(ctx, getVariables());
    AndValidator.putValidators(
        ctx, ActionValidatorUtils.notNullValidator(), ActionValidatorUtils.fileExistsValidator());

    for (int i = 0; i < tableNames.length; i++) {
      ActionValidatorUtils.andValidator().validate(this, "arguments[" + i + "]", remarks, ctx);
    }
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    if (tableNames != null) {
      ResourceReference reference = null;
      for (int i = 0; i < tableNames.length; i++) {
        String filename = resolve(tableNames[i]);
        if (reference == null) {
          reference = new ResourceReference(this);
          references.add(reference);
        }
        reference.getEntries().add(new ResourceEntry(filename, ResourceType.FILE));
      }
    }
    return references;
  }

  public boolean isArgFromPrevious() {
    return argFromPrevious;
  }

  public void setArgFromPrevious(boolean argFromPrevious) {
    this.argFromPrevious = argFromPrevious;
  }

  public String[] getTableNames() {
    return tableNames;
  }

  public void setTableNames(String[] tableNames) {
    this.tableNames = tableNames;
  }

  public String[] getSchemaNames() {
    return schemaNames;
  }

  public void setSchemaNames(String[] schemaNames) {
    this.schemaNames = schemaNames;
  }
}
