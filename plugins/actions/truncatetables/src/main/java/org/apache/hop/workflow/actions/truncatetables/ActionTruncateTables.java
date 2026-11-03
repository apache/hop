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

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;

/** This defines a Truncate Tables action. */
@Action(
    id = "TRUNCATE_TABLES",
    name = "i18n::ActionTruncateTables.Name",
    description = "i18n::ActionTruncateTables.Description",
    image = "TruncateTables.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Utility",
    keywords = "i18n::ActionTruncateTables.keyword",
    documentationUrl = "/workflow/actions/truncatetables.html")
public class ActionTruncateTables extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionTruncateTables.class;

  @Getter
  @Setter
  @HopMetadataProperty(key = "arg_from_previous")
  private boolean argFromPrevious;

  @Getter
  @Setter
  @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String connection;

  @Getter
  @Setter
  @HopMetadataProperty(key = "field", groupKey = "fields")
  private List<TruncateTableItem> items;

  // Runtime counters for a single execute(); keep package-private accessors out of the public API.
  private int nrErrors = 0;
  private int nrSuccess = 0;
  private boolean continueProcess = true;

  public ActionTruncateTables(String name) {
    super(name, "");
    this.argFromPrevious = false;
    this.connection = null;
  }

  public ActionTruncateTables() {
    this("");
    this.items = new ArrayList<>();
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return true;
  }

  private boolean truncateTables(String tableName, String schemaName, Database db) {
    boolean retval = false;
    try {
      // check if table exists!
      if (db.checkTableExists(schemaName, tableName)) {
        if (!Utils.isEmpty(schemaName)) {
          db.truncateTable(schemaName, tableName);
        } else {
          db.truncateTable(tableName);
        }

        if (isDetailed()) {
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
      if (isDetailed()) {
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
    String realConnection = resolve(connection);
    if (Utils.isEmpty(realConnection)) {
      updateErrors();
      logError(BaseMessages.getString(PKG, "ActionTruncateTables.NoDbConnection"));
    } else {
      // Keep findDatabase() inside try/catch so HopRuntimeException becomes an action failure.
      try {
        DatabaseMeta databaseMeta =
            getParentWorkflowMeta() == null
                ? null
                : getParentWorkflowMeta().findDatabase(realConnection, getVariables());
        if (databaseMeta == null) {
          updateErrors();
          logError(
              BaseMessages.getString(
                  PKG, "ActionTruncateTables.Error.ConnectionNotFound", realConnection));
        } else {
          try (Database db = new Database(this, this, databaseMeta)) {
            db.connect();
            if (argFromPrevious
                && rows != null) { // Copy the input row to the (command line) arguments

              for (int iteration = 0;
                  iteration < rows.size() && !parentWorkflow.isStopped() && continueProcess;
                  iteration++) {
                resultRow = rows.get(iteration);

                // Get values from previous result
                String tableNamePrevious = resultRow.getString(0, null);
                String schemaNamePrevious = resultRow.getString(1, null);

                if (!Utils.isEmpty(tableNamePrevious)) {
                  if (isDetailed()) {
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
                  // Count as error but keep going so later valid table names are still processed
                  logError(BaseMessages.getString(PKG, "ActionTruncateTables.RowEmpty"));
                  nrErrors++;
                }
              }

            } else if (!Utils.isEmpty(this.items)) {

              for (int i = 0;
                  i < this.items.size() && !parentWorkflow.isStopped() && continueProcess;
                  i++) {
                TruncateTableItem tableItem = this.items.get(i);
                String realTableName = resolve(tableItem.getTableName());
                String realSchemaName = resolve(tableItem.getSchemaName());
                if (!Utils.isEmpty(realTableName)) {
                  if (isDetailed()) {
                    logDetailed(
                        BaseMessages.getString(
                            PKG,
                            "ActionTruncateTables.ProcessingArg",
                            realTableName,
                            realSchemaName));
                  }

                  // let's truncate table
                  if (truncateTables(realTableName, realSchemaName, db)) {
                    updateSuccess();
                  } else {
                    updateErrors();
                  }
                } else {
                  // Count as error but keep going so later valid table names are still processed
                  logError(
                      BaseMessages.getString(
                          PKG, "ActionTruncateTables.ArgEmpty", realTableName, realSchemaName));
                  nrErrors++;
                }
              }
            }
          }
        }
      } catch (Exception dbe) {
        updateErrors();
        logError(
            BaseMessages.getString(
                PKG, "ActionTruncateTables.Error.RunningEntry", dbe.getMessage()));
      }
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
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    if (!Utils.isEmpty(items)) {
      ResourceReference reference = null;
      for (TruncateTableItem item : this.items) {
        String filename = resolve(item.getTableName());
        if (reference == null) {
          reference = new ResourceReference(this);
          references.add(reference);
        }
        reference.getEntries().add(new ResourceEntry(filename, ResourceType.FILE));
      }
    }
    return references;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {

    String realConnection = variables.resolve(connection);
    if (Utils.isEmpty(realConnection)
        || workflowMeta == null
        || workflowMeta.findDatabase(realConnection, variables) == null) {
      String message = BaseMessages.getString(PKG, "ActionTruncateTables.NoDbConnection");
      remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_WARNING, message, this));
    }

    if (!argFromPrevious) {
      if (Utils.isEmpty(items)) {
        String message =
            BaseMessages.getString(PKG, "ActionTruncateTables.CheckResult.NoTablesSelected");
        remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_WARNING, message, this));
      } else {
        for (TruncateTableItem item : items) {
          if (StringUtils.isEmpty(variables.resolve(item.getTableName()))) {
            String message =
                BaseMessages.getString(PKG, "ActionTruncateTables.CheckResult.TableNameIsEmpty");
            remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_WARNING, message, this));
          }
        }
      }
    }
  }
}
