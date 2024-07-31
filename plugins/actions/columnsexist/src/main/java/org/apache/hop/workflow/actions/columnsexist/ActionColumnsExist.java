/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.workflow.actions.columnsexist;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;

/** This defines a column exists action. */
@Action(
    id = "COLUMNS_EXIST",
    name = "i18n::ActionColumnsExist.Name",
    description = "i18n::ActionColumnsExist.Description",
    image = "ColumnsExist.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Conditions",
    keywords = "i18n::ActionColumnsExist.keyword",
    documentationUrl = "/workflow/actions/columnsexist.html")
public class ActionColumnsExist extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionColumnsExist.class;

  public static final class ColumnExist {
    public ColumnExist() {}

    public ColumnExist(String name) {
      this.name = name;
    }

    @HopMetadataProperty(key = "name")
    private String name;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }
  }

  @HopMetadataProperty(key = "schemaname")
  private String schemaName;

  @HopMetadataProperty(key = "tablename")
  private String tableName;

  @HopMetadataProperty(key = "connection", storeWithName = true)
  private DatabaseMeta databaseMeta;

  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<ColumnExist> columns;

  public ActionColumnsExist(String n) {
    super(n, "");
    this.schemaName = null;
    this.tableName = null;
    this.databaseMeta = null;
    this.columns = new ArrayList<>();
  }

  public ActionColumnsExist() {
    this("");
  }

  public ActionColumnsExist(ActionColumnsExist meta) {
    super(meta.getName(), meta.getDescription(), meta.getPluginId());
    this.schemaName = meta.schemaName;
    this.tableName = meta.tableName;
    this.databaseMeta = meta.databaseMeta;
    this.columns = meta.columns;
  }

  @Override
  public Object clone() {
    return new ActionColumnsExist(this);
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setSchemaName(String schemaname) {
    this.schemaName = schemaname;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public List<ColumnExist> getColumns() {
    return columns;
  }

  public void setColumns(List<ColumnExist> columns) {
    this.columns = columns;
  }

  public void setDatabaseMeta(DatabaseMeta database) {
    this.databaseMeta = database;
  }

  public DatabaseMeta getDatabaseMeta() {
    return databaseMeta;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return false;
  }

  @Override
  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;
    result.setResult(false);
    result.setNrErrors(1);

    int nrexistcolums = 0;
    int nrnotexistcolums = 0;

    if (Utils.isEmpty(tableName)) {
      logError(BaseMessages.getString(PKG, "ActionColumnsExist.Error.TablenameEmpty"));
      return result;
    }
    if (columns == null) {
      logError(BaseMessages.getString(PKG, "ActionColumnsExist.Error.ColumnameEmpty"));
      return result;
    }
    if (databaseMeta != null) {
      try (Database db = new Database(this, this, databaseMeta)) {
        String realSchemaname = resolve(schemaName);
        String realTablename = resolve(tableName);

        db.connect();

        if (db.checkTableExists(realSchemaname, realTablename)) {
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(PKG, "ActionColumnsExist.Log.TableExists", realTablename));
          }

          for (ColumnExist column : columns) {
            if (parentWorkflow.isStopped()) {
              break;
            }

            String realColumnname = resolve(column.getName());

            if (db.checkColumnExists(realSchemaname, realTablename, realColumnname)) {
              if (isDetailed()) {
                logDetailed(
                    BaseMessages.getString(
                        PKG, "ActionColumnsExist.Log.ColumnExists", realColumnname, realTablename));
              }
              nrexistcolums++;
            } else {
              logError(
                  BaseMessages.getString(
                      PKG,
                      "ActionColumnsExist.Log.ColumnNotExists",
                      realColumnname,
                      realTablename));
              nrnotexistcolums++;
            }
          }
        } else {
          logError(
              BaseMessages.getString(PKG, "ActionColumnsExist.Log.TableNotExists", realTablename));
        }
      } catch (HopDatabaseException dbe) {
        logError(
            BaseMessages.getString(
                PKG, "ActionColumnsExist.Error.UnexpectedError", dbe.getMessage()));
      }
    } else {
      logError(BaseMessages.getString(PKG, "ActionColumnsExist.Error.NoDbConnection"));
    }

    result.setEntryNr(nrnotexistcolums);
    result.setNrLinesWritten(nrexistcolums);
    // result is true only if all columns found
    if (nrexistcolums == columns.size()) {
      result.setNrErrors(0);
      result.setResult(true);
    }
    return result;
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    if (databaseMeta != null) {
      ResourceReference reference = new ResourceReference(this);
      reference
          .getEntries()
          .add(new ResourceEntry(databaseMeta.getHostname(), ResourceType.SERVER));
      reference
          .getEntries()
          .add(new ResourceEntry(databaseMeta.getDatabaseName(), ResourceType.DATABASENAME));
      references.add(reference);
    }
    return references;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    ActionValidatorUtils.andValidator()
        .validate(
            this,
            "tablename",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
    ActionValidatorUtils.andValidator()
        .validate(
            this,
            "columnname",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
  }
}
