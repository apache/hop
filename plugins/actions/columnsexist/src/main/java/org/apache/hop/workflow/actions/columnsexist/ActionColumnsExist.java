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
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.CheckResult;
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

  @Setter
  @Getter
  public static final class ColumnExist {
    public ColumnExist() {}

    public ColumnExist(String name) {
      this.name = name;
    }

    @HopMetadataProperty(key = "name")
    private String name;
  }

  @Getter
  @Setter
  @HopMetadataProperty(key = "schemaname")
  private String schemaName;

  @Setter
  @Getter
  @HopMetadataProperty(key = "tablename")
  private String tableName;

  @Setter
  @Getter
  @HopMetadataProperty(key = "connection", storeWithName = true)
  private DatabaseMeta databaseMeta;

  @Setter
  @Getter
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

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return false;
  }

  @Override
  public Result execute(Result result, int nr) {
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
        String realSchemaName = resolve(schemaName);
        String realTableName = resolve(tableName);

        db.connect();

        if (db.checkTableExists(realSchemaName, realTableName)) {
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(PKG, "ActionColumnsExist.Log.TableExists", realTableName));
          }

          for (ColumnExist column : columns) {
            if (parentWorkflow.isStopped()) {
              break;
            }

            String realColumnname = resolve(column.getName());

            if (db.checkColumnExists(realSchemaName, realTableName, realColumnname)) {
              if (isDetailed()) {
                logDetailed(
                    BaseMessages.getString(
                        PKG, "ActionColumnsExist.Log.ColumnExists", realColumnname, realTableName));
              }
              nrexistcolums++;
            } else {
              logError(
                  BaseMessages.getString(
                      PKG,
                      "ActionColumnsExist.Log.ColumnNotExists",
                      realColumnname,
                      realTableName));
              nrnotexistcolums++;
            }
          }
        } else {
          logError(
              BaseMessages.getString(PKG, "ActionColumnsExist.Log.TableNotExists", realTableName));
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

    if (StringUtils.isEmpty(variables.resolve(tableName))) {
      String message =
          BaseMessages.getString(PKG, "ActionColumnsExist.CheckResult.TableNameIsEmpty");
      remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, message, this));
    }

    if (Utils.isEmpty(columns)) {
      String message = BaseMessages.getString(PKG, "ActionColumnsExist.CheckResult.NothingToCheck");
      remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_WARNING, message, this));
    }
  }
}
