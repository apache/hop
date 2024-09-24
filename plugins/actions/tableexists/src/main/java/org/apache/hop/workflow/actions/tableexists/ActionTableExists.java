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

package org.apache.hop.workflow.actions.tableexists;

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
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;

/** This defines a table exists action. */
@Action(
    id = "TABLE_EXISTS",
    name = "i18n::ActionTableExists.Name",
    description = "i18n::ActionTableExists.Description",
    image = "TableExists.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Conditions",
    keywords = "i18n::ActionTableExists.keyword",
    documentationUrl = "/workflow/actions/tableexists.html")
public class ActionTableExists extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionTableExists.class;

  @HopMetadataProperty(
      key = "tablename",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
  private String tableName;

  @HopMetadataProperty(
      key = "schemaname",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_SCHEMA)
  private String schemaName;

  @HopMetadataProperty(
      key = "connection",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String connection;

  public ActionTableExists(String n) {
    super(n, "");
    schemaName = null;
    tableName = null;
    connection = null;
  }

  public ActionTableExists() {
    this("");
  }

  @Override
  public Object clone() {
    ActionTableExists je = (ActionTableExists) super.clone();
    return je;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public String getConnection() {
    return connection;
  }

  public void setConnection(String connection) {
    this.connection = connection;
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

    if (!Utils.isEmpty(connection)) {
      DatabaseMeta dbMeta = parentWorkflowMeta.findDatabase(connection, getVariables());
      if (dbMeta != null) {
        try (Database db = new Database(this, this, dbMeta)) {
          db.connect();
          String realTableName = resolve(tableName);
          String realSchemaName = resolve(schemaName);

          if (db.checkTableExists(realSchemaName, realTableName)) {
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(PKG, "TableExists.Log.TableExists", realTableName));
            }
            result.setResult(true);
          } else {
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(PKG, "TableExists.Log.TableNotExists", realTableName));
            }
          }
        } catch (HopDatabaseException dbe) {
          result.setNrErrors(1);
          logError(
              BaseMessages.getString(PKG, "TableExists.Error.RunningAction", dbe.getMessage()));
        }
      }
    } else {
      result.setNrErrors(1);
      logError(BaseMessages.getString(PKG, "TableExists.Error.NoConnectionDefined"));
    }

    return result;
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    DatabaseMeta dbMeta = parentWorkflowMeta.findDatabase(connection, getVariables());
    if (dbMeta != null) {
      ResourceReference reference = new ResourceReference(this);
      reference.getEntries().add(new ResourceEntry(dbMeta.getHostname(), ResourceType.SERVER));
      reference
          .getEntries()
          .add(new ResourceEntry(dbMeta.getDatabaseName(), ResourceType.DATABASENAME));
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
  }
}
