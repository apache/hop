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

package org.apache.hop.workflow.actions.sql;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
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

/** This defines an SQL action. */
@Action(
    id = "SQL",
    name = "i18n::ActionSQL.Name",
    description = "i18n::ActionSQL.Description",
    image = "sql.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Scripting",
    keywords = "i18n::ActionSql.keyword",
    documentationUrl = "/workflow/actions/sql.html",
    actionTransformTypes = {ActionTransformType.RDBMS})
public class ActionSql extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionSql.class;

  @HopMetadataProperty(
      key = "sql",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_SQL_SELECT)
  private String sql;

  @HopMetadataProperty(
      key = "connection",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String connection;

  @HopMetadataProperty(key = "useVariableSubstitution")
  private boolean useVariableSubstitution = false;

  @HopMetadataProperty(key = "sqlfromfile")
  private boolean sqlFromFile = false;

  @HopMetadataProperty(key = "sqlfilename")
  private String sqlFilename;

  @HopMetadataProperty(key = "sendOneStatement")
  private boolean sendOneStatement = false;

  public ActionSql(String n) {
    super(n, "");
    sql = null;
    connection = null;
  }

  public ActionSql() {
    this("");
  }

  @Override
  public Object clone() {
    ActionSql je = (ActionSql) super.clone();
    return je;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  public String getSql() {
    return sql;
  }

  public String getSqlFilename() {
    return sqlFilename;
  }

  public void setSqlFilename(String sqlfilename) {
    this.sqlFilename = sqlfilename;
  }

  public boolean isUseVariableSubstitution() {
    return useVariableSubstitution;
  }

  public void setUseVariableSubstitution(boolean subs) {
    useVariableSubstitution = subs;
  }

  public void setSqlFromFile(boolean sqlfromfilein) {
    sqlFromFile = sqlfromfilein;
  }

  public boolean isSqlFromFile() {
    return sqlFromFile;
  }

  public boolean isSendOneStatement() {
    return sendOneStatement;
  }

  public void setSendOneStatement(boolean sendOneStatementin) {
    sendOneStatement = sendOneStatementin;
  }

  public void setConnection(String connection) {
    this.connection = connection;
  }

  public String getConnection() {
    return connection;
  }

  @Override
  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;

    DatabaseMeta databaseMeta = parentWorkflowMeta.findDatabase(connection, getVariables());
    if (databaseMeta != null) {
      FileObject sqlFile = null;
      try (Database db = new Database(this, this, databaseMeta)) {
        StringBuilder theSql = new StringBuilder();
        db.connect();

        if (sqlFromFile) {
          if (sqlFilename == null) {
            throw new HopDatabaseException(
                BaseMessages.getString(PKG, "ActionSQL.NoSQLFileSpecified"));
          }

          try {
            String realfilename = resolve(sqlFilename);
            sqlFile = HopVfs.getFileObject(realfilename);
            if (!sqlFile.exists()) {
              logError(BaseMessages.getString(PKG, "ActionSQL.SQLFileNotExist", realfilename));
              throw new HopDatabaseException(
                  BaseMessages.getString(PKG, "ActionSQL.SQLFileNotExist", realfilename));
            }
            if (isDetailed()) {
              logDetailed(BaseMessages.getString(PKG, "ActionSQL.SQLFileExists", realfilename));
            }

            InputStream inputStream = HopVfs.getInputStream(sqlFile);
            try {
              InputStreamReader inputStreamReader =
                  new InputStreamReader(new BufferedInputStream(inputStream, 500));
              StringBuilder lineSB = new StringBuilder(256);
              lineSB.setLength(0);

              BufferedReader buff = new BufferedReader(inputStreamReader);
              String sLine = null;
              theSql.append(Const.CR);

              while ((sLine = buff.readLine()) != null) {
                if (Utils.isEmpty(sLine)) {
                  theSql.append(Const.CR);
                } else {
                  theSql.append(Const.CR).append(sLine);
                }
              }
            } finally {
              inputStream.close();
            }
          } catch (Exception e) {
            throw new HopDatabaseException(
                BaseMessages.getString(PKG, "ActionSQL.ErrorRunningSQLfromFile"), e);
          }

        } else {
          theSql.append(sql);
        }
        if (!Utils.isEmpty(theSql)) {
          // let it run
          String executionSql = theSql.toString();
          if (useVariableSubstitution) {
            executionSql = resolve(executionSql);
          }
          if (isDetailed()) {
            logDetailed(BaseMessages.getString(PKG, "ActionSQL.Log.SQlStatement", executionSql));
          }
          if (sendOneStatement) {
            db.execStatement(executionSql);
          } else {
            db.execStatements(executionSql);
          }
        }
      } catch (HopDatabaseException je) {
        result.setNrErrors(1);
        logError(BaseMessages.getString(PKG, "ActionSQL.ErrorRunAction", je.getMessage()));
      } finally {
        if (sqlFile != null) {
          try {
            sqlFile.close();
          } catch (Exception e) {
            // Ignore errors
          }
        }
      }
    } else {
      result.setNrErrors(1);
      logError(BaseMessages.getString(PKG, "ActionSQL.NoDatabaseConnection"));
    }

    if (result.getNrErrors() == 0) {
      result.setResult(true);
    } else {
      result.setResult(false);
    }

    return result;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return true;
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    DatabaseMeta databaseMeta = parentWorkflowMeta.findDatabase(connection, getVariables());
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
            "SQL",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
  }
}
