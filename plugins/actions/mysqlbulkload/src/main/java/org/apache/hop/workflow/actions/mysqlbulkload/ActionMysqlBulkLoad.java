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

package org.apache.hop.workflow.actions.mysqlbulkload;

import java.io.File;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.provider.local.LocalFile;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.validator.AbstractFileValidator;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.action.validator.ValidatorContext;

/** This defines a MySQL action. */
@Action(
    id = "MYSQL_BULK_LOAD",
    name = "i18n::ActionMysqlBulkLoad.Name",
    description = "i18n::ActionMysqlBulkLoad.Description",
    image = "MysqlBulkLoad.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.BulkLoading",
    keywords = "i18n::ActionMysqlBulkLoad.keyword",
    documentationUrl = "/workflow/actions/mysqlbulkload.html")
@Getter
@Setter
public class ActionMysqlBulkLoad extends ActionBase {
  private static final Class<?> PKG = ActionMysqlBulkLoad.class;

  @HopMetadataProperty(key = "schemaname")
  private String schemaName;

  @HopMetadataProperty(key = "tablename")
  private String tableName;

  @HopMetadataProperty(key = "filename")
  private String fileName;

  @HopMetadataProperty(key = "separator")
  private String separator;

  @HopMetadataProperty(key = "enclosed")
  private String enclosed;

  @HopMetadataProperty(key = "escaped")
  private String escaped;

  @HopMetadataProperty(key = "linestarted")
  private String lineStarted;

  @HopMetadataProperty(key = "lineterminated")
  private String lineTerminated;

  @HopMetadataProperty(key = "ignorelines")
  private String ignoreLines;

  @HopMetadataProperty(key = "replacedata")
  private boolean replaceData;

  @HopMetadataProperty(key = "listattribut")
  private String listAttribute;

  @HopMetadataProperty(key = "localinfile")
  private boolean localInFile;

  @HopMetadataProperty(key = "prorityvalue")
  public int prorityValue;

  @HopMetadataProperty(key = "addfiletoresult")
  private boolean addFileToResult;

  @HopMetadataProperty(key = "connection")
  private String connection;

  public ActionMysqlBulkLoad(String n) {
    super(n, "");
    tableName = null;
    schemaName = null;
    fileName = null;
    separator = null;
    enclosed = null;
    escaped = null;
    lineTerminated = null;
    lineStarted = null;
    replaceData = true;
    ignoreLines = "0";
    listAttribute = null;
    localInFile = true;
    connection = null;
    addFileToResult = false;
  }

  public ActionMysqlBulkLoad() {
    this("");
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
  public Result execute(Result previousResult, int nr) {
    String replaceIgnore;
    String ignoreNbrLignes = "";
    String listOfColumn = "";
    String localExec = "";
    String priorityText = "";
    String lineTerminatedby = "";
    String fieldTerminatedby = "";

    Result result = previousResult;
    result.setResult(false);

    String vfsFilename = resolve(fileName);

    // Let's check the filename ...
    if (!Utils.isEmpty(vfsFilename)) {
      try {
        // User has specified a file, We can continue ...
        //

        // This is running over VFS but we need a normal file.
        // As such, we're going to verify that it's a local file...
        // We're also going to convert VFS FileObject to File
        //
        FileObject fileObject = HopVfs.getFileObject(vfsFilename);
        if (!(fileObject instanceof LocalFile)) {
          // MySQL LOAD DATA can only use local files, so that's what we limit ourselves to.
          //
          throw new HopException(
              "Only local files are supported at this time, file ["
                  + vfsFilename
                  + "] is not a local file.");
        }

        // Convert it to a regular platform specific file name
        //
        String realFilename = HopVfs.getFilename(fileObject);

        // Here we go... back to the regular scheduled program...
        //
        File file = new File(realFilename);
        if ((file.exists() && file.canRead()) || isLocalInFile() == false) {
          // User has specified an existing file, We can continue ...
          if (isDetailed()) {
            logDetailed("File [" + realFilename + "] exists.");
          }

          if (connection != null) {
            DatabaseMeta databaseMeta = null;
            try {
              databaseMeta = DatabaseMeta.loadDatabase(getMetadataProvider(), connection);
            } catch (Exception e) {
              logError("Unable to load database :" + connection, e);
            }
            // User has specified a connection, We can continue ...
            try (Database db = new Database(this, this, databaseMeta)) {
              db.connect();
              // Get schemaname
              String realSchemaname = resolve(schemaName);
              // Get tablename
              String realTablename = resolve(tableName);

              if (db.checkTableExists(realSchemaname, realTablename)) {
                // The table existe, We can continue ...
                if (isDetailed()) {
                  logDetailed("Table [" + realTablename + "] exists.");
                }

                // Add schemaname (Most the time Schemaname.Tablename)
                if (schemaName != null) {
                  realTablename = realSchemaname + "." + realTablename;
                }

                // Set the REPLACE or IGNORE
                if (isReplaceData()) {
                  replaceIgnore = "REPLACE";
                } else {
                  replaceIgnore = "IGNORE";
                }

                // Set the IGNORE LINES
                if (Const.toInt(getRealIgnorelines(), 0) > 0) {
                  ignoreNbrLignes = "IGNORE " + getRealIgnorelines() + " LINES";
                }

                // Set list of Column
                if (getRealListattribut() != null) {
                  listOfColumn = "(" + mysqlString(getRealListattribut()) + ")";
                }

                // Local File execution
                if (isLocalInFile()) {
                  localExec = "LOCAL";
                }

                // Prority
                if (prorityValue == 1) {
                  // LOW
                  priorityText = "LOW_PRIORITY";
                } else if (prorityValue == 2) {
                  // CONCURRENT
                  priorityText = "CONCURRENT";
                }

                // Fields ....
                if (getRealSeparator() != null
                    || getRealEnclosed() != null
                    || getRealEscaped() != null) {
                  fieldTerminatedby = "FIELDS ";

                  if (getRealSeparator() != null) {
                    fieldTerminatedby =
                        fieldTerminatedby
                            + "TERMINATED BY '"
                            + Const.replace(getRealSeparator(), "'", "''")
                            + "'";
                  }
                  if (getRealEnclosed() != null) {
                    fieldTerminatedby =
                        fieldTerminatedby
                            + " ENCLOSED BY '"
                            + Const.replace(getRealEnclosed(), "'", "''")
                            + "'";
                  }
                  if (getRealEscaped() != null) {

                    fieldTerminatedby =
                        fieldTerminatedby
                            + " ESCAPED BY '"
                            + Const.replace(getRealEscaped(), "'", "''")
                            + "'";
                  }
                }

                // LINES ...
                if (getRealLinestarted() != null || getRealLineterminated() != null) {
                  lineTerminatedby = "LINES ";

                  // Line starting By
                  if (getRealLinestarted() != null) {
                    lineTerminatedby =
                        lineTerminatedby
                            + "STARTING BY '"
                            + Const.replace(getRealLinestarted(), "'", "''")
                            + "'";
                  }

                  // Line terminating By
                  if (getRealLineterminated() != null) {
                    lineTerminatedby =
                        lineTerminatedby
                            + " TERMINATED BY '"
                            + Const.replace(getRealLineterminated(), "'", "''")
                            + "'";
                  }
                }

                String sqlBulkLoad =
                    "LOAD DATA "
                        + priorityText
                        + " "
                        + localExec
                        + " INFILE '"
                        + realFilename.replace('\\', '/')
                        + "' "
                        + replaceIgnore
                        + " INTO TABLE "
                        + realTablename
                        + " "
                        + fieldTerminatedby
                        + " "
                        + lineTerminatedby
                        + " "
                        + ignoreNbrLignes
                        + " "
                        + listOfColumn
                        + ";";

                try {
                  // Run the SQL
                  db.execStatement(sqlBulkLoad);

                  if (isAddFileToResult()) {
                    // Add zip filename to output files
                    ResultFile resultFile =
                        new ResultFile(
                            ResultFile.FILE_TYPE_GENERAL,
                            HopVfs.getFileObject(realFilename),
                            parentWorkflow.getWorkflowName(),
                            toString());
                    result.getResultFiles().put(resultFile.getFile().toString(), resultFile);
                  }

                  result.setResult(true);
                } catch (HopDatabaseException je) {
                  result.setNrErrors(1);
                  logError("An error occurred executing this action : " + je.getMessage());
                } catch (HopFileException e) {
                  logError("An error occurred executing this action : " + e.getMessage());
                  result.setNrErrors(1);
                }
              } else {
                // Of course, the table should have been created already before the bulk load
                // operation
                result.setNrErrors(1);
                if (isDetailed()) {
                  logDetailed("Table [" + realTablename + "] doesn't exist!");
                }
              }
            } catch (HopDatabaseException dbe) {
              result.setNrErrors(1);
              logError("An error occurred executing this entry: " + dbe.getMessage());
            }
          } else {
            // No database connection is defined
            result.setNrErrors(1);
            logError(BaseMessages.getString(PKG, "ActionMysqlBulkLoad.Nodatabase.Label"));
          }
        } else {
          // the file doesn't exist
          result.setNrErrors(1);
          logError("File [" + realFilename + "] doesn't exist!");
        }
      } catch (Exception e) {
        // An unexpected error occurred
        result.setNrErrors(1);
        logError(BaseMessages.getString(PKG, "ActionMysqlBulkLoad.UnexpectedError.Label"), e);
      }
    } else {
      // No file was specified
      result.setNrErrors(1);
      logError(BaseMessages.getString(PKG, "ActionMysqlBulkLoad.Nofilename.Label"));
    }
    return result;
  }

  public String getRealEnclosed() {
    return resolve(getEnclosed());
  }

  public String getRealEscaped() {
    return resolve(getEscaped());
  }

  public String getRealLinestarted() {
    return resolve(getLineStarted());
  }

  public String getRealLineterminated() {
    return resolve(getLineTerminated());
  }

  public String getRealSeparator() {
    return resolve(getSeparator());
  }

  public String getRealIgnorelines() {
    return resolve(getIgnoreLines());
  }

  public String getRealListattribut() {
    return resolve(getListAttribute());
  }

  private String mysqlString(String listcolumns) {
    /*
     * Handle forbiden char like '
     */
    String returnString = "";
    String[] split = listcolumns.split(",");

    for (int i = 0; i < split.length; i++) {
      if (returnString.equals("")) {
        returnString = "`" + Const.trim(split[i]) + "`";
      } else {
        returnString = returnString + ", `" + Const.trim(split[i]) + "`";
      }
    }

    return returnString;
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    ResourceReference reference = null;
    if (connection != null) {
      DatabaseMeta databaseMeta = null;
      try {
        databaseMeta = DatabaseMeta.loadDatabase(getMetadataProvider(), connection);
      } catch (Exception e) {
        logError("Unable to load database :" + connection, e);
      }
      reference = new ResourceReference(this);
      references.add(reference);
      reference
          .getEntries()
          .add(new ResourceEntry(databaseMeta.getHostname(), ResourceType.SERVER));
      reference
          .getEntries()
          .add(new ResourceEntry(databaseMeta.getDatabaseName(), ResourceType.DATABASENAME));
    }
    if (fileName != null) {
      String realFilename = getRealFilename();
      if (reference == null) {
        reference = new ResourceReference(this);
        references.add(reference);
      }
      reference.getEntries().add(new ResourceEntry(realFilename, ResourceType.FILE));
    }
    return references;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace(ctx, getVariables());
    AndValidator.putValidators(
        ctx, ActionValidatorUtils.notBlankValidator(), ActionValidatorUtils.fileExistsValidator());
    ActionValidatorUtils.andValidator().validate(this, "filename", remarks, ctx);

    ActionValidatorUtils.andValidator()
        .validate(
            this,
            "tablename",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
  }
}
