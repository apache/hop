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

package org.apache.hop.workflow.actions.mysqlbulkfile;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.util.StringUtil;
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
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;

/** This defines an MYSQL Bulk file action. */
@Action(
    id = "MYSQL_BULK_FILE",
    name = "i18n::ActionMysqlBulkFile.Name",
    description = "i18n::ActionMysqlBulkFile.Description",
    image = "MysqlBulkFile.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.BulkLoading",
    keywords = "i18n::ActionMysqlBulkFile.keyword",
    documentationUrl = "/workflow/actions/mysqlbulkfile.html",
    classLoaderGroup = "mysql-db",
    isIncludeJdbcDrivers = true)
@Getter
@Setter
public class ActionMysqlBulkFile extends ActionBase {
  private static final Class<?> PKG = ActionMysqlBulkFile.class;
  public static final String CONST_ACTION_MYSQL_BULK_FILE_FILE_EXISTS_1_LABEL =
      "ActionMysqlBulkFile.FileExists1.Label";
  public static final String CONST_ACTION_MYSQL_BULK_FILE_FILE_EXISTS_2_LABEL =
      "ActionMysqlBulkFile.FileExists2.Label";
  public static final String CONST_ACTION_MYSQL_BULK_FILE_ERROR_LABEL =
      "ActionMysqlBulkFile.Error.Label";

  @HopMetadataProperty(key = "tablename")
  private String tableName;

  @HopMetadataProperty(key = "schemaname")
  private String schemaName;

  @HopMetadataProperty(key = "filename")
  private String fileName;

  @HopMetadataProperty(key = "separator")
  private String separator;

  @HopMetadataProperty(key = "enclosed")
  private String enclosed;

  @HopMetadataProperty(key = "lineterminated")
  private String lineTerminated;

  @HopMetadataProperty(key = "limitlines")
  private String limitLines;

  @HopMetadataProperty(key = "listcolumn")
  private String listColumn;

  @HopMetadataProperty(key = "highpriority")
  private boolean highPriority;

  @HopMetadataProperty(key = "optionenclosed")
  private boolean optionEnclosed;

  @HopMetadataProperty(key = "outdumpvalue")
  public int outDumpValue;

  @HopMetadataProperty(key = "iffileexists")
  public int ifFileExists;

  @HopMetadataProperty(key = "addfiletoresult")
  private boolean addFileToResult;

  @HopMetadataProperty(key = "connection")
  private String connection;

  public ActionMysqlBulkFile(String n) {
    super(n, "");
    tableName = null;
    schemaName = null;
    fileName = null;
    separator = null;
    enclosed = null;
    limitLines = "0";
    listColumn = null;
    lineTerminated = null;
    highPriority = true;
    optionEnclosed = false;
    ifFileExists = 2;
    connection = null;
    addFileToResult = false;
  }

  public ActionMysqlBulkFile() {
    this("");
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

    String limitNbrLignes = "";
    String listOfColumn = "*";
    String strHighPriority = "";
    String outDumpText = "";
    String optionEnclosed = "";
    String fieldSeparator = "";
    String linesTerminated = "";

    Result result = previousResult;
    result.setResult(false);

    // Let's check the filename ...
    if (fileName != null) {
      // User has specified a file, We can continue ...
      String realFilename = getRealFilename();
      File file = new File(realFilename);

      if (file.exists() && ifFileExists == 2) {
        // the file exists and user want to Fail
        result.setResult(false);
        result.setNrErrors(1);
        logError(
            BaseMessages.getString(PKG, CONST_ACTION_MYSQL_BULK_FILE_FILE_EXISTS_1_LABEL)
                + realFilename
                + BaseMessages.getString(PKG, CONST_ACTION_MYSQL_BULK_FILE_FILE_EXISTS_2_LABEL));

      } else if (file.exists() && ifFileExists == 1) {
        // the file exists and user want to do nothing
        result.setResult(true);
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(PKG, CONST_ACTION_MYSQL_BULK_FILE_FILE_EXISTS_1_LABEL)
                  + realFilename
                  + BaseMessages.getString(PKG, CONST_ACTION_MYSQL_BULK_FILE_FILE_EXISTS_2_LABEL));
        }

      } else {

        if (file.exists() && ifFileExists == 0) {
          // File exists and user want to renamme it with unique name

          // Format Date

          // Try to clean filename (without wildcard)
          String wildcard =
              realFilename.substring(realFilename.length() - 4, realFilename.length());
          if (wildcard.substring(0, 1).equals(".")) {
            // Find wildcard
            realFilename =
                realFilename.substring(0, realFilename.length() - 4)
                    + "_"
                    + StringUtil.getFormattedDateTimeNow(true)
                    + wildcard;
          } else {
            // did not find wildcard
            realFilename = realFilename + "_" + StringUtil.getFormattedDateTimeNow(true);
          }

          logDebug(
              BaseMessages.getString(PKG, "ActionMysqlBulkFile.FileNameChange1.Label")
                  + realFilename
                  + BaseMessages.getString(PKG, "ActionMysqlBulkFile.FileNameChange1.Label"));
        }

        // User has specified an existing file, We can continue ...
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(PKG, CONST_ACTION_MYSQL_BULK_FILE_FILE_EXISTS_1_LABEL)
                  + realFilename
                  + BaseMessages.getString(PKG, CONST_ACTION_MYSQL_BULK_FILE_FILE_EXISTS_2_LABEL));
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
                logDetailed(
                    BaseMessages.getString(PKG, "ActionMysqlBulkFile.TableExists1.Label")
                        + realTablename
                        + BaseMessages.getString(PKG, "ActionMysqlBulkFile.TableExists2.Label"));
              }

              // Add schemaname (Most the time Schemaname.Tablename)
              if (schemaName != null) {
                realTablename = realSchemaname + "." + realTablename;
              }

              // Set the Limit lines
              if (Const.toInt(getRealLimitlines(), 0) > 0) {
                limitNbrLignes = "LIMIT " + getRealLimitlines();
              }

              // Set list of Column, if null get all columns (*)
              if (getRealListColumn() != null) {
                listOfColumn = mysqlString(getRealListColumn());
              }

              // Fields separator
              if (getRealSeparator() != null && outDumpValue == 0) {
                fieldSeparator =
                    "FIELDS TERMINATED BY '" + Const.replace(getRealSeparator(), "'", "''") + "'";
              }

              // Lines Terminated by
              if (getRealLineterminated() != null && outDumpValue == 0) {
                linesTerminated =
                    "LINES TERMINATED BY '"
                        + Const.replace(getRealLineterminated(), "'", "''")
                        + "'";
              }

              // High Priority ?
              if (isHighPriority()) {
                strHighPriority = "HIGH_PRIORITY";
              }

              if (getRealEnclosed() != null && outDumpValue == 0) {
                if (isOptionEnclosed()) {
                  optionEnclosed = "OPTIONALLY ";
                }
                optionEnclosed =
                    optionEnclosed
                        + "ENCLOSED BY '"
                        + Const.replace(getRealEnclosed(), "'", "''")
                        + "'";
              }

              // OutFile or Dumpfile
              if (outDumpValue == 0) {
                outDumpText = "INTO OUTFILE";
              } else {
                outDumpText = "INTO DUMPFILE";
              }

              String fileBulkFile =
                  "SELECT "
                      + strHighPriority
                      + " "
                      + listOfColumn
                      + " "
                      + outDumpText
                      + " '"
                      + realFilename
                      + "' "
                      + fieldSeparator
                      + " "
                      + optionEnclosed
                      + " "
                      + linesTerminated
                      + " FROM "
                      + realTablename
                      + " "
                      + limitNbrLignes
                      + " LOCK IN SHARE MODE";

              try {
                if (isDetailed()) {
                  logDetailed(fileBulkFile);
                }
                // Run the SQL
                PreparedStatement ps = db.prepareSql(fileBulkFile);
                ps.execute();

                if (isAddFileToResult()) {
                  // Add filename to output files
                  ResultFile resultFile =
                      new ResultFile(
                          ResultFile.FILE_TYPE_GENERAL,
                          HopVfs.getFileObject(realFilename),
                          parentWorkflow.getWorkflowName(),
                          toString());
                  result.getResultFiles().put(resultFile.getFile().toString(), resultFile);
                }

                result.setResult(true);

              } catch (SQLException je) {
                result.setNrErrors(1);
                logError(
                    BaseMessages.getString(PKG, CONST_ACTION_MYSQL_BULK_FILE_ERROR_LABEL)
                        + " "
                        + je.getMessage());
              } catch (HopFileException e) {
                logError(
                    BaseMessages.getString(PKG, CONST_ACTION_MYSQL_BULK_FILE_ERROR_LABEL)
                        + e.getMessage());
                result.setNrErrors(1);
              }

            } else {
              // Of course, the table should have been created already before the bulk load
              // operation
              result.setNrErrors(1);
              if (isDetailed()) {
                logDetailed(
                    BaseMessages.getString(PKG, "ActionMysqlBulkFile.TableNotExists1.Label")
                        + realTablename
                        + BaseMessages.getString(PKG, "ActionMysqlBulkFile.TableNotExists2.Label"));
              }
            }

          } catch (HopDatabaseException dbe) {
            result.setNrErrors(1);
            logError(
                BaseMessages.getString(PKG, CONST_ACTION_MYSQL_BULK_FILE_ERROR_LABEL)
                    + " "
                    + dbe.getMessage());
          }

        } else {
          // No database connection is defined
          result.setNrErrors(1);
          logError(BaseMessages.getString(PKG, "ActionMysqlBulkFile.NoDatabase.Label"));
        }
      }

    } else {
      // No file was specified
      result.setNrErrors(1);
      logError(BaseMessages.getString(PKG, "ActionMysqlBulkFile.NoFileName.Label"));
    }

    return result;
  }

  @Override
  public String getRealFilename() {
    String realFile = resolve(getFileName());
    return realFile.replace('\\', '/');
  }

  public String getRealLineterminated() {
    return resolve(getLineTerminated());
  }

  public String getRealSeparator() {
    return resolve(getSeparator());
  }

  public String getRealEnclosed() {
    return resolve(getEnclosed());
  }

  public String getRealLimitlines() {
    return resolve(getLimitLines());
  }

  public String getRealListColumn() {
    return resolve(getListColumn());
  }

  private String mysqlString(String listcolumns) {
    /*
     * handle forbiden char like '
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
    if (connection != null) {
      DatabaseMeta databaseMeta = null;
      try {
        databaseMeta = DatabaseMeta.loadDatabase(getMetadataProvider(), connection);
      } catch (Exception e) {
        logError("Unable to load database :" + connection, e);
      }
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
            "filename",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
    ActionValidatorUtils.andValidator()
        .validate(
            this,
            "tablename",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
  }
}
