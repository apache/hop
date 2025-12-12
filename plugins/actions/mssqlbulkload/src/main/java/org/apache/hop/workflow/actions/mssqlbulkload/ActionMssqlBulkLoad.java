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

package org.apache.hop.workflow.actions.mssqlbulkload;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.provider.local.LocalFile;
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

/** This defines a MSSQL Bulk action. */
@Action(
    id = "MSSQL_BULK_LOAD",
    name = "i18n::ActionMssqlBulkLoad.Name",
    description = "i18n::ActionMssqlBulkLoad.Description",
    image = "MssqlBulkLoad.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.BulkLoading",
    keywords = "i18n::ActionMssqlBulkLoad.keyword",
    documentationUrl = "/workflow/actions/mssqlbulkload.html")
@Getter
@Setter
public class ActionMssqlBulkLoad extends ActionBase {
  private static final Class<?> PKG = ActionMssqlBulkLoad.class;

  @HopMetadataProperty(key = "schemaname")
  private String schemaName;

  @HopMetadataProperty(key = "tablename")
  private String tableName;

  @HopMetadataProperty(key = "filename")
  private String fileName;

  @HopMetadataProperty(key = "datafiletype")
  private String dataFileType;

  @HopMetadataProperty(key = "fieldterminator")
  private String fieldTerminator;

  @HopMetadataProperty(key = "lineterminated")
  private String lineTerminated;

  @HopMetadataProperty(key = "codepage")
  private String codePage;

  @HopMetadataProperty(key = "specificcodepage")
  private String specificCodePage;

  @HopMetadataProperty(key = "startfile")
  private int startFile;

  @HopMetadataProperty(key = "endfile")
  private int endFile;

  @HopMetadataProperty(key = "orderby")
  private String orderBy;

  @HopMetadataProperty(key = "addfiletoresult")
  private boolean addFileToResult;

  @HopMetadataProperty(key = "formatfilename")
  private String formatFileName;

  @HopMetadataProperty(key = "firetriggers")
  private boolean fireTriggers;

  @HopMetadataProperty(key = "checkconstraints")
  private boolean checkConstraints;

  @HopMetadataProperty(key = "keepnulls")
  private boolean keepNulls;

  @HopMetadataProperty(key = "tablock")
  private boolean tabLock;

  @HopMetadataProperty(key = "errorfilename")
  private String errorFileName;

  @HopMetadataProperty(key = "adddatetime")
  private boolean addDatetime;

  @HopMetadataProperty(key = "orderdirection")
  private String orderDirection;

  @HopMetadataProperty(key = "maxerrors")
  private int maxErrors;

  @HopMetadataProperty(key = "batchsize")
  private int batchSize;

  @HopMetadataProperty(key = "rowsperbatch")
  private int rowsPerBatch;

  @HopMetadataProperty(key = "keepidentity")
  private boolean keepIdentity;

  @HopMetadataProperty(key = "truncate")
  private boolean truncate;

  @HopMetadataProperty(key = "connection")
  private String connection;

  public ActionMssqlBulkLoad(String n) {
    super(n, "");
    tableName = null;
    schemaName = null;
    fileName = null;
    dataFileType = "char";
    fieldTerminator = null;
    lineTerminated = null;
    codePage = "OEM";
    specificCodePage = null;
    checkConstraints = false;
    keepNulls = false;
    tabLock = false;
    startFile = 0;
    endFile = 0;
    orderBy = null;

    errorFileName = null;
    addDatetime = false;
    orderDirection = "Asc";
    maxErrors = 0;
    batchSize = 0;
    rowsPerBatch = 0;

    connection = null;
    addFileToResult = false;
    formatFileName = null;
    fireTriggers = false;
    keepIdentity = false;
    truncate = false;
  }

  public ActionMssqlBulkLoad() {
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
    String takeFirstNbrLines = "";
    String lineTerminatedby = "";
    String fieldTerminatedby = "";
    boolean useFieldSeparator = false;
    String useCodepage = "";
    String errorfileName = "";

    Result result = previousResult;
    result.setResult(false);

    String vfsFilename = resolve(fileName);
    FileObject fileObject = null;
    // Let's check the filename ...
    if (!Utils.isEmpty(vfsFilename)) {
      try {
        // User has specified a file, We can continue ...
        //
        // This is running over VFS but we need a normal file.
        // As such, we're going to verify that it's a local file...
        // We're also going to convert VFS FileObject to File
        //
        fileObject = HopVfs.getFileObject(vfsFilename);
        if (!(fileObject instanceof LocalFile)) {
          // MSSQL BUKL INSERT can only use local files, so that's what we limit ourselves to.
          //
          throw new HopException(
              BaseMessages.getString(
                  PKG, "ActionMssqlBulkLoad.Error.OnlyLocalFileSupported", vfsFilename));
        }

        // Convert it to a regular platform specific file name
        //
        String realFilename = HopVfs.getFilename(fileObject);

        // Here we go... back to the regular scheduled program...
        //
        File file = new File(realFilename);
        if (file.exists() && file.canRead()) {
          // User has specified an existing file, We can continue ...
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(PKG, "ActionMssqlBulkLoad.FileExists.Label", realFilename));
          }

          if (connection != null) {

            DatabaseMeta dbMeta = parentWorkflowMeta.findDatabase(connection, getVariables());

            // User has specified a connection, We can continue ...
            String pluginId = dbMeta.getPluginId();
            if (!("MSSQL".equals(pluginId) || "MSSQLNATIVE".equals(pluginId))) {

              logError(
                  BaseMessages.getString(
                      PKG, "ActionMssqlBulkLoad.Error.DbNotMSSQL", dbMeta.getDatabaseName()));
              return result;
            }
            try (Database db = new Database(this, this, dbMeta)) {
              db.connect();
              // Get schemaname
              String realSchemaname = resolve(schemaName);
              // Get tablename
              String realTablename = resolve(tableName);

              if (db.checkTableExists(realSchemaname, realTablename)) {
                // The table existe, We can continue ...
                if (isDetailed()) {
                  logDetailed(
                      BaseMessages.getString(
                          PKG, "ActionMssqlBulkLoad.TableExists.Label", realTablename));
                }

                // FIELDTERMINATOR
                String fieldTerminator = getRealFieldTerminator();
                if (Utils.isEmpty(fieldTerminator)
                    && (dataFileType.equals("char") || dataFileType.equals("widechar"))) {
                  logError(
                      BaseMessages.getString(
                          PKG, "ActionMssqlBulkLoad.Error.FieldTerminatorMissing"));
                  return result;
                } else {
                  if (dataFileType.equals("char") || dataFileType.equals("widechar")) {
                    useFieldSeparator = true;
                    fieldTerminatedby = "FIELDTERMINATOR='" + fieldTerminator + "'";
                  }
                }
                // Check Specific Code page
                if (codePage.equals("Specific")) {
                  String realCodePage = resolve(codePage);
                  if (specificCodePage.length() < 0) {
                    logError(
                        BaseMessages.getString(
                            PKG, "ActionMssqlBulkLoad.Error.SpecificCodePageMissing"));
                    return result;

                  } else {
                    useCodepage = "CODEPAGE = '" + realCodePage + "'";
                  }
                } else {
                  useCodepage = "CODEPAGE = '" + codePage + "'";
                }

                // Check Error file
                String realErrorFile = resolve(errorFileName);
                if (realErrorFile != null) {
                  File errorfile = new File(realErrorFile);
                  if (errorfile.exists() && !addDatetime) {
                    // The error file is created when the command is executed. An error occurs if
                    // the file already
                    // exists.
                    logError(
                        BaseMessages.getString(PKG, "ActionMssqlBulkLoad.Error.ErrorFileExists"));
                    return result;
                  }
                  if (addDatetime) {
                    // Add date time to filename...
                    SimpleDateFormat daf = new SimpleDateFormat();
                    Date now = new Date();
                    daf.applyPattern("yyyMMdd_HHmmss");
                    String d = daf.format(now);

                    errorfileName = "ERRORFILE ='" + realErrorFile + "_" + d + "'";
                  } else {
                    errorfileName = "ERRORFILE ='" + realErrorFile + "'";
                  }
                }

                // ROWTERMINATOR
                String rowterminator = getRealLineterminated();
                if (!Utils.isEmpty(rowterminator)) {
                  lineTerminatedby = "ROWTERMINATOR='" + rowterminator + "'";
                }

                // Start file at
                if (startFile > 0) {
                  takeFirstNbrLines = "FIRSTROW=" + startFile;
                }

                // End file at
                if (endFile > 0) {
                  takeFirstNbrLines = "LASTROW=" + endFile;
                }

                // Truncate table?
                String sqlBulkLoad = "";
                if (truncate) {
                  sqlBulkLoad = "TRUNCATE TABLE " + realTablename + ";";
                }

                // Build BULK Command
                sqlBulkLoad =
                    sqlBulkLoad
                        + "BULK INSERT "
                        + realTablename
                        + " FROM "
                        + "'"
                        + realFilename.replace('\\', '/')
                        + "'";
                sqlBulkLoad = sqlBulkLoad + " WITH (";
                if (useFieldSeparator) {
                  sqlBulkLoad = sqlBulkLoad + fieldTerminatedby;
                } else {
                  sqlBulkLoad = sqlBulkLoad + "DATAFILETYPE ='" + dataFileType + "'";
                }

                if (!lineTerminatedby.isEmpty()) {
                  sqlBulkLoad = sqlBulkLoad + "," + lineTerminatedby;
                }
                if (!takeFirstNbrLines.isEmpty()) {
                  sqlBulkLoad = sqlBulkLoad + "," + takeFirstNbrLines;
                }
                if (!useCodepage.isEmpty()) {
                  sqlBulkLoad = sqlBulkLoad + "," + useCodepage;
                }
                String realFormatFile = resolve(formatFileName);
                if (realFormatFile != null) {
                  sqlBulkLoad = sqlBulkLoad + ", FORMATFILE='" + realFormatFile + "'";
                }
                if (fireTriggers) {
                  sqlBulkLoad = sqlBulkLoad + ",FIRE_TRIGGERS";
                }
                if (keepNulls) {
                  sqlBulkLoad = sqlBulkLoad + ",KEEPNULLS";
                }
                if (keepIdentity) {
                  sqlBulkLoad = sqlBulkLoad + ",KEEPIDENTITY";
                }
                if (checkConstraints) {
                  sqlBulkLoad = sqlBulkLoad + ",CHECK_CONSTRAINTS";
                }
                if (tabLock) {
                  sqlBulkLoad = sqlBulkLoad + ",TABLOCK";
                }
                if (orderBy != null) {
                  sqlBulkLoad = sqlBulkLoad + ",ORDER ( " + orderBy + " " + orderDirection + ")";
                }
                if (!errorfileName.isEmpty()) {
                  sqlBulkLoad = sqlBulkLoad + ", " + errorfileName;
                }
                if (maxErrors > 0) {
                  sqlBulkLoad = sqlBulkLoad + ", MAXERRORS=" + maxErrors;
                }
                if (batchSize > 0) {
                  sqlBulkLoad = sqlBulkLoad + ", BATCHSIZE=" + batchSize;
                }
                if (rowsPerBatch > 0) {
                  sqlBulkLoad = sqlBulkLoad + ", ROWS_PER_BATCH=" + rowsPerBatch;
                }
                // End of Bulk command
                sqlBulkLoad = sqlBulkLoad + ")";

                try {
                  // Run the SQL
                  db.execStatement(sqlBulkLoad);

                  // Everything is OK...we can disconnect now
                  db.disconnect();

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
                } catch (HopDatabaseException je) {
                  result.setNrErrors(1);
                  logError("An error occurred executing this action : " + je.getMessage(), je);
                } catch (HopFileException e) {
                  logError("An error occurred executing this action : " + e.getMessage(), e);
                  result.setNrErrors(1);
                }
              } else {
                // Of course, the table should have been created already before the bulk load
                // operation
                result.setNrErrors(1);
                logError(
                    BaseMessages.getString(
                        PKG, "ActionMssqlBulkLoad.Error.TableNotExists", realTablename));
              }
            } catch (HopDatabaseException dbe) {
              result.setNrErrors(1);
              logError("An error occurred executing this entry: " + dbe.getMessage());
            }
          } else {
            // No database connection is defined
            result.setNrErrors(1);
            logError(BaseMessages.getString(PKG, "ActionMssqlBulkLoad.Nodatabase.Label"));
          }
        } else {
          // the file doesn't exist
          result.setNrErrors(1);
          logError(
              BaseMessages.getString(PKG, "ActionMssqlBulkLoad.Error.FileNotExists", realFilename));
        }
      } catch (Exception e) {
        // An unexpected error occurred
        result.setNrErrors(1);
        logError(BaseMessages.getString(PKG, "ActionMssqlBulkLoad.UnexpectedError.Label"), e);
      } finally {
        try {
          if (fileObject != null) {
            fileObject.close();
          }
        } catch (Exception e) {
          // Ignore errors
        }
      }
    } else {
      // No file was specified
      result.setNrErrors(1);
      logError(BaseMessages.getString(PKG, "ActionMssqlBulkLoad.Nofilename.Label"));
    }
    return result;
  }

  public String getRealLineterminated() {
    return resolve(getLineTerminated());
  }

  public String getRealFieldTerminator() {
    return resolve(getFieldTerminator());
  }

  public String getRealOrderBy() {
    return resolve(getOrderBy());
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    ResourceReference reference = null;
    DatabaseMeta dbMeta = null;
    if (connection != null) {
      dbMeta = parentWorkflowMeta.findDatabase(connection, getVariables());
      reference = new ResourceReference(this);
      references.add(reference);
      reference.getEntries().add(new ResourceEntry(dbMeta.getHostname(), ResourceType.SERVER));
      reference
          .getEntries()
          .add(new ResourceEntry(dbMeta.getDatabaseName(), ResourceType.DATABASENAME));
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
