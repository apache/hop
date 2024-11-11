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
package org.apache.hop.pipeline.transforms.orabulkloader;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "OraBulkLoader",
    image = "orabulkloader.svg",
    description = "i18n::OraBulkLoader.Description",
    name = "i18n::OraBulkLoader.Name",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Bulk",
    keywords = "i18n::OraBulkLoader.Keywords",
    documentationUrl = "/pipeline/transforms/orabulkloader.html",
    actionTransformTypes = {ActionTransformType.RDBMS, ActionTransformType.OUTPUT})
public class OraBulkLoaderMeta extends BaseTransformMeta<OraBulkLoader, OraBulkLoaderData> {
  private static final Class<?> PKG =
      OraBulkLoaderMeta.class; // for i18n purposes, needed by Translator2!!

  private static final int DEFAULT_COMMIT_SIZE = 100000; // The bigger the better for Oracle
  private static final int DEFAULT_BIND_SIZE = 0;
  private static final int DEFAULT_READ_SIZE = 0;
  private static final int DEFAULT_MAX_ERRORS = 50;

  /** Database connection */
  @HopMetadataProperty(
      key = "connection",
      injectionKeyDescription = "OraBulkLoader.Injection.Connection",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String connection;

  /** Schema for the target */
  @HopMetadataProperty(
      key = "shema",
      injectionKey = "SCHEMA_NAME",
      injectionKeyDescription = "OraBulkLoader.Injection.Schema",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_SCHEMA)
  private String schemaName;

  /** Table for the target */
  @HopMetadataProperty(
      key = "table",
      injectionKey = "TABLE_NAME",
      injectionKeyDescription = "OraBulkLoader.Injection.Table",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
  private String tableName;

  /** Path to the sqlldr utility */
  @HopMetadataProperty(
      key = "sqlldr",
      injectionKey = "SQLLDR_PATH",
      injectionKeyDescription = "OraBulkLoader.Injection.Sqlldr")
  private String sqlldr;

  /** Path to the control file */
  @HopMetadataProperty(
      key = "control_file",
      injectionKey = "CONTROL_FILE",
      injectionKeyDescription = "OraBulkLoader.Injection.ControlFile")
  private String controlFile;

  /** Path to the data file */
  @HopMetadataProperty(
      key = "data_file",
      injectionKey = "DATA_FILE",
      injectionKeyDescription = "OraBulkLoader.Injection.DataFile")
  private String dataFile;

  /** Path to the log file */
  @HopMetadataProperty(
      key = "log_file",
      injectionKey = "LOG_FILE",
      injectionKeyDescription = "OraBulkLoader.Injection.LogFile")
  private String logFile;

  /** Path to the bad file */
  @HopMetadataProperty(
      key = "bad_file",
      injectionKey = "BAD_FILE",
      injectionKeyDescription = "OraBulkLoader.Injection.BadFile")
  private String badFile;

  /** Path to the discard file */
  @HopMetadataProperty(
      key = "discard_file",
      injectionKey = "DISCARD_FILE",
      injectionKeyDescription = "OraBulkLoader.Injection.DiscardFile")
  private String discardFile;

  /** Commit size (ROWS) */
  @HopMetadataProperty(
      key = "commit",
      injectionKey = "COMMIT_SIZE",
      injectionKeyDescription = "OraBulkLoader.Injection.CommitSize")
  private String commitSize;

  /** Bind size */
  @HopMetadataProperty(
      key = "bind_size",
      injectionKey = "BIND_SIZE",
      injectionKeyDescription = "OraBulkLoader.Injection.BindSize")
  private String bindSize;

  /** Read size */
  @HopMetadataProperty(
      key = "read_size",
      injectionKey = "READ_SIZE",
      injectionKeyDescription = "OraBulkLoader.Injection.ReadSize")
  private String readSize;

  /** Maximum errors */
  @HopMetadataProperty(
      key = "errors",
      injectionKey = "MAX_ERRORS",
      injectionKeyDescription = "OraBulkLoader.Injection.MaxErros")
  private String maxErrors;

  /** Load method */
  @HopMetadataProperty(
      key = "load_method",
      injectionKey = "LOAD_METHOD",
      injectionKeyDescription = "OraBulkLoader.Injection.LoadMethod")
  private String loadMethod;

  /** Load action */
  @HopMetadataProperty(
      key = "load_action",
      injectionKey = "LOAD_ACTION",
      injectionKeyDescription = "OraBulkLoader.Injection.LoadAction")
  private String loadAction;

  /** Encoding to use */
  @HopMetadataProperty(
      key = "encoding",
      injectionKey = "ENCODING",
      injectionKeyDescription = "OraBulkLoader.Injection.Encoding")
  private String encoding;

  /** Character set name used for Oracle */
  @HopMetadataProperty(
      key = "character_set",
      injectionKey = "ORACLE_CHARSET_NAME",
      injectionKeyDescription = "OraBulkLoader.Injection.CharacterSet")
  private String characterSetName;

  /** Direct Path? */
  @HopMetadataProperty(
      key = "direct_path",
      injectionKey = "DIRECT_PATH",
      injectionKeyDescription = "OraBulkLoader.Injection.DirectPath")
  private boolean directPath;

  /** Erase files after use */
  @HopMetadataProperty(
      key = "erase_files",
      injectionKey = "ERASE_FILES",
      injectionKeyDescription = "OraBulkLoader.Injection.EraseFiles")
  private boolean eraseFiles;

  /** Fails when sqlldr returns a warning * */
  @HopMetadataProperty(
      key = "fail_on_warning",
      injectionKey = "FAIL_ON_WARNING",
      injectionKeyDescription = "OraBulkLoader.Injection.FailOnWarning")
  private boolean failOnWarning;

  /** Fails when sqlldr returns anything else than a warning or OK * */
  @HopMetadataProperty(
      key = "fail_on_error",
      injectionKey = "FAIL_ON_ERROR",
      injectionKeyDescription = "OraBulkLoader.Injection.FailOnError")
  private boolean failOnError;

  /** Allow Oracle to load data in parallel * */
  @HopMetadataProperty(
      key = "parallel",
      injectionKey = "PARALLEL",
      injectionKeyDescription = "OraBulkLoader.Injection.Parallel")
  private boolean parallel;

  /** If not empty, use this record terminator instead of default one * */
  @HopMetadataProperty(
      key = "alt_rec_term",
      injectionKey = "RECORD_TERMINATOR",
      injectionKeyDescription = "OraBulkLoader.Injection.RecordTerminator")
  private String altRecordTerm;

  /** Field value to dateMask after lookup */
  @HopMetadataProperty(
      key = "mapping",
      injectionGroupKey = "DATABASE_FIELDS",
      injectionGroupDescription = "OraBulkLoader.Injection.Mapping")
  private List<OraBulkLoaderMappingMeta> mappings;

  /*
   * Do not translate following values!!! They are will end up in the job export.
   */
  public static final String ACTION_APPEND = "APPEND";
  public static final String ACTION_INSERT = "INSERT";
  public static final String ACTION_REPLACE = "REPLACE";
  public static final String ACTION_TRUNCATE = "TRUNCATE";

  /*
   * Do not translate following values!!! They are will end up in the job export.
   */
  public static final String METHOD_AUTO_CONCURRENT = "AUTO_CONCURRENT";
  public static final String METHOD_AUTO_END = "AUTO_END";
  public static final String METHOD_MANUAL = "MANUAL";

  /*
   * Do not translate following values!!! They are will end up in the job export.
   */
  public static final String DATE_MASK_DATE = "DATE";
  public static final String DATE_MASK_DATETIME = "DATETIME";

  public OraBulkLoaderMeta() {
    super();
  }

  public int getCommitSizeAsInt(IVariables variables) {
    try {
      return Integer.valueOf(variables.resolve(getCommitSize()));
    } catch (NumberFormatException ex) {
      return DEFAULT_COMMIT_SIZE;
    }
  }

  /**
   * @return Returns the commitSize.
   */
  public String getCommitSize() {
    return commitSize;
  }

  /**
   * @param commitSize The commitSize to set.
   */
  public void setCommitSize(String commitSize) {
    this.commitSize = commitSize;
  }

  public String getConnection() {
    return connection;
  }

  public void setConnection(String connection) {
    this.connection = connection;
  }

  /**
   * @return Returns the tableName.
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName The tableName to set.
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getSqlldr() {
    return sqlldr;
  }

  public void setSqlldr(String sqlldr) {
    this.sqlldr = sqlldr;
  }

  public List<OraBulkLoaderMappingMeta> getMappings() {
    return mappings;
  }

  public void setMappings(List<OraBulkLoaderMappingMeta> mappings) {
    this.mappings = mappings;
  }

  public boolean isFailOnWarning() {
    return failOnWarning;
  }

  public void setFailOnWarning(boolean failOnWarning) {
    this.failOnWarning = failOnWarning;
  }

  public boolean isFailOnError() {
    return failOnError;
  }

  public void setFailOnError(boolean failOnError) {
    this.failOnError = failOnError;
  }

  public String getCharacterSetName() {
    return characterSetName;
  }

  public void setCharacterSetName(String characterSetName) {
    this.characterSetName = characterSetName;
  }

  public String getAltRecordTerm() {
    return altRecordTerm;
  }

  public void setAltRecordTerm(String altRecordTerm) {
    this.altRecordTerm = altRecordTerm;
  }

  @Override
  public void setDefault() {
    connection = null;
    commitSize = Integer.toString(DEFAULT_COMMIT_SIZE);
    bindSize = Integer.toString(DEFAULT_BIND_SIZE); // Use platform default
    readSize = Integer.toString(DEFAULT_READ_SIZE); // Use platform default
    maxErrors = Integer.toString(DEFAULT_MAX_ERRORS);
    schemaName = "";
    tableName = BaseMessages.getString(PKG, "OraBulkLoaderMeta.DefaultTableName");
    loadMethod = METHOD_AUTO_END;
    loadAction = ACTION_APPEND;
    sqlldr = "sqlldr";
    controlFile = "control${Internal.Transform.CopyNr}.cfg";
    dataFile = "load${Internal.Transform.CopyNr}.dat";
    logFile = "";
    badFile = "";
    discardFile = "";
    encoding = "";
    directPath = false;
    eraseFiles = true;
    characterSetName = "";
    failOnWarning = false;
    failOnError = false;
    parallel = false;
    altRecordTerm = "";
    mappings = new ArrayList<>();
  }

  @Override
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
    String errorMessage = "";

    DatabaseMeta databaseMeta =
        getParentTransformMeta().getParentPipelineMeta().findDatabase(connection, variables);

    if (databaseMeta != null) {
      Database db = new Database(loggingObject, variables, databaseMeta);
      try {
        db.connect();

        if (!Utils.isEmpty(tableName)) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(PKG, "OraBulkLoaderMeta.CheckResult.TableNameOK"),
                  transformMeta);
          remarks.add(cr);

          boolean first = true;
          boolean errorFound = false;
          errorMessage = "";

          // Check fields in table
          String schemaTable =
              databaseMeta.getQuotedSchemaTableCombination(variables, schemaName, tableName);
          IRowMeta rowMeta = db.getTableFields(schemaTable);
          if (rowMeta != null) {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(PKG, "OraBulkLoaderMeta.CheckResult.TableExists"),
                    transformMeta);
            remarks.add(cr);

            // How about the fields to insert/dateMask in the table?
            first = true;
            errorFound = false;
            errorMessage = "";

            for (int i = 0; i < mappings.size(); i++) {
              String field = mappings.get(i).getFieldTable();

              IValueMeta v = rowMeta.searchValueMeta(field);
              if (v == null) {
                if (first) {
                  first = false;
                  errorMessage +=
                      BaseMessages.getString(
                              PKG, "OraBulkLoaderMeta.CheckResult.MissingFieldsToLoadInTargetTable")
                          + Const.CR;
                }
                errorFound = true;
                errorMessage += "\t\t" + field + Const.CR;
              }
            }
            if (errorFound) {
              cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
            } else {
              cr =
                  new CheckResult(
                      ICheckResult.TYPE_RESULT_OK,
                      BaseMessages.getString(
                          PKG, "OraBulkLoaderMeta.CheckResult.AllFieldsFoundInTargetTable"),
                      transformMeta);
            }
            remarks.add(cr);
          } else {
            errorMessage =
                BaseMessages.getString(PKG, "OraBulkLoaderMeta.CheckResult.CouldNotReadTableInfo");
            cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
            remarks.add(cr);
          }
        }

        // Look up fields in the input stream <prev>
        if (prev != null && prev.size() > 0) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(
                      PKG,
                      "OraBulkLoaderMeta.CheckResult.TransformReceivingDatas",
                      prev.size() + ""),
                  transformMeta);
          remarks.add(cr);

          boolean first = true;
          errorMessage = "";
          boolean errorFound = false;

          for (int i = 0; i < mappings.size(); i++) {
            IValueMeta valueMeta = prev.searchValueMeta(mappings.get(i).getFieldStream());
            if (valueMeta == null) {
              if (first) {
                first = false;
                errorMessage +=
                    BaseMessages.getString(
                            PKG, "OraBulkLoaderMeta.CheckResult.MissingFieldsInInput")
                        + Const.CR;
              }
              errorFound = true;
              errorMessage += "\t\t" + mappings.get(i).getFieldStream() + Const.CR;
            }
          }
          if (errorFound) {
            cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
          } else {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(
                        PKG, "OraBulkLoaderMeta.CheckResult.AllFieldsFoundInInput"),
                    transformMeta);
          }
          remarks.add(cr);
        } else {
          errorMessage =
              BaseMessages.getString(PKG, "OraBulkLoaderMeta.CheckResult.MissingFieldsInInput3")
                  + Const.CR;
          cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
          remarks.add(cr);
        }
      } catch (HopException e) {
        errorMessage =
            BaseMessages.getString(PKG, "OraBulkLoaderMeta.CheckResult.DatabaseErrorOccurred")
                + e.getMessage();
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } finally {
        db.disconnect();
      }
    } else {
      errorMessage = BaseMessages.getString(PKG, "OraBulkLoaderMeta.CheckResult.InvalidConnection");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "OraBulkLoaderMeta.CheckResult.TransformReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "OraBulkLoaderMeta.CheckResult.NoInputError"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public SqlStatement getSqlStatements(
      IVariables variables,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(connection, variables);

    SqlStatement retval =
        new SqlStatement(transformMeta.getName(), databaseMeta, null); // default: nothing to do!

    if (databaseMeta != null) {
      if (prev != null && prev.size() > 0) {
        // Copy the row
        IRowMeta tableFields = new RowMeta();

        // Now change the field names
        for (int i = 0; i < mappings.size(); i++) {
          IValueMeta v = prev.searchValueMeta(mappings.get(i).getFieldStream());
          if (v != null) {
            IValueMeta tableField = v.clone();
            tableField.setName(mappings.get(i).getFieldTable());
            tableFields.addValueMeta(tableField);
          } else {
            throw new HopTransformException(
                "Unable to find field [" + mappings.get(i).getFieldTable() + "] in the input rows");
          }
        }

        if (!Utils.isEmpty(tableName)) {
          Database db = new Database(loggingObject, variables, databaseMeta);
          try {
            db.connect();

            String schemaTable =
                databaseMeta.getQuotedSchemaTableCombination(variables, schemaName, tableName);
            String sql = db.getDDL(schemaTable, tableFields, null, false, null, true);

            if (sql.length() == 0) {
              retval.setSql(null);
            } else {
              retval.setSql(sql);
            }
          } catch (HopException e) {
            retval.setError(
                BaseMessages.getString(PKG, "OraBulkLoaderMeta.GetSQL.ErrorOccurred")
                    + e.getMessage());
          }
        } else {
          retval.setError(
              BaseMessages.getString(PKG, "OraBulkLoaderMeta.GetSQL.NoTableDefinedOnConnection"));
        }
      } else {
        retval.setError(
            BaseMessages.getString(PKG, "OraBulkLoaderMeta.GetSQL.NotReceivingAnyFields"));
      }
    } else {
      retval.setError(BaseMessages.getString(PKG, "OraBulkLoaderMeta.GetSQL.NoConnectionDefined"));
    }

    return retval;
  }

  @Override
  public void analyseImpact(
      IVariables variables,
      List<DatabaseImpact> impact,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    DatabaseMeta databaseMeta =
        getParentTransformMeta().getParentPipelineMeta().findDatabase(connection, variables);

    if (prev != null) {
      /* DEBUG CHECK THIS */
      // Insert dateMask fields : read/write
      for (int i = 0; i < mappings.size(); i++) {
        IValueMeta valueMeta = prev.searchValueMeta(mappings.get(i).getFieldStream());

        DatabaseImpact ii =
            new DatabaseImpact(
                DatabaseImpact.TYPE_IMPACT_READ_WRITE,
                pipelineMeta.getName(),
                transformMeta.getName(),
                databaseMeta.getDatabaseName(),
                variables.resolve(tableName),
                mappings.get(i).getFieldTable(),
                mappings.get(i).getFieldStream(),
                valueMeta != null ? valueMeta.getOrigin() : "?",
                "",
                "Type = " + valueMeta.toStringMeta());
        impact.add(ii);
      }
    }
  }

  /**
   * @return Do we want direct path loading.
   */
  public boolean isDirectPath() {
    return directPath;
  }

  /**
   * @param directPath do we want direct path
   */
  public void setDirectPath(boolean directPath) {
    this.directPath = directPath;
  }

  @Override
  public IRowMeta getRequiredFields(IVariables variables) throws HopException {
    String realTableName = variables.resolve(tableName);
    String realSchemaName = variables.resolve(schemaName);
    DatabaseMeta databaseMeta =
        getParentTransformMeta().getParentPipelineMeta().findDatabase(connection, variables);

    if (databaseMeta != null) {
      Database database = new Database(loggingObject, variables, databaseMeta);
      try {
        database.connect();

        if (!Utils.isEmpty(realTableName)) {
          String schemaTable =
              databaseMeta.getQuotedSchemaTableCombination(
                  variables, realSchemaName, realTableName);

          // Check if this table exists...
          if (database.checkTableExists(realSchemaName, realTableName)) {
            return database.getTableFields(schemaTable);
          } else {
            throw new HopException(
                BaseMessages.getString(PKG, "OraBulkLoaderMeta.Exception.TableNotFound"));
          }
        } else {
          throw new HopException(
              BaseMessages.getString(PKG, "OraBulkLoaderMeta.Exception.TableNotSpecified"));
        }
      } catch (Exception e) {
        throw new HopException(
            BaseMessages.getString(PKG, "OraBulkLoaderMeta.Exception.ErrorGettingFields"), e);
      } finally {
        database.disconnect();
      }
    } else {
      throw new HopException(
          BaseMessages.getString(PKG, "OraBulkLoaderMeta.Exception.ConnectionNotDefined"));
    }
  }

  /**
   * @return the schemaName
   */
  public String getSchemaName() {
    return schemaName;
  }

  /**
   * @param schemaName the schemaName to set
   */
  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public String getBadFile() {
    return badFile;
  }

  public void setBadFile(String badFile) {
    this.badFile = badFile;
  }

  public String getControlFile() {
    return controlFile;
  }

  public void setControlFile(String controlFile) {
    this.controlFile = controlFile;
  }

  public String getDataFile() {
    return dataFile;
  }

  public void setDataFile(String dataFile) {
    this.dataFile = dataFile;
  }

  public String getDiscardFile() {
    return discardFile;
  }

  public void setDiscardFile(String discardFile) {
    this.discardFile = discardFile;
  }

  public String getLogFile() {
    return logFile;
  }

  public void setLogFile(String logFile) {
    this.logFile = logFile;
  }

  public void setLoadAction(String action) {
    this.loadAction = action;
  }

  public String getLoadAction() {
    return this.loadAction;
  }

  public void setLoadMethod(String method) {
    this.loadMethod = method;
  }

  public String getLoadMethod() {
    return this.loadMethod;
  }

  public String getEncoding() {
    return encoding;
  }

  public void setEncoding(String encoding) {
    this.encoding = encoding;
  }

  public String getDelimiter() {
    return ",";
  }

  public String getEnclosure() {
    return "\"";
  }

  public boolean isEraseFiles() {
    return eraseFiles;
  }

  public void setEraseFiles(boolean eraseFiles) {
    this.eraseFiles = eraseFiles;
  }

  public int getBindSizeAsInt(IVariables variables) {
    try {
      return Integer.valueOf(variables.resolve(getBindSize()));
    } catch (NumberFormatException ex) {
      return DEFAULT_BIND_SIZE;
    }
  }

  public String getBindSize() {
    return bindSize;
  }

  public void setBindSize(String bindSize) {
    this.bindSize = bindSize;
  }

  public int getMaxErrorsAsInt(IVariables variables) {
    try {
      return Integer.valueOf(variables.resolve(getMaxErrors()));
    } catch (NumberFormatException ex) {
      return DEFAULT_MAX_ERRORS;
    }
  }

  public String getMaxErrors() {
    return maxErrors;
  }

  public void setMaxErrors(String maxErrors) {
    this.maxErrors = maxErrors;
  }

  public int getReadSizeAsInt(IVariables variables) {
    try {
      return Integer.valueOf(variables.resolve(getReadSize()));
    } catch (NumberFormatException ex) {
      return DEFAULT_READ_SIZE;
    }
  }

  public String getReadSize() {
    return readSize;
  }

  public void setReadSize(String readSize) {
    this.readSize = readSize;
  }

  /**
   * @return the parallel
   */
  public boolean isParallel() {
    return parallel;
  }

  /**
   * @param parallel the parallel to set
   */
  public void setParallel(boolean parallel) {
    this.parallel = parallel;
  }
}
