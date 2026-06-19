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

package org.apache.hop.pipeline.transforms.cratedbbulkloader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.IProvidesModelerMeta;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "CrateDBBulkLoader",
    image = "cratedbbulkloader.svg",
    name = "i18n::BaseTransform.TypeLongDesc.CrateDBBulkLoaderMessage",
    description = "i18n::BaseTransform.TypeTooltipDesc.CrateDBBulkLoaderMessage",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Bulk",
    documentationUrl = "/pipeline/transforms/cratedb-bulkloader.html",
    isIncludeJdbcDrivers = true,
    classLoaderGroup = "crate-db",
    actionTransformTypes = {ActionTransformType.RDBMS})
public class CrateDBBulkLoaderMeta
    extends BaseTransformMeta<CrateDBBulkLoader, CrateDBBulkLoaderData>
    implements IProvidesModelerMeta {
  private static final Class<?> PKG = CrateDBBulkLoaderMeta.class;

  public static final String DEFAULT_CSV_DELIMITER = ",";
  public static final String CSV_RECORD_DELIMITER = "\n";
  public static final String CSV_ESCAPE_CHAR = "\"";
  public static final String ENCLOSURE = "\"";

  @HopMetadataProperty(
      key = "connection",
      injectionKey = "CONNECTIONNAME",
      injectionKeyDescription = "CrateDBBulkLoader.Injection.CONNECTIONNAME",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String connection;

  @HopMetadataProperty(
      key = "schema",
      injectionKey = "SCHEMANAME",
      injectionKeyDescription = "CrateDBBulkLoader.Injection.SCHEMANAME",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_SCHEMA)
  private String schemaName;

  @HopMetadataProperty(
      key = "table",
      injectionKey = "TABLENAME",
      injectionKeyDescription = "CrateDBBulkLoader.Injection.TABLENAME",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
  private String tablename;

  @HopMetadataProperty(
      key = "use_system_env_vars",
      injectionKey = "USE_SYSTEM_ENV_VARS",
      injectionKeyDescription = "")
  private boolean useSystemEnvVars;

  @HopMetadataProperty(key = "use_http_endpoint")
  private boolean useHttpEndpoint;

  @HopMetadataProperty(key = "http_endpoint")
  private String httpEndpoint;

  @HopMetadataProperty(key = "batch_size")
  private String batchSize;

  @HopMetadataProperty(
      key = "aws_access_key_id",
      injectionKey = "AWS_ACCESS_KEY_ID",
      injectionKeyDescription = "")
  private String awsAccessKeyId;

  @HopMetadataProperty(
      key = "aws_secret_access_key",
      injectionKey = "AWS_SECRET_ACCESS_KEY",
      injectionKeyDescription = "")
  private String awsSecretAccessKey;

  @HopMetadataProperty(
      key = "truncate",
      injectionKey = "TRUNCATE_TABLE",
      injectionKeyDescription = "CrateDBBulkLoader.Injection.TruncateTable.Field")
  private boolean truncateTable;

  @HopMetadataProperty(
      key = "only_when_have_rows",
      injectionKey = "ONLY_WHEN_HAVE_ROWS",
      injectionKeyDescription = "CrateDBBulkLoader.Inject.OnlyWhenHaveRows.Field")
  private boolean onlyWhenHaveRows;

  @HopMetadataProperty(
      key = "stream_to_s3",
      injectionKey = "STREAM_TO_S3",
      injectionKeyDescription = "")
  private boolean streamToS3Csv = true;

  @HopMetadataProperty(
      key = "volume_mapping",
      injectionKey = "VOLUME_MAPPING",
      injectionKeyDescription = "")
  private String volumeMapping;

  /** CSV: Trim whitespace */
  @HopMetadataProperty(key = "trim_whitespace", injectionKeyDescription = "")
  private boolean trimWhitespace;

  /** CSV: Convert column value to null if */
  @HopMetadataProperty(key = "null_if", injectionKeyDescription = "")
  private String nullIf;

  /**
   * CSV: Should the load fail if the column count in the row does not match the column count in the
   * table
   */
  @HopMetadataProperty(key = "error_column_mismatch", injectionKeyDescription = "")
  private boolean errorColumnMismatch;

  /** JSON: Strip nulls from JSON */
  @HopMetadataProperty(key = "strip_null", injectionKeyDescription = "")
  private boolean stripNull;

  /** Do we explicitly select the fields to update in the database */
  @HopMetadataProperty(key = "specify_fields", injectionKeyDescription = "")
  private boolean specifyFields;

  @HopMetadataProperty(
      key = "load_from_filename",
      injectionKey = "LOAD_FROM_FILENAME",
      injectionKeyDescription = "")
  private String readFromFilename;

  @HopMetadataProperty(key = "http_login")
  private String httpLogin;

  @HopMetadataProperty(key = "http_password", password = true)
  private String httpPassword;

  @HopMetadataProperty(
      groupKey = "fields",
      key = "field",
      injectionGroupKey = "FIELDS",
      injectionGroupDescription = "CrateDBBulkLoader.Injection.FIELDS",
      injectionKey = "FIELDSTREAM",
      injectionKeyDescription = "CrateDBBulkLoader.Injection.FIELDSTREAM")
  /** Fields containing the values in the input stream to insert */
  private List<CrateDBBulkLoaderField> fields;

  @HopMetadataProperty(
      groupKey = "fields",
      key = "field",
      injectionGroupKey = "FIELDS",
      injectionGroupDescription = "CrateDBBulkLoader.Injection.FIELDS",
      injectionKey = "FIELDDATABASE",
      injectionKeyDescription = "CrateDBBulkLoader.Injection.FIELDDATABASE")
  /** Fields in the table to insert */
  private String[] fieldDatabase;

  public CrateDBBulkLoaderMeta() {
    super(); // allocate BaseTransformMeta

    fields = new ArrayList<>();
  }

  @Override
  public Object clone() {
    return super.clone();
  }

  public String getHttpLogin() {
    return httpLogin;
  }

  public void setHttpLogin(String httpLogin) {
    this.httpLogin = httpLogin;
  }

  public String getHttpPassword() {
    return httpPassword;
  }

  public void setHttpPassword(String httpPassword) {
    this.httpPassword = httpPassword;
  }

  /**
   * @return returns the database connection name
   */
  public String getConnection() {
    return connection;
  }

  /**
   * sets the database connection name
   *
   * @param connection the database connection name to set
   */
  public void setConnection(String connection) {
    this.connection = connection;
  }

  public boolean isUseHttpEndpoint() {
    return useHttpEndpoint;
  }

  public void setUseHttpEndpoint(boolean useHttpEndpoint) {
    this.useHttpEndpoint = useHttpEndpoint;
  }

  public String getHttpEndpoint() {
    return httpEndpoint == null ? "" : httpEndpoint;
  }

  public void setHttpEndpoint(String httpEndpoint) {
    this.httpEndpoint = httpEndpoint;
  }

  public String getBatchSize() {
    return batchSize == null ? "0" : batchSize;
  }

  public void setBatchSize(String batchSize) {
    this.batchSize = batchSize;
  }

  /*
   */
  /**
   * @return Returns the database.
   */
  public DatabaseMeta getDatabaseMeta() {
    return null;
  }

  /**
   * @return Returns the tablename.
   */
  public String getTableName() {
    return tablename;
  }

  /**
   * @deprecated use {@link #getTableName()}
   */
  @Deprecated(since = "2.10")
  public String getTablename() {
    return getTableName();
  }

  /**
   * @param tablename The tablename to set.
   */
  public void setTablename(String tablename) {
    this.tablename = tablename;
  }

  /**
   * @return Returns the truncate table flag.
   */
  public boolean isTruncateTable() {
    return truncateTable;
  }

  /**
   * @param truncateTable The truncate table flag to set.
   */
  public void setTruncateTable(boolean truncateTable) {
    this.truncateTable = truncateTable;
  }

  /**
   * @return Returns the onlyWhenHaveRows flag.
   */
  public boolean isOnlyWhenHaveRows() {
    return onlyWhenHaveRows;
  }

  /**
   * @param onlyWhenHaveRows The onlyWhenHaveRows to set.
   */
  public void setOnlyWhenHaveRows(boolean onlyWhenHaveRows) {
    this.onlyWhenHaveRows = onlyWhenHaveRows;
  }

  /**
   * @param specifyFields The specify fields flag to set.
   */
  public void setSpecifyFields(boolean specifyFields) {
    this.specifyFields = specifyFields;
  }

  public boolean isStreamToS3Csv() {
    return streamToS3Csv;
  }

  public void setStreamToS3Csv(boolean streamToS3Csv) {
    this.streamToS3Csv = streamToS3Csv;
  }

  public String getVolumeMapping() {
    return volumeMapping;
  }

  public void setVolumeMapping(String volumeMapping) {
    this.volumeMapping = volumeMapping;
  }

  /**
   * CSV:
   *
   * @return Should whitespace in the fields be trimmed
   */
  public boolean isTrimWhitespace() {
    return trimWhitespace;
  }

  /**
   * CSV: Set if the whitespace in the files should be trimmmed
   *
   * @param trimWhitespace true/false
   */
  public void setTrimWhitespace(boolean trimWhitespace) {
    this.trimWhitespace = trimWhitespace;
  }

  /**
   * CSV:
   *
   * @return Comma delimited list of strings to convert to Null
   */
  public String getNullIf() {
    return nullIf;
  }

  /**
   * CSV: Set the string constants to convert to Null
   *
   * @param nullIf Comma delimited list of constants
   */
  public void setNullIf(String nullIf) {
    this.nullIf = nullIf;
  }

  /**
   * CSV:
   *
   * @return Should the load error if the number of columns in the table and in the CSV do not match
   */
  public boolean isErrorColumnMismatch() {
    return errorColumnMismatch;
  }

  /**
   * CSV: Set if the load should error if the number of columns in the table and in the CSV do not
   * match
   *
   * @param errorColumnMismatch true/false
   */
  public void setErrorColumnMismatch(boolean errorColumnMismatch) {
    this.errorColumnMismatch = errorColumnMismatch;
  }

  /**
   * JSON:
   *
   * @return Should null values be stripped out of the JSON
   */
  public boolean isStripNull() {
    return stripNull;
  }

  /**
   * JSON: Set if null values should be stripped out of the JSON
   *
   * @param stripNull true/false
   */
  public void setStripNull(boolean stripNull) {
    this.stripNull = stripNull;
  }

  public String getReadFromFilename() {
    return readFromFilename;
  }

  public void setReadFromFilename(String readFromFilename) {
    this.readFromFilename = readFromFilename;
  }

  public List<CrateDBBulkLoaderField> getFields() {
    return fields;
  }

  public void setFields(List<CrateDBBulkLoaderField> fields) {
    this.fields = fields;
  }

  /**
   * @return Returns the specify fields flag.
   */
  public boolean specifyFields() {
    return specifyFields;
  }

  public boolean isSpecifyFields() {
    return specifyFields;
  }

  public String getAwsAccessKeyId() {
    return awsAccessKeyId;
  }

  public void setAwsAccessKeyId(String awsAccessKeyId) {
    this.awsAccessKeyId = awsAccessKeyId;
  }

  public String getAwsSecretAccessKey() {
    return awsSecretAccessKey;
  }

  public void setAwsSecretAccessKey(String awsSecretAccessKey) {
    this.awsSecretAccessKey = awsSecretAccessKey;
  }

  public boolean isUseSystemEnvVars() {
    return useSystemEnvVars;
  }

  public void setUseSystemEnvVars(boolean useSystemEnvVars) {
    this.useSystemEnvVars = useSystemEnvVars;
  }

  @Override
  public void setDefault() {
    tablename = "";

    // To be compatible with pre-v3.2 (SB)
    specifyFields = false;
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

    Database db = null;

    try {

      DatabaseMeta databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connection));

      if (databaseMeta != null) {
        CheckResult cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "CrateDBBulkLoaderMeta.CheckResult.ConnectionExists"),
                transformMeta);
        remarks.add(cr);

        db = new Database(loggingObject, variables, databaseMeta);

        try {
          db.connect();

          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(PKG, "CrateDBBulkLoaderMeta.CheckResult.ConnectionOk"),
                  transformMeta);
          remarks.add(cr);

          if (!StringUtil.isEmpty(tablename)) {
            String schemaTable =
                databaseMeta.getQuotedSchemaTableCombination(
                    variables, db.resolve(schemaName), db.resolve(tablename));
            // Check if this table exists...
            String realSchemaName = db.resolve(schemaName);
            String realTableName = db.resolve(tablename);
            if (db.checkTableExists(realSchemaName, realTableName)) {
              cr =
                  new CheckResult(
                      ICheckResult.TYPE_RESULT_OK,
                      BaseMessages.getString(
                          PKG, "CrateDBBulkLoaderMeta.CheckResult.TableAccessible", schemaTable),
                      transformMeta);
              remarks.add(cr);

              IRowMeta r = db.getTableFields(schemaTable);
              if (r != null) {
                cr =
                    new CheckResult(
                        ICheckResult.TYPE_RESULT_OK,
                        BaseMessages.getString(
                            PKG, "CrateDBBulkLoaderMeta.CheckResult.TableOk", schemaTable),
                        transformMeta);
                remarks.add(cr);

                String error_message = "";
                boolean error_found = false;
                // OK, we have the table fields.
                // Now see what we can find as previous transform...
                if (prev != null && !prev.isEmpty()) {
                  cr =
                      new CheckResult(
                          ICheckResult.TYPE_RESULT_OK,
                          BaseMessages.getString(
                              PKG,
                              "CrateDBBulkLoaderMeta.CheckResult.FieldsReceived",
                              "" + prev.size()),
                          transformMeta);
                  remarks.add(cr);

                  if (!specifyFields()) {
                    // Starting from prev...
                    for (int i = 0; i < prev.size(); i++) {
                      IValueMeta pv = prev.getValueMeta(i);
                      int idx = r.indexOfValue(pv.getName());
                      if (idx < 0) {
                        error_message +=
                            "\t\t" + pv.getName() + " (" + pv.getTypeDesc() + ")" + Const.CR;
                        error_found = true;
                      }
                    }
                    if (error_found) {
                      error_message =
                          BaseMessages.getString(
                              PKG,
                              "CrateDBBulkLoaderMeta.CheckResult.FieldsNotFoundInOutput",
                              error_message);

                      cr =
                          new CheckResult(
                              ICheckResult.TYPE_RESULT_ERROR, error_message, transformMeta);
                      remarks.add(cr);
                    } else {
                      cr =
                          new CheckResult(
                              ICheckResult.TYPE_RESULT_OK,
                              BaseMessages.getString(
                                  PKG, "CrateDBBulkLoaderMeta.CheckResult.AllFieldsFoundInOutput"),
                              transformMeta);
                      remarks.add(cr);
                    }
                  } else {
                    // Specifying the column names explicitly
                    for (int i = 0; i < getFieldDatabase().length; i++) {
                      int idx = r.indexOfValue(getFieldDatabase()[i]);
                      if (idx < 0) {
                        error_message += "\t\t" + getFieldDatabase()[i] + Const.CR;
                        error_found = true;
                      }
                    }
                    if (error_found) {
                      error_message =
                          BaseMessages.getString(
                              PKG,
                              "CrateDBBulkLoaderMeta.CheckResult.FieldsSpecifiedNotInTable",
                              error_message);

                      cr =
                          new CheckResult(
                              ICheckResult.TYPE_RESULT_ERROR, error_message, transformMeta);
                      remarks.add(cr);
                    } else {
                      cr =
                          new CheckResult(
                              ICheckResult.TYPE_RESULT_OK,
                              BaseMessages.getString(
                                  PKG, "CrateDBBulkLoaderMeta.CheckResult.AllFieldsFoundInOutput"),
                              transformMeta);
                      remarks.add(cr);
                    }
                  }

                  error_message = "";
                  if (!specifyFields()) {
                    // Starting from table fields in r...
                    for (int i = 0; i < getFieldDatabase().length; i++) {
                      IValueMeta rv = r.getValueMeta(i);
                      int idx = prev.indexOfValue(rv.getName());
                      if (idx < 0) {
                        error_message +=
                            "\t\t" + rv.getName() + " (" + rv.getTypeDesc() + ")" + Const.CR;
                        error_found = true;
                      }
                    }
                    if (error_found) {
                      error_message =
                          BaseMessages.getString(
                              PKG,
                              "CrateDBBulkLoaderMeta.CheckResult.FieldsNotFound",
                              error_message);

                      cr =
                          new CheckResult(
                              ICheckResult.TYPE_RESULT_WARNING, error_message, transformMeta);
                      remarks.add(cr);
                    } else {
                      cr =
                          new CheckResult(
                              ICheckResult.TYPE_RESULT_OK,
                              BaseMessages.getString(
                                  PKG, "CrateDBBulkLoaderMeta.CheckResult.AllFieldsFound"),
                              transformMeta);
                      remarks.add(cr);
                    }
                  } else {
                    // Specifying the column names explicitly
                    for (CrateDBBulkLoaderField vbf : fields) {
                      int idx = prev.indexOfValue(vbf.getStreamField());
                      if (idx < 0) {
                        error_message += "\t\t" + vbf.getStreamField() + Const.CR;
                        error_found = true;
                      }
                    }
                    if (error_found) {
                      error_message =
                          BaseMessages.getString(
                              PKG,
                              "CrateDBBulkLoaderMeta.CheckResult.FieldsSpecifiedNotFound",
                              error_message);

                      cr =
                          new CheckResult(
                              ICheckResult.TYPE_RESULT_ERROR, error_message, transformMeta);
                      remarks.add(cr);
                    } else {
                      cr =
                          new CheckResult(
                              ICheckResult.TYPE_RESULT_OK,
                              BaseMessages.getString(
                                  PKG, "CrateDBBulkLoaderMeta.CheckResult.AllFieldsFound"),
                              transformMeta);
                      remarks.add(cr);
                    }
                  }
                } else {
                  cr =
                      new CheckResult(
                          ICheckResult.TYPE_RESULT_ERROR,
                          BaseMessages.getString(PKG, "CrateDBBulkLoaderMeta.CheckResult.NoFields"),
                          transformMeta);
                  remarks.add(cr);
                }
              } else {
                cr =
                    new CheckResult(
                        ICheckResult.TYPE_RESULT_ERROR,
                        BaseMessages.getString(
                            PKG, "CrateDBBulkLoaderMeta.CheckResult.TableNotAccessible"),
                        transformMeta);
                remarks.add(cr);
              }
            } else {
              cr =
                  new CheckResult(
                      ICheckResult.TYPE_RESULT_ERROR,
                      BaseMessages.getString(
                          PKG, "CrateDBBulkLoaderMeta.CheckResult.TableError", schemaTable),
                      transformMeta);
              remarks.add(cr);
            }
          } else {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_ERROR,
                    BaseMessages.getString(PKG, "CrateDBBulkLoaderMeta.CheckResult.NoTableName"),
                    transformMeta);
            remarks.add(cr);
          }
        } catch (HopException e) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(
                      PKG, "CrateDBBulkLoaderMeta.CheckResult.UndefinedError", e.getMessage()),
                  transformMeta);
          remarks.add(cr);
        } finally {
          db.disconnect();
        }
      } else {
        CheckResult cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "CrateDBBulkLoaderMeta.CheckResult.NoConnection"),
                transformMeta);
        remarks.add(cr);
      }
    } catch (HopException e) {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "CrateDBBulkLoaderMeta.CheckResult.UndefinedError", e.getMessage()),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "CrateDBBulkLoaderMeta.CheckResult.ExpectedInputOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "CrateDBBulkLoaderMeta.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    }
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

    try {
      DatabaseMeta databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connection));

      // The values that are entering this transform are in "prev":
      if (prev != null) {
        for (int i = 0; i < prev.size(); i++) {
          IValueMeta v = prev.getValueMeta(i);
          DatabaseImpact ii =
              new DatabaseImpact(
                  DatabaseImpact.TYPE_IMPACT_WRITE,
                  pipelineMeta.getName(),
                  transformMeta.getName(),
                  databaseMeta.getDatabaseName(),
                  tablename,
                  v.getName(),
                  v.getName(),
                  v != null ? v.getOrigin() : "?",
                  "",
                  "Type = " + v.toStringMeta());
          impact.add(ii);
        }
      }
    } catch (HopException e) {
      throw new HopTransformException(
          "Unable to get databaseMeta for connection: " + Const.CR + variables.resolve(connection));
    }
  }

  @Override
  public SqlStatement getSqlStatements(
      IVariables variables,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      IHopMetadataProvider metadataProvider) {

    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(connection, variables);

    SqlStatement retval =
        new SqlStatement(transformMeta.getName(), databaseMeta, null); // default: nothing to do!

    if (databaseMeta != null) {
      if (prev != null && !prev.isEmpty()) {
        if (!StringUtil.isEmpty(tablename)) {
          Database db = new Database(loggingObject, variables, databaseMeta);
          try {
            db.connect();

            String schemaTable =
                databaseMeta.getQuotedSchemaTableCombination(variables, schemaName, tablename);
            String cr_table = db.getDDL(schemaTable, prev);

            // Empty string means: nothing to do: set it to null...
            if (Utils.isEmpty(cr_table)) {
              cr_table = null;
            }

            retval.setSql(cr_table);
          } catch (HopDatabaseException dbe) {
            retval.setError(
                BaseMessages.getString(
                    PKG, "CrateDBBulkLoaderMeta.Error.ErrorConnecting", dbe.getMessage()));
          } finally {
            db.disconnect();
          }
        } else {
          retval.setError(BaseMessages.getString(PKG, "CrateDBBulkLoaderMeta.Error.NoTable"));
        }
      } else {
        retval.setError(BaseMessages.getString(PKG, "CrateDBBulkLoaderMeta.Error.NoInput"));
      }
    } else {
      retval.setError(BaseMessages.getString(PKG, "CrateDBBulkLoaderMeta.Error.NoConnection"));
    }

    return retval;
  }

  @Override
  public IRowMeta getRequiredFields(IVariables variables) throws HopException {
    String realTableName = variables.resolve(tablename);
    String realSchemaName = variables.resolve(schemaName);

    DatabaseMeta databaseMeta =
        getParentTransformMeta().getParentPipelineMeta().findDatabase(connection, variables);

    if (databaseMeta != null) {
      Database db = new Database(loggingObject, variables, databaseMeta);
      try {
        db.connect();

        if (!StringUtil.isEmpty(realTableName)) {
          String schemaTable =
              databaseMeta.getQuotedSchemaTableCombination(
                  variables, realSchemaName, realTableName);

          // Check if this table exists...
          if (db.checkTableExists(realSchemaName, realTableName)) {
            return db.getTableFields(schemaTable);
          } else {
            throw new HopException(
                BaseMessages.getString(PKG, "CrateDBBulkLoaderMeta.Exception.TableNotFound"));
          }
        } else {
          throw new HopException(
              BaseMessages.getString(PKG, "CrateDBBulkLoaderMeta.Exception.TableNotSpecified"));
        }
      } catch (Exception e) {
        throw new HopException(
            BaseMessages.getString(PKG, "CrateDBBulkLoaderMeta.Exception.ErrorGettingFields"), e);
      } finally {
        db.disconnect();
      }
    } else {
      throw new HopException(
          BaseMessages.getString(PKG, "CrateDBBulkLoaderMeta.Exception.ConnectionNotDefined"));
    }
  }

  /**
   * @return Fields containing the fieldnames in the database insert.
   */
  public String[] getFieldDatabase() {
    return fieldDatabase;
  }

  /**
   * @param fieldDatabase The fields containing the names of the fields to insert.
   */
  public void setFieldDatabase(String[] fieldDatabase) {
    this.fieldDatabase = fieldDatabase;
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

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  @Override
  public String getMissingDatabaseConnectionInformationMessage() {
    // use default message
    return null;
  }

  @Override
  public RowMeta getRowMeta(IVariables variables, ITransformData transformData) {
    return (RowMeta) ((CrateDBBulkLoaderData) transformData).getInsertRowMeta();
  }

  @Override
  public List<String> getDatabaseFields() {
    List<String> items = Collections.emptyList();
    if (specifyFields()) {
      items = new ArrayList<>();
      for (CrateDBBulkLoaderField vbf : fields) {
        items.add(vbf.getDatabaseField());
      }
    }
    return items;
  }

  @Override
  public List<String> getStreamFields() {
    List<String> items = Collections.emptyList();
    if (specifyFields()) {
      items = new ArrayList<>();
      for (CrateDBBulkLoaderField vbf : fields) {
        items.add(vbf.getStreamField());
      }
    }
    return items;
  }
}
