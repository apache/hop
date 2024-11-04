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

package org.apache.hop.pipeline.transforms.vertica.bulkloader;

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
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.StringUtil;
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
    id = "VerticaBulkLoader",
    image = "vertica.svg",
    name = "i18n::BaseTransform.TypeLongDesc.VerticaBulkLoaderMessage",
    description = "i18n::BaseTransform.TypeTooltipDesc.VerticaBulkLoaderMessage",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Bulk",
    documentationUrl = "pipeline/transforms/verticabulkloader.html",
    isIncludeJdbcDrivers = true,
    classLoaderGroup = "vertica5",
    actionTransformTypes = {ActionTransformType.RDBMS, ActionTransformType.OUTPUT})
public class VerticaBulkLoaderMeta
    extends BaseTransformMeta<VerticaBulkLoader, VerticaBulkLoaderData> {
  private static final Class<?> PKG = VerticaBulkLoaderMeta.class;

  @HopMetadataProperty(
      key = "connection",
      injectionKey = "CONNECTIONNAME",
      injectionKeyDescription = "VerticaBulkLoader.Injection.CONNECTIONNAME",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String connection;

  @HopMetadataProperty(
      key = "schema",
      injectionKey = "SCHEMANAME",
      injectionKeyDescription = "VerticaBulkLoader.Injection.SCHEMANAME",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_SCHEMA)
  private String schemaName;

  @HopMetadataProperty(
      key = "table",
      injectionKey = "TABLENAME",
      injectionKeyDescription = "VerticaBulkLoader.Injection.TABLENAME",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
  private String tablename;

  @HopMetadataProperty(
      key = "truncate",
      injectionKey = "TRUNCATE_TABLE",
      injectionKeyDescription = "VerticaBulkLoader.Injection.TruncateTable.Field",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TRUNCATE)
  private boolean truncateTable;

  @HopMetadataProperty(
      key = "only_when_have_rows",
      injectionKey = "ONLY_WHEN_HAVE_ROWS",
      injectionKeyDescription = "VerticaBulkLoader.Inject.OnlyWhenHaveRows.Field")
  private boolean onlyWhenHaveRows;

  @HopMetadataProperty(
      key = "direct",
      injectionKey = "DIRECT",
      injectionKeyDescription = "VerticaBulkLoader.Injection.DIRECT")
  private boolean direct = true;

  @HopMetadataProperty(
      key = "abort_on_error",
      injectionKey = "ABORTONERROR",
      injectionKeyDescription = "VerticaBulkLoader.Injection.ABORTONERROR")
  private boolean abortOnError = true;

  @HopMetadataProperty(
      key = "exceptions_filename",
      injectionKey = "EXCEPTIONSFILENAME",
      injectionKeyDescription = "VerticaBulkLoader.Injection.EXCEPTIONSFILENAME")
  private String exceptionsFileName;

  @HopMetadataProperty(
      key = "rejected_data_filename",
      injectionKey = "REJECTEDDATAFILENAME",
      injectionKeyDescription = "VerticaBulkLoader.Injection.REJECTEDDATAFILENAME")
  private String rejectedDataFileName;

  @HopMetadataProperty(
      key = "stream_name",
      injectionKey = "STREAMNAME",
      injectionKeyDescription = "VerticaBulkLoader.Injection.STREAMNAME")
  private String streamName;

  /** Do we explicitly select the fields to update in the database */
  @HopMetadataProperty(key = "specify_fields", injectionKeyDescription = "")
  private boolean specifyFields;

  @HopMetadataProperty(
      groupKey = "fields",
      key = "field",
      injectionGroupKey = "FIELDS",
      injectionGroupDescription = "VerticaBulkLoader.Injection.FIELDS",
      injectionKey = "FIELDSTREAM",
      injectionKeyDescription = "VerticaBulkLoader.Injection.FIELDSTREAM")
  /** Fields containing the values in the input stream to insert */
  private List<VerticaBulkLoaderField> fields;

  public List<VerticaBulkLoaderField> getFields() {
    return fields;
  }

  public void setFields(List<VerticaBulkLoaderField> fields) {
    this.fields = fields;
  }

  @HopMetadataProperty(
      groupKey = "fields",
      key = "field",
      injectionGroupKey = "FIELDS",
      injectionGroupDescription = "VerticaBulkLoader.Injection.FIELDS",
      injectionKey = "FIELDDATABASE",
      injectionKeyDescription = "VerticaBulkLoader.Injection.FIELDDATABASE")
  /** Fields in the table to insert */
  private String[] fieldDatabase;

  public VerticaBulkLoaderMeta() {
    super(); // allocate BaseTransformMeta

    fields = new ArrayList<>();
  }

  @Override
  public Object clone() {
    return super.clone();
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

  /*
   */
  /**
   * @return Returns the database.
   */
  public DatabaseMeta getDatabaseMeta() {
    return null;
  }

  /**
   * @deprecated use {@link #getTableName()}
   */
  @Deprecated(since = "2.10")
  public String getTablename() {
    return getTableName();
  }

  /**
   * Returns the table name.
   *
   * @return
   */
  public String getTableName() {
    return tablename;
  }

  /**
   * @deprecated use {@link #setTableName}
   */
  @Deprecated(since = "2.10")
  public void setTablename(String name) {
    setTableName(name);
  }

  /**
   * @param name The tablename to set.
   */
  public void setTableName(String name) {
    this.tablename = name;
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

  /**
   * @return Returns the specify fields flag.
   */
  public boolean specifyFields() {
    return specifyFields;
  }

  public boolean isDirect() {
    return direct;
  }

  public void setDirect(boolean direct) {
    this.direct = direct;
  }

  public boolean isAbortOnError() {
    return abortOnError;
  }

  public void setAbortOnError(boolean abortOnError) {
    this.abortOnError = abortOnError;
  }

  public String getExceptionsFileName() {
    return exceptionsFileName;
  }

  public void setExceptionsFileName(String exceptionsFileName) {
    this.exceptionsFileName = exceptionsFileName;
  }

  public String getRejectedDataFileName() {
    return rejectedDataFileName;
  }

  public void setRejectedDataFileName(String rejectedDataFileName) {
    this.rejectedDataFileName = rejectedDataFileName;
  }

  public String getStreamName() {
    return streamName;
  }

  public void setStreamName(String streamName) {
    this.streamName = streamName;
  }

  public boolean isSpecifyFields() {
    return specifyFields;
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
                BaseMessages.getString(PKG, "VerticaBulkLoaderMeta.CheckResult.ConnectionExists"),
                transformMeta);
        remarks.add(cr);

        db = new Database(loggingObject, variables, databaseMeta);

        try {
          db.connect();

          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(PKG, "VerticaBulkLoaderMeta.CheckResult.ConnectionOk"),
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
                          PKG, "VerticaBulkLoaderMeta.CheckResult.TableAccessible", schemaTable),
                      transformMeta);
              remarks.add(cr);

              IRowMeta r = db.getTableFields(schemaTable);
              if (r != null) {
                cr =
                    new CheckResult(
                        ICheckResult.TYPE_RESULT_OK,
                        BaseMessages.getString(
                            PKG, "VerticaBulkLoaderMeta.CheckResult.TableOk", schemaTable),
                        transformMeta);
                remarks.add(cr);

                String error_message = "";
                boolean error_found = false;
                // OK, we have the table fields.
                // Now see what we can find as previous transform...
                if (prev != null && prev.size() > 0) {
                  cr =
                      new CheckResult(
                          ICheckResult.TYPE_RESULT_OK,
                          BaseMessages.getString(
                              PKG,
                              "VerticaBulkLoaderMeta.CheckResult.FieldsReceived",
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
                              "VerticaBulkLoaderMeta.CheckResult.FieldsNotFoundInOutput",
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
                                  PKG, "VerticaBulkLoaderMeta.CheckResult.AllFieldsFoundInOutput"),
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
                              "VerticaBulkLoaderMeta.CheckResult.FieldsSpecifiedNotInTable",
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
                                  PKG, "VerticaBulkLoaderMeta.CheckResult.AllFieldsFoundInOutput"),
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
                              "VerticaBulkLoaderMeta.CheckResult.FieldsNotFound",
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
                                  PKG, "VerticaBulkLoaderMeta.CheckResult.AllFieldsFound"),
                              transformMeta);
                      remarks.add(cr);
                    }
                  } else {
                    // Specifying the column names explicitly
                    for (int i = 0; i < fields.size(); i++) {
                      VerticaBulkLoaderField vbf = fields.get(i);
                      int idx = prev.indexOfValue(vbf.getFieldStream());
                      if (idx < 0) {
                        error_message += "\t\t" + vbf.getFieldStream() + Const.CR;
                        error_found = true;
                      }
                    }
                    if (error_found) {
                      error_message =
                          BaseMessages.getString(
                              PKG,
                              "VerticaBulkLoaderMeta.CheckResult.FieldsSpecifiedNotFound",
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
                                  PKG, "VerticaBulkLoaderMeta.CheckResult.AllFieldsFound"),
                              transformMeta);
                      remarks.add(cr);
                    }
                  }
                } else {
                  cr =
                      new CheckResult(
                          ICheckResult.TYPE_RESULT_ERROR,
                          BaseMessages.getString(PKG, "VerticaBulkLoaderMeta.CheckResult.NoFields"),
                          transformMeta);
                  remarks.add(cr);
                }
              } else {
                cr =
                    new CheckResult(
                        ICheckResult.TYPE_RESULT_ERROR,
                        BaseMessages.getString(
                            PKG, "VerticaBulkLoaderMeta.CheckResult.TableNotAccessible"),
                        transformMeta);
                remarks.add(cr);
              }
            } else {
              cr =
                  new CheckResult(
                      ICheckResult.TYPE_RESULT_ERROR,
                      BaseMessages.getString(
                          PKG, "VerticaBulkLoaderMeta.CheckResult.TableError", schemaTable),
                      transformMeta);
              remarks.add(cr);
            }
          } else {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_ERROR,
                    BaseMessages.getString(PKG, "VerticaBulkLoaderMeta.CheckResult.NoTableName"),
                    transformMeta);
            remarks.add(cr);
          }
        } catch (HopException e) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(
                      PKG, "VerticaBulkLoaderMeta.CheckResult.UndefinedError", e.getMessage()),
                  transformMeta);
          remarks.add(cr);
        } finally {
          db.disconnect();
        }
      } else {
        CheckResult cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "VerticaBulkLoaderMeta.CheckResult.NoConnection"),
                transformMeta);
        remarks.add(cr);
      }
    } catch (HopException e) {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "VerticaBulkLoaderMeta.CheckResult.UndefinedError", e.getMessage()),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "VerticaBulkLoaderMeta.CheckResult.ExpectedInputOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "VerticaBulkLoaderMeta.CheckResult.ExpectedInputError"),
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
      if (prev != null && prev.size() > 0) {
        if (!StringUtil.isEmpty(tablename)) {
          Database db = new Database(loggingObject, variables, databaseMeta);
          try {
            db.connect();

            String schemaTable =
                databaseMeta.getQuotedSchemaTableCombination(variables, schemaName, tablename);
            String cr_table = db.getDDL(schemaTable, prev);

            // Empty string means: nothing to do: set it to null...
            if (cr_table == null || cr_table.length() == 0) {
              cr_table = null;
            }

            retval.setSql(cr_table);
          } catch (HopDatabaseException dbe) {
            retval.setError(
                BaseMessages.getString(
                    PKG, "VerticaBulkLoaderMeta.Error.ErrorConnecting", dbe.getMessage()));
          } finally {
            db.disconnect();
          }
        } else {
          retval.setError(BaseMessages.getString(PKG, "VerticaBulkLoaderMeta.Error.NoTable"));
        }
      } else {
        retval.setError(BaseMessages.getString(PKG, "VerticaBulkLoaderMeta.Error.NoInput"));
      }
    } else {
      retval.setError(BaseMessages.getString(PKG, "VerticaBulkLoaderMeta.Error.NoConnection"));
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
                BaseMessages.getString(PKG, "VerticaBulkLoaderMeta.Exception.TableNotFound"));
          }
        } else {
          throw new HopException(
              BaseMessages.getString(PKG, "VerticaBulkLoaderMeta.Exception.TableNotSpecified"));
        }
      } catch (Exception e) {
        throw new HopException(
            BaseMessages.getString(PKG, "VerticaBulkLoaderMeta.Exception.ErrorGettingFields"), e);
      } finally {
        db.disconnect();
      }
    } else {
      throw new HopException(
          BaseMessages.getString(PKG, "VerticaBulkLoaderMeta.Exception.ConnectionNotDefined"));
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
   * Returns the schema name.
   *
   * @return
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
}
