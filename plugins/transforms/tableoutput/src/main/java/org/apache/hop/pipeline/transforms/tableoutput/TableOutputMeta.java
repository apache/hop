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

package org.apache.hop.pipeline.transforms.tableoutput;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
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
import org.apache.hop.core.row.value.ValueMetaInteger;
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
    id = "TableOutput",
    image = "tableoutput.svg",
    name = "i18n::TableOutput.Name",
    description = "i18n::TableOutput.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    keywords = "i18n::TableOutputMeta.keyword",
    documentationUrl = "/pipeline/transforms/tableoutput.html",
    actionTransformTypes = {ActionTransformType.OUTPUT, ActionTransformType.RDBMS})
@Getter
@Setter
public class TableOutputMeta extends BaseTransformMeta<TableOutput, TableOutputData> {
  private static final Class<?> PKG = TableOutputMeta.class;

  private static final String PARTITION_PER_DAY = "DAY";
  private static final String PARTITION_PER_MONTH = "MONTH";

  @HopMetadataProperty(
      key = "connection",
      injectionKeyDescription = "TableOutputMeta.Injection.Connection",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String connection;

  @HopMetadataProperty(
      key = "schema",
      injectionKey = "TARGET_SCHEMA",
      injectionKeyDescription = "TableOutputMeta.Injection.SchemaName.Field",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_SCHEMA)
  private String schemaName;

  @HopMetadataProperty(
      key = "table",
      injectionKey = "TARGET_TABLE",
      injectionKeyDescription = "TableOutputMeta.Injection.TableName.Field",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
  private String tableName;

  @HopMetadataProperty(
      key = "commit",
      injectionKey = "COMMIT_SIZE",
      injectionKeyDescription = "TableOutputMeta.Injection.CommitSize.Field")
  private String commitSize;

  @HopMetadataProperty(
      key = "truncate",
      injectionKey = "TRUNCATE_TABLE",
      injectionKeyDescription = "TableOutputMeta.Injection.TruncateTable.Field",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TRUNCATE)
  private boolean truncateTable;

  @HopMetadataProperty(
      key = "only_when_have_rows",
      injectionKey = "ONLY_WHEN_HAVE_ROWS",
      injectionKeyDescription = "TableOutputMeta.Inject.OnlyWhenHaveRows.Field")
  private boolean onlyWhenHaveRows;

  @HopMetadataProperty(
      key = "ignore_errors",
      injectionKey = "IGNORE_INSERT_ERRORS",
      injectionKeyDescription = "TableOutputMeta.Injection.IgnoreErrors.Field")
  private boolean ignoreErrors;

  @HopMetadataProperty(
      key = "use_batch",
      injectionKey = "USE_BATCH_UPDATE",
      defaultBoolean = true,
      injectionKeyDescription = "TableOutputMeta.Injection.UseBatch.Field")
  private boolean useBatchUpdate;

  @HopMetadataProperty(
      key = "partitioning_enabled",
      injectionKey = "PARTITION_OVER_TABLES",
      injectionKeyDescription = "TableOutputMeta.Injection.PartitioningEnabled.Field")
  private boolean partitioningEnabled;

  @HopMetadataProperty(
      key = "partitioning_field",
      injectionKey = "PARTITIONING_FIELD",
      injectionKeyDescription = "TableOutputMeta.Injection.PartitioningField.Field")
  private String partitioningField;

  @HopMetadataProperty(key = "partitioning_daily", isExcludedFromInjection = true)
  private boolean partitioningDaily;

  @HopMetadataProperty(key = "partitioning_monthly", isExcludedFromInjection = true)
  private boolean partitioningMonthly;

  @HopMetadataProperty(
      injectionKey = "PARTITION_DATA_PER",
      injectionKeyDescription = "TableOutputMeta.Injection.PartitionDataPer.Field")
  @SuppressWarnings("java:S2065") // disable sonar warning on transient
  private transient String partitionDataPer;

  @HopMetadataProperty(
      key = "tablename_in_field",
      injectionKey = "TABLE_NAME_DEFINED_IN_FIELD",
      injectionKeyDescription = "TableOutputMeta.Injection.TableNameInField.Field")
  private boolean tableNameInField;

  @HopMetadataProperty(
      key = "tablename_field",
      injectionKey = "TABLE_NAME_FIELD",
      injectionKeyDescription = "TableOutputMeta.Injection.TableNameField.Field")
  private String tableNameField;

  @HopMetadataProperty(
      key = "tablename_in_table",
      injectionKey = "STORE_TABLE_NAME",
      injectionKeyDescription = "TableOutputMeta.Injection.TableNameInTable.Field")
  private boolean tableNameInTable;

  @HopMetadataProperty(
      key = "return_keys",
      injectionKeyDescription = "TableOutputMeta.Injection.ReturningGeneratedKeys.Field")
  private boolean returningGeneratedKeys;

  @HopMetadataProperty(
      key = "return_field",
      injectionKey = "RETURN_AUTO_GENERATED_KEY",
      injectionKeyDescription = "TableOutputMeta.Injection.GeneratedKeys.Field")
  private String generatedKeyField;

  /** Do we explicitly select the fields to update in the database */
  @HopMetadataProperty(
      key = "specify_fields",
      injectionKey = "AUTO_GENERATED_KEY_FIELD",
      injectionKeyDescription = "TableOutputMeta.Injection.SpecifyFields.Field")
  private boolean specifyFields;

  @HopMetadataProperty(
      groupKey = "fields",
      key = "field",
      injectionKey = "DATABASE_FIELD",
      injectionGroupKey = "DATABASE_FIELDS",
      injectionGroupDescription = "TableOutputMeta.Injection.Fields",
      injectionKeyDescription = "TableOutputMeta.Injection.Field",
      hopMetadataPropertyType = HopMetadataPropertyType.FIELD_LIST)
  private List<TableOutputField> fields;

  /**
   * @param partitionDataPer The partitionDataPer to set
   */
  public void setPartitionDataPer(String partitionDataPer) {
    this.partitionDataPer = partitionDataPer;
    this.partitioningDaily = partitionDataPer.equals(PARTITION_PER_DAY);
    this.partitioningMonthly = partitionDataPer.equals(PARTITION_PER_MONTH);
  }

  public TableOutputMeta() {
    super(); // allocate BaseTransformMeta
    useBatchUpdate = true;
    commitSize = "1000";

    fields = new ArrayList<>();
  }

  @Override
  public Object clone() {
    return super.clone();
  }

  @Override
  public void setDefault() {
    tableName = "";
    commitSize = "1000";

    partitioningEnabled = false;
    partitioningDaily = false;
    partitioningMonthly = true;
    partitioningField = "";
    tableNameInTable = true;
    tableNameField = "";

    specifyFields = false;
  }

  @Override
  public void getFields(
      IRowMeta row,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // Just add the returning key field...
    if (returningGeneratedKeys && !Utils.isEmpty(generatedKeyField)) {
      IValueMeta key = new ValueMetaInteger(variables.resolve(generatedKeyField));
      key.setOrigin(origin);
      row.addValueMeta(key);
    }
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

    try {
      DatabaseMeta databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connection));

      if (databaseMeta != null) {
        CheckResult cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "TableOutputMeta.CheckResult.ConnectionExists"),
                transformMeta);
        remarks.add(cr);

        try (Database db = new Database(loggingObject, variables, databaseMeta)) {
          db.connect();

          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(PKG, "TableOutputMeta.CheckResult.ConnectionOk"),
                  transformMeta);
          remarks.add(cr);

          if (!Utils.isEmpty(tableName)) {
            String realSchemaName = db.resolve(schemaName);
            String realTableName = db.resolve(tableName);
            String schemaTable =
                databaseMeta.getQuotedSchemaTableCombination(
                    variables, realSchemaName, realTableName);
            // Check if this table exists...
            if (db.checkTableExists(realSchemaName, realTableName)) {
              cr =
                  new CheckResult(
                      ICheckResult.TYPE_RESULT_OK,
                      BaseMessages.getString(
                          PKG, "TableOutputMeta.CheckResult.TableAccessible", schemaTable),
                      transformMeta);
              remarks.add(cr);

              IRowMeta r = db.getTableFieldsMeta(realSchemaName, realTableName);
              if (r != null) {
                cr =
                    new CheckResult(
                        ICheckResult.TYPE_RESULT_OK,
                        BaseMessages.getString(
                            PKG, "TableOutputMeta.CheckResult.TableOk", schemaTable),
                        transformMeta);
                remarks.add(cr);

                StringBuilder errorMessage = new StringBuilder();
                boolean errorFound = false;
                // OK, we have the table fields.
                // Now see what we can find as previous transform...
                if (!Utils.isEmpty(prev)) {
                  cr =
                      new CheckResult(
                          ICheckResult.TYPE_RESULT_OK,
                          BaseMessages.getString(
                              PKG, "TableOutputMeta.CheckResult.FieldsReceived", "" + prev.size()),
                          transformMeta);
                  remarks.add(cr);

                  if (!isSpecifyFields()) {
                    // Starting from prev...
                    for (int i = 0; i < prev.size(); i++) {
                      IValueMeta pv = prev.getValueMeta(i);
                      int idx = r.indexOfValue(pv.getName());
                      if (idx < 0) {
                        errorMessage
                            .append("\t\t")
                            .append(pv.getName())
                            .append(" (")
                            .append(pv.getTypeDesc())
                            .append(")")
                            .append(Const.CR);
                        errorFound = true;
                      }
                    }
                    if (errorFound) {
                      cr =
                          new CheckResult(
                              ICheckResult.TYPE_RESULT_ERROR,
                              BaseMessages.getString(
                                  PKG,
                                  "TableOutputMeta.CheckResult.FieldsNotFoundInOutput",
                                  errorMessage),
                              transformMeta);
                      remarks.add(cr);
                    } else {
                      cr =
                          new CheckResult(
                              ICheckResult.TYPE_RESULT_OK,
                              BaseMessages.getString(
                                  PKG, "TableOutputMeta.CheckResult.AllFieldsFoundInOutput"),
                              transformMeta);
                      remarks.add(cr);
                    }
                  } else {
                    // Specifying the column names explicitly
                    for (TableOutputField tf : fields) {
                      int idx = r.indexOfValue(tf.getFieldDatabase());
                      if (idx < 0) {
                        errorMessage.append("\t\t").append(tf.getFieldDatabase()).append(Const.CR);
                        errorFound = true;
                      }
                    }
                    if (errorFound) {
                      cr =
                          new CheckResult(
                              ICheckResult.TYPE_RESULT_ERROR,
                              BaseMessages.getString(
                                  PKG,
                                  "TableOutputMeta.CheckResult.FieldsSpecifiedNotInTable",
                                  errorMessage),
                              transformMeta);
                      remarks.add(cr);
                    } else {
                      cr =
                          new CheckResult(
                              ICheckResult.TYPE_RESULT_OK,
                              BaseMessages.getString(
                                  PKG, "TableOutputMeta.CheckResult.AllFieldsFoundInOutput"),
                              transformMeta);
                      remarks.add(cr);
                    }
                  }

                  errorMessage.setLength(0);
                  if (!isSpecifyFields()) {
                    // Starting from table fields in r...
                    for (int i = 0; i < fields.size(); i++) {
                      IValueMeta rv = r.getValueMeta(i);
                      int idx = prev.indexOfValue(rv.getName());
                      if (idx < 0) {
                        errorMessage
                            .append("\t\t")
                            .append(rv.getName())
                            .append(" (")
                            .append(rv.getTypeDesc())
                            .append(")")
                            .append(Const.CR);
                        errorFound = true;
                      }
                    }
                    if (errorFound) {
                      cr =
                          new CheckResult(
                              ICheckResult.TYPE_RESULT_WARNING,
                              BaseMessages.getString(
                                  PKG, "TableOutputMeta.CheckResult.FieldsNotFound", errorMessage),
                              transformMeta);
                      remarks.add(cr);
                    } else {
                      cr =
                          new CheckResult(
                              ICheckResult.TYPE_RESULT_OK,
                              BaseMessages.getString(
                                  PKG, "TableOutputMeta.CheckResult.AllFieldsFound"),
                              transformMeta);
                      remarks.add(cr);
                    }
                  } else {
                    // Specifying the column names explicitly
                    for (TableOutputField tf : fields) {
                      int idx = prev.indexOfValue(tf.getFieldStream());
                      if (idx < 0) {
                        errorMessage.append("\t\t").append(tf.getFieldStream()).append(Const.CR);
                        errorFound = true;
                      }
                    }
                    if (errorFound) {
                      cr =
                          new CheckResult(
                              ICheckResult.TYPE_RESULT_ERROR,
                              BaseMessages.getString(
                                  PKG,
                                  "TableOutputMeta.CheckResult.FieldsSpecifiedNotFound",
                                  errorMessage),
                              transformMeta);
                      remarks.add(cr);
                    } else {
                      cr =
                          new CheckResult(
                              ICheckResult.TYPE_RESULT_OK,
                              BaseMessages.getString(
                                  PKG, "TableOutputMeta.CheckResult.AllFieldsFound"),
                              transformMeta);
                      remarks.add(cr);
                    }
                  }
                } else {
                  cr =
                      new CheckResult(
                          ICheckResult.TYPE_RESULT_ERROR,
                          BaseMessages.getString(PKG, "TableOutputMeta.CheckResult.NoFields"),
                          transformMeta);
                  remarks.add(cr);
                }
              } else {
                cr =
                    new CheckResult(
                        ICheckResult.TYPE_RESULT_ERROR,
                        BaseMessages.getString(
                            PKG, "TableOutputMeta.CheckResult.TableNotAccessible"),
                        transformMeta);
                remarks.add(cr);
              }
            } else {
              cr =
                  new CheckResult(
                      ICheckResult.TYPE_RESULT_ERROR,
                      BaseMessages.getString(
                          PKG, "TableOutputMeta.CheckResult.TableError", schemaTable),
                      transformMeta);
              remarks.add(cr);
            }
          } else {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_ERROR,
                    BaseMessages.getString(PKG, "TableOutputMeta.CheckResult.NoTableName"),
                    transformMeta);
            remarks.add(cr);
          }
        }
      } else {
        CheckResult cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "TableOutputMeta.CheckResult.NoConnection"),
                transformMeta);
        remarks.add(cr);
      }
    } catch (HopDatabaseException e) {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "TableOutputMeta.Error.ErrorConnecting", e.getMessage()),
              transformMeta);
      remarks.add(cr);
    } catch (HopException e) {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "TableOutputMeta.CheckResult.UndefinedError", e.getMessage()),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "TableOutputMeta.CheckResult.ExpectedInputOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "TableOutputMeta.CheckResult.ExpectedInputError"),
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

      if (truncateTable) {
        DatabaseImpact ii =
            new DatabaseImpact(
                DatabaseImpact.TYPE_IMPACT_TRUNCATE,
                pipelineMeta.getName(),
                transformMeta.getName(),
                databaseMeta.getDatabaseName(),
                tableName,
                "",
                "",
                "",
                "",
                "Truncate of table");
        impact.add(ii);
      }
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
                  tableName,
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
    return getSqlStatements(variables, pipelineMeta, transformMeta, prev, null, false, null);
  }

  public SqlStatement getSqlStatements(
      IVariables variables,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String tk,
      boolean useAutoIncrement,
      String pk) {

    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(connection, variables);

    SqlStatement retval =
        new SqlStatement(transformMeta.getName(), databaseMeta, null); // default: nothing to do!

    if (databaseMeta != null) {
      if (!Utils.isEmpty(prev)) {
        if (!Utils.isEmpty(tableName)) {
          try (Database db = new Database(loggingObject, variables, databaseMeta)) {
            db.connect();

            String schemaTable =
                databaseMeta.getQuotedSchemaTableCombination(variables, schemaName, tableName);
            String crTable = db.getDDL(schemaTable, prev, tk, useAutoIncrement, pk);

            // Empty string means: nothing to do: set it to null...
            if (Utils.isEmpty(crTable)) {
              crTable = null;
            }

            retval.setSql(crTable);
          } catch (HopDatabaseException dbe) {
            retval.setError(
                BaseMessages.getString(
                    PKG, "TableOutputMeta.Error.ErrorConnecting", dbe.getMessage()));
          }
        } else {
          retval.setError(BaseMessages.getString(PKG, "TableOutputMeta.Error.NoTable"));
        }
      } else {
        retval.setError(BaseMessages.getString(PKG, "TableOutputMeta.Error.NoInput"));
      }
    } else {
      retval.setError(BaseMessages.getString(PKG, "TableOutputMeta.Error.NoConnection"));
    }

    return retval;
  }

  @Override
  public IRowMeta getRequiredFields(IVariables variables) throws HopException {
    String realTableName = variables.resolve(tableName);
    String realSchemaName = variables.resolve(schemaName);

    DatabaseMeta databaseMeta =
        getParentTransformMeta().getParentPipelineMeta().findDatabase(connection, variables);

    return getTableFields(databaseMeta, realTableName, realSchemaName, variables);
  }

  public IRowMeta getTableFields(
      DatabaseMeta databaseMeta, String tableName, String schemaName, IVariables variables)
      throws HopException {

    if (databaseMeta != null) {
      try (Database db = new Database(loggingObject, variables, databaseMeta)) {
        db.connect();

        if (!Utils.isEmpty(tableName)) {
          // Check if this table exists...
          if (db.checkTableExists(schemaName, tableName)) {
            return db.getTableFieldsMeta(schemaName, tableName);
          } else {
            throw new HopException(
                BaseMessages.getString(PKG, "TableOutputMeta.Exception.TableNotFound"));
          }
        } else {
          throw new HopException(
              BaseMessages.getString(PKG, "TableOutputMeta.Exception.TableNotSpecified"));
        }
      } catch (Exception e) {
        throw new HopException(
            BaseMessages.getString(PKG, "TableOutputMeta.Exception.ErrorGettingFields"), e);
      }
    } else {
      throw new HopException(
          BaseMessages.getString(PKG, "TableOutputMeta.Exception.ConnectionNotDefined"));
    }
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

  public DatabaseMeta getDatabaseMeta() {
    return null;
  }
}
