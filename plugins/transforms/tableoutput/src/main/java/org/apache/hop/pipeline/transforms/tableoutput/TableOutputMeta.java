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

import org.apache.hop.core.*;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Transform(
    id = "TableOutput",
    image = "tableoutput.svg",
    name = "i18n::BaseTransform.TypeLongDesc.TableOutput",
    description = "i18n::BaseTransform.TypeTooltipDesc.TableOutput",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    keywords = "i18n::TableOutputMeta.keyword",
    documentationUrl = "/pipeline/transforms/tableoutput.html")
public class TableOutputMeta extends BaseTransformMeta
    implements ITransformMeta<TableOutput, TableOutputData>, IProvidesModelerMeta {
  private static final Class<?> PKG = TableOutputMeta.class; // For Translator

  private static final String PARTION_PER_DAY = "DAY";
  private static final String PARTION_PER_MONTH = "MONTH";

  @HopMetadataProperty(
      key = "connection",
      injectionKeyDescription = "TableOutputMeta.Injection.Connection")
  private String connection;

  @HopMetadataProperty(
      key = "schema",
      injectionKey = "TARGET_SCHEMA",
      injectionKeyDescription = "TableOutputMeta.Injection.SchemaName.Field")
  private String schemaName;

  @HopMetadataProperty(
      key = "table",
      injectionKey = "TARGET_TABLE",
      injectionKeyDescription = "TableOutputMeta.Injection.TableName.Field")
  private String tableName;

  @HopMetadataProperty(
      key = "commit",
      injectionKey = "COMMIT_SIZE",
      injectionKeyDescription = "TableOutputMeta.Injection.CommitSize.Field")
  private String commitSize;

  @HopMetadataProperty(
      key = "truncate",
      injectionKey = "TRUNCATE_TABLE",
      injectionKeyDescription = "TableOutputMeta.Injection.TruncateTable.Field")
  private boolean truncateTable;

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
      injectionKeyDescription = "TableOutputMeta.Injection.Field")
  private List<TableOutputField> fields;

  public List<TableOutputField> getFields() {
    return fields;
  }

  public void setFields(List<TableOutputField> fields) {
    this.fields = fields;
  }

  /** @return Returns the generatedKeyField. */
  public String getGeneratedKeyField() {
    return generatedKeyField;
  }

  /** @param generatedKeyField The generatedKeyField to set. */
  public void setGeneratedKeyField(String generatedKeyField) {
    this.generatedKeyField = generatedKeyField;
  }

  /** @return Returns the returningGeneratedKeys. */
  public boolean isReturningGeneratedKeys() {
    return returningGeneratedKeys;
  }

  /** @param returningGeneratedKeys The returningGeneratedKeys to set. */
  public void setReturningGeneratedKeys(boolean returningGeneratedKeys) {
    this.returningGeneratedKeys = returningGeneratedKeys;
  }

  /** @return Returns the tableNameInTable. */
  public boolean isTableNameInTable() {
    return tableNameInTable;
  }

  /** @param tableNameInTable The tableNameInTable to set. */
  public void setTableNameInTable(boolean tableNameInTable) {
    this.tableNameInTable = tableNameInTable;
  }

  /** @return Returns the tableNameField. */
  public String getTableNameField() {
    return tableNameField;
  }

  /** @param tableNameField The tableNameField to set. */
  public void setTableNameField(String tableNameField) {
    this.tableNameField = tableNameField;
  }

  /** @return Returns the tableNameInField. */
  public boolean isTableNameInField() {
    return tableNameInField;
  }

  /** @param tableNameInField The tableNameInField to set. */
  public void setTableNameInField(boolean tableNameInField) {
    this.tableNameInField = tableNameInField;
  }

  /** @return Returns the partitioningDaily. */
  public boolean isPartitioningDaily() {
    return partitioningDaily;
  }

  /** @param partitioningDaily The partitioningDaily to set. */
  public void setPartitioningDaily(boolean partitioningDaily) {
    this.partitioningDaily = partitioningDaily;
  }

  /** @return Returns the partitioningMontly. */
  public boolean isPartitioningMonthly() {
    return partitioningMonthly;
  }

  /** @param partitioningMontly The partitioningMontly to set. */
  public void setPartitioningMonthly(boolean partitioningMontly) {
    this.partitioningMonthly = partitioningMontly;
  }

  /** @return Returns the partitioningEnabled. */
  public boolean isPartitioningEnabled() {
    return partitioningEnabled;
  }

  /** @param partitioningEnabled The partitioningEnabled to set. */
  public void setPartitioningEnabled(boolean partitioningEnabled) {
    this.partitioningEnabled = partitioningEnabled;
  }

  /** @return Returns the partitioningField. */
  public String getPartitioningField() {
    return partitioningField;
  }

  /** @param partitioningField The partitioningField to set. */
  public void setPartitioningField(String partitioningField) {
    this.partitioningField = partitioningField;
  }

  /** @return Returns the partitionDataPer value */
  public String getPartitionDataPer() {
    return partitionDataPer;
  }

  /** @param partitionDataPer The partitionDataPer to set */
  public void setPartitionDataPer(String partitionDataPer) {
    this.partitionDataPer = partitionDataPer;

    this.partitioningDaily = partitionDataPer.equals(PARTION_PER_DAY);
    this.partitioningMonthly = partitionDataPer.equals(PARTION_PER_MONTH);
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

  public String getConnection() {
    return connection;
  }

  public void setConnection(String connection) {
    this.connection = connection;
  }

  /** @return Returns the commitSize. */
  public String getCommitSize() {
    return commitSize;
  }

  /** @param commitSizeInt The commitSize to set. */
  public void setCommitSize(int commitSizeInt) {
    this.commitSize = Integer.toString(commitSizeInt);
  }

  /** @param commitSize The commitSize to set. */
  public void setCommitSize(String commitSize) {
    this.commitSize = commitSize;
  }

  /** @return the table name */
  @Override
  public String getTableName() {
    return tableName;
  }

  /**
   * Assign the table name to write to.
   *
   * @param tableName The table name to set
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /** @return Returns the truncate table flag. */
  public boolean isTruncateTable() {
    return truncateTable;
  }

  /** @param truncateTable The truncate table flag to set. */
  public void setTruncateTable(boolean truncateTable) {
    this.truncateTable = truncateTable;
  }

  /** @param ignoreErrors The ignore errors flag to set. */
  public void setIgnoreErrors(boolean ignoreErrors) {
    this.ignoreErrors = ignoreErrors;
  }

  /** @return Returns the ignore errors flag. */
  public boolean isIgnoreErrors() {
    return ignoreErrors;
  }

  /** @param specifyFields The specify fields flag to set. */
  public void setSpecifyFields(boolean specifyFields) {
    this.specifyFields = specifyFields;
  }

  /** @return Returns the specify fields flag. */
  public boolean isSpecifyFields() {
    return specifyFields;
  }

  /** @param useBatchUpdate The useBatchUpdate flag to set. */
  public void setUseBatchUpdate(boolean useBatchUpdate) {
    this.useBatchUpdate = useBatchUpdate;
  }

  /** @return Returns the useBatchUpdate flag. */
  public boolean isUseBatchUpdate() {
    return useBatchUpdate;
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
    if (returningGeneratedKeys && generatedKeyField != null && generatedKeyField.length() > 0) {
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

    Database db = null;

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

        db = new Database(loggingObject, variables, databaseMeta);
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

              String errorMessage = "";
              boolean errorFound = false;
              // OK, we have the table fields.
              // Now see what we can find as previous transform...
              if (prev != null && prev.size() > 0) {
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
                      errorMessage +=
                          "\t\t" + pv.getName() + " (" + pv.getTypeDesc() + ")" + Const.CR;
                      errorFound = true;
                    }
                  }
                  if (errorFound) {
                    errorMessage =
                        BaseMessages.getString(
                            PKG,
                            "TableOutputMeta.CheckResult.FieldsNotFoundInOutput",
                            errorMessage);

                    cr =
                        new CheckResult(
                            ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
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
                  for (int i = 0; i < fields.size(); i++) {
                    TableOutputField tf = fields.get(i);
                    int idx = r.indexOfValue(tf.getFieldDatabase());
                    if (idx < 0) {
                      errorMessage += "\t\t" + tf.getFieldDatabase() + Const.CR;
                      errorFound = true;
                    }
                  }
                  if (errorFound) {
                    errorMessage =
                        BaseMessages.getString(
                            PKG,
                            "TableOutputMeta.CheckResult.FieldsSpecifiedNotInTable",
                            errorMessage);

                    cr =
                        new CheckResult(
                            ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
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

                errorMessage = "";
                if (!isSpecifyFields()) {
                  // Starting from table fields in r...
                  for (int i = 0; i < fields.size(); i++) {
                    IValueMeta rv = r.getValueMeta(i);
                    int idx = prev.indexOfValue(rv.getName());
                    if (idx < 0) {
                      errorMessage +=
                          "\t\t" + rv.getName() + " (" + rv.getTypeDesc() + ")" + Const.CR;
                      errorFound = true;
                    }
                  }
                  if (errorFound) {
                    errorMessage =
                        BaseMessages.getString(
                            PKG, "TableOutputMeta.CheckResult.FieldsNotFound", errorMessage);

                    cr =
                        new CheckResult(
                            ICheckResult.TYPE_RESULT_WARNING, errorMessage, transformMeta);
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
                  for (int i = 0; i < fields.size(); i++) {
                    TableOutputField tf = fields.get(i);
                    int idx = prev.indexOfValue(tf.getFieldStream());
                    if (idx < 0) {
                      errorMessage += "\t\t" + tf.getFieldStream() + Const.CR;
                      errorFound = true;
                    }
                  }
                  if (errorFound) {
                    errorMessage =
                        BaseMessages.getString(
                            PKG,
                            "TableOutputMeta.CheckResult.FieldsSpecifiedNotFound",
                            errorMessage);

                    cr =
                        new CheckResult(
                            ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
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
                      BaseMessages.getString(PKG, "TableOutputMeta.CheckResult.TableNotAccessible"),
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
      } else {
        CheckResult cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "TableOutputMeta.CheckResult.NoConnection"),
                transformMeta);
        remarks.add(cr);
      }
    } catch (HopException e) {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "TableOutputMeta.CheckResult.UndefinedError", e.getMessage()),
              transformMeta);
      remarks.add(cr);
    } finally {
      db.disconnect();
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
  public ITransform createTransform(
      TransformMeta transformMeta,
      TableOutputData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new TableOutput(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  @Override
  public TableOutputData getTransformData() {
    return new TableOutputData();
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
      if (prev != null && prev.size() > 0) {
        if (!Utils.isEmpty(tableName)) {
          Database db = new Database(loggingObject, variables, databaseMeta);
          try {
            db.connect();

            String schemaTable =
                databaseMeta.getQuotedSchemaTableCombination(variables, schemaName, tableName);
            String crTable = db.getDDL(schemaTable, prev, tk, useAutoIncrement, pk);

            // Empty string means: nothing to do: set it to null...
            if (crTable == null || crTable.length() == 0) {
              crTable = null;
            }

            retval.setSql(crTable);
          } catch (HopDatabaseException dbe) {
            retval.setError(
                BaseMessages.getString(
                    PKG, "TableOutputMeta.Error.ErrorConnecting", dbe.getMessage()));
          } finally {
            db.disconnect();
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

  public IRowMeta getTableFields(DatabaseMeta databaseMeta, String tableName, String schemaName, IVariables variables) throws HopException {

    if (databaseMeta != null) {
      Database db = new Database(loggingObject, variables, databaseMeta);
      try {
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
      } finally {
        db.disconnect();
      }
    } else {
      throw new HopException(
          BaseMessages.getString(PKG, "TableOutputMeta.Exception.ConnectionNotDefined"));
    }
  }

  @Override
  public RowMeta getRowMeta(IVariables variables, ITransformData transformData) {
    return (RowMeta) ((TableOutputData) transformData).insertRowMeta;
  }

  @Override
  public List<String> getDatabaseFields() {

    List<String> items = Collections.emptyList();
    if (isSpecifyFields()) {
      items = new ArrayList<>();
      for (TableOutputField tf : fields) {
        items.add(tf.getFieldDatabase());
      }
    }
    return items;
  }

  @Override
  public List<String> getStreamFields() {

    List<String> items = Collections.emptyList();
    if (isSpecifyFields()) {
      items = new ArrayList<>();
      for (TableOutputField tf : fields) {
        items.add(tf.getFieldStream());
      }
    }
    return items;
  }

  /** @return the schemaName */
  @Override
  public String getSchemaName() {
    return schemaName;
  }

  /** @param schemaName the schemaName to set */
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

  @Override
  public String getMissingDatabaseConnectionInformationMessage() {
    return null;
  }
}
