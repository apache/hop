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

package org.apache.hop.pipeline.transforms.synchronizeaftermerge;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "SynchronizeAfterMerge",
    image = "synchronizeaftermerge.svg",
    name = "i18n::SynchronizeAfterMerge.Name",
    description = "i18n::SynchronizeAfterMerge.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    keywords = "i18n::SynchronizeAfterMergeMeta.keyword",
    documentationUrl = "/pipeline/transforms/synchronizeaftermerge.html")
@Getter
@Setter
public class SynchronizeAfterMergeMeta
    extends BaseTransformMeta<SynchronizeAfterMerge, SynchronizeAfterMergeData> {
  private static final Class<?> PKG = SynchronizeAfterMergeMeta.class;
  public static final String CONST_LOOKUP = "lookup";
  public static final String CONST_SPACES = "        ";

  @HopMetadataProperty(
      key = "connection",
      injectionKey = "CONNECTION_NAME",
      injectionKeyDescription = "SynchronizeAfterMerge.Injection.CONNECTION_NAME")
  private String connection;

  /** Commit size for inserts/updates */
  @HopMetadataProperty(
      key = "commit",
      injectionKey = "COMMIT_SIZE",
      injectionKeyDescription = "SynchronizeAfterMerge.Injection.COMMIT_SIZE")
  private String commitSize;

  @HopMetadataProperty(
      key = "tablename_in_field",
      injectionKey = "TABLE_NAME_IN_FIELD",
      injectionKeyDescription = "SynchronizeAfterMerge.Injection.TABLE_NAME_IN_FIELD")
  private boolean tableNameInField;

  @HopMetadataProperty(
      key = "tablename_field",
      injectionKey = "TABLE_NAME_FIELD",
      injectionKeyDescription = "SynchronizeAfterMerge.Injection.TABLE_NAME_FIELD")
  private String tableNameField;

  @HopMetadataProperty(
      key = "operation_order_field",
      injectionKey = "OPERATION_ORDER_FIELD",
      injectionKeyDescription = "SynchronizeAfterMerge.Injection.OPERATION_ORDER_FIELD")
  private String operationOrderField;

  @HopMetadataProperty(
      key = "use_batch",
      injectionKey = "USE_BATCH_UPDATE",
      injectionKeyDescription = "SynchronizeAfterMerge.Injection.USE_BATCH_UPDATE")
  private boolean usingBatchUpdates;

  @HopMetadataProperty(
      key = "perform_lookup",
      injectionKey = "PERFORM_LOOKUP",
      injectionKeyDescription = "SynchronizeAfterMerge.Injection.PERFORM_LOOKUP")
  private boolean performingLookup;

  @HopMetadataProperty(
      key = "order_insert",
      injectionKey = "ORDER_INSERT",
      injectionKeyDescription = "SynchronizeAfterMerge.Injection.ORDER_INSERT")
  private String orderInsert;

  @HopMetadataProperty(
      key = "order_update",
      injectionKey = "ORDER_UPDATE",
      injectionKeyDescription = "SynchronizeAfterMerge.Injection.ORDER_UPDATE")
  private String orderUpdate;

  @HopMetadataProperty(
      key = "order_delete",
      injectionKey = "ORDER_DELETE",
      injectionKeyDescription = "SynchronizeAfterMerge.Injection.ORDER_DELETE")
  private String orderDelete;

  @HopMetadataProperty(key = "lookup")
  private Lookup lookup;

  public SynchronizeAfterMergeMeta() {
    super();
    this.lookup = new Lookup();
    this.commitSize = "100";
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
    StringBuilder errorMessage = new StringBuilder();

    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(connection, variables);
    if (databaseMeta != null) {
      try (Database db = new Database(loggingObject, variables, databaseMeta)) {
        db.connect();

        if (!Utils.isEmpty(lookup.tableName)) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(PKG, "SynchronizeAfterMergeMeta.CheckResult.TableNameOK"),
                  transformMeta);
          remarks.add(cr);

          boolean first = true;
          boolean errorFound = false;

          // Check fields in table
          IRowMeta r = db.getTableFieldsMeta(lookup.schemaName, lookup.tableName);
          if (r != null) {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(
                        PKG, "SynchronizeAfterMergeMeta.CheckResult.TableExists"),
                    transformMeta);
            remarks.add(cr);

            for (KeyCondition keyCondition : lookup.getKeyConditions()) {
              IValueMeta v = r.searchValueMeta(keyCondition.columnName);
              if (v == null) {
                if (first) {
                  first = false;
                  errorMessage
                      .append(
                          BaseMessages.getString(
                              PKG,
                              "SynchronizeAfterMergeMeta.CheckResult.MissingCompareFieldsInTargetTable"))
                      .append(Const.CR);
                }
                errorFound = true;
                errorMessage.append("\t\t").append(keyCondition.columnName).append(Const.CR);
              }
            }
            if (errorFound) {
              cr =
                  new CheckResult(
                      ICheckResult.TYPE_RESULT_ERROR, errorMessage.toString(), transformMeta);
            } else {
              cr =
                  new CheckResult(
                      ICheckResult.TYPE_RESULT_OK,
                      BaseMessages.getString(
                          PKG, "SynchronizeAfterMergeMeta.CheckResult.AllLookupFieldsFound"),
                      transformMeta);
            }
            remarks.add(cr);

            // How about the fields to insert/update in the table?
            first = true;
            errorFound = false;
            errorMessage.setLength(0);

            for (ValueUpdate valueUpdate : lookup.getValueUpdates()) {
              IValueMeta v = r.searchValueMeta(valueUpdate.columnName);
              if (v == null) {
                if (first) {
                  first = false;
                  errorMessage
                      .append(
                          BaseMessages.getString(
                              PKG,
                              "SynchronizeAfterMergeMeta.CheckResult.MissingFieldsToUpdateInTargetTable"))
                      .append(Const.CR);
                }
                errorFound = true;
                errorMessage.append("\t\t").append(valueUpdate.columnName).append(Const.CR);
              }
            }
            if (errorFound) {
              cr =
                  new CheckResult(
                      ICheckResult.TYPE_RESULT_ERROR, errorMessage.toString(), transformMeta);
            } else {
              cr =
                  new CheckResult(
                      ICheckResult.TYPE_RESULT_OK,
                      BaseMessages.getString(
                          PKG,
                          "SynchronizeAfterMergeMeta.CheckResult.AllFieldsToUpdateFoundInTargetTable"),
                      transformMeta);
            }
            remarks.add(cr);
          } else {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_ERROR,
                    BaseMessages.getString(
                        PKG, "SynchronizeAfterMergeMeta.CheckResult.CouldNotReadTableInfo"),
                    transformMeta);
            remarks.add(cr);
          }
        }

        // Look up fields in the input stream <prev>
        if (prev != null && !prev.isEmpty()) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(
                      PKG,
                      "SynchronizeAfterMergeMeta.CheckResult.TransformReceivingDatas",
                      prev.size() + ""),
                  transformMeta);
          remarks.add(cr);

          boolean first = true;
          errorMessage.setLength(0);
          boolean errorFound = false;

          for (KeyCondition keyCondition : lookup.keyConditions) {
            IValueMeta v = prev.searchValueMeta(keyCondition.fieldName);
            if (v == null) {
              if (first) {
                first = false;
                errorMessage
                    .append(
                        BaseMessages.getString(
                            PKG, "SynchronizeAfterMergeMeta.CheckResult.MissingFieldsInInput"))
                    .append(Const.CR);
              }
              errorFound = true;
              errorMessage.append("\t\t").append(keyCondition.fieldName).append(Const.CR);
            }
          }
          for (KeyCondition keyCondition : lookup.keyConditions) {
            if (!Utils.isEmpty(keyCondition.fieldName2)) {
              IValueMeta v = prev.searchValueMeta(keyCondition.fieldName2);
              if (v == null) {
                if (first) {
                  first = false;
                  errorMessage
                      .append(
                          BaseMessages.getString(
                              PKG, "SynchronizeAfterMergeMeta.CheckResult.MissingFieldsInInput"))
                      .append(Const.CR);
                }
                errorFound = true;
                errorMessage.append("\t\t").append(keyCondition.fieldName2).append(Const.CR);
              }
            }
          }
          if (errorFound) {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_ERROR, errorMessage.toString(), transformMeta);
          } else {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(
                        PKG, "SynchronizeAfterMergeMeta.CheckResult.AllFieldsFoundInInput"),
                    transformMeta);
          }
          remarks.add(cr);

          // How about the fields to insert/update the table with?
          first = true;
          errorFound = false;
          errorMessage.setLength(0);

          for (ValueUpdate valueUpdate : lookup.valueUpdates) {
            IValueMeta v = prev.searchValueMeta(valueUpdate.fieldName);

            if (v == null) {
              if (first) {
                first = false;
                errorMessage
                    .append(
                        BaseMessages.getString(
                            PKG, "SynchronizeAfterMergeMeta.CheckResult.MissingInputStreamFields"))
                    .append(Const.CR);
              }
              errorFound = true;
              errorMessage.append("\t\t").append(valueUpdate.fieldName).append(Const.CR);
            }
          }
          if (errorFound) {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_ERROR, errorMessage.toString(), transformMeta);
          } else {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(
                        PKG, "SynchronizeAfterMergeMeta.CheckResult.AllFieldsFoundInInput2"),
                    transformMeta);
            remarks.add(cr);
          }

          // --------------------------> check fields in stream and tables (type)
          // Check fields in table

          StringBuilder errorMsgDiffField = new StringBuilder();
          boolean errorDiffLenField = false;
          StringBuilder errorMsgDiffLenField = new StringBuilder();
          boolean errorDiffField = false;

          IRowMeta r = db.getTableFieldsMeta(lookup.schemaName, lookup.tableName);
          if (r != null) {
            for (ValueUpdate valueUpdate : lookup.valueUpdates) {
              // get value from previous
              IValueMeta vs = prev.searchValueMeta(valueUpdate.fieldName);
              // get value from table fields
              IValueMeta vt = r.searchValueMeta(valueUpdate.columnName);
              if (vs != null && vt != null) {
                if (!vs.getTypeDesc().equalsIgnoreCase(vt.getTypeDesc())) {
                  errorMsgDiffField
                      .append(Const.CR)
                      .append("The input field [")
                      .append(vs.getName())
                      .append("] ( Type=")
                      .append(vs.getTypeDesc())
                      .append(") is not the same as the type in the target table (Type=")
                      .append(vt.getTypeDesc())
                      .append(")")
                      .append(Const.CR);
                  errorDiffField = true;
                } else {
                  // check Length
                  if ((vt.getLength() < vs.getLength() || vs.getLength() == -1)
                      && vt.getLength() != -1) {
                    errorMsgDiffLenField
                        .append(Const.CR)
                        .append("The input field [")
                        .append(vs.getName())
                        .append("] ")
                        .append("(")
                        .append(vs.getTypeDesc())
                        .append(")")
                        .append(" has a length (")
                        .append(vs.getLength())
                        .append(")")
                        .append(" that is higher than that in the target table (")
                        .append(vt.getLength())
                        .append(").")
                        .append(Const.CR);
                    errorDiffLenField = true;
                  }
                }
              }
            }
            // add error/Warning
            if (errorDiffField) {
              cr =
                  new CheckResult(
                      ICheckResult.TYPE_RESULT_ERROR,
                      BaseMessages.getString(
                              PKG, "SynchronizeAfterMergeMeta.CheckResult.FieldsTypeDifferent")
                          + Const.CR
                          + errorMsgDiffField,
                      transformMeta);
            }
            if (errorDiffLenField) {
              cr =
                  new CheckResult(
                      ICheckResult.TYPE_RESULT_WARNING,
                      BaseMessages.getString(
                              PKG, "SynchronizeAfterMergeMeta.CheckResult.FieldsLenDifferent")
                          + Const.CR
                          + errorMsgDiffLenField,
                      transformMeta);
            }
            remarks.add(cr);
          }
          // --------------------------> check fields in stream and tables (type)
        } else {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(
                          PKG, "SynchronizeAfterMergeMeta.CheckResult.MissingFieldsInInput3")
                      + Const.CR,
                  transformMeta);
          remarks.add(cr);
        }
      } catch (HopException e) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                        PKG, "SynchronizeAfterMergeMeta.CheckResult.DatabaseErrorOccurred")
                    + e.getMessage(),
                transformMeta);
        remarks.add(cr);
      }
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "SynchronizeAfterMergeMeta.CheckResult.InvalidConnection"),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "SynchronizeAfterMergeMeta.CheckResult.TransformReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SynchronizeAfterMergeMeta.CheckResult.NoInputError"),
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
    SqlStatement sqlStatement =
        new SqlStatement(transformMeta.getName(), databaseMeta, null); // default: nothing to do!

    if (databaseMeta != null) {
      if (prev != null && !prev.isEmpty()) {
        // Copy the row
        IRowMeta tableFields = new RowMeta();

        // Now change the field names
        // the key fields
        for (KeyCondition keyCondition : lookup.keyConditions) {
          IValueMeta v = prev.searchValueMeta(keyCondition.fieldName);
          if (v != null) {
            IValueMeta tableField = v.clone();
            tableField.setName(keyCondition.columnName);
            tableFields.addValueMeta(tableField);
          } else {
            throw new HopTransformException(
                "Unable to find field [" + keyCondition.fieldName + "] in the input rows");
          }
        }
        // the lookup fields
        for (ValueUpdate valueUpdate : lookup.valueUpdates) {
          IValueMeta v = prev.searchValueMeta(valueUpdate.fieldName);
          if (v != null) {
            IValueMeta vk = tableFields.searchValueMeta(valueUpdate.fieldName);
            if (vk == null) { // do not add again when already added as key fields
              IValueMeta tableField = v.clone();
              tableField.setName(valueUpdate.columnName);
              tableFields.addValueMeta(tableField);
            }
          } else {
            throw new HopTransformException(
                "Unable to find field [" + valueUpdate.fieldName + "] in the input rows");
          }
        }

        if (!Utils.isEmpty(lookup.tableName)) {
          try (Database db = new Database(loggingObject, variables, databaseMeta)) {
            db.connect();

            String schemaTable =
                databaseMeta.getQuotedSchemaTableCombination(
                    variables, lookup.schemaName, lookup.tableName);
            String crTable = db.getDDL(schemaTable, tableFields, null, false, null, true);

            String crIndex = "";
            String[] idxFields = null;

            if (lookup.keyConditions.isEmpty()) {
              sqlStatement.setError(
                  BaseMessages.getString(
                      PKG, "SynchronizeAfterMergeMeta.CheckResult.MissingKeyFields"));

            } else {
              idxFields = new String[lookup.keyConditions.size()];
              for (int i = 0; i < lookup.keyConditions.size(); i++) {
                idxFields[i] = lookup.keyConditions.get(i).columnName;
              }
            }

            // Key lookup dimensions...
            if (!db.checkIndexExists(lookup.schemaName, lookup.tableName, idxFields)) {
              String indexName = "idx_" + lookup.tableName + "_lookup";
              crIndex =
                  db.getCreateIndexStatement(
                      schemaTable, indexName, idxFields, false, false, false, true);
            }

            String sql = crTable + crIndex;
            if (sql.isEmpty()) {
              sqlStatement.setSql(null);
            } else {
              sqlStatement.setSql(sql);
            }
          } catch (HopException e) {
            sqlStatement.setError(
                BaseMessages.getString(PKG, "SynchronizeAfterMergeMeta.ReturnValue.ErrorOccurred")
                    + e.getMessage());
          }
        } else {
          sqlStatement.setError(
              BaseMessages.getString(
                  PKG, "SynchronizeAfterMergeMeta.ReturnValue.NoTableDefinedOnConnection"));
        }
      } else {
        sqlStatement.setError(
            BaseMessages.getString(
                PKG, "SynchronizeAfterMergeMeta.ReturnValue.NotReceivingAnyFields"));
      }
    } else {
      sqlStatement.setError(
          BaseMessages.getString(PKG, "SynchronizeAfterMergeMeta.ReturnValue.NoConnectionDefined"));
    }

    return sqlStatement;
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

    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(connection, variables);
    if (databaseMeta != null && prev != null) {
      // Lookup: we do a lookup on the natural keys
      for (KeyCondition keyCondition : lookup.keyConditions) {
        IValueMeta v = prev.searchValueMeta(keyCondition.fieldName);
        if (v == null) {
          continue;
        }
        DatabaseImpact ii =
            new DatabaseImpact(
                DatabaseImpact.TYPE_IMPACT_READ,
                pipelineMeta.getName(),
                transformMeta.getName(),
                databaseMeta.getDatabaseName(),
                lookup.tableName,
                keyCondition.columnName,
                keyCondition.fieldName,
                v.getOrigin(),
                "",
                "Type = " + v.toStringMeta());
        impact.add(ii);
      }

      // Insert update fields : read/write
      for (ValueUpdate valueUpdate : lookup.valueUpdates) {
        IValueMeta v = prev.searchValueMeta(valueUpdate.fieldName);
        if (v == null) {
          continue;
        }
        DatabaseImpact ii =
            new DatabaseImpact(
                DatabaseImpact.TYPE_IMPACT_READ_WRITE,
                pipelineMeta.getName(),
                transformMeta.getName(),
                databaseMeta.getDatabaseName(),
                lookup.tableName,
                valueUpdate.columnName,
                valueUpdate.fieldName,
                v.getOrigin(),
                "",
                "Type = " + v.toStringMeta());
        impact.add(ii);
      }
    }
  }

  @Override
  public IRowMeta getRequiredFields(IVariables variables) throws HopException {
    String realTableName = variables.resolve(lookup.tableName);
    String realSchemaName = variables.resolve(lookup.schemaName);
    DatabaseMeta databaseMeta =
        getParentTransformMeta().getParentPipelineMeta().findDatabase(connection, variables);
    if (databaseMeta != null) {
      try (Database db = new Database(loggingObject, variables, databaseMeta)) {
        db.connect();

        if (!Utils.isEmpty(realTableName)) {
          // Check if this table exists...
          if (db.checkTableExists(realSchemaName, realTableName)) {
            return db.getTableFieldsMeta(realSchemaName, realTableName);
          } else {
            throw new HopException(
                BaseMessages.getString(PKG, "SynchronizeAfterMergeMeta.Exception.TableNotFound"));
          }
        } else {
          throw new HopException(
              BaseMessages.getString(PKG, "SynchronizeAfterMergeMeta.Exception.TableNotSpecified"));
        }
      } catch (Exception e) {
        throw new HopException(
            BaseMessages.getString(PKG, "SynchronizeAfterMergeMeta.Exception.ErrorGettingFields"),
            e);
      }
    } else {
      throw new HopException(
          BaseMessages.getString(PKG, "SynchronizeAfterMergeMeta.Exception.ConnectionNotDefined"));
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  @Getter
  @Setter
  public static class Lookup {
    /** what's the lookup schema? */
    @Injection(name = "SHEMA_NAME")
    @HopMetadataProperty(
        key = "schema",
        injectionKey = "SHEMA_NAME",
        injectionKeyDescription = "SynchronizeAfterMerge.Injection.SHEMA_NAME")
    private String schemaName;

    /** what's the lookup table? */
    @Injection(name = "TABLE_NAME")
    @HopMetadataProperty(
        key = "table",
        injectionKey = "TABLE_NAME",
        injectionKeyDescription = "SynchronizeAfterMerge.Injection.TABLE_NAME")
    private String tableName;

    @HopMetadataProperty(
        key = "key",
        injectionKey = "KEY_TO_LOOKUP",
        injectionKeyDescription = "SynchronizeAfterMerge.Injection.KEY_TO_LOOKUP",
        injectionGroupKey = "KEYS_TO_LOOKUP",
        injectionGroupDescription = "SynchronizeAfterMerge.Injection.KEYS_TO_LOOKUP")
    private List<KeyCondition> keyConditions;

    @HopMetadataProperty(
        key = "value",
        injectionKey = "UPDATE_FIELD",
        injectionKeyDescription = "SynchronizeAfterMerge.Injection.UPDATE_FIELD",
        injectionGroupKey = "UPDATE_FIELDS",
        injectionGroupDescription = "SynchronizeAfterMerge.Injection.UPDATE_FIELDS")
    private List<ValueUpdate> valueUpdates;

    public Lookup() {
      this.keyConditions = new ArrayList<>();
      this.valueUpdates = new ArrayList<>();
      this.schemaName = "";
      this.tableName = BaseMessages.getString(PKG, "SynchronizeAfterMergeMeta.DefaultTableName");
    }

    public Lookup(Lookup l) {
      this();
      this.schemaName = l.schemaName;
      this.tableName = l.tableName;
      l.keyConditions.forEach(c -> this.keyConditions.add(new KeyCondition(c)));
      l.valueUpdates.forEach(v -> this.valueUpdates.add(new ValueUpdate(v)));
    }
  }

  @Getter
  @Setter
  public static class KeyCondition {

    /** field in table */
    @HopMetadataProperty(
        key = "field",
        injectionKey = "TABLE_FIELD",
        injectionKeyDescription = "SynchronizeAfterMerge.Injection.TABLE_FIELD")
    private String columnName;

    /** Comparator: =, <>, BETWEEN, ... */
    @HopMetadataProperty(
        key = "condition",
        injectionKey = "COMPARATOR",
        injectionKeyDescription = "SynchronizeAfterMerge.Injection.COMPARATOR")
    private String condition;

    /** which field in input stream to compare with? */
    @HopMetadataProperty(
        key = "name",
        injectionKey = "STREAM_FIELD1",
        injectionKeyDescription = "SynchronizeAfterMerge.Injection.STREAM_FIELD1")
    private String fieldName;

    /** Extra field for between... */
    @HopMetadataProperty(
        key = "name2",
        injectionKey = "STREAM_FIELD2",
        injectionKeyDescription = "SynchronizeAfterMerge.Injection.STREAM_FIELD2")
    private String fieldName2;

    public KeyCondition() {}

    public KeyCondition(KeyCondition c) {
      this();
      this.condition = c.condition;
      this.columnName = c.columnName;
      this.fieldName2 = c.fieldName2;
      this.fieldName = c.fieldName;
    }
  }

  @Getter
  @Setter
  public static class ValueUpdate {
    /** Field value to update after lookup */
    @HopMetadataProperty(
        key = "name",
        injectionKey = "UPDATE_TABLE_FIELD",
        injectionKeyDescription = "SynchronizeAfterMerge.Injection.UPDATE_TABLE_FIELD")
    private String columnName;

    /** Stream name to update value with */
    @HopMetadataProperty(
        key = "rename",
        injectionKey = "STREAM_FIELD",
        injectionKeyDescription = "SynchronizeAfterMerge.Injection.STREAM_FIELD")
    private String fieldName;

    /** boolean indicating if field needs to be updated */
    @HopMetadataProperty(
        key = "update",
        injectionKey = "UPDATE",
        injectionKeyDescription = "SynchronizeAfterMerge.Injection.UPDATE")
    private boolean update;

    public ValueUpdate() {}

    public ValueUpdate(ValueUpdate v) {
      this();
      this.update = v.update;
      this.columnName = v.columnName;
      this.fieldName = v.fieldName;
    }
  }
}
