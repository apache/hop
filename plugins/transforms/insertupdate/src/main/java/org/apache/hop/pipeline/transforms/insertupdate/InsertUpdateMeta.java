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

package org.apache.hop.pipeline.transforms.insertupdate;

import java.util.List;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.hop.pipeline.transform.utils.RowMetaUtils;

@Transform(
    id = "InsertUpdate",
    image = "insertupdate.svg",
    name = "i18n::InsertUpdate.Name",
    description = "i18n::InsertUpdate.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    keywords = "i18n::InsertUpdateMeta.keyword",
    documentationUrl = "/pipeline/transforms/insertupdate.html",
    actionTransformTypes = {ActionTransformType.OUTPUT, ActionTransformType.RDBMS})
public class InsertUpdateMeta extends BaseTransformMeta<InsertUpdate, InsertUpdateData> {
  private static final Class<?> PKG = InsertUpdateMeta.class;

  /** Lookup key fields * */
  @HopMetadataProperty(key = "lookup")
  private InsertUpdateLookupField insertUpdateLookupField;

  public InsertUpdateMeta() {
    super();
    insertUpdateLookupField = new InsertUpdateLookupField();
  }

  /** Commit size for inserts/updates */
  @HopMetadataProperty(
      key = "commit",
      injectionKeyDescription = "InsertUpdateMeta.Injection.COMMIT_SIZE",
      injectionKey = "COMMIT_SIZE")
  private String commitSize;

  /** Bypass any updates */
  @HopMetadataProperty(
      key = "update_bypassed",
      injectionKeyDescription = "InsertUpdateMeta.Injection.DO_NOT",
      injectionKey = "DO_NOT")
  private boolean updateBypassed;

  /** database connection */
  @HopMetadataProperty(
      key = "connection",
      injectionKeyDescription = "InsertUpdateMeta.Injection.CONNECTIONNAME",
      injectionKey = "CONNECTIONNAME",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String connection;

  public String getConnection() {
    return connection;
  }

  public void setConnection(String connection) {
    this.connection = connection;
  }

  public DatabaseMeta getDatabaseMeta() {
    return null;
  }

  /**
   * @return Returns the commitSize.
   */
  public String getCommitSize() {
    return commitSize;
  }

  /**
   * @param vs - variable variables to be used for searching variable value usually "this" for a
   *     calling transform
   * @return Returns the commitSize.
   */
  public int getCommitSizeVar(IVariables vs) {
    // this happens when the transform is created via API and no setDefaults was called
    commitSize = (commitSize == null) ? "0" : commitSize;
    return Integer.parseInt(vs.resolve(commitSize));
  }

  /**
   * @param commitSize The commitSize to set.
   */
  public void setCommitSize(String commitSize) {
    this.commitSize = commitSize;
  }

  @Override
  public Object clone() {
    InsertUpdateMeta retval = (InsertUpdateMeta) super.clone();

    return retval;
  }

  /**
   * Returns the table name.
   *
   * @return
   */
  public String getTableName() {
    return insertUpdateLookupField.getTableName();
  }

  /**
   * Returns the schema name.
   *
   * @return
   */
  public String getSchemaName() {
    return insertUpdateLookupField.getSchemaName();
  }

  @Override
  public void setDefault() {
    commitSize = "100";
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // Default: nothing changes to rowMeta
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
    Database db = null;

    try {
      DatabaseMeta databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connection));

      if (databaseMeta != null) {
        db = new Database(loggingObject, variables, databaseMeta);
        db.connect();

        if (!Utils.isEmpty(insertUpdateLookupField.getTableName())) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(PKG, "InsertUpdateMeta.CheckResult.TableNameOK"),
                  transformMeta);
          remarks.add(cr);

          boolean first = true;
          boolean errorFound = false;

          // Check fields in table
          IRowMeta r =
              db.getTableFieldsMeta(
                  variables.resolve(insertUpdateLookupField.getSchemaName()),
                  variables.resolve(insertUpdateLookupField.getTableName()));
          if (r != null) {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(PKG, "InsertUpdateMeta.CheckResult.TableExists"),
                    transformMeta);
            remarks.add(cr);

            for (int i = 0; i < insertUpdateLookupField.getLookupKeys().size(); i++) {
              InsertUpdateKeyField insertUpdateKeyField =
                  insertUpdateLookupField.getLookupKeys().get(i);

              IValueMeta v = r.searchValueMeta(insertUpdateKeyField.getKeyLookup());
              if (v == null) {
                if (first) {
                  first = false;
                  errorMessage
                      .append(
                          BaseMessages.getString(
                              PKG,
                              "InsertUpdateMeta.CheckResult.MissingCompareFieldsInTargetTable"))
                      .append(Const.CR);
                }
                errorFound = true;
                errorMessage
                    .append("\t\t")
                    .append(insertUpdateKeyField.getKeyLookup())
                    .append(Const.CR);
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
                          PKG, "InsertUpdateMeta.CheckResult.AllLookupFieldsFound"),
                      transformMeta);
            }
            remarks.add(cr);

            // How about the fields to insert/update in the table?
            first = true;
            errorFound = false;
            errorMessage.setLength(0);

            for (int i = 0; i < insertUpdateLookupField.getValueFields().size(); i++) {
              InsertUpdateValue insertUpdateValue = insertUpdateLookupField.getValueFields().get(i);

              IValueMeta v = r.searchValueMeta(insertUpdateValue.getUpdateLookup());
              if (v == null) {
                if (first) {
                  first = false;
                  errorMessage
                      .append(
                          BaseMessages.getString(
                              PKG,
                              "InsertUpdateMeta.CheckResult.MissingFieldsToUpdateInTargetTable"))
                      .append(Const.CR);
                }
                errorFound = true;
                errorMessage
                    .append("\t\t")
                    .append(insertUpdateValue.getUpdateLookup())
                    .append(Const.CR);
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
                          PKG, "InsertUpdateMeta.CheckResult.AllFieldsToUpdateFoundInTargetTable"),
                      transformMeta);
            }
            remarks.add(cr);
          } else {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_ERROR,
                    BaseMessages.getString(
                        PKG, "InsertUpdateMeta.CheckResult.CouldNotReadTableInfo"),
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
                      "InsertUpdateMeta.CheckResult.TransformReceivingDatas",
                      prev.size() + ""),
                  transformMeta);
          remarks.add(cr);

          boolean first = true;
          errorMessage.setLength(0);
          boolean errorFound = false;

          for (int i = 0; i < insertUpdateLookupField.getLookupKeys().size(); i++) {
            InsertUpdateKeyField insertUpdateKeyField =
                insertUpdateLookupField.getLookupKeys().get(i);
            IValueMeta v = prev.searchValueMeta(insertUpdateKeyField.getKeyStream());
            if (v == null) {
              if (first) {
                first = false;
                errorMessage
                    .append(
                        BaseMessages.getString(
                            PKG, "InsertUpdateMeta.CheckResult.MissingFieldsInInput"))
                    .append(Const.CR);
              }
              errorFound = true;
              errorMessage
                  .append("\t\t")
                  .append(insertUpdateKeyField.getKeyStream())
                  .append(Const.CR);
            }

            if (!Utils.isEmpty(insertUpdateKeyField.getKeyStream2())) {
              v = prev.searchValueMeta(insertUpdateKeyField.getKeyStream2());
              if (v == null) {
                if (first) {
                  first = false;
                  errorMessage
                      .append(
                          BaseMessages.getString(
                              PKG, "InsertUpdateMeta.CheckResult.MissingFieldsInInput"))
                      .append(Const.CR);
                }
                errorFound = true;
                errorMessage
                    .append("\t\t")
                    .append(insertUpdateKeyField.getKeyStream2())
                    .append(Const.CR);
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
                        PKG, "InsertUpdateMeta.CheckResult.AllFieldsFoundInInput"),
                    transformMeta);
          }
          remarks.add(cr);

          // How about the fields to insert/update the table with?
          first = true;
          errorFound = false;
          errorMessage.setLength(0);

          for (int i = 0; i < insertUpdateLookupField.getValueFields().size(); i++) {
            InsertUpdateValue insertUpdateValue = insertUpdateLookupField.getValueFields().get(i);

            IValueMeta v = prev.searchValueMeta(insertUpdateValue.getUpdateStream());
            if (v == null) {
              if (first) {
                first = false;
                errorMessage
                    .append(
                        BaseMessages.getString(
                            PKG, "InsertUpdateMeta.CheckResult.MissingInputStreamFields"))
                    .append(Const.CR);
              }
              errorFound = true;
              errorMessage
                  .append("\t\t")
                  .append(insertUpdateValue.getUpdateStream())
                  .append(Const.CR);
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
                        PKG, "InsertUpdateMeta.CheckResult.AllFieldsFoundInInput2"),
                    transformMeta);
          }
          remarks.add(cr);
        } else {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(PKG, "InsertUpdateMeta.CheckResult.MissingFieldsInInput3")
                      + Const.CR,
                  transformMeta);
          remarks.add(cr);
        }
      }
    } catch (HopException e) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "InsertUpdateMeta.CheckResult.DatabaseErrorOccurred")
                  + e.getMessage(),
              transformMeta);
      remarks.add(cr);
    } finally {
      db.disconnect();
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "InsertUpdateMeta.CheckResult.TransformReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "InsertUpdateMeta.CheckResult.NoInputError"),
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

    SqlStatement sqlStatement =
        new SqlStatement(transformMeta.getName(), null, null); // default: nothing to do!

    String connectionName = variables.resolve(connection);

    if (StringUtils.isEmpty(connectionName)) {
      sqlStatement.setError(
          BaseMessages.getString(PKG, "InsertUpdateMeta.ReturnValue.NoConnectionDefined"));
      return sqlStatement;
    }

    DatabaseMeta databaseMeta;

    try {
      databaseMeta = metadataProvider.getSerializer(DatabaseMeta.class).load(connectionName);
      if (databaseMeta == null) {
        sqlStatement.setError(
            "Error finding database connection " + connectionName + " in the metadata");
        return sqlStatement;
      }
    } catch (Exception e) {
      sqlStatement.setError(
          "Error loading database connection "
              + connectionName
              + " from Hop metadata: "
              + Const.getSimpleStackTrace(e));
      return sqlStatement;
    }

    if (prev != null && !prev.isEmpty()) {
      String[] keyLookup = null;
      String[] keyStream = null;
      String[] updateLookup = null;
      String[] updateStream = null;

      if (!insertUpdateLookupField.getLookupKeys().isEmpty()) {
        keyLookup = new String[insertUpdateLookupField.getLookupKeys().size()];
        for (int i = 0; i < insertUpdateLookupField.getLookupKeys().size(); i++) {
          keyLookup[i] = insertUpdateLookupField.getLookupKeys().get(i).getKeyLookup();
        }
      }

      if (!insertUpdateLookupField.getLookupKeys().isEmpty()) {
        keyStream = new String[insertUpdateLookupField.getLookupKeys().size()];
        for (int i = 0; i < insertUpdateLookupField.getLookupKeys().size(); i++) {
          keyStream[i] = insertUpdateLookupField.getLookupKeys().get(i).getKeyStream();
        }
      }

      if (!insertUpdateLookupField.getValueFields().isEmpty()) {
        updateLookup = new String[insertUpdateLookupField.getValueFields().size()];
        for (int i = 0; i < insertUpdateLookupField.getValueFields().size(); i++) {
          updateLookup[i] = insertUpdateLookupField.getValueFields().get(i).getUpdateLookup();
        }
      }

      if (!insertUpdateLookupField.getValueFields().isEmpty()) {
        updateStream = new String[insertUpdateLookupField.getValueFields().size()];
        for (int i = 0; i < insertUpdateLookupField.getValueFields().size(); i++) {
          updateStream[i] = insertUpdateLookupField.getValueFields().get(i).getUpdateStream();
        }
      }

      IRowMeta tableFields =
          RowMetaUtils.getRowMetaForUpdate(prev, keyLookup, keyStream, updateLookup, updateStream);

      if (!Utils.isEmpty(insertUpdateLookupField.getTableName())) {
        Database db = new Database(loggingObject, variables, databaseMeta);
        try {
          db.connect();

          String schemaTable =
              databaseMeta.getQuotedSchemaTableCombination(
                  variables,
                  variables.resolve(insertUpdateLookupField.getSchemaName()),
                  variables.resolve(insertUpdateLookupField.getTableName()));
          String crTable = db.getDDL(schemaTable, tableFields, null, false, null, true);

          String crIndex = "";
          String[] idxFields = null;

          if (keyLookup != null && keyLookup.length > 0) {
            idxFields = new String[keyLookup.length];
            System.arraycopy(keyLookup, 0, idxFields, 0, keyLookup.length);
          } else {
            sqlStatement.setError(
                BaseMessages.getString(PKG, "InsertUpdateMeta.CheckResult.MissingKeyFields"));
          }

          // Key lookup dimensions...
          if (idxFields != null
              && !db.checkIndexExists(
                  variables.resolve(insertUpdateLookupField.getSchemaName()),
                  variables.resolve(insertUpdateLookupField.getTableName()),
                  idxFields)) {
            String indexName =
                "idx_" + variables.resolve(insertUpdateLookupField.getTableName()) + "_lookup";
            crIndex =
                db.getCreateIndexStatement(
                    schemaTable, indexName, idxFields, false, false, false, true);
          }

          String sql = crTable + Const.CR + crIndex;
          if (sql.isEmpty()) {
            sqlStatement.setSql(null);
          } else {
            sqlStatement.setSql(sql);
          }
        } catch (HopException e) {
          sqlStatement.setError(
              BaseMessages.getString(PKG, "InsertUpdateMeta.ReturnValue.ErrorOccurred")
                  + e.getMessage());
        }
      } else {
        sqlStatement.setError(
            BaseMessages.getString(PKG, "InsertUpdateMeta.ReturnValue.NoTableDefinedOnConnection"));
      }
    } else {
      sqlStatement.setError(
          BaseMessages.getString(PKG, "InsertUpdateMeta.ReturnValue.NotReceivingAnyFields"));
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

    DatabaseMeta databaseMeta = null;
    try {
      databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connection));
      if (prev != null) {
        // Lookup: we do a lookup on the natural keys
        for (int i = 0; i < insertUpdateLookupField.getLookupKeys().size(); i++) {
          InsertUpdateKeyField keyField = insertUpdateLookupField.getLookupKeys().get(i);
          IValueMeta v = prev.searchValueMeta(keyField.getKeyStream());

          DatabaseImpact ii =
              new DatabaseImpact(
                  DatabaseImpact.TYPE_IMPACT_READ,
                  pipelineMeta.getName(),
                  transformMeta.getName(),
                  databaseMeta.getDatabaseName(),
                  variables.resolve(insertUpdateLookupField.getTableName()),
                  keyField.getKeyLookup(),
                  keyField.getKeyStream(),
                  v != null ? v.getOrigin() : "?",
                  "",
                  "Type = " + v.toStringMeta());
          impact.add(ii);
        }

        // Insert update fields : read/write
        for (int i = 0; i < insertUpdateLookupField.getValueFields().size(); i++) {
          InsertUpdateValue valueField = insertUpdateLookupField.getValueFields().get(i);
          IValueMeta v = prev.searchValueMeta(valueField.getUpdateStream());

          DatabaseImpact ii =
              new DatabaseImpact(
                  DatabaseImpact.TYPE_IMPACT_READ_WRITE,
                  pipelineMeta.getName(),
                  transformMeta.getName(),
                  databaseMeta.getDatabaseName(),
                  variables.resolve(insertUpdateLookupField.getTableName()),
                  valueField.getUpdateLookup(),
                  valueField.getUpdateStream(),
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

  /**
   * @return Returns the updateBypassed.
   */
  public boolean isUpdateBypassed() {
    return updateBypassed;
  }

  /**
   * @param updateBypassed The updateBypassed to set.
   */
  public void setUpdateBypassed(boolean updateBypassed) {
    this.updateBypassed = updateBypassed;
  }

  @Override
  public IRowMeta getRequiredFields(IVariables variables) throws HopException {

    String realSchemaName = variables.resolve(insertUpdateLookupField.getSchemaName());
    String realTableName = variables.resolve(insertUpdateLookupField.getTableName());
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
                BaseMessages.getString(PKG, "InsertUpdateMeta.Exception.TableNotFound"));
          }
        } else {
          throw new HopException(
              BaseMessages.getString(PKG, "InsertUpdateMeta.Exception.TableNotSpecified"));
        }
      } catch (Exception e) {
        throw new HopException(
            BaseMessages.getString(PKG, "InsertUpdateMeta.Exception.ErrorGettingFields"), e);
      }
    } else {
      throw new HopException(
          BaseMessages.getString(PKG, "InsertUpdateMeta.Exception.ConnectionNotDefined"));
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  public InsertUpdateLookupField getInsertUpdateLookupField() {
    return insertUpdateLookupField;
  }

  public void setInsertUpdateLookupField(InsertUpdateLookupField insertUpdateLookupField) {
    this.insertUpdateLookupField = insertUpdateLookupField;
  }
}
