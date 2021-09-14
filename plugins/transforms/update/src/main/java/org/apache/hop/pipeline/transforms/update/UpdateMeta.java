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

package org.apache.hop.pipeline.transforms.update;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.utils.RowMetaUtils;

import java.util.List;

@Transform(
    id = "Update",
    image = "update.svg",
    name = "i18n::BaseTransform.TypeLongDesc.Update",
    description = "i18n::BaseTransform.TypeTooltipDesc.Update",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    documentationUrl = "/pipeline/transforms/update.html")
public class UpdateMeta extends BaseTransformMeta implements ITransformMeta<Update, UpdateData> {
  private static final Class<?> PKG = UpdateMeta.class; // For Translator

  private IHopMetadataProvider metadataProvider;

  /** database connection */
  private DatabaseMeta databaseMeta;

  /** Commit size for inserts/updates */
  @HopMetadataProperty(
      key = "commit",
      injectionKeyDescription = "UpdateMeta.Injection.CommitSize",
      injectionKey = "COMMIT_SIZE")
  private String commitSize;

  /** Lookup key fields * */
  @HopMetadataProperty(key = "lookup")
  private UpdateLookupField lookupField;

  /** update errors are ignored if this flag is set to true */
  @HopMetadataProperty(
      key = "error_ignored",
      injectionKeyDescription = "UpdateMeta.Injection.IgnoreLookupFailure",
      injectionKey = "IGNORE_LOOKUP_FAILURE")
  private boolean errorIgnored;

  /** adds a boolean field to the output indicating success of the update */
  @HopMetadataProperty(
      key = "ignore_flag_field",
      injectionKeyDescription = "UpdateMeta.Injection.IgnoreFlagField",
      injectionKey = "FLAG_FIELD")
  private String ignoreFlagField;

  /** adds a boolean field to skip lookup and directly update selected fields */
  @HopMetadataProperty(
      key = "skip_lookup",
      injectionKeyDescription = "UpdateMeta.Injection.SkipLookup",
      injectionKey = "SKIP_LOOKUP")
  private boolean skipLookup;

  /**
   * Flag to indicate the use of batch updates, enabled by default but disabled for backward
   * compatibility
   */
  @HopMetadataProperty(
      key = "use_batch",
      injectionKeyDescription = "UpdateMeta.Injection.UseBatchUpdate",
      injectionKey = "BATCH_UPDATE")
  private boolean useBatchUpdate;

  /** database connection */
  @HopMetadataProperty(
      key = "connection",
      injectionKeyDescription = "UpdateMeta.Injection.Connection",
      injectionKey = "CONNECTIONNAME")
  private String connection;

  public String getConnection() {
    return connection;
  }

  public void setConnection(String connection) {
    this.connection = connection;
  }

  public UpdateLookupField getLookupField() {
    return lookupField;
  }

  public void setLookupField(UpdateLookupField lookupField) {
    this.lookupField = lookupField;
  }

  /**
   * @return Returns the commitSize.
   * @deprecated use public String getCommitSizeVar() instead
   */
  @Deprecated
  public int getCommitSize() {
    return Integer.parseInt(commitSize);
  }

  /** @return Returns the commitSize. */
  public String getCommitSizeVar() {
    return commitSize;
  }

  /**
   * @param vs - variable variables to be used for searching variable value usually "this" for a
   *     calling transform
   * @return Returns the commitSize.
   */
  public int getCommitSize(IVariables vs) {
    // this happens when the transform is created via API and no setDefaults was called
    commitSize = (commitSize == null) ? "0" : commitSize;
    return Integer.parseInt(vs.resolve(commitSize));
  }

  /**
   * @param commitSize The commitSize to set.
   * @deprecated use public void setCommitSize( String commitSize ) instead
   */
  @Deprecated
  public void setCommitSize(int commitSize) {
    this.commitSize = Integer.toString(commitSize);
  }

  /** @param commitSize The commitSize to set. */
  public void setCommitSize(String commitSize) {
    this.commitSize = commitSize;
  }

  /** @return Returns the skipLookup. */
  public boolean isSkipLookup() {
    return skipLookup;
  }

  /** @param skipLookup The skipLookup to set. */
  public void setSkipLookup(boolean skipLookup) {
    this.skipLookup = skipLookup;
  }

  /** @return Returns the database. */
  public DatabaseMeta getDatabaseMeta() {
    return databaseMeta;
  }

  /** @param database The database to set. */
  public void setDatabaseMeta(DatabaseMeta database) {
    this.databaseMeta = database;
  }

  /** @return Returns the ignoreError. */
  public boolean isErrorIgnored() {
    return errorIgnored;
  }

  /** @param ignoreError The ignoreError to set. */
  public void setErrorIgnored(boolean ignoreError) {
    this.errorIgnored = ignoreError;
  }

  /** @return Returns the ignoreFlagField. */
  public String getIgnoreFlagField() {
    return ignoreFlagField;
  }

  /** @param ignoreFlagField The ignoreFlagField to set. */
  public void setIgnoreFlagField(String ignoreFlagField) {
    this.ignoreFlagField = ignoreFlagField;
  }

  public UpdateMeta() {
    super();
    lookupField = new UpdateLookupField();
  }

  @Override
  public Object clone() {
    UpdateMeta retval = (UpdateMeta) super.clone();
    return retval;
  }

  @Override
  public Update createTransform(
      TransformMeta transformMeta,
      UpdateData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new Update(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public void setDefault() {
    skipLookup = false;
    databaseMeta = null;
    commitSize = "100";

    lookupField.setSchemaName("");
    lookupField.setTableName(BaseMessages.getString(PKG, "UpdateMeta.DefaultTableName"));
  }

  @Override
  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    if (ignoreFlagField != null && ignoreFlagField.length() > 0) {
      IValueMeta v = new ValueMetaBoolean(ignoreFlagField);
      v.setOrigin(name);

      row.addValueMeta(v);
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
    CheckResult cr;
    String errorMessage = "";

    if (databaseMeta != null) {
      Database db = new Database(loggingObject, variables, databaseMeta);
      try {
        db.connect();

        if (!Utils.isEmpty(lookupField.getTableName())) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(PKG, "UpdateMeta.CheckResult.TableNameOK"),
                  transformMeta);
          remarks.add(cr);

          boolean first = true;
          boolean errorFound = false;
          errorMessage = "";

          // Check fields in table
          IRowMeta r =
              db.getTableFieldsMeta(lookupField.getSchemaName(), lookupField.getTableName());
          if (r != null) {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(PKG, "UpdateMeta.CheckResult.TableExists"),
                    transformMeta);
            remarks.add(cr);

            for (int i = 0; i < lookupField.getLookupKeys().size(); i++) {
              UpdateKeyField keyItem = lookupField.getLookupKeys().get(i);
              IValueMeta v = r.searchValueMeta(keyItem.getKeyLookup());
              if (v == null) {
                if (first) {
                  first = false;
                  errorMessage +=
                      BaseMessages.getString(
                              PKG, "UpdateMeta.CheckResult.MissingCompareFieldsInTargetTable")
                          + Const.CR;
                }
                errorFound = true;
                errorMessage += "\t\t" + keyItem.getKeyLookup() + Const.CR;
              }
            }
            if (errorFound) {
              cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
            } else {
              cr =
                  new CheckResult(
                      ICheckResult.TYPE_RESULT_OK,
                      BaseMessages.getString(PKG, "UpdateMeta.CheckResult.AllLookupFieldsFound"),
                      transformMeta);
            }
            remarks.add(cr);

            // How about the fields to insert/update in the table?
            first = true;
            errorFound = false;
            errorMessage = "";

            for (int i = 0; i < lookupField.getUpdateFields().size(); i++) {

              UpdateField fieldItem = lookupField.getUpdateFields().get(i);
              IValueMeta v = r.searchValueMeta(fieldItem.getUpdateLookup());
              if (v == null) {
                if (first) {
                  first = false;
                  errorMessage +=
                      BaseMessages.getString(
                              PKG, "UpdateMeta.CheckResult.MissingFieldsToUpdateInTargetTable")
                          + Const.CR;
                }
                errorFound = true;
                errorMessage += "\t\t" + fieldItem.getUpdateLookup() + Const.CR;
              }
            }
            if (errorFound) {
              cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
            } else {
              cr =
                  new CheckResult(
                      ICheckResult.TYPE_RESULT_OK,
                      BaseMessages.getString(
                          PKG, "UpdateMeta.CheckResult.AllFieldsToUpdateFoundInTargetTable"),
                      transformMeta);
            }
            remarks.add(cr);
          } else {
            errorMessage =
                BaseMessages.getString(PKG, "UpdateMeta.CheckResult.CouldNotReadTableInfo");
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
                      PKG, "UpdateMeta.CheckResult.TransformReceivingDatas", prev.size() + ""),
                  transformMeta);
          remarks.add(cr);

          boolean first = true;
          errorMessage = "";
          boolean errorFound = false;

          for (int i = 0; i < lookupField.getLookupKeys().size(); i++) {
            UpdateKeyField keyItem = lookupField.getLookupKeys().get(i);
            IValueMeta v = prev.searchValueMeta(keyItem.getKeyStream());
            if (v == null) {
              if (first) {
                first = false;
                errorMessage +=
                    BaseMessages.getString(PKG, "UpdateMeta.CheckResult.MissingFieldsInInput")
                        + Const.CR;
              }
              errorFound = true;
              errorMessage += "\t\t" + keyItem.getKeyStream() + Const.CR;
            }
          }
          for (int i = 0; i < lookupField.getLookupKeys().size(); i++) {
            UpdateKeyField keyItem = lookupField.getLookupKeys().get(i);
            if (!StringUtil.isEmpty(keyItem.getKeyStream2())) {
              IValueMeta v = prev.searchValueMeta(keyItem.getKeyStream2());
              if (v == null) {
                if (first) {
                  first = false;
                  errorMessage +=
                      BaseMessages.getString(PKG, "UpdateMeta.CheckResult.MissingFieldsInInput2")
                          + Const.CR;
                }
                errorFound = true;
                errorMessage += "\t\t" + keyItem.getKeyStream2() + Const.CR;
              }
            }
          }
          if (errorFound) {
            cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
          } else {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(PKG, "UpdateMeta.CheckResult.AllFieldsFoundInInput"),
                    transformMeta);
          }
          remarks.add(cr);

          // How about the fields to insert/update the table with?
          first = true;
          errorFound = false;
          errorMessage = "";

          for (int i = 0; i < lookupField.getUpdateFields().size(); i++) {
            UpdateField fieldItem = lookupField.getUpdateFields().get(i);
            IValueMeta v = prev.searchValueMeta(fieldItem.getUpdateStream());
            if (v == null) {
              if (first) {
                first = false;
                errorMessage +=
                    BaseMessages.getString(PKG, "UpdateMeta.CheckResult.MissingInputStreamFields")
                        + Const.CR;
              }
              errorFound = true;
              errorMessage += "\t\t" + fieldItem.getUpdateStream() + Const.CR;
            }
          }
          if (errorFound) {
            cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
          } else {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(PKG, "UpdateMeta.CheckResult.AllFieldsFoundInInput2"),
                    transformMeta);
          }
          remarks.add(cr);
        } else {
          errorMessage =
              BaseMessages.getString(PKG, "UpdateMeta.CheckResult.MissingFieldsInInput3")
                  + Const.CR;
          cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
          remarks.add(cr);
        }
      } catch (HopException e) {
        errorMessage =
            BaseMessages.getString(PKG, "UpdateMeta.CheckResult.DatabaseErrorOccurred")
                + e.getMessage();
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } finally {
        db.disconnect();
      }
    } else {
      errorMessage = BaseMessages.getString(PKG, "UpdateMeta.CheckResult.InvalidConnection");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "UpdateMeta.CheckResult.TransformReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "UpdateMeta.CheckResult.NoInputError"),
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
    SqlStatement retval =
        new SqlStatement(transformMeta.getName(), databaseMeta, null); // default: nothing to do!

    if (databaseMeta != null) {
      if (prev != null && prev.size() > 0) {

        String[] keyLookup = null;
        String[] keyStream = null;
        String[] updateLookup = null;
        String[] updateStream = null;

        if (lookupField.getLookupKeys().size() > 0) {
          keyLookup = new String[lookupField.getLookupKeys().size()];
          for (int i = 0; i < lookupField.getLookupKeys().size(); i++) {
            keyLookup[i] = lookupField.getLookupKeys().get(i).getKeyLookup();
          }
        }

        if (lookupField.getLookupKeys().size() > 0) {
          keyStream = new String[lookupField.getLookupKeys().size()];
          for (int i = 0; i < lookupField.getLookupKeys().size(); i++) {
            keyStream[i] = lookupField.getLookupKeys().get(i).getKeyStream();
          }
        }

        if (lookupField.getLookupKeys().size() > 0) {
          updateLookup = new String[lookupField.getUpdateFields().size()];
          for (int i = 0; i < lookupField.getUpdateFields().size(); i++) {
            updateLookup[i] = lookupField.getUpdateFields().get(i).getUpdateLookup();
          }
        }

        if (lookupField.getLookupKeys().size() > 0) {
          updateStream = new String[lookupField.getUpdateFields().size()];
          for (int i = 0; i < lookupField.getUpdateFields().size(); i++) {
            updateStream[i] = lookupField.getUpdateFields().get(i).getUpdateStream();
          }
        }
        // Copy the row
        IRowMeta tableFields =
            RowMetaUtils.getRowMetaForUpdate(
                prev, keyLookup, keyStream, updateLookup, updateStream);
        if (!Utils.isEmpty(lookupField.getTableName())) {
          String schemaTable =
              databaseMeta.getQuotedSchemaTableCombination(
                  variables, lookupField.getSchemaName(), lookupField.getTableName());

          Database db = new Database(loggingObject, variables, databaseMeta);
          try {
            db.connect();

            if (getIgnoreFlagField() != null && getIgnoreFlagField().length() > 0) {
              prev.addValueMeta(new ValueMetaBoolean(getIgnoreFlagField()));
            }

            String crTable = db.getDDL(schemaTable, tableFields, null, false, null, true);

            String crIndex = "";
            String[] idxFields = null;

            if (lookupField.getLookupKeys().size() > 0) {
              idxFields = new String[lookupField.getLookupKeys().size()];
              for (int i = 0; i < lookupField.getLookupKeys().size(); i++) {
                idxFields[i] = lookupField.getLookupKeys().get(i).getKeyLookup();
              }
            } else {
              retval.setError(
                  BaseMessages.getString(PKG, "UpdateMeta.CheckResult.MissingKeyFields"));
            }

            // Key lookup dimensions...
            if (idxFields != null
                && idxFields.length > 0
                && !db.checkIndexExists(schemaTable, idxFields)) {
              String indexname = "idx_" + lookupField.getTableName() + "_lookup";
              crIndex =
                  db.getCreateIndexStatement(
                      schemaTable, indexname, idxFields, false, false, false, true);
            }

            String sql = crTable + crIndex;
            if (sql.length() == 0) {
              retval.setSql(null);
            } else {
              retval.setSql(sql);
            }
          } catch (HopException e) {
            retval.setError(
                BaseMessages.getString(PKG, "UpdateMeta.ReturnValue.ErrorOccurred")
                    + e.getMessage());
          }
        } else {
          retval.setError(
              BaseMessages.getString(PKG, "UpdateMeta.ReturnValue.NoTableDefinedOnConnection"));
        }
      } else {
        retval.setError(
            BaseMessages.getString(PKG, "UpdateMeta.ReturnValue.NotReceivingAnyFields"));
      }
    } else {
      retval.setError(BaseMessages.getString(PKG, "UpdateMeta.ReturnValue.NoConnectionDefined"));
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
    if (prev != null) {
      // Lookup: we do a lookup on the natural keys
      for (int i = 0; i < lookupField.getLookupKeys().size(); i++) {
        UpdateKeyField keyFieldItem = lookupField.getLookupKeys().get(i);
        IValueMeta v = prev.searchValueMeta(keyFieldItem.getKeyStream());

        DatabaseImpact ii =
            new DatabaseImpact(
                DatabaseImpact.TYPE_IMPACT_READ,
                pipelineMeta.getName(),
                transformMeta.getName(),
                databaseMeta.getDatabaseName(),
                lookupField.getTableName(),
                keyFieldItem.getKeyLookup(),
                keyFieldItem.getKeyStream(),
                v != null ? v.getOrigin() : "?",
                "",
                "Type = " + v.toStringMeta());
        impact.add(ii);
      }

      // Update fields : read/write
      for (int i = 0; i < lookupField.getUpdateFields().size(); i++) {
        UpdateField fieldItem = lookupField.getUpdateFields().get(i);
        IValueMeta v = prev.searchValueMeta(fieldItem.getUpdateStream());

        DatabaseImpact ii =
            new DatabaseImpact(
                DatabaseImpact.TYPE_IMPACT_UPDATE,
                pipelineMeta.getName(),
                transformMeta.getName(),
                databaseMeta.getDatabaseName(),
                lookupField.getTableName(),
                fieldItem.getUpdateLookup(),
                fieldItem.getUpdateStream(),
                v != null ? v.getOrigin() : "?",
                "",
                "Type = " + v.toStringMeta());
        impact.add(ii);
      }
    }
  }

  @Override
  public UpdateData getTransformData() {
    return new UpdateData();
  }

  @Override
  public DatabaseMeta[] getUsedDatabaseConnections() {
    if (databaseMeta != null) {
      return new DatabaseMeta[] {databaseMeta};
    } else {
      return super.getUsedDatabaseConnections();
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  /** @return the useBatchUpdate */
  public boolean isUseBatchUpdate() {
    return useBatchUpdate;
  }

  /** @param useBatchUpdate the useBatchUpdate to set */
  public void setUseBatchUpdate(boolean useBatchUpdate) {
    this.useBatchUpdate = useBatchUpdate;
  }
}
