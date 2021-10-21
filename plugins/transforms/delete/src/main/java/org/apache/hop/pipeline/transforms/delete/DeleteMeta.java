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

package org.apache.hop.pipeline.transforms.delete;

import org.apache.commons.lang.StringUtils;
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

import java.util.List;

/**
 * This class takes care of deleting values in a table using a certain condition and values for
 * input.
 */
@Transform(
    id = "Delete",
    image = "delete.svg",
    name = "i18n::BaseTransform.TypeLongDesc.Delete",
    description = "i18n::BaseTransform.TypeTooltipDesc.Delete",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    documentationUrl = "/pipeline/transforms/delete.html")
public class DeleteMeta extends BaseTransformMeta implements ITransformMeta<Delete, DeleteData> {
  private static final Class<?> PKG = DeleteMeta.class; // For Translator

  /** database connection */
  @HopMetadataProperty(key = "lookup")
  private DeleteLookupField lookup;

  /** database connection */
  @HopMetadataProperty(key = "connection", injectionKeyDescription = "Delete.Injection.Connection")
  private String connection;

  /** Commit size for inserts/updates */
  @HopMetadataProperty(
      key = "commit",
      injectionKeyDescription = "Delete.Injection.CommitSize.Field")
  private String commitSize;

  public DeleteMeta() {
    super();
    lookup = new DeleteLookupField();
    // allocate BaseTransformMeta
  }

  public String getConnection() {
    return connection;
  }

  public void setConnection(String connection) {
    this.connection = connection;
  }

  /** @return Returns the commitSize. */
  public String getCommitSizeVar() {
    return commitSize;
  }

  public DeleteLookupField getLookup() {
    return lookup;
  }

  public void setLookup(DeleteLookupField lookup) {
    this.lookup = lookup;
  }

  public String getCommitSize() {
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

  /** @param commitSize The commitSize to set. */
  public void setCommitSize(String commitSize) {
    this.commitSize = commitSize;
  }

  public DeleteMeta(DeleteMeta obj) {

    this.connection = obj.connection;
    this.commitSize = obj.commitSize;
    this.lookup = new DeleteLookupField(obj.lookup);
  }

  @Override
  public Object clone() {
    return new DeleteMeta(this);
  }

  @Override
  public void setDefault() {
    commitSize = "100";
    lookup.setSchemaName("");
    lookup.setTableName(BaseMessages.getString(PKG, "DeleteMeta.DefaultTableName.Label"));
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
    String errorMessage = "";
    DatabaseMeta databaseMeta = null;

    try {
      databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connection));
    } catch (HopException e) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "DeleteMeta.CheckResult.DatabaseMetaError", variables.resolve(connection)),
              transformMeta);
      remarks.add(cr);
    }

    if (databaseMeta != null) {
      Database db = new Database(loggingObject, variables, databaseMeta);
      try {
        db.connect();

        if (!Utils.isEmpty(lookup.getTableName())) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(PKG, "DeleteMeta.CheckResult.TablenameOK"),
                  transformMeta);
          remarks.add(cr);

          boolean first = true;
          boolean errorFound = false;
          errorMessage = "";

          // Check fields in table
          IRowMeta r = db.getTableFieldsMeta(lookup.getSchemaName(), lookup.getTableName());
          if (r != null) {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(PKG, "DeleteMeta.CheckResult.VisitTableSuccessfully"),
                    transformMeta);
            remarks.add(cr);

            List<DeleteKeyField> keyFields = lookup.getFields();
            for (int i = 0; i < keyFields.size(); i++) {
              String lufield = keyFields.get(i).getKeyLookup();

              IValueMeta v = r.searchValueMeta(lufield);
              if (v == null) {
                if (first) {
                  first = false;
                  errorMessage +=
                      BaseMessages.getString(
                              PKG, "DeleteMeta.CheckResult.MissingCompareFieldsInTargetTable")
                          + Const.CR;
                }
                errorFound = true;
                errorMessage += "\t\t" + lufield + Const.CR;
              }
            }
            if (errorFound) {
              cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
            } else {
              cr =
                  new CheckResult(
                      ICheckResult.TYPE_RESULT_OK,
                      BaseMessages.getString(PKG, "DeleteMeta.CheckResult.FoundLookupFields"),
                      transformMeta);
            }
            remarks.add(cr);
          } else {
            errorMessage =
                BaseMessages.getString(PKG, "DeleteMeta.CheckResult.CouldNotReadTableInfo");
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
                      "DeleteMeta.CheckResult.ConnectedTransformSuccessfully",
                      String.valueOf(prev.size())),
                  transformMeta);
          remarks.add(cr);

          boolean first = true;
          errorMessage = "";
          boolean errorFound = false;

          List<DeleteKeyField> keyFields = lookup.getFields();
          for (int i = 0; i < keyFields.size(); i++) {
            String keyStr = keyFields.get(i).getKeyStream();
            IValueMeta v = prev.searchValueMeta(keyStr);
            if (v == null) {
              if (first) {
                first = false;
                errorMessage +=
                    BaseMessages.getString(PKG, "DeleteMeta.CheckResult.MissingFields") + Const.CR;
              }
              errorFound = true;
              errorMessage += "\t\t" + keyStr + Const.CR;
            }
          }
          for (int i = 0; i < keyFields.size(); i++) {
            String keyStr2 = keyFields.get(i).getKeyStream2();
            if (!StringUtils.isEmpty(keyStr2)) {
              IValueMeta v = prev.searchValueMeta(keyStr2);
              if (v == null) {
                if (first) {
                  first = false;
                  errorMessage +=
                      BaseMessages.getString(PKG, "DeleteMeta.CheckResult.MissingFields2")
                          + Const.CR;
                }
                errorFound = true;
                errorMessage += "\t\t" + keyStr2 + Const.CR;
              }
            }
          }
          if (errorFound) {
            cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
          } else {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(PKG, "DeleteMeta.CheckResult.AllFieldsFound"),
                    transformMeta);
          }
          remarks.add(cr);

        } else {
          errorMessage =
              BaseMessages.getString(PKG, "DeleteMeta.CheckResult.MissingFields3") + Const.CR;
          cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
          remarks.add(cr);
        }
      } catch (HopException e) {
        errorMessage =
            BaseMessages.getString(PKG, "DeleteMeta.CheckResult.DatabaseError") + e.getMessage();
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } finally {
        db.disconnect();
      }
    } else {
      errorMessage = BaseMessages.getString(PKG, "DeleteMeta.CheckResult.InvalidConnection");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "DeleteMeta.CheckResult.TransformReceivingInfo"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "DeleteMeta.CheckResult.NoInputReceived"),
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
      IHopMetadataProvider metadataProvider) {

    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(connection, variables);

    SqlStatement retval =
        new SqlStatement(transformMeta.getName(), databaseMeta, null); // default: nothing to do!

    if (databaseMeta != null) {
      if (prev != null && prev.size() > 0) {
        if (!Utils.isEmpty(lookup.getTableName())) {
          Database db = new Database(loggingObject, variables, databaseMeta);
          try {
            db.connect();

            String schemaTable =
                databaseMeta.getQuotedSchemaTableCombination(
                    variables, lookup.getSchemaName(), lookup.getTableName());
            String crTable = db.getDDL(schemaTable, prev, null, false, null, true);

            String crIndex = "";
            String[] idxFields = null;
            List<DeleteKeyField> keyFields = lookup.getFields();
            if (keyFields.size() > 0) {
              idxFields = new String[keyFields.size()];
              for (int i = 0; i < keyFields.size(); i++) {
                idxFields[i] = keyFields.get(i).getKeyLookup();
              }
            } else {
              retval.setError(
                  BaseMessages.getString(PKG, "DeleteMeta.CheckResult.KeyFieldsRequired"));
            }

            // Key lookup dimensions...
            if (idxFields != null
                && idxFields.length > 0
                && !db.checkIndexExists(schemaTable, idxFields)) {
              String indexname = "idx_" + lookup.getTableName() + "_lookup";
              crIndex =
                  db.getCreateIndexStatement(
                      lookup.getSchemaName(),
                      lookup.getTableName(),
                      indexname,
                      idxFields,
                      false,
                      false,
                      false,
                      true);
            }

            String sql = crTable + crIndex;
            if (sql.length() == 0) {
              retval.setSql(null);
            } else {
              retval.setSql(sql);
            }
          } catch (HopException e) {
            retval.setError(
                BaseMessages.getString(PKG, "DeleteMeta.Returnvalue.ErrorOccurred")
                    + e.getMessage());
          }
        } else {
          retval.setError(
              BaseMessages.getString(PKG, "DeleteMeta.Returnvalue.NoTableDefinedOnConnection"));
        }
      } else {
        retval.setError(BaseMessages.getString(PKG, "DeleteMeta.Returnvalue.NoReceivingAnyFields"));
      }
    } else {
      retval.setError(BaseMessages.getString(PKG, "DeleteMeta.Returnvalue.NoConnectionDefined"));
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
      List<DeleteKeyField> keyFields = lookup.getFields();
      for (int i = 0; i < keyFields.size(); i++) {
        String keyStr = keyFields.get(i).getKeyStream();
        IValueMeta v = prev.searchValueMeta(keyStr);

        try {
          DatabaseMeta databaseMeta =
              metadataProvider
                  .getSerializer(DatabaseMeta.class)
                  .load(variables.resolve(connection));
          DatabaseImpact ii =
              new DatabaseImpact(
                  DatabaseImpact.TYPE_IMPACT_DELETE,
                  pipelineMeta.getName(),
                  transformMeta.getName(),
                  databaseMeta.getDatabaseName(),
                  lookup.getTableName(),
                  keyFields.get(i).getKeyLookup(),
                  keyFields.get(i).getKeyStream(),
                  v != null ? v.getOrigin() : "?",
                  "",
                  "Type = " + v.toStringMeta());
          impact.add(ii);
        } catch (HopException e) {
          throw new HopTransformException(
              "Unable to get databaseMeta for connection: "
                  + Const.CR
                  + variables.resolve(connection),
              e);
        }
      }
    }
  }

  @Override
  public Delete createTransform(
      TransformMeta transformMeta, DeleteData data, int cnr, PipelineMeta tr, Pipeline pipeline) {
    return new Delete(transformMeta, this, data, cnr, tr, pipeline);
  }

  @Override
  public DeleteData getTransformData() {
    return new DeleteData();
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
