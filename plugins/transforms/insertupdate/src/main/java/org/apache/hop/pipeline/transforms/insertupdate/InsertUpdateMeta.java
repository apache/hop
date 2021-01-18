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

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.IProvidesModelerMeta;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.AfterInjection;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.utils.RowMetaUtils;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.List;

/*
 * Created on 26-apr-2003
 *
 */
@InjectionSupported(
    localizationPrefix = "InsertUpdateMeta.Injection.",
    groups = {"KEYS", "UPDATES"})
@Transform(
    id = "InsertUpdate",
    image = "insertupdate.svg",
    name = "i18n::BaseTransform.TypeLongDesc.InsertUpdate",
    description = "i18n::BaseTransform.TypeTooltipDesc.InsertUpdate",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/insertupdate.html")
public class InsertUpdateMeta extends BaseTransformMeta
    implements ITransformMeta<InsertUpdate, InsertUpdateData>, IProvidesModelerMeta {
  private static final Class<?> PKG = InsertUpdateMeta.class; // For Translator

  private IHopMetadataProvider metadataProvider;

  /** what's the lookup schema? */
  @Injection(name = "SCHEMA_NAME")
  private String schemaName;

  /** what's the lookup table? */
  @Injection(name = "TABLE_NAME")
  private String tableName;

  /** database connection */
  private DatabaseMeta databaseMeta;

  /** which field in input stream to compare with? */
  @Injection(name = "KEY_STREAM", group = "KEYS")
  private String[] keyStream;

  /** field in table */
  @Injection(name = "KEY_LOOKUP", group = "KEYS")
  private String[] keyLookup;

  /** Comparator: =, <>, BETWEEN, ... */
  @Injection(name = "KEY_CONDITION", group = "KEYS")
  private String[] keyCondition;

  /** Extra field for between... */
  @Injection(name = "KEY_STREAM2", group = "KEYS")
  private String[] keyStream2;

  /** Field value to update after lookup */
  @Injection(name = "UPDATE_LOOKUP", group = "UPDATES")
  private String[] updateLookup;

  /** Stream name to update value with */
  @Injection(name = "UPDATE_STREAM", group = "UPDATES")
  private String[] updateStream;

  /** boolean indicating if field needs to be updated */
  @Injection(name = "UPDATE_FLAG", group = "UPDATES")
  private Boolean[] update;

  /** Commit size for inserts/updates */
  @Injection(name = "COMMIT_SIZE")
  private String commitSize;

  /** Bypass any updates */
  @Injection(name = "DO_NOT")
  private boolean updateBypassed;

  @Injection(name = "CONNECTIONNAME")
  public void setConnection(String connectionName) {
    try {
      databaseMeta = DatabaseMeta.loadDatabase(metadataProvider, connectionName);
    } catch (HopXmlException e) {
      throw new RuntimeException("Unable to load connection '" + connectionName + "'", e);
    }
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

  /** @return Returns the database. */
  public DatabaseMeta getDatabaseMeta() {
    return databaseMeta;
  }

  /** @param database The database to set. */
  public void setDatabaseMeta(DatabaseMeta database) {
    this.databaseMeta = database;
  }

  /** @return Returns the keyCondition. */
  public String[] getKeyCondition() {
    return keyCondition;
  }

  /** @param keyCondition The keyCondition to set. */
  public void setKeyCondition(String[] keyCondition) {
    this.keyCondition = keyCondition;
  }

  /** @return Returns the keyLookup. */
  public String[] getKeyLookup() {
    return keyLookup;
  }

  /** @param keyLookup The keyLookup to set. */
  public void setKeyLookup(String[] keyLookup) {
    this.keyLookup = keyLookup;
  }

  /** @return Returns the keyStream. */
  public String[] getKeyStream() {
    return keyStream;
  }

  /** @param keyStream The keyStream to set. */
  public void setKeyStream(String[] keyStream) {
    this.keyStream = keyStream;
  }

  /** @return Returns the keyStream2. */
  public String[] getKeyStream2() {
    return keyStream2;
  }

  /** @param keyStream2 The keyStream2 to set. */
  public void setKeyStream2(String[] keyStream2) {
    this.keyStream2 = keyStream2;
  }

  /** @return Returns the tableName. */
  public String getTableName() {
    return tableName;
  }

  /** @param tableName The tableName to set. */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /** @return Returns the updateLookup. */
  public String[] getUpdateLookup() {
    return updateLookup;
  }

  /** @param updateLookup The updateLookup to set. */
  public void setUpdateLookup(String[] updateLookup) {
    this.updateLookup = updateLookup;
  }

  /** @return Returns the updateStream. */
  public String[] getUpdateStream() {
    return updateStream;
  }

  /** @param updateStream The updateStream to set. */
  public void setUpdateStream(String[] updateStream) {
    this.updateStream = updateStream;
  }

  public Boolean[] getUpdate() {
    return update;
  }

  public void setUpdate(Boolean[] update) {
    this.update = update;
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode, metadataProvider);
  }

  public void allocate(int nrkeys, int nrvalues) {
    keyStream = new String[nrkeys];
    keyLookup = new String[nrkeys];
    keyCondition = new String[nrkeys];
    keyStream2 = new String[nrkeys];
    updateLookup = new String[nrvalues];
    updateStream = new String[nrvalues];
    update = new Boolean[nrvalues];
  }

  public Object clone() {
    InsertUpdateMeta retval = (InsertUpdateMeta) super.clone();
    int nrkeys = keyStream.length;
    int nrvalues = updateLookup.length;

    retval.allocate(nrkeys, nrvalues);

    System.arraycopy(keyStream, 0, retval.keyStream, 0, nrkeys);
    System.arraycopy(keyLookup, 0, retval.keyLookup, 0, nrkeys);
    System.arraycopy(keyCondition, 0, retval.keyCondition, 0, nrkeys);
    System.arraycopy(keyStream2, 0, retval.keyStream2, 0, nrkeys);

    System.arraycopy(updateLookup, 0, retval.updateLookup, 0, nrvalues);
    System.arraycopy(updateStream, 0, retval.updateStream, 0, nrvalues);
    System.arraycopy(update, 0, retval.update, 0, nrvalues);

    return retval;
  }

  @Override
  public InsertUpdate createTransform(
      TransformMeta transformMeta,
      InsertUpdateData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new InsertUpdate(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  private void readData(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    this.metadataProvider = metadataProvider;
    try {
      String csize;
      int nrkeys, nrvalues;

      String con = XmlHandler.getTagValue(transformNode, "connection");
      databaseMeta = DatabaseMeta.loadDatabase(metadataProvider, con);
      csize = XmlHandler.getTagValue(transformNode, "commit");
      commitSize = (csize != null) ? csize : "0";
      schemaName = XmlHandler.getTagValue(transformNode, "lookup", "schema");
      tableName = XmlHandler.getTagValue(transformNode, "lookup", "table");
      updateBypassed =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "update_bypassed"));

      Node lookup = XmlHandler.getSubNode(transformNode, "lookup");
      nrkeys = XmlHandler.countNodes(lookup, "key");
      nrvalues = XmlHandler.countNodes(lookup, "value");

      allocate(nrkeys, nrvalues);

      for (int i = 0; i < nrkeys; i++) {
        Node knode = XmlHandler.getSubNodeByNr(lookup, "key", i);

        keyStream[i] = XmlHandler.getTagValue(knode, "name");
        keyLookup[i] = XmlHandler.getTagValue(knode, "field");
        keyCondition[i] = XmlHandler.getTagValue(knode, "condition");
        if (keyCondition[i] == null) {
          keyCondition[i] = "=";
        }
        keyStream2[i] = XmlHandler.getTagValue(knode, "name2");
      }

      for (int i = 0; i < nrvalues; i++) {
        Node vnode = XmlHandler.getSubNodeByNr(lookup, "value", i);

        updateLookup[i] = XmlHandler.getTagValue(vnode, "name");
        updateStream[i] = XmlHandler.getTagValue(vnode, "rename");
        if (updateStream[i] == null) {
          updateStream[i] = updateLookup[i]; // default: the same name!
        }
        String updateValue = XmlHandler.getTagValue(vnode, "update");
        if (updateValue == null) {
          // default TRUE
          update[i] = Boolean.TRUE;
        } else {
          if (updateValue.equalsIgnoreCase("Y")) {
            update[i] = Boolean.TRUE;
          } else {
            update[i] = Boolean.FALSE;
          }
        }
      }
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(
              PKG, "InsertUpdateMeta.Exception.UnableToReadTransformInfoFromXML"),
          e);
    }
  }

  public void setDefault() {
    keyStream = null;
    updateLookup = null;
    databaseMeta = null;
    commitSize = "100";
    schemaName = "";
    tableName = BaseMessages.getString(PKG, "InsertUpdateMeta.DefaultTableName");

    int nrkeys = 0;
    int nrvalues = 0;

    allocate(nrkeys, nrvalues);

    for (int i = 0; i < nrkeys; i++) {
      keyLookup[i] = "age";
      keyCondition[i] = "BETWEEN";
      keyStream[i] = "age_from";
      keyStream2[i] = "age_to";
    }

    for (int i = 0; i < nrvalues; i++) {
      updateLookup[i] = BaseMessages.getString(PKG, "InsertUpdateMeta.ColumnName.ReturnField") + i;
      updateStream[i] = BaseMessages.getString(PKG, "InsertUpdateMeta.ColumnName.NewName") + i;
      update[i] = Boolean.TRUE;
    }
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder(400);

    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "connection", databaseMeta == null ? "" : databaseMeta.getName()));
    retval.append("    ").append(XmlHandler.addTagValue("commit", commitSize));
    retval.append("    ").append(XmlHandler.addTagValue("update_bypassed", updateBypassed));
    retval.append("    <lookup>").append(Const.CR);
    retval.append("      ").append(XmlHandler.addTagValue("schema", schemaName));
    retval.append("      ").append(XmlHandler.addTagValue("table", tableName));

    for (int i = 0; i < keyStream.length; i++) {
      retval.append("      <key>").append(Const.CR);
      retval.append("        ").append(XmlHandler.addTagValue("name", keyStream[i]));
      retval.append("        ").append(XmlHandler.addTagValue("field", keyLookup[i]));
      retval.append("        ").append(XmlHandler.addTagValue("condition", keyCondition[i]));
      retval.append("        ").append(XmlHandler.addTagValue("name2", keyStream2[i]));
      retval.append("      </key>").append(Const.CR);
    }

    for (int i = 0; i < updateLookup.length; i++) {
      retval.append("      <value>").append(Const.CR);
      retval.append("        ").append(XmlHandler.addTagValue("name", updateLookup[i]));
      retval.append("        ").append(XmlHandler.addTagValue("rename", updateStream[i]));
      retval.append("        ").append(XmlHandler.addTagValue("update", update[i].booleanValue()));
      retval.append("      </value>").append(Const.CR);
    }

    retval.append("    </lookup>").append(Const.CR);

    return retval.toString();
  }

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
      Database db = new Database(loggingObject, variables, databaseMeta );
      try {
        db.connect();

        if (!Utils.isEmpty(tableName)) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(PKG, "InsertUpdateMeta.CheckResult.TableNameOK"),
                  transformMeta);
          remarks.add(cr);

          boolean first = true;
          boolean errorFound = false;
          errorMessage = "";

          // Check fields in table
          IRowMeta r = db.getTableFieldsMeta(schemaName, tableName);
          if (r != null) {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(PKG, "InsertUpdateMeta.CheckResult.TableExists"),
                    transformMeta);
            remarks.add(cr);

            for (int i = 0; i < keyLookup.length; i++) {
              String lufield = keyLookup[i];

              IValueMeta v = r.searchValueMeta(lufield);
              if (v == null) {
                if (first) {
                  first = false;
                  errorMessage +=
                      BaseMessages.getString(
                              PKG, "InsertUpdateMeta.CheckResult.MissingCompareFieldsInTargetTable")
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
                      BaseMessages.getString(
                          PKG, "InsertUpdateMeta.CheckResult.AllLookupFieldsFound"),
                      transformMeta);
            }
            remarks.add(cr);

            // How about the fields to insert/update in the table?
            first = true;
            errorFound = false;
            errorMessage = "";

            for (int i = 0; i < updateLookup.length; i++) {
              String lufield = updateLookup[i];

              IValueMeta v = r.searchValueMeta(lufield);
              if (v == null) {
                if (first) {
                  first = false;
                  errorMessage +=
                      BaseMessages.getString(
                              PKG,
                              "InsertUpdateMeta.CheckResult.MissingFieldsToUpdateInTargetTable")
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
                      BaseMessages.getString(
                          PKG, "InsertUpdateMeta.CheckResult.AllFieldsToUpdateFoundInTargetTable"),
                      transformMeta);
            }
            remarks.add(cr);
          } else {
            errorMessage =
                BaseMessages.getString(PKG, "InsertUpdateMeta.CheckResult.CouldNotReadTableInfo");
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
                      "InsertUpdateMeta.CheckResult.TransformReceivingDatas",
                      prev.size() + ""),
                  transformMeta);
          remarks.add(cr);

          boolean first = true;
          errorMessage = "";
          boolean errorFound = false;

          for (int i = 0; i < keyStream.length; i++) {
            IValueMeta v = prev.searchValueMeta(keyStream[i]);
            if (v == null) {
              if (first) {
                first = false;
                errorMessage +=
                    BaseMessages.getString(PKG, "InsertUpdateMeta.CheckResult.MissingFieldsInInput")
                        + Const.CR;
              }
              errorFound = true;
              errorMessage += "\t\t" + keyStream[i] + Const.CR;
            }
          }
          for (int i = 0; i < keyStream2.length; i++) {
            if (keyStream2[i] != null && keyStream2[i].length() > 0) {
              IValueMeta v = prev.searchValueMeta(keyStream2[i]);
              if (v == null) {
                if (first) {
                  first = false;
                  errorMessage +=
                      BaseMessages.getString(
                              PKG, "InsertUpdateMeta.CheckResult.MissingFieldsInInput")
                          + Const.CR;
                }
                errorFound = true;
                errorMessage += "\t\t" + keyStream[i] + Const.CR;
              }
            }
          }
          if (errorFound) {
            cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
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
          errorMessage = "";

          for (int i = 0; i < updateStream.length; i++) {
            String lufield = updateStream[i];

            IValueMeta v = prev.searchValueMeta(lufield);
            if (v == null) {
              if (first) {
                first = false;
                errorMessage +=
                    BaseMessages.getString(
                            PKG, "InsertUpdateMeta.CheckResult.MissingInputStreamFields")
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
                    BaseMessages.getString(
                        PKG, "InsertUpdateMeta.CheckResult.AllFieldsFoundInInput2"),
                    transformMeta);
          }
          remarks.add(cr);
        } else {
          errorMessage =
              BaseMessages.getString(PKG, "InsertUpdateMeta.CheckResult.MissingFieldsInInput3")
                  + Const.CR;
          cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
          remarks.add(cr);
        }
      } catch (HopException e) {
        errorMessage =
            BaseMessages.getString(PKG, "InsertUpdateMeta.CheckResult.DatabaseErrorOccurred")
                + e.getMessage();
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } finally {
        db.disconnect();
      }
    } else {
      errorMessage = BaseMessages.getString(PKG, "InsertUpdateMeta.CheckResult.InvalidConnection");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
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
        // Copy the row
        IRowMeta tableFields =
            RowMetaUtils.getRowMetaForUpdate(
                prev, keyLookup, keyStream, updateLookup, updateStream);

        if (!Utils.isEmpty(tableName)) {
          Database db = new Database(loggingObject, variables, databaseMeta );
          try {
            db.connect();

            String schemaTable =
                databaseMeta.getQuotedSchemaTableCombination(variables, schemaName, tableName);
            String crTable = db.getDDL(schemaTable, tableFields, null, false, null, true);

            String crIndex = "";
            String[] idxFields = null;

            if (keyLookup != null && keyLookup.length > 0) {
              idxFields = new String[keyLookup.length];
              for (int i = 0; i < keyLookup.length; i++) {
                idxFields[i] = keyLookup[i];
              }
            } else {
              retval.setError(
                  BaseMessages.getString(PKG, "InsertUpdateMeta.CheckResult.MissingKeyFields"));
            }

            // Key lookup dimensions...
            if (idxFields != null
                && idxFields.length > 0
                && !db.checkIndexExists(schemaName, tableName, idxFields)) {
              String indexname = "idx_" + tableName + "_lookup";
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
                BaseMessages.getString(PKG, "InsertUpdateMeta.ReturnValue.ErrorOccurred")
                    + e.getMessage());
          }
        } else {
          retval.setError(
              BaseMessages.getString(
                  PKG, "InsertUpdateMeta.ReturnValue.NoTableDefinedOnConnection"));
        }
      } else {
        retval.setError(
            BaseMessages.getString(PKG, "InsertUpdateMeta.ReturnValue.NotReceivingAnyFields"));
      }
    } else {
      retval.setError(
          BaseMessages.getString(PKG, "InsertUpdateMeta.ReturnValue.NoConnectionDefined"));
    }

    return retval;
  }

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
      for (int i = 0; i < keyLookup.length; i++) {
        IValueMeta v = prev.searchValueMeta(keyStream[i]);

        DatabaseImpact ii =
            new DatabaseImpact(
                DatabaseImpact.TYPE_IMPACT_READ,
                pipelineMeta.getName(),
                transformMeta.getName(),
                databaseMeta.getDatabaseName(),
                tableName,
                keyLookup[i],
                keyStream[i],
                v != null ? v.getOrigin() : "?",
                "",
                "Type = " + v.toStringMeta());
        impact.add(ii);
      }

      // Insert update fields : read/write
      for (int i = 0; i < updateLookup.length; i++) {
        IValueMeta v = prev.searchValueMeta(updateStream[i]);

        DatabaseImpact ii =
            new DatabaseImpact(
                DatabaseImpact.TYPE_IMPACT_READ_WRITE,
                pipelineMeta.getName(),
                transformMeta.getName(),
                databaseMeta.getDatabaseName(),
                tableName,
                updateLookup[i],
                updateStream[i],
                v != null ? v.getOrigin() : "?",
                "",
                "Type = " + v.toStringMeta());
        impact.add(ii);
      }
    }
  }

  public InsertUpdateData getTransformData() {
    return new InsertUpdateData();
  }

  public DatabaseMeta[] getUsedDatabaseConnections() {
    if (databaseMeta != null) {
      return new DatabaseMeta[] {databaseMeta};
    } else {
      return super.getUsedDatabaseConnections();
    }
  }

  /** @return Returns the updateBypassed. */
  public boolean isUpdateBypassed() {
    return updateBypassed;
  }

  /** @param updateBypassed The updateBypassed to set. */
  public void setUpdateBypassed(boolean updateBypassed) {
    this.updateBypassed = updateBypassed;
  }

  public IRowMeta getRequiredFields(IVariables variables) throws HopException {
    String realSchemaName = variables.resolve(schemaName);
    String realTableName = variables.resolve(tableName);

    if (databaseMeta != null) {
      Database db = new Database(loggingObject, variables, databaseMeta );
      try {
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
      } finally {
        db.disconnect();
      }
    } else {
      throw new HopException(
          BaseMessages.getString(PKG, "InsertUpdateMeta.Exception.ConnectionNotDefined"));
    }
  }

  /** @return the schemaName */
  public String getSchemaName() {
    return schemaName;
  }

  @Override
  public String getMissingDatabaseConnectionInformationMessage() {
    return null;
  }

  /** @param schemaName the schemaName to set */
  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public boolean supportsErrorHandling() {
    return true;
  }

  @Override
  public RowMeta getRowMeta( IVariables variables, ITransformData transformData ) {
    return (RowMeta) ((InsertUpdateData) transformData).insertRowMeta;
  }

  @Override
  public List<String> getDatabaseFields() {
    return Arrays.asList(updateLookup);
  }

  @Override
  public List<String> getStreamFields() {
    return Arrays.asList(updateStream);
  }

  /**
   * If we use injection we can have different arrays lengths. We need synchronize them for
   * consistency behavior with UI
   */
  @AfterInjection
  public void afterInjectionSynchronization() {
    if (keyStream == null || keyStream.length == 0) {
      return;
    }
    int nrFields = keyStream.length;
    // PDI-16349
    if (keyStream2.length < nrFields) {
      String[] newKeyStream2 = new String[nrFields];
      System.arraycopy(keyStream2, 0, newKeyStream2, 0, keyStream2.length);
      keyStream2 = newKeyStream2;
    }
  }
}
