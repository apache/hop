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

package org.apache.hop.pipeline.transforms.databaselookup;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.IProvidesModelerMeta;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
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
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.List;

@Transform(
    id = "DBLookup",
    image = "dblookup.svg",
    name = "i18n::BaseTransform.TypeLongDesc.DatabaseLookup",
    description = "i18n::BaseTransform.TypeTooltipDesc.DatabaseLookup",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Lookup",
    documentationUrl =
        "https://hop.apache.org/manual/latest/plugins/transforms/databaselookup.html")
public class DatabaseLookupMeta extends BaseTransformMeta
    implements ITransformMeta<DatabaseLookup, DatabaseLookupData>, IProvidesModelerMeta {

  private static final Class<?> PKG = DatabaseLookupMeta.class; // For Translator

  public static final String[] conditionStrings =
      new String[] {
        "=", "<>", "<", "<=", ">", ">=", "LIKE", "BETWEEN", "IS NULL", "IS NOT NULL",
      };

  public static final int CONDITION_EQ = 0;
  public static final int CONDITION_NE = 1;
  public static final int CONDITION_LT = 2;
  public static final int CONDITION_LE = 3;
  public static final int CONDITION_GT = 4;
  public static final int CONDITION_GE = 5;
  public static final int CONDITION_LIKE = 6;
  public static final int CONDITION_BETWEEN = 7;
  public static final int CONDITION_IS_NULL = 8;
  public static final int CONDITION_IS_NOT_NULL = 9;

  /** what's the lookup schema name? */
  private String schemaName;

  /** what's the lookup table? */
  private String tableName;

  /** database connection */
  private DatabaseMeta databaseMeta;

  /** which field in input stream to compare with? */
  private String[] streamKeyField1;

  /** Extra field for between... */
  private String[] streamKeyField2;

  /** Comparator: =, <>, BETWEEN, ... */
  private String[] keyCondition;

  /** field in table */
  private String[] tableKeyField;

  /** return these field values after lookup */
  private String[] returnValueField;

  /** new name for value ... */
  private String[] returnValueNewName;

  /** default value in case not found... */
  private String[] returnValueDefault;

  /** type of default value */
  private int[] returnValueDefaultType;

  /** order by clause... */
  private String orderByClause;

  /** ICache values we look up --> faster */
  private boolean cached;

  /** Limit the cache size to this! */
  private int cacheSize;

  /** Flag to make it load all data into the cache at startup */
  private boolean loadingAllDataInCache;

  /** Have the lookup fail if multiple results were found, renders the orderByClause useless */
  private boolean failingOnMultipleResults;

  /** Have the lookup eat the incoming row when nothing gets found */
  private boolean eatingRowOnLookupFailure;

  public DatabaseLookupMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the cached. */
  public boolean isCached() {
    return cached;
  }

  /** @param cached The cached to set. */
  public void setCached(boolean cached) {
    this.cached = cached;
  }

  /** @return Returns the cacheSize. */
  public int getCacheSize() {
    return cacheSize;
  }

  /** @param cacheSize The cacheSize to set. */
  public void setCacheSize(int cacheSize) {
    this.cacheSize = cacheSize;
  }

  /** @return Returns the database. */
  @Override
  public DatabaseMeta getDatabaseMeta() {
    return databaseMeta;
  }

  @Override
  public String getTableName() {
    return tableName;
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

  /** @return Returns the orderByClause. */
  public String getOrderByClause() {
    return orderByClause;
  }

  /** @param orderByClause The orderByClause to set. */
  public void setOrderByClause(String orderByClause) {
    this.orderByClause = orderByClause;
  }

  /** @return Returns the returnValueDefault. */
  public String[] getReturnValueDefault() {
    return returnValueDefault;
  }

  /** @param returnValueDefault The returnValueDefault to set. */
  public void setReturnValueDefault(String[] returnValueDefault) {
    this.returnValueDefault = returnValueDefault;
  }

  /** @return Returns the returnValueDefaultType. */
  public int[] getReturnValueDefaultType() {
    return returnValueDefaultType;
  }

  /** @param returnValueDefaultType The returnValueDefaultType to set. */
  public void setReturnValueDefaultType(int[] returnValueDefaultType) {
    this.returnValueDefaultType = returnValueDefaultType;
  }

  /** @return Returns the returnValueField. */
  public String[] getReturnValueField() {
    return returnValueField;
  }

  /** @param returnValueField The returnValueField to set. */
  public void setReturnValueField(String[] returnValueField) {
    this.returnValueField = returnValueField;
  }

  /** @return Returns the returnValueNewName. */
  public String[] getReturnValueNewName() {
    return returnValueNewName;
  }

  /** @param returnValueNewName The returnValueNewName to set. */
  public void setReturnValueNewName(String[] returnValueNewName) {
    this.returnValueNewName = returnValueNewName;
  }

  /** @return Returns the streamKeyField1. */
  public String[] getStreamKeyField1() {
    return streamKeyField1;
  }

  /** @param streamKeyField1 The streamKeyField1 to set. */
  public void setStreamKeyField1(String[] streamKeyField1) {
    this.streamKeyField1 = streamKeyField1;
  }

  /** @return Returns the streamKeyField2. */
  public String[] getStreamKeyField2() {
    return streamKeyField2;
  }

  /** @param streamKeyField2 The streamKeyField2 to set. */
  public void setStreamKeyField2(String[] streamKeyField2) {
    this.streamKeyField2 = streamKeyField2;
  }

  /** @return Returns the tableKeyField. */
  public String[] getTableKeyField() {
    return tableKeyField;
  }

  /** @param tableKeyField The tableKeyField to set. */
  public void setTableKeyField(String[] tableKeyField) {
    this.tableKeyField = tableKeyField;
  }

  /** @param tableName The table name to set. */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /** @return Returns the failOnMultipleResults. */
  public boolean isFailingOnMultipleResults() {
    return failingOnMultipleResults;
  }

  /** @param failOnMultipleResults The failOnMultipleResults to set. */
  public void setFailingOnMultipleResults(boolean failOnMultipleResults) {
    this.failingOnMultipleResults = failOnMultipleResults;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    streamKeyField1 = null;
    returnValueField = null;

    readData(transformNode, metadataProvider);
  }

  public void allocate(int nrkeys, int nrvalues) {
    streamKeyField1 = new String[nrkeys];
    tableKeyField = new String[nrkeys];
    keyCondition = new String[nrkeys];
    streamKeyField2 = new String[nrkeys];
    returnValueField = new String[nrvalues];
    returnValueNewName = new String[nrvalues];
    returnValueDefault = new String[nrvalues];
    returnValueDefaultType = new int[nrvalues];
  }

  @Override
  public Object clone() {
    DatabaseLookupMeta retval = (DatabaseLookupMeta) super.clone();

    int nrkeys = streamKeyField1.length;
    int nrvalues = returnValueField.length;

    retval.allocate(nrkeys, nrvalues);

    System.arraycopy(streamKeyField1, 0, retval.streamKeyField1, 0, nrkeys);
    System.arraycopy(tableKeyField, 0, retval.tableKeyField, 0, nrkeys);
    System.arraycopy(keyCondition, 0, retval.keyCondition, 0, nrkeys);
    System.arraycopy(streamKeyField2, 0, retval.streamKeyField2, 0, nrkeys);

    System.arraycopy(returnValueField, 0, retval.returnValueField, 0, nrvalues);
    System.arraycopy(returnValueNewName, 0, retval.returnValueNewName, 0, nrvalues);
    System.arraycopy(returnValueDefault, 0, retval.returnValueDefault, 0, nrvalues);
    System.arraycopy(returnValueDefaultType, 0, retval.returnValueDefaultType, 0, nrvalues);

    return retval;
  }

  private void readData(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      String dtype;
      String csize;

      String con = XmlHandler.getTagValue(transformNode, "connection");
      databaseMeta = DatabaseMeta.loadDatabase(metadataProvider, con);
      cached = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "cache"));
      loadingAllDataInCache =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "cache_load_all"));
      csize = XmlHandler.getTagValue(transformNode, "cache_size");
      cacheSize = Const.toInt(csize, 0);
      schemaName = XmlHandler.getTagValue(transformNode, "lookup", "schema");
      tableName = XmlHandler.getTagValue(transformNode, "lookup", "table");

      Node lookup = XmlHandler.getSubNode(transformNode, "lookup");

      int nrkeys = XmlHandler.countNodes(lookup, "key");
      int nrvalues = XmlHandler.countNodes(lookup, "value");

      allocate(nrkeys, nrvalues);

      for (int i = 0; i < nrkeys; i++) {
        Node knode = XmlHandler.getSubNodeByNr(lookup, "key", i);

        streamKeyField1[i] = XmlHandler.getTagValue(knode, "name");
        tableKeyField[i] = XmlHandler.getTagValue(knode, "field");
        keyCondition[i] = XmlHandler.getTagValue(knode, "condition");
        if (keyCondition[i] == null) {
          keyCondition[i] = "=";
        }
        streamKeyField2[i] = XmlHandler.getTagValue(knode, "name2");
      }

      for (int i = 0; i < nrvalues; i++) {
        Node vnode = XmlHandler.getSubNodeByNr(lookup, "value", i);

        returnValueField[i] = XmlHandler.getTagValue(vnode, "name");
        returnValueNewName[i] = XmlHandler.getTagValue(vnode, "rename");
        if (returnValueNewName[i] == null) {
          returnValueNewName[i] = returnValueField[i]; // default: the same name!
        }
        returnValueDefault[i] = XmlHandler.getTagValue(vnode, "default");
        dtype = XmlHandler.getTagValue(vnode, "type");
        returnValueDefaultType[i] = ValueMetaFactory.getIdForValueMeta(dtype);
        if (returnValueDefaultType[i] < 0) {
          // logError("unknown default value type: "+dtype+" for value "+value[i]+", default to
          // type: String!");
          returnValueDefaultType[i] = IValueMeta.TYPE_STRING;
        }
      }
      orderByClause = XmlHandler.getTagValue(lookup, "orderby"); // Optional, can by null
      failingOnMultipleResults =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(lookup, "fail_on_multiple"));
      eatingRowOnLookupFailure =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(lookup, "eat_row_on_failure"));
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "DatabaseLookupMeta.ERROR0001.UnableToLoadTransformFromXML"),
          e);
    }
  }

  @Override
  public void setDefault() {
    streamKeyField1 = null;
    returnValueField = null;
    databaseMeta = null;
    cached = false;
    cacheSize = 0;
    schemaName = "";
    tableName = BaseMessages.getString(PKG, "DatabaseLookupMeta.Default.TableName");

    int nrkeys = 0;
    int nrvalues = 0;

    allocate(nrkeys, nrvalues);

    for (int i = 0; i < nrkeys; i++) {
      tableKeyField[i] = BaseMessages.getString(PKG, "DatabaseLookupMeta.Default.KeyFieldPrefix");
      keyCondition[i] = BaseMessages.getString(PKG, "DatabaseLookupMeta.Default.KeyCondition");
      streamKeyField1[i] =
          BaseMessages.getString(PKG, "DatabaseLookupMeta.Default.KeyStreamField1");
      streamKeyField2[i] =
          BaseMessages.getString(PKG, "DatabaseLookupMeta.Default.KeyStreamField2");
    }

    for (int i = 0; i < nrvalues; i++) {
      returnValueField[i] =
          BaseMessages.getString(PKG, "DatabaseLookupMeta.Default.ReturnFieldPrefix") + i;
      returnValueNewName[i] =
          BaseMessages.getString(PKG, "DatabaseLookupMeta.Default.ReturnNewNamePrefix") + i;
      returnValueDefault[i] =
          BaseMessages.getString(PKG, "DatabaseLookupMeta.Default.ReturnDefaultValuePrefix") + i;
      returnValueDefaultType[i] = IValueMeta.TYPE_STRING;
    }

    orderByClause = "";
    failingOnMultipleResults = false;
    eatingRowOnLookupFailure = false;
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
    if (Utils.isEmpty(info) || info[0] == null) { // null or length 0 : no info from database
      for (int i = 0; i < getReturnValueNewName().length; i++) {
        try {
          IValueMeta v =
              ValueMetaFactory.createValueMeta(
                  getReturnValueNewName()[i], getReturnValueDefaultType()[i]);
          v.setOrigin(name);
          row.addValueMeta(v);
        } catch (Exception e) {
          throw new HopTransformException(e);
        }
      }
    } else {
      for (int i = 0; i < returnValueNewName.length; i++) {
        IValueMeta v = info[0].searchValueMeta(returnValueField[i]);
        if (v != null) {
          IValueMeta copy = v.clone(); // avoid renaming other value meta - PDI-9844
          copy.setName(returnValueNewName[i]);
          copy.setOrigin(name);
          row.addValueMeta(copy);
        }
      }
    }
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(500);

    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "connection", databaseMeta == null ? "" : databaseMeta.getName()));
    retval.append("    ").append(XmlHandler.addTagValue("cache", cached));
    retval.append("    ").append(XmlHandler.addTagValue("cache_load_all", loadingAllDataInCache));
    retval.append("    ").append(XmlHandler.addTagValue("cache_size", cacheSize));
    retval.append("    <lookup>").append(Const.CR);
    retval.append("      ").append(XmlHandler.addTagValue("schema", schemaName));
    retval.append("      ").append(XmlHandler.addTagValue("table", tableName));
    retval.append("      ").append(XmlHandler.addTagValue("orderby", orderByClause));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("fail_on_multiple", failingOnMultipleResults));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("eat_row_on_failure", eatingRowOnLookupFailure));

    for (int i = 0; i < streamKeyField1.length; i++) {
      retval.append("      <key>").append(Const.CR);
      retval.append("        ").append(XmlHandler.addTagValue("name", streamKeyField1[i]));
      retval.append("        ").append(XmlHandler.addTagValue("field", tableKeyField[i]));
      retval.append("        ").append(XmlHandler.addTagValue("condition", keyCondition[i]));
      retval.append("        ").append(XmlHandler.addTagValue("name2", streamKeyField2[i]));
      retval.append("      </key>").append(Const.CR);
    }

    for (int i = 0; i < returnValueField.length; i++) {
      retval.append("      <value>").append(Const.CR);
      retval.append("        ").append(XmlHandler.addTagValue("name", returnValueField[i]));
      retval.append("        ").append(XmlHandler.addTagValue("rename", returnValueNewName[i]));
      retval.append("        ").append(XmlHandler.addTagValue("default", returnValueDefault[i]));
      retval
          .append("        ")
          .append(
              XmlHandler.addTagValue(
                  "type", ValueMetaFactory.getValueMetaName(returnValueDefaultType[i])));
      retval.append("      </value>").append(Const.CR);
    }

    retval.append("    </lookup>").append(Const.CR);

    return retval.toString();
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
      Database db = new Database(loggingObject, variables, databaseMeta );
      databases = new Database[] {db}; // Keep track of this one for cancelQuery

      try {
        db.connect();

        if (!Utils.isEmpty(tableName)) {
          boolean first = true;
          boolean errorFound = false;
          errorMessage = "";

          String schemaTable =
              databaseMeta.getQuotedSchemaTableCombination(variables, schemaName, tableName);
          IRowMeta r = db.getTableFields(schemaTable);

          if (r != null) {
            // Check the keys used to do the lookup...

            for (int i = 0; i < tableKeyField.length; i++) {
              String lufield = tableKeyField[i];

              IValueMeta v = r.searchValueMeta(lufield);
              if (v == null) {
                if (first) {
                  first = false;
                  errorMessage +=
                      BaseMessages.getString(
                              PKG, "DatabaseLookupMeta.Check.MissingCompareFieldsInLookupTable")
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
                          PKG, "DatabaseLookupMeta.Check.AllLookupFieldsFoundInTable"),
                      transformMeta);
            }
            remarks.add(cr);

            // Also check the returned values!

            for (int i = 0; i < returnValueField.length; i++) {
              String lufield = returnValueField[i];

              IValueMeta v = r.searchValueMeta(lufield);
              if (v == null) {
                if (first) {
                  first = false;
                  errorMessage +=
                      BaseMessages.getString(
                              PKG, "DatabaseLookupMeta.Check.MissingReturnFieldsInLookupTable")
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
                          PKG, "DatabaseLookupMeta.Check.AllReturnFieldsFoundInTable"),
                      transformMeta);
            }
            remarks.add(cr);

          } else {
            errorMessage =
                BaseMessages.getString(PKG, "DatabaseLookupMeta.Check.CouldNotReadTableInfo");
            cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
            remarks.add(cr);
          }
        }

        // Look up fields in the input stream <prev>
        if (prev != null && prev.size() > 0) {
          boolean first = true;
          errorMessage = "";
          boolean errorFound = false;

          for (int i = 0; i < streamKeyField1.length; i++) {
            IValueMeta v = prev.searchValueMeta(streamKeyField1[i]);
            if (v == null) {
              if (first) {
                first = false;
                errorMessage +=
                    BaseMessages.getString(
                            PKG, "DatabaseLookupMeta.Check.MissingFieldsNotFoundInInput")
                        + Const.CR;
              }
              errorFound = true;
              errorMessage += "\t\t" + streamKeyField1[i] + Const.CR;
            }
          }
          if (errorFound) {
            cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
          } else {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(PKG, "DatabaseLookupMeta.Check.AllFieldsFoundInInput"),
                    transformMeta);
          }
          remarks.add(cr);
        } else {
          errorMessage =
              BaseMessages.getString(
                      PKG, "DatabaseLookupMeta.Check.CouldNotReadFromPreviousTransforms")
                  + Const.CR;
          cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
          remarks.add(cr);
        }
      } catch (HopDatabaseException dbe) {
        errorMessage =
            BaseMessages.getString(PKG, "DatabaseLookupMeta.Check.DatabaseErrorWhileChecking")
                + dbe.getMessage();
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } finally {
        db.disconnect();
      }
    } else {
      errorMessage = BaseMessages.getString(PKG, "DatabaseLookupMeta.Check.MissingConnectionError");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "DatabaseLookupMeta.Check.TransformIsReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "DatabaseLookupMeta.Check.NoInputReceivedFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public IRowMeta getTableFields(IVariables variables) {
    IRowMeta fields = null;
    if (databaseMeta != null) {
      Database db = new Database(loggingObject, variables, databaseMeta );
      databases = new Database[] {db}; // Keep track of this one for cancelQuery

      try {
        db.connect();
        String realTableName = variables.resolve(tableName);
        String schemaTable =
            databaseMeta.getQuotedSchemaTableCombination(variables, schemaName, realTableName);
        fields = db.getTableFields(schemaTable);

      } catch (HopDatabaseException dbe) {
        logError(
            BaseMessages.getString(PKG, "DatabaseLookupMeta.ERROR0004.ErrorGettingTableFields")
                + dbe.getMessage());
      } finally {
        db.disconnect();
      }
    }
    return fields;
  }

  @Override
  public DatabaseLookup createTransform(
      TransformMeta transformMeta,
      DatabaseLookupData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new DatabaseLookup(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  @Override
  public DatabaseLookupData getTransformData() {
    return new DatabaseLookupData();
  }

  @Override
  public void analyseImpact(
      IVariables variables,
      List<DatabaseImpact> impact,
      PipelineMeta pipelineMeta,
      TransformMeta transforminfo,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IHopMetadataProvider metadataProvider) {
    // The keys are read-only...
    for (int i = 0; i < streamKeyField1.length; i++) {
      IValueMeta v = prev.searchValueMeta(streamKeyField1[i]);
      DatabaseImpact ii =
          new DatabaseImpact(
              DatabaseImpact.TYPE_IMPACT_READ,
              pipelineMeta.getName(),
              transforminfo.getName(),
              databaseMeta.getDatabaseName(),
              tableName,
              tableKeyField[i],
              streamKeyField1[i],
              v != null ? v.getOrigin() : "?",
              "",
              BaseMessages.getString(PKG, "DatabaseLookupMeta.Impact.Key"));
      impact.add(ii);
    }

    // The Return fields are read-only too...
    for (int i = 0; i < returnValueField.length; i++) {
      DatabaseImpact ii =
          new DatabaseImpact(
              DatabaseImpact.TYPE_IMPACT_READ,
              pipelineMeta.getName(),
              transforminfo.getName(),
              databaseMeta.getDatabaseName(),
              tableName,
              returnValueField[i],
              "",
              "",
              "",
              BaseMessages.getString(PKG, "DatabaseLookupMeta.Impact.ReturnValue"));
      impact.add(ii);
    }
  }

  @Override
  public DatabaseMeta[] getUsedDatabaseConnections() {
    if (databaseMeta != null) {
      return new DatabaseMeta[] {databaseMeta};
    } else {
      return super.getUsedDatabaseConnections();
    }
  }

  /** @return Returns the eatingRowOnLookupFailure. */
  public boolean isEatingRowOnLookupFailure() {
    return eatingRowOnLookupFailure;
  }

  /** @param eatingRowOnLookupFailure The eatingRowOnLookupFailure to set. */
  public void setEatingRowOnLookupFailure(boolean eatingRowOnLookupFailure) {
    this.eatingRowOnLookupFailure = eatingRowOnLookupFailure;
  }

  /** @return the schemaName */
  @Override
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

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  /** @return the loadingAllDataInCache */
  public boolean isLoadingAllDataInCache() {
    return loadingAllDataInCache;
  }

  /** @param loadingAllDataInCache the loadingAllDataInCache to set */
  public void setLoadingAllDataInCache(boolean loadingAllDataInCache) {
    this.loadingAllDataInCache = loadingAllDataInCache;
  }

  @Override
  public RowMeta getRowMeta( IVariables variables, ITransformData transformData ) {
    return (RowMeta) ((DatabaseLookupData) transformData).returnMeta;
  }

  @Override
  public List<String> getDatabaseFields() {
    return Arrays.asList(returnValueField);
  }

  @Override
  public List<String> getStreamFields() {
    return Arrays.asList(returnValueNewName);
  }
}
