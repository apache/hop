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

package org.apache.hop.pipeline.transforms.databaselookup;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.IProvidesModelerMeta;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@Transform(
    id = "DBLookup",
    image = "dblookup.svg",
    name = "i18n::DatabaseLookup.Name",
    description = "i18n::DatabaseLookup.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Lookup",
    keywords = "i18n::DatabaseLookupMeta.keyword",
    documentationUrl = "/pipeline/transforms/databaselookup.html")
public class DatabaseLookupMeta extends BaseTransformMeta<DatabaseLookup, DatabaseLookupData>
    implements IProvidesModelerMeta {

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

  /** database connection */
  @HopMetadataProperty(injectionKeyDescription = "DatabaseLookupMeta.Injection.Connection")
  private String connection;

  /** ICache values we look up --> faster */
  @HopMetadataProperty(
      key = "cache",
      injectionKeyDescription = "DatabaseLookupMeta.Injection.Cache")
  private boolean cached;

  /** Limit the cache size to this! */
  @HopMetadataProperty(
      key = "cache_size",
      injectionKeyDescription = "DatabaseLookupMeta.Injection.CacheSize")
  private int cacheSize;

  /** Flag to make it load all data into the cache at startup */
  @HopMetadataProperty(
      key = "cache_load_all",
      injectionKeyDescription = "DatabaseLookupMeta.Injection.CacheLoadAll")
  private boolean loadingAllDataInCache;

  @HopMetadataProperty(key = "lookup")
  private Lookup lookup;

  public DatabaseLookupMeta() {
    lookup = new Lookup();
  }

  public DatabaseLookupMeta(DatabaseLookupMeta m) {
    this.cached = m.cached;
    this.cacheSize = m.cacheSize;
    this.loadingAllDataInCache = m.loadingAllDataInCache;
    this.lookup = new Lookup(m.lookup);
  }

  @Override
  public DatabaseLookupMeta clone() {
    return new DatabaseLookupMeta(this);
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

      for (ReturnValue returnValue : lookup.getReturnValues()) {
        try {
          IValueMeta v =
              ValueMetaFactory.createValueMeta(
                  !Utils.isEmpty(returnValue.getNewName())
                      ? returnValue.getNewName()
                      : returnValue.getTableField(),
                  ValueMetaFactory.getIdForValueMeta(returnValue.getDefaultType()));
          v.setOrigin(name);
          row.addValueMeta(v);
        } catch (Exception e) {
          throw new HopTransformException(e);
        }
      }
    } else {

      for (ReturnValue returnValue : lookup.getReturnValues()) {
        IValueMeta v = info[0].searchValueMeta(returnValue.getTableField());
        if (v != null) {
          IValueMeta copy = v.clone(); // avoid renaming other value meta
          copy.setName(
              !Utils.isEmpty(returnValue.getNewName())
                  ? returnValue.getNewName()
                  : returnValue.getTableField());
          copy.setOrigin(name);
          row.addValueMeta(copy);
        }
      }
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
    Database db = null;

    try {
      DatabaseMeta databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connection));

      if (databaseMeta != null) {
        db = new Database(loggingObject, variables, databaseMeta);
        databases = new Database[] {db}; // Keep track of this one for cancelQuery

        db.connect();

        List<KeyField> keyFields = lookup.getKeyFields();
        if (!Utils.isEmpty(lookup.getTableName())) {
          boolean first = true;
          boolean errorFound = false;
          errorMessage = "";

          String schemaTable =
              databaseMeta.getQuotedSchemaTableCombination(
                  variables, lookup.getSchemaName(), lookup.getTableName());
          IRowMeta r = db.getTableFields(schemaTable);

          if (r != null) {
            // Check the keys used to do the lookup...
            for (int i = 0; i < keyFields.size(); i++) {
              KeyField keyField = keyFields.get(i);
              String luField = keyField.getTableField();
              ;

              IValueMeta v = r.searchValueMeta(luField);
              if (v == null) {
                if (first) {
                  first = false;
                  errorMessage +=
                      BaseMessages.getString(
                              PKG, "DatabaseLookupMeta.Check.MissingCompareFieldsInLookupTable")
                          + Const.CR;
                }
                errorFound = true;
                errorMessage += "\t\t" + luField + Const.CR;
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
            List<ReturnValue> returnValues = lookup.getReturnValues();
            for (int i = 0; i < returnValues.size(); i++) {
              ReturnValue returnValue = returnValues.get(i);
              String luField = returnValue.getTableField();

              IValueMeta v = r.searchValueMeta(luField);
              if (v == null) {
                if (first) {
                  first = false;
                  errorMessage +=
                      BaseMessages.getString(
                              PKG, "DatabaseLookupMeta.Check.MissingReturnFieldsInLookupTable")
                          + Const.CR;
                }
                errorFound = true;
                errorMessage += "\t\t" + luField + Const.CR;
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

          for (int i = 0; i < keyFields.size(); i++) {
            KeyField keyField = keyFields.get(i);

            IValueMeta v = prev.searchValueMeta(keyField.getStreamField1());
            if (v == null) {
              if (first) {
                first = false;
                errorMessage +=
                    BaseMessages.getString(
                            PKG, "DatabaseLookupMeta.Check.MissingFieldsNotFoundInInput")
                        + Const.CR;
              }
              errorFound = true;
              errorMessage += "\t\t" + keyField.getStreamField1() + Const.CR;
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
      } else {
        errorMessage =
            BaseMessages.getString(PKG, "DatabaseLookupMeta.Check.MissingConnectionError");
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      }
    } catch (HopException dbe) {
      errorMessage =
          BaseMessages.getString(PKG, "DatabaseLookupMeta.Check.DatabaseErrorWhileChecking")
              + dbe.getMessage();
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
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
    DatabaseMeta databaseMeta =
        getParentTransformMeta().getParentPipelineMeta().findDatabase(connection, variables);
    if (databaseMeta != null) {
      Database db = new Database(loggingObject, variables, databaseMeta);
      databases = new Database[] {db}; // Keep track of this one for cancelQuery

      try {
        db.connect();
        String schemaTable =
            databaseMeta.getQuotedSchemaTableCombination(
                variables, lookup.getSchemaName(), lookup.getTableName());
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
  public void analyseImpact(
      IVariables variables,
      List<DatabaseImpact> impact,
      PipelineMeta pipelineMeta,
      TransformMeta transforminfo,
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
      // The keys are read-only...
      List<KeyField> keyFields = lookup.getKeyFields();
      for (int i = 0; i < keyFields.size(); i++) {
        KeyField keyField = keyFields.get(i);

        IValueMeta v = prev.searchValueMeta(keyField.getStreamField1());
        DatabaseImpact ii =
            new DatabaseImpact(
                DatabaseImpact.TYPE_IMPACT_READ,
                pipelineMeta.getName(),
                transforminfo.getName(),
                databaseMeta.getDatabaseName(),
                lookup.getTableName(),
                keyField.getTableField(),
                keyField.getStreamField1(),
                v != null ? v.getOrigin() : "?",
                "",
                BaseMessages.getString(PKG, "DatabaseLookupMeta.Impact.Key"));
        impact.add(ii);
      }

      // The Return fields are read-only too...
      List<ReturnValue> returnValues = lookup.getReturnValues();
      for (int i = 0; i < returnValues.size(); i++) {
        ReturnValue returnValue = returnValues.get(i);
        DatabaseImpact ii =
            new DatabaseImpact(
                DatabaseImpact.TYPE_IMPACT_READ,
                pipelineMeta.getName(),
                transforminfo.getName(),
                databaseMeta.getDatabaseName(),
                lookup.getTableName(),
                returnValue.getTableField(),
                "",
                "",
                "",
                BaseMessages.getString(PKG, "DatabaseLookupMeta.Impact.ReturnValue"));
        impact.add(ii);
      }
    } catch (HopException e) {
      throw new HopTransformException(
          "Unable to get databaseMeta for connection: " + Const.CR + variables.resolve(connection));
    }
  }

  @Override
  public String getTableName() {
    return lookup.getTableName();
  }

  @Override
  public String getSchemaName() {
    return lookup.getSchemaName();
  }

  @Override
  public List<String> getDatabaseFields() {
    Set<String> fields = new LinkedHashSet<>();
    for (KeyField keyField : lookup.getKeyFields()) {
      if (StringUtils.isNotEmpty(keyField.getTableField())) {
        fields.add(keyField.getTableField());
      }
    }
    for (ReturnValue returnValue : lookup.getReturnValues()) {
      if (StringUtils.isNotEmpty(returnValue.getTableField())) {
        fields.add(returnValue.getTableField());
      }
    }
    return new ArrayList<>(fields);
  }

  @Override
  public List<String> getStreamFields() {
    Set<String> fields = new LinkedHashSet<>();
    for (KeyField keyField : lookup.getKeyFields()) {
      if (StringUtils.isNotEmpty(keyField.getStreamField1())) {
        fields.add(keyField.getStreamField1());
      }
      if (StringUtils.isNotEmpty(keyField.getStreamField2())) {
        fields.add(keyField.getStreamField2());
      }
    }
    for (ReturnValue returnValue : lookup.getReturnValues()) {
      if (StringUtils.isNotEmpty(returnValue.getNewName())) {
        fields.add(returnValue.getNewName());
      }
    }
    return new ArrayList<>(fields);
  }

  @Override
  public String getMissingDatabaseConnectionInformationMessage() {
    return null;
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  @Override
  public RowMeta getRowMeta(IVariables variables, ITransformData transformData) {
    return (RowMeta) ((DatabaseLookupData) transformData).returnMeta;
  }

  /**
   * Gets databaseMeta
   *
   * @return value of databaseMeta
   */
  @Override
  public DatabaseMeta getDatabaseMeta() {
    return null;
  }

  public String getConnection() {
    return connection;
  }

  public void setConnection(String connection) {
    this.connection = connection;
  }

  /**
   * Gets cached
   *
   * @return value of cached
   */
  public boolean isCached() {
    return cached;
  }

  /**
   * @param cached The cached to set
   */
  public void setCached(boolean cached) {
    this.cached = cached;
  }

  /**
   * Gets cacheSize
   *
   * @return value of cacheSize
   */
  public int getCacheSize() {
    return cacheSize;
  }

  /**
   * @param cacheSize The cacheSize to set
   */
  public void setCacheSize(int cacheSize) {
    this.cacheSize = cacheSize;
  }

  /**
   * Gets loadingAllDataInCache
   *
   * @return value of loadingAllDataInCache
   */
  public boolean isLoadingAllDataInCache() {
    return loadingAllDataInCache;
  }

  /**
   * @param loadingAllDataInCache The loadingAllDataInCache to set
   */
  public void setLoadingAllDataInCache(boolean loadingAllDataInCache) {
    this.loadingAllDataInCache = loadingAllDataInCache;
  }

  /**
   * Gets lookup
   *
   * @return value of lookup
   */
  public Lookup getLookup() {
    return lookup;
  }

  /**
   * @param lookup The lookup to set
   */
  public void setLookup(Lookup lookup) {
    this.lookup = lookup;
  }
}
