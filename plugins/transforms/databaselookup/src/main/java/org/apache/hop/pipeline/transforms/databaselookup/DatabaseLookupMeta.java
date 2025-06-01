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

import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
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

@Setter
@Getter
@Transform(
    id = "DBLookup",
    image = "dblookup.svg",
    name = "i18n::DatabaseLookup.Name",
    description = "i18n::DatabaseLookup.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Lookup",
    keywords = "i18n::DatabaseLookupMeta.keyword",
    documentationUrl = "/pipeline/transforms/databaselookup.html",
    actionTransformTypes = {ActionTransformType.LOOKUP, ActionTransformType.RDBMS})
public class DatabaseLookupMeta extends BaseTransformMeta<DatabaseLookup, DatabaseLookupData> {

  private static final Class<?> PKG = DatabaseLookupMeta.class;

  protected static final String[] conditionStrings =
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
  @HopMetadataProperty(
      injectionKeyDescription = "DatabaseLookupMeta.Injection.Connection",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
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
    this.connection = m.connection;
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
      IRowMeta[] infoRowMeta,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    try {
      // info row metadata: null or length 0 : no lookup row metadata from database
      //
      if (Utils.isEmpty(infoRowMeta) || infoRowMeta[0] == null) {
        for (ReturnValue returnValue : lookup.getReturnValues()) {
          IValueMeta v =
              ValueMetaFactory.createValueMeta(
                  !Utils.isEmpty(returnValue.getNewName())
                      ? returnValue.getNewName()
                      : returnValue.getTableField(),
                  ValueMetaFactory.getIdForValueMeta(returnValue.getDefaultType()));
          v.setOrigin(name);
          row.addValueMeta(v);
        }
        return;
      }

      for (ReturnValue returnValue : lookup.getReturnValues()) {
        IValueMeta v = infoRowMeta[0].searchValueMeta(returnValue.getTableField());
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
    } catch (HopException e) {
      throw new HopTransformException("Error getting fields metadata", e);
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
    StringBuilder errorMessage = new StringBuilder();

    try {
      DatabaseMeta databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connection));

      // If we don't have a database connection, there is nothing else to check.
      //
      if (databaseMeta == null) {
        errorMessage =
            new StringBuilder(
                BaseMessages.getString(PKG, "DatabaseLookupMeta.Check.MissingConnectionError"));
        cr =
            new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage.toString(), transformMeta);
        remarks.add(cr);
        return;
      }

      try (Database db = new Database(loggingObject, variables, databaseMeta)) {
        databases = new Database[] {db}; // Keep track of this one for cancelQuery

        db.connect();

        List<KeyField> keyFields = lookup.getKeyFields();
        if (!Utils.isEmpty(lookup.getTableName())) {
          boolean first = true;
          boolean errorFound = false;
          errorMessage = new StringBuilder();

          String schemaTable =
              databaseMeta.getQuotedSchemaTableCombination(
                  variables, lookup.getSchemaName(), lookup.getTableName());
          IRowMeta r = db.getTableFields(schemaTable);

          if (r != null) {
            // Check the keys used to do the lookup...
            for (KeyField keyField : keyFields) {
              String luField = keyField.getTableField();

              IValueMeta v = r.searchValueMeta(luField);
              if (v == null) {
                if (first) {
                  first = false;
                  errorMessage
                      .append(
                          BaseMessages.getString(
                              PKG, "DatabaseLookupMeta.Check.MissingCompareFieldsInLookupTable"))
                      .append(Const.CR);
                }
                errorFound = true;
                errorMessage.append("\t\t").append(luField).append(Const.CR);
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
                          PKG, "DatabaseLookupMeta.Check.AllLookupFieldsFoundInTable"),
                      transformMeta);
            }
            remarks.add(cr);

            // Also check the returned values!
            List<ReturnValue> returnValues = lookup.getReturnValues();
            for (ReturnValue returnValue : returnValues) {
              String luField = returnValue.getTableField();

              IValueMeta v = r.searchValueMeta(luField);
              if (v == null) {
                if (first) {
                  first = false;
                  errorMessage
                      .append(
                          BaseMessages.getString(
                              PKG, "DatabaseLookupMeta.Check.MissingReturnFieldsInLookupTable"))
                      .append(Const.CR);
                }
                errorFound = true;
                errorMessage.append("\t\t").append(luField).append(Const.CR);
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
                          PKG, "DatabaseLookupMeta.Check.AllReturnFieldsFoundInTable"),
                      transformMeta);
            }
            remarks.add(cr);

          } else {
            errorMessage =
                new StringBuilder(
                    BaseMessages.getString(PKG, "DatabaseLookupMeta.Check.CouldNotReadTableInfo"));
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_ERROR, errorMessage.toString(), transformMeta);
            remarks.add(cr);
          }
        }

        // Look up fields in the input stream <prev>
        if (!Utils.isEmpty(prev)) {
          boolean first = true;
          errorMessage = new StringBuilder();
          boolean errorFound = false;

          for (KeyField keyField : keyFields) {
            IValueMeta v = prev.searchValueMeta(keyField.getStreamField1());
            if (v == null) {
              if (first) {
                first = false;
                errorMessage
                    .append(
                        BaseMessages.getString(
                            PKG, "DatabaseLookupMeta.Check.MissingFieldsNotFoundInInput"))
                    .append(Const.CR);
              }
              errorFound = true;
              errorMessage.append("\t\t").append(keyField.getStreamField1()).append(Const.CR);
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
                    BaseMessages.getString(PKG, "DatabaseLookupMeta.Check.AllFieldsFoundInInput"),
                    transformMeta);
          }
          remarks.add(cr);
        } else {
          errorMessage =
              new StringBuilder(
                  BaseMessages.getString(
                          PKG, "DatabaseLookupMeta.Check.CouldNotReadFromPreviousTransforms")
                      + Const.CR);
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR, errorMessage.toString(), transformMeta);
          remarks.add(cr);
        }
      }
    } catch (HopException dbe) {
      errorMessage =
          new StringBuilder(
              BaseMessages.getString(PKG, "DatabaseLookupMeta.Check.DatabaseErrorWhileChecking")
                  + dbe.getMessage());
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage.toString(), transformMeta);
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
    DatabaseMeta databaseMeta =
        getParentTransformMeta().getParentPipelineMeta().findDatabase(connection, variables);
    if (databaseMeta != null) {
      try (Database db = new Database(loggingObject, variables, databaseMeta)) {
        databases = new Database[] {db}; // Keep track of this one for cancelQuery

        db.connect();
        String schemaTable =
            databaseMeta.getQuotedSchemaTableCombination(
                variables, lookup.getSchemaName(), lookup.getTableName());
        fields = db.getTableFields(schemaTable);

      } catch (HopDatabaseException dbe) {
        logError(
            BaseMessages.getString(PKG, "DatabaseLookupMeta.ERROR0004.ErrorGettingTableFields")
                + dbe.getMessage());
      }
    }
    return fields;
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
      // The keys are read-only...
      List<KeyField> keyFields = lookup.getKeyFields();
      for (KeyField keyField : keyFields) {
        IValueMeta v = prev.searchValueMeta(keyField.getStreamField1());
        DatabaseImpact ii =
            new DatabaseImpact(
                DatabaseImpact.TYPE_IMPACT_READ,
                pipelineMeta.getName(),
                transformMeta.getName(),
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
      for (ReturnValue returnValue : returnValues) {
        DatabaseImpact ii =
            new DatabaseImpact(
                DatabaseImpact.TYPE_IMPACT_READ,
                pipelineMeta.getName(),
                transformMeta.getName(),
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

  /**
   * Returns the table name.
   *
   * @return the table name
   */
  public String getTableName() {
    return lookup.getTableName();
  }

  /**
   * Returns the schema name.
   *
   * @return the schema name
   */
  public String getSchemaName() {
    return lookup.getSchemaName();
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  public static final List<String> getConditionStrings() {
    return Arrays.asList(conditionStrings);
  }
}
