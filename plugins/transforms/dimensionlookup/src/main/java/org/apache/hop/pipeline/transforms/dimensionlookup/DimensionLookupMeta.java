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

package org.apache.hop.pipeline.transforms.dimensionlookup;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import org.apache.commons.lang.StringUtils;
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
import org.apache.hop.core.row.RowMetaBuilder;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "DimensionLookup",
    image = "dimensionlookup.svg",
    name = "i18n::DimensionUpdate.Name",
    description = "i18n::DimensionUpdate.Description",
    categoryDescription =
        "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.DataWarehouse",
    keywords = "i18n::DimensionLookupMeta.keyword",
    documentationUrl = "/pipeline/transforms/dimensionlookup.html",
    actionTransformTypes = {ActionTransformType.RDBMS, ActionTransformType.LOOKUP})
public class DimensionLookupMeta extends BaseTransformMeta<DimensionLookup, DimensionLookupData> {
  private static final Class<?> PKG = DimensionLookupMeta.class;
  public static final String CONST_DIMENSION_LOOKUP_META_CHECK_RESULT_KEY_HAS_PROBLEM =
      "DimensionLookupMeta.CheckResult.KeyHasProblem";
  public static final String CONST_TYPE = "Type = ";

  /** The lookup schema name */
  @HopMetadataProperty(
      key = "schema",
      injectionKey = "TARGET_SCHEMA",
      injectionKeyDescription = "DimensionLookup.Injection.TARGET_SCHEMA",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_SCHEMA)
  private String schemaName;

  /** The lookup table */
  @HopMetadataProperty(
      key = "table",
      injectionKey = "TARGET_TABLE",
      injectionKeyDescription = "DimensionLookup.Injection.TARGET_TABLE",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
  private String tableName;

  /** The database connection */
  @HopMetadataProperty(
      key = "connection",
      injectionKey = "CONNECTION_NAME",
      injectionKeyDescription = "DimensionLookup.Injection.CONNECTION_NAME",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String connection;

  /** Update the dimension or just lookup? */
  @HopMetadataProperty(
      key = "update",
      injectionKey = "UPDATE_DIMENSION",
      injectionKeyDescription = "DimensionLookup.Injection.UPDATE_DIMENSION")
  private boolean update;

  @HopMetadataProperty(key = "fields", injectionGroupKey = "FIELDS")
  private DLFields fields;

  /** Sequence name to get the sequence from */
  @HopMetadataProperty(
      key = "sequence",
      injectionKey = "TECHNICAL_KEY_SEQUENCE",
      injectionKeyDescription = "DimensionLookup.Injection.TECHNICAL_KEY_SEQUENCE")
  private String sequenceName;

  /** The number of rows between commits */
  @HopMetadataProperty(
      key = "commit",
      injectionKey = "COMMIT_SIZE",
      injectionKeyDescription = "DimensionLookup.Injection.COMMIT_SIZE")
  private int commitSize;

  /** Flag to indicate the use of batch updates, default disabled for backward compatibility */
  @HopMetadataProperty(key = "useBatch")
  private boolean useBatchUpdate;

  /** The year to use as minus infinity in the dimensions date range */
  @HopMetadataProperty(
      key = "min_year",
      injectionKey = "MIN_YEAR",
      injectionKeyDescription = "DimensionLookup.Injection.MIN_YEAR")
  private int minYear;

  /** The year to use as plus infinity in the dimensions date range */
  @HopMetadataProperty(
      key = "max_year",
      injectionKey = "MAX_YEAR",
      injectionKeyDescription = "DimensionLookup.Injection.MAX_YEAR")
  private int maxYear;

  /** The size of the cache in ROWS : -1 means: not set, 0 means: cache all */
  @HopMetadataProperty(
      key = "cache_size",
      injectionKey = "CACHE_SIZE",
      injectionKeyDescription = "DimensionLookup.Injection.CACHE_SIZE")
  private int cacheSize;

  /** Flag to indicate we're going to use an alternative start date */
  @HopMetadataProperty(
      key = "use_start_date_alternative",
      injectionKey = "USE_ALTERNATIVE_START_DATE",
      injectionKeyDescription = "DimensionLookup.Injection.USE_ALTERNATIVE_START_DATE")
  private boolean usingStartDateAlternative;

  /** The type of alternative */
  @HopMetadataProperty(
      key = "start_date_alternative",
      storeWithCode = true,
      injectionKey = "ALTERNATIVE_START_OPTION",
      injectionKeyDescription = "DimensionLookup.Injection.ALTERNATIVE_START_OPTION")
  private StartDateAlternative startDateAlternative;

  /** The field name in case we select the column value option as an alternative start date */
  @HopMetadataProperty(
      key = "start_date_field_name",
      injectionKey = "ALTERNATIVE_START_COLUMN",
      injectionKeyDescription = "DimensionLookup.Injection.ALTERNATIVE_START_COLUMN")
  private String startDateFieldName;

  @HopMetadataProperty(
      key = "preload_cache",
      injectionKey = "PRELOAD_CACHE",
      injectionKeyDescription = "DimensionLookup.Injection.PRELOAD_CACHE")
  private boolean preloadingCache;

  public DimensionLookupMeta() {
    super();
    this.fields = new DLFields();
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  @Override
  public DimensionLookupMeta clone() {
    return new DimensionLookupMeta(this);
  }

  public DimensionLookupMeta(DimensionLookupMeta m) {
    this();
    this.schemaName = m.schemaName;
    this.tableName = m.tableName;
    this.connection = m.connection;
    this.update = m.update;
    this.fields = new DLFields(m.fields);
    this.sequenceName = m.sequenceName;
    this.commitSize = m.commitSize;
    this.useBatchUpdate = m.useBatchUpdate;
    this.minYear = m.minYear;
    this.maxYear = m.maxYear;
    this.cacheSize = m.cacheSize;
    this.usingStartDateAlternative = m.usingStartDateAlternative;
    this.startDateAlternative = m.startDateAlternative;
    this.startDateFieldName = m.startDateFieldName;
    this.preloadingCache = m.preloadingCache;
  }

  @Override
  public void setDefault() {
    schemaName = "";
    tableName = BaseMessages.getString(PKG, "DimensionLookupMeta.DefaultTableName");
    connection = "";
    commitSize = 100;
    update = true;

    // Only one date is supported
    // No datefield: use system date...
    fields.date.name = "";
    fields.date.from = "date_from";
    fields.date.to = "date_to";

    minYear = Const.MIN_YEAR;
    maxYear = Const.MAX_YEAR;

    fields.returns.keyField = "";
    fields.returns.keyRename = "";
    fields.returns.versionField = "version";

    cacheSize = 5000;
    preloadingCache = false;
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

    // We need a database connection
    //
    if (connection == null) {
      String message =
          BaseMessages.getString(
              PKG, "DimensionLookupMeta.Exception.UnableToRetrieveDataTypeOfReturnField");
      logError(message);
      throw new HopTransformException(message);
    }

    // Change all the fields to normal storage, this is the fastest way to handle lazy conversion.
    // It doesn't make sense to use it in the SCD context but people try it anyway
    //
    for (IValueMeta valueMeta : row.getValueMetaList()) {
      valueMeta.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);

      // Also change the trim type to "None" as this can cause trouble
      // during compare of the data when there are leading/trailing spaces in the target table
      //
      valueMeta.setTrimType(IValueMeta.TRIM_TYPE_NONE);
    }

    // technical key can't be null
    //
    if (StringUtils.isEmpty(fields.returns.keyField)) {
      String message =
          BaseMessages.getString(PKG, "DimensionLookupMeta.Error.NoTechnicalKeySpecified");

      logError(message);
      throw new HopTransformException(message);
    }

    IValueMeta v = new ValueMetaInteger(fields.returns.keyField);
    if (StringUtils.isNotEmpty(fields.returns.keyRename)) {
      v.setName(fields.returns.keyRename);
    }

    v.setLength(9);
    v.setPrecision(0);
    v.setOrigin(name);
    row.addValueMeta(v);

    // retrieve extra fields on lookup?
    // Don't bother if there are no return values specified.
    if (update || fields.fields.isEmpty()) {
      return;
    }

    try {
      // Get the rows from the table...
      IRowMeta extraFields = getTableFields(variables);

      for (DLField field : fields.fields) {
        v = extraFields.searchValueMeta(field.getLookup());
        if (v == null) {
          String message =
              BaseMessages.getString(
                  PKG, "DimensionLookupMeta.Exception.UnableToFindReturnField", field.getLookup());
          logError(message);
          throw new HopTransformException(message);
        }

        // If the field needs to be renamed, rename
        if (StringUtils.isNotEmpty(field.getName())) {
          v.setName(field.getName());
        }
        v.setOrigin(name);
        row.addValueMeta(v);
      }
    } catch (Exception e) {
      String message =
          BaseMessages.getString(
              PKG, "DimensionLookupMeta.Exception.UnableToRetrieveDataTypeOfReturnField2");
      logError(message);
      throw new HopTransformException(message, e);
    }
  }

  public Date getMinDate() {
    Calendar mincal = Calendar.getInstance();
    mincal.set(Calendar.YEAR, minYear);
    mincal.set(Calendar.MONTH, 0);
    mincal.set(Calendar.DAY_OF_MONTH, 1);
    mincal.set(Calendar.HOUR_OF_DAY, 0);
    mincal.set(Calendar.MINUTE, 0);
    mincal.set(Calendar.SECOND, 0);
    mincal.set(Calendar.MILLISECOND, 0);

    return mincal.getTime();
  }

  public Date getMaxDate() {
    Calendar mincal = Calendar.getInstance();
    mincal.set(Calendar.YEAR, maxYear);
    mincal.set(Calendar.MONTH, 11);
    mincal.set(Calendar.DAY_OF_MONTH, 31);
    mincal.set(Calendar.HOUR_OF_DAY, 23);
    mincal.set(Calendar.MINUTE, 59);
    mincal.set(Calendar.SECOND, 59);
    mincal.set(Calendar.MILLISECOND, 999);

    return mincal.getTime();
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta previousRowMeta,
      String[] input,
      String[] output,
      IRowMeta infoRowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    // Check the absolute basics first.
    //
    List<ICheckResult> newRemarks = new ArrayList<>();
    checkDatabase(transformMeta, newRemarks, variables);
    checkTable(transformMeta, variables, newRemarks);
    if (!newRemarks.isEmpty()) {
      // No point in going on if we don't have the minimum set of information points.
      //
      remarks.addAll(newRemarks);
      return;
    }

    String realSchema = variables.resolve(schemaName);
    String realTable = variables.resolve(tableName);

    // Validate settings against the database.
    //
    try {
      DatabaseMeta databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connection));

      try (Database db = new Database(loggingObject, variables, databaseMeta)) {
        db.connect();

        IRowMeta tableRowMeta = checkTableFields(transformMeta, db, realSchema, realTable, remarks);
        if (tableRowMeta != null) {
          checkKeys(
              transformMeta,
              variables,
              realSchema,
              realTable,
              tableRowMeta,
              previousRowMeta,
              remarks);
          checkReturns(transformMeta, tableRowMeta, remarks);
          checkDateFields(transformMeta, tableRowMeta, remarks);
        }
        checkPreviousFields(transformMeta, previousRowMeta, remarks);
        checkSequence(transformMeta, db, variables, remarks);
      } catch (HopException e) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "DimensionLookupMeta.CheckResult.CouldNotConectToDB")
                    + e.getMessage(),
                transformMeta));
      }
    } catch (HopException e) {
      String errorMessage =
          BaseMessages.getString(PKG, "DimensionLookupMeta.Exception.DatabaseErrorOccurred")
              + e.getMessage();
      CheckResult cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    }
    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "DimensionLookupMeta.CheckResult.TransformReceiveInfoOK"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "DimensionLookupMeta.CheckResult.NoInputReceiveFromOtherTransforms"),
              transformMeta));
    }
  }

  private void checkSequence(
      TransformMeta transformMeta, Database db, IVariables variables, List<ICheckResult> remarks)
      throws HopDatabaseException {
    String sequence = variables.resolve(sequenceName);

    DatabaseMeta databaseMeta =
        getParentTransformMeta().getParentPipelineMeta().findDatabase(connection, variables);

    // Check sequence
    if (databaseMeta.supportsSequences()
        && fields.returns.creationMethod == TechnicalKeyCreationMethod.SEQUENCE
        && StringUtils.isNotEmpty(variables.resolve(sequence))) {
      if (db.checkSequenceExists(sequence)) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG, "DimensionLookupMeta.CheckResult.SequenceExists", sequence),
                transformMeta));
      } else {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG, "DimensionLookupMeta.CheckResult.SequenceCouldNotFound", sequence),
                transformMeta));
      }
    }
  }

  private void checkPreviousFields(
      TransformMeta transformMeta, IRowMeta previousFields, List<ICheckResult> remarks) {
    // Look up fields in the input stream <prev>
    if (previousFields == null) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "DimensionLookupMeta.CheckResult.CouldNotReadFieldsFromPreviousTransform"),
              transformMeta));
      return;
    }

    boolean allOk = true;
    for (DLField field : fields.fields) {
      DimensionUpdateType updateType = field.getUpdateType();
      if (updateType != null && updateType.isWithArgument()) {
        IValueMeta valueMeta = previousFields.searchValueMeta(field.getName());
        if (valueMeta == null) {
          allOk = false;
          remarks.add(
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(PKG, "DimensionLookupMeta.CheckResult.MissingFields")
                      + " "
                      + field.getName(),
                  transformMeta));
        }
      }
    }
    if (allOk) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "DimensionLookupMeta.CheckResult.AllFieldsFound"),
              transformMeta));
    }
  }

  private void checkDateFields(
      TransformMeta transformMeta, IRowMeta tableRowMeta, List<ICheckResult> remarks) {
    if (StringUtils.isNotEmpty(fields.date.from)) {
      if (tableRowMeta.indexOfValue(fields.date.from) < 0) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG,
                    "DimensionLookupMeta.CheckResult.StartPointOfDaterangeNotFound",
                    fields.date.from),
                transformMeta));
      } else {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG,
                    "DimensionLookupMeta.CheckResult.StartPointOfDaterangeFound",
                    fields.date.from),
                transformMeta));
      }
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "DimensionLookupMeta.CheckResult.StartKeyRequired"),
              transformMeta));
    }

    if (StringUtils.isNotEmpty(fields.date.to)) {
      if (tableRowMeta.indexOfValue(fields.date.to) < 0) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG,
                    "DimensionLookupMeta.CheckResult.EndPointOfDaterangeNotFound",
                    fields.date.to),
                transformMeta));
      } else {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG,
                    "DimensionLookupMeta.CheckResult.EndPointOfDaterangeFound",
                    fields.date.to),
                transformMeta));
      }
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "DimensionLookupMeta.CheckResult.EndKeyRequired"),
              transformMeta));
    }
  }

  private void checkReturns(
      TransformMeta transformMeta, IRowMeta tableRowMeta, List<ICheckResult> remarks) {
    /* Also, check the fields: tk, version, from-to, ... */
    if (StringUtils.isNotEmpty(fields.returns.keyField)) {
      if (tableRowMeta.indexOfValue(fields.returns.keyField) < 0) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG,
                    "DimensionLookupMeta.CheckResult.TechnicalKeyNotFound",
                    fields.returns.keyField),
                transformMeta));
      } else {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG,
                    "DimensionLookupMeta.CheckResult.TechnicalKeyFound",
                    fields.returns.keyField),
                transformMeta));
      }
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "DimensionLookupMeta.CheckResult.TechnicalKeyRequired"),
              transformMeta));
    }

    if (StringUtils.isNotEmpty(fields.returns.versionField)) {
      if (tableRowMeta.indexOfValue(fields.returns.versionField) < 0) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG,
                    "DimensionLookupMeta.CheckResult.VersionFieldNotFound",
                    fields.returns.versionField),
                transformMeta));
      } else {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG,
                    "DimensionLookupMeta.CheckResult.VersionFieldFound",
                    fields.returns.versionField),
                transformMeta));
      }
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "DimensionLookupMeta.CheckResult.VersionKeyRequired"),
              transformMeta));
    }

    TechnicalKeyCreationMethod method = fields.returns.creationMethod;
    if (method == null) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "DimensionLookupMeta.CheckResult.ErrorTechKeyCreation")
                  + ": "
                  + "Not specified!",
              transformMeta));
    }
  }

  private IRowMeta checkTableFields(
      TransformMeta transformMeta,
      Database db,
      String schemaName,
      String tableName,
      List<ICheckResult> remarks)
      throws HopDatabaseException {
    IRowMeta rowMeta = db.getTableFieldsMeta(schemaName, tableName);
    if (rowMeta == null) {
      // If the table is not found there is no point in continuing below.
      //
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "DimensionLookupMeta.CheckResult.CouldNotReadTableInfo"),
              transformMeta));
      return null;
    }

    boolean allOk = true;
    for (DLField field : fields.getFields()) {
      IValueMeta v = rowMeta.searchValueMeta(field.getLookup());
      if (v == null) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                        PKG, "DimensionLookupMeta.CheckResult.MissingCompareFieldsInTargetTable")
                    + field.getName()
                    + " --> "
                    + field.getLookup(),
                transformMeta));
        allOk = false;
      }
      if (update) {
        // If we are updating the dimension we need to check the update type
        //
        if (field.getUpdateType() == null) {
          remarks.add(
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  "The update type specified is not valid for field '"
                      + Const.NVL(field.getName(), field.getLookup())
                      + "' : '"
                      + field.getUpdate()
                      + "'",
                  transformMeta));
          allOk = false;
        }
      } else {
        // Check the type of the dimension field to look up
        //
        int type = ValueMetaFactory.getIdForValueMeta(field.getReturnType());
        if (type == IValueMeta.TYPE_NONE) {
          remarks.add(
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  "The return type specified is not valid for field '"
                      + Const.NVL(field.getName(), field.getLookup())
                      + "' : '"
                      + field.getReturnType()
                      + "'",
                  transformMeta));
          allOk = false;
        }
      }
    }
    if (allOk) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "DimensionLookupMeta.CheckResult.AllLookupFieldFound"),
              transformMeta));
    }

    return rowMeta;
  }

  private void checkTable(
      TransformMeta transformMeta, IVariables variables, List<ICheckResult> remarks) {
    if (StringUtils.isEmpty(variables.resolve(tableName))) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "DimensionLookupMeta.CheckResult.CouldNotReadTableInfo"),
              transformMeta));
    }
  }

  private void checkDatabase(
      TransformMeta transformMeta, List<ICheckResult> remarks, IVariables variables) {
    DatabaseMeta databaseMeta =
        getParentTransformMeta().getParentPipelineMeta().findDatabase(connection, variables);
    if (databaseMeta == null) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "DimensionLookupMeta.CheckResult.InvalidConnectionName"),
              transformMeta));
    }
  }

  private void checkKeys(
      TransformMeta transformMeta,
      IVariables variables,
      String schemaName,
      String tableName,
      IRowMeta tableRowMeta,
      IRowMeta previousRowMeta,
      List<ICheckResult> remarks) {
    boolean allOk = true;

    DatabaseMeta databaseMeta =
        getParentTransformMeta().getParentPipelineMeta().findDatabase(connection, variables);

    for (DLKey key : fields.keys) {
      IValueMeta prevValueMeta = previousRowMeta.searchValueMeta(key.getName());
      if (prevValueMeta == null) {
        // Key field being used is not delivered to the transform
        //
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                        PKG, CONST_DIMENSION_LOOKUP_META_CHECK_RESULT_KEY_HAS_PROBLEM)
                    + " "
                    + key.getName()
                    + BaseMessages.getString(
                        PKG, "DimensionLookupMeta.CheckResult.KeyNotPresentInStream"),
                transformMeta));
        allOk = false;
      }
      IValueMeta tableValueMeta = tableRowMeta.searchValueMeta(key.getLookup());
      if (tableValueMeta == null) {
        // Table column being used is not present
        //
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                        PKG, CONST_DIMENSION_LOOKUP_META_CHECK_RESULT_KEY_HAS_PROBLEM)
                    + " "
                    + BaseMessages.getString(
                        PKG, "DimensionLookupMeta.CheckResult.KeyNotPresentInDimensionTable")
                    + databaseMeta.getQuotedSchemaTableCombination(
                        variables, schemaName, tableName),
                transformMeta));
        allOk = false;
      }
      // Different data types can indicate a data conversion issue down the line.
      //
      if (prevValueMeta != null
          && tableValueMeta != null
          && prevValueMeta.getType() != tableValueMeta.getType()) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                        PKG, CONST_DIMENSION_LOOKUP_META_CHECK_RESULT_KEY_HAS_PROBLEM)
                    + " "
                    + prevValueMeta.getName()
                    + " ("
                    + prevValueMeta.getOrigin()
                    + BaseMessages.getString(
                        PKG, "DimensionLookupMeta.CheckResult.KeyNotTheSameTypeAs")
                    + tableValueMeta.getName()
                    + " ("
                    + databaseMeta.getQuotedSchemaTableCombination(variables, schemaName, tableName)
                    + ")",
                transformMeta));
        allOk = false;
      }
    }
    if (allOk) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "DimensionLookupMeta.CheckResult.AllKeysFieldsFound"),
              transformMeta));
    }
  }

  @Override
  public IRowMeta getTableFields(IVariables variables) {

    IRowMeta tableRowMeta = null;
    DatabaseMeta databaseMeta =
        getParentTransformMeta().getParentPipelineMeta().findDatabase(connection, variables);

    if (databaseMeta != null) {
      try (Database db = createDatabaseObject(variables)) {
        db.connect();
        tableRowMeta = db.getTableFieldsMeta(schemaName, tableName);
      } catch (HopDatabaseException dbe) {
        logError(
            BaseMessages.getString(PKG, "DimensionLookupMeta.Log.DatabaseErrorOccurred")
                + dbe.getMessage());
      }
    }
    return tableRowMeta;
  }

  @Override
  public SqlStatement getSqlStatements(
      IVariables variables,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta previousRowMeta,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    // Verify the absolute basic settings like having a database, table, input fields, technical
    // key, ...
    //
    validateBasicSettings(variables, pipelineMeta, previousRowMeta);

    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(connection, variables);

    SqlStatement statement =
        new SqlStatement(transformMeta.getName(), databaseMeta, null); // default: nothing to do!

    if (!update) {
      // Only bother in case of update, not lookup!
      //
      return statement;
    }

    String realSchema = variables.resolve(schemaName);
    String realTable = variables.resolve(tableName);

    if (StringUtils.isEmpty(realTable)) {
      throw new HopTransformException(
          BaseMessages.getString(
              PKG, "DimensionLookupMeta.ReturnValue.NoTableDefinedOnConnection"));
    }

    String schemaTable =
        databaseMeta.getQuotedSchemaTableCombination(variables, realSchema, realTable);
    try (Database db = new Database(loggingObject, variables, databaseMeta)) {
      db.connect();

      IRowMeta tableRowMeta = buildTableFields(previousRowMeta, statement);
      String sql =
          db.getDDL(
              schemaTable,
              tableRowMeta,
              StringUtils.isNotEmpty(sequenceName) ? null : fields.returns.keyField,
              fields.returns.creationMethod == TechnicalKeyCreationMethod.AUTO_INCREMENT,
              null,
              true);

      // Key lookup dimensions...
      //
      String[] idxFields = new String[fields.keys.size()];
      for (int i = 0; i < idxFields.length; i++) {
        idxFields[i] = fields.keys.get(i).getLookup();
      }

      if (!db.checkIndexExists(schemaTable, idxFields)) {
        String indexname = "idx_" + tableName + "_lookup";
        sql +=
            db.getCreateIndexStatement(
                schemaTable, indexname, idxFields, false, false, false, true);
      }

      // (Bitmap) index on technical key

      if (!db.checkIndexExists(schemaTable, idxFields)) {
        String indexName = "idx_" + tableName + "_tk";
        sql +=
            db.getCreateIndexStatement(
                schemaTable,
                indexName,
                new String[] {fields.returns.keyField},
                true,
                false,
                true,
                true);
      }

      // The optional Oracle sequence
      if (fields.returns.creationMethod == TechnicalKeyCreationMethod.SEQUENCE
          && StringUtils.isNotEmpty(sequenceName)
          && !db.checkSequenceExists(schemaName, sequenceName)) {
        sql += db.getCreateSequenceStatement(schemaName, sequenceName, 1L, 1L, -1L, true);
      }

      if (sql.length() == 0) {
        statement.setSql(null);
      } else {
        statement.setSql(variables.resolve(sql));
      }
    } catch (HopDatabaseException dbe) {
      statement.setError(
          BaseMessages.getString(PKG, "DimensionLookupMeta.ReturnValue.ErrorOccurred")
              + dbe.getMessage());
    }

    return statement;
  }

  private void validateBasicSettings(
      IVariables variables, PipelineMeta pipelineMeta, IRowMeta previousRowMeta)
      throws HopTransformException {

    // Raise an exception in case connection is missing
    pipelineMeta.findDatabase(connection, variables, true);

    if (fields.keys.isEmpty()) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "DimensionLookupMeta.ReturnValue.NoKeyFieldsSpecified"));
    }

    if (StringUtils.isEmpty(fields.returns.keyField)) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "DimensionLookupMeta.ReturnValue.TechnicalKeyFieldRequired"));
    }

    if (previousRowMeta == null || previousRowMeta.isEmpty()) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "DimensionLookupMeta.ReturnValue.NotReceivingAnyFields"));
    }

    if (StringUtils.isEmpty(variables.resolve(tableName))) {
      throw new HopTransformException(
          BaseMessages.getString(
              PKG, "DimensionLookupMeta.ReturnValue.NoTableDefinedOnConnection"));
    }
  }

  private IRowMeta buildTableFields(IRowMeta prev, SqlStatement statement) {
    // How does the table look like?
    //
    // Technical key, version, from and to dates.
    //
    IRowMeta tableRowMeta =
        new RowMetaBuilder()
            .addInteger(fields.returns.keyField, 10)
            .addInteger(fields.returns.versionField, 5)
            .addDate(fields.date.from)
            .addDate(fields.date.to)
            .build();

    List<String> errorFields = new ArrayList<>();

    // Then the keys
    //
    for (DLKey key : fields.keys) {
      IValueMeta prevValueMeta = prev.searchValueMeta(key.getName());
      if (prevValueMeta == null) {
        errorFields.add(key.getName());
        continue;
      }
      IValueMeta field = prevValueMeta.clone();
      field.setName(key.getLookup());
      tableRowMeta.addValueMeta(field);
    }

    //
    // Then the fields to update...
    //
    for (DLField field : fields.fields) {
      IValueMeta valueMeta = null;
      DimensionUpdateType updateType = field.getUpdateType();
      if (updateType == null) {
        errorFields.add(
            "Unknown update type for field: " + field.getName() + " : '" + field.getUpdate());
        break;
      }
      switch (updateType) {
        case DATE_UPDATED, DATE_INSERTED, DATE_INSERTED_UPDATED:
          valueMeta = new ValueMetaDate(field.getLookup());
          break;
        case LAST_VERSION:
          valueMeta = new ValueMetaBoolean(field.getLookup());
          break;
        case INSERT, UPDATE, PUNCH_THROUGH:
          IValueMeta prevValueMeta = prev.searchValueMeta(field.getName());
          if (prevValueMeta == null) {
            errorFields.add(field.getName());
            continue;
          }
          valueMeta = prevValueMeta.clone();
          valueMeta.setName(field.getLookup());
          break;
      }
      if (valueMeta != null) {
        tableRowMeta.addValueMeta(valueMeta);
      }
    }

    if (!errorFields.isEmpty()) {
      statement.setError(
          BaseMessages.getString(PKG, "DimensionLookupMeta.ReturnValue.UnableToFindFields")
              + StringUtils.join(errorFields, ", "));
    }

    return tableRowMeta;
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
      IHopMetadataProvider metadataProvider) {
    if (prev == null) {
      return;
    }

    if (update) {
      analyzeImpactUpdate(impact, pipelineMeta, transformMeta, prev, variables);
    } else {
      analyzeImpactLookup(impact, pipelineMeta, transformMeta, prev, variables);
    }
  }

  private void analyzeImpactUpdate(
      List<DatabaseImpact> impact,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      IVariables variables) {

    DatabaseMeta databaseMeta =
        getParentTransformMeta().getParentPipelineMeta().findDatabase(connection, variables);

    // Update: insert/update on all specified fields...
    // Lookup: we do a lookup on the natural keys + the return fields!
    for (DLKey key : fields.keys) {
      IValueMeta v = prev.searchValueMeta(key.getName());

      DatabaseImpact ii =
          new DatabaseImpact(
              DatabaseImpact.TYPE_IMPACT_READ_WRITE,
              pipelineMeta.getName(),
              transformMeta.getName(),
              databaseMeta.getDatabaseName(),
              tableName,
              key.getLookup(),
              key.getName(),
              v == null ? "" : v.getOrigin(),
              "",
              v == null ? "" : CONST_TYPE + v.toStringMeta());
      impact.add(ii);
    }

    // Return fields...
    for (DLField field : fields.fields) {
      IValueMeta v = prev.searchValueMeta(field.getName());

      DatabaseImpact ii =
          new DatabaseImpact(
              DatabaseImpact.TYPE_IMPACT_READ_WRITE,
              pipelineMeta.getName(),
              transformMeta.getName(),
              databaseMeta.getDatabaseName(),
              tableName,
              field.getLookup(),
              field.getLookup(),
              v == null ? "" : v.getOrigin(),
              "",
              v == null ? "" : CONST_TYPE + v.toStringMeta());
      impact.add(ii);
    }
  }

  private void analyzeImpactLookup(
      List<DatabaseImpact> impact,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      IVariables variables) {

    DatabaseMeta databaseMeta =
        getParentTransformMeta().getParentPipelineMeta().findDatabase(connection, variables);

    // Lookup: we do a lookup on the natural keys + the return fields!
    for (DLKey key : fields.keys) {
      IValueMeta v = prev.searchValueMeta(key.getName());

      DatabaseImpact ii =
          new DatabaseImpact(
              DatabaseImpact.TYPE_IMPACT_READ,
              pipelineMeta.getName(),
              transformMeta.getName(),
              databaseMeta.getDatabaseName(),
              tableName,
              key.getLookup(),
              key.getName(),
              v != null ? v.getOrigin() : "?",
              "",
              v == null ? "" : CONST_TYPE + v.toStringMeta());
      impact.add(ii);
    }

    // Return fields...
    for (DLField field : fields.fields) {
      IValueMeta v = prev.searchValueMeta(field.getName());

      DatabaseImpact ii =
          new DatabaseImpact(
              DatabaseImpact.TYPE_IMPACT_READ,
              pipelineMeta.getName(),
              transformMeta.getName(),
              databaseMeta.getDatabaseName(),
              tableName,
              field.getLookup(),
              field.getLookup(),
              v == null ? "" : v.getOrigin(),
              "",
              v == null ? "" : CONST_TYPE + v.toStringMeta());
      impact.add(ii);
    }
  }

  Database createDatabaseObject(IVariables variables) {
    DatabaseMeta databaseMeta =
        getParentTransformMeta().getParentPipelineMeta().findDatabase(connection, variables);

    return new Database(loggingObject, variables, databaseMeta);
  }

  public enum TechnicalKeyCreationMethod implements IEnumHasCode {
    AUTO_INCREMENT("autoinc"),
    SEQUENCE("sequence"),
    TABLE_MAXIMUM("tablemax");

    private final String code;

    TechnicalKeyCreationMethod(String code) {
      this.code = code;
    }

    /**
     * Gets code
     *
     * @return value of code
     */
    public String getCode() {
      return code;
    }
  }

  public enum DimensionUpdateType implements IEnumHasCodeAndDescription {
    INSERT("Insert", BaseMessages.getString(PKG, "DimensionLookupMeta.TypeDesc.Insert"), true),
    UPDATE("Update", BaseMessages.getString(PKG, "DimensionLookupMeta.TypeDesc.Update"), true),
    PUNCH_THROUGH(
        "Punch through",
        BaseMessages.getString(PKG, "DimensionLookupMeta.TypeDesc.PunchThrough"),
        true),
    DATE_INSERTED_UPDATED(
        "DateInsertedOrUpdated",
        BaseMessages.getString(PKG, "DimensionLookupMeta.TypeDesc.DateInsertedOrUpdated"),
        false),
    DATE_INSERTED(
        "DateInserted",
        BaseMessages.getString(PKG, "DimensionLookupMeta.TypeDesc.DateInserted"),
        false),
    DATE_UPDATED(
        "DateUpdated",
        BaseMessages.getString(PKG, "DimensionLookupMeta.TypeDesc.DateUpdated"),
        false),
    LAST_VERSION(
        "LastVersion",
        BaseMessages.getString(PKG, "DimensionLookupMeta.TypeDesc.LastVersion"),
        false);
    private final String code;
    private final String description;
    private final boolean isWithArgument;

    DimensionUpdateType(String code, String description, boolean isWithArgument) {
      this.code = code;
      this.description = description;
      this.isWithArgument = isWithArgument;
    }

    public static String[] getDescriptions() {
      String[] descriptions = new String[values().length];
      for (int i = 0; i < descriptions.length; i++) {
        descriptions[i] = values()[i].description;
      }
      return descriptions;
    }

    /**
     * Gets code
     *
     * @return value of code
     */
    @Override
    public String getCode() {
      return code;
    }

    /**
     * Gets description
     *
     * @return value of description
     */
    @Override
    public String getDescription() {
      return description;
    }

    public boolean isWithArgument() {
      return isWithArgument;
    }

    public static DimensionUpdateType lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(
          DimensionUpdateType.class, description, null);
    }
  }

  public enum StartDateAlternative implements IEnumHasCodeAndDescription {
    NONE(
        "none", BaseMessages.getString(PKG, "DimensionLookupMeta.StartDateAlternative.None.Label")),
    SYSTEM_DATE(
        "sysdate",
        BaseMessages.getString(PKG, "DimensionLookupMeta.StartDateAlternative.Sysdate.Label")),
    PIPELINE_START(
        "pipeline_start",
        BaseMessages.getString(
            PKG, "DimensionLookupMeta.StartDateAlternative.PipelineStart.Label")),
    NULL(
        "null", BaseMessages.getString(PKG, "DimensionLookupMeta.StartDateAlternative.Null.Label")),
    COLUMN_VALUE(
        "column_value",
        BaseMessages.getString(PKG, "DimensionLookupMeta.StartDateAlternative.ColumnValue.Label"));
    private final String code;
    private final String description;

    StartDateAlternative(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static String[] getDescriptions() {
      String[] descriptions = new String[values().length];
      for (int i = 0; i < descriptions.length; i++) {
        descriptions[i] = values()[i].description;
      }
      return descriptions;
    }

    public static StartDateAlternative lookupWithDescription(String description) {
      for (StartDateAlternative value : values()) {
        if (value.description.equalsIgnoreCase(description)) {
          return value;
        }
      }
      return null;
    }

    /**
     * Gets code
     *
     * @return value of code
     */
    public String getCode() {
      return code;
    }

    /**
     * Gets description
     *
     * @return value of description
     */
    public String getDescription() {
      return description;
    }
  }

  public static class DLFields {
    @HopMetadataProperty(key = "key")
    private List<DLKey> keys;

    @HopMetadataProperty(key = "date")
    private DLDate date;

    @HopMetadataProperty(key = "field")
    private List<DLField> fields;

    @HopMetadataProperty(key = "return")
    private DLReturn returns;

    public DLFields() {
      this.keys = new ArrayList<>();
      this.date = new DLDate();
      this.fields = new ArrayList<>();
      this.returns = new DLReturn();
    }

    public DLFields(DLFields f) {
      this();
      for (DLKey key : f.keys) {
        this.keys.add(new DLKey(key));
      }
      this.date = new DLDate(f.date);
      for (DLField field : f.fields) {
        this.fields.add(new DLField(field));
      }
      this.returns = new DLReturn(f.returns);
    }

    /**
     * Gets keys
     *
     * @return value of keys
     */
    public List<DLKey> getKeys() {
      return keys;
    }

    /**
     * Sets keys
     *
     * @param keys value of keys
     */
    public void setKeys(List<DLKey> keys) {
      this.keys = keys;
    }

    /**
     * Gets date
     *
     * @return value of date
     */
    public DLDate getDate() {
      return date;
    }

    /**
     * Sets date
     *
     * @param date value of date
     */
    public void setDate(DLDate date) {
      this.date = date;
    }

    /**
     * Gets fields
     *
     * @return value of fields
     */
    public List<DLField> getFields() {
      return fields;
    }

    /**
     * Sets fields
     *
     * @param fields value of fields
     */
    public void setFields(List<DLField> fields) {
      this.fields = fields;
    }

    /**
     * Gets returns
     *
     * @return value of returns
     */
    public DLReturn getReturns() {
      return returns;
    }

    /**
     * Sets returns
     *
     * @param returns value of returns
     */
    public void setReturns(DLReturn returns) {
      this.returns = returns;
    }
  }

  public static class DLReturn {
    @HopMetadataProperty(
        key = "name",
        injectionKey = "TECHNICAL_KEY_FIELD",
        injectionKeyDescription = "DimensionLookup.Injection.TECHNICAL_KEY_FIELD",
        hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_COLUMN)
    private String keyField;

    @HopMetadataProperty(
        key = "rename",
        injectionKey = "TECHNICAL_KEY_NEW_NAME",
        injectionKeyDescription = "DimensionLookup.Injection.TECHNICAL_KEY_NEW_NAME")
    private String keyRename;

    @HopMetadataProperty(
        key = "creation_method",
        storeWithCode = true,
        injectionKey = "TECHNICAL_KEY_CREATION",
        injectionKeyDescription = "DimensionLookup.Injection.TECHNICAL_KEY_CREATION")
    private TechnicalKeyCreationMethod creationMethod;

    @HopMetadataProperty(
        key = "version",
        injectionKey = "VERSION_FIELD",
        injectionKeyDescription = "DimensionLookup.Injection.VERSION_FIELD")
    private String versionField;

    public DLReturn() {}

    public DLReturn(DLReturn r) {
      this.keyField = r.keyField;
      this.keyRename = r.keyRename;
      this.creationMethod = r.creationMethod;
      this.versionField = r.versionField;
    }

    /**
     * Gets name
     *
     * @return value of name
     */
    public String getKeyField() {
      return keyField;
    }

    /**
     * Sets name
     *
     * @param keyField value of name
     */
    public void setKeyField(String keyField) {
      this.keyField = keyField;
    }

    /**
     * Gets rename
     *
     * @return value of rename
     */
    public String getKeyRename() {
      return keyRename;
    }

    /**
     * Sets rename
     *
     * @param keyRename value of rename
     */
    public void setKeyRename(String keyRename) {
      this.keyRename = keyRename;
    }

    /**
     * Gets creationMethod
     *
     * @return value of creationMethod
     */
    public TechnicalKeyCreationMethod getCreationMethod() {
      return creationMethod;
    }

    /**
     * Sets creationMethod
     *
     * @param creationMethod value of creationMethod
     */
    public void setCreationMethod(TechnicalKeyCreationMethod creationMethod) {
      this.creationMethod = creationMethod;
    }

    /**
     * Gets version
     *
     * @return value of version
     */
    public String getVersionField() {
      return versionField;
    }

    /**
     * Sets version
     *
     * @param versionField value of version
     */
    public void setVersionField(String versionField) {
      this.versionField = versionField;
    }
  }

  public static class DLField {
    /** Fields containing the values in the input stream to update the dimension with */
    @HopMetadataProperty(
        key = "name",
        injectionKey = "STREAM_FIELDNAME",
        injectionKeyDescription = "DimensionLookup.Injection.STREAM_FIELDNAME")
    private String name;

    /** Fields in the dimension to update or retrieve */
    @HopMetadataProperty(
        key = "lookup",
        injectionKey = "DATABASE_FIELDNAME",
        injectionKeyDescription = "DimensionLookup.Injection.DATABASE_FIELDNAME",
        hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_COLUMN)
    private String lookup;

    /** The type of update to perform on the fields: insert, update, punch-through */
    @HopMetadataProperty(
        key = "update",
        injectionKey = "UPDATE_TYPE",
        injectionKeyDescription = "DimensionLookup.Injection.UPDATE_TYPE")
    private String update;

    @HopMetadataProperty(
        key = "type",
        injectionKey = "TYPE_OF_RETURN_FIELD",
        injectionKeyDescription = "DimensionLookup.Injection.TYPE_OF_RETURN_FIELD")
    private String returnType;

    /** Not serialized. This is used to cache the lookup of the dimension type */
    private DimensionUpdateType updateType;

    public DLField() {
      this.updateType = null;
      this.returnType = null;
    }

    public DLField(DLField f) {
      this.name = f.name;
      this.lookup = f.lookup;
      this.update = f.update;
      this.updateType = null;
      this.returnType = f.returnType;
    }

    public DimensionUpdateType getUpdateType() {
      if (updateType != null) {
        return updateType;
      }
      for (DimensionUpdateType type : DimensionUpdateType.values()) {
        if (type.getCode().equals(update)) {
          updateType = type;
          return type;
        }
      }
      return null;
    }

    /**
     * Gets name
     *
     * @return value of name
     */
    public String getName() {
      return name;
    }

    /**
     * Sets name
     *
     * @param name value of name
     */
    public void setName(String name) {
      this.name = name;
    }

    /**
     * Gets lookup
     *
     * @return value of lookup
     */
    public String getLookup() {
      return lookup;
    }

    /**
     * Sets lookup
     *
     * @param lookup value of lookup
     */
    public void setLookup(String lookup) {
      this.lookup = lookup;
    }

    /**
     * Gets update type code
     *
     * @return value of update
     */
    public String getUpdate() {
      return update;
    }

    /**
     * Sets update type code
     *
     * @param update value of update
     */
    public void setUpdate(String update) {
      this.update = update;
      this.updateType = null;
    }

    /**
     * Gets return type for read only lookup
     *
     * @return type of
     */
    public String getReturnType() {
      return returnType;
    }

    /**
     * Sets return type for read only lookup
     *
     * @param type the return type
     */
    public void setReturnType(String type) {
      this.returnType = type;
    }
  }

  public static class DLKey {
    /** Fields used to look up a value in the dimension */
    @HopMetadataProperty(
        injectionKey = "KEY_STREAM_FIELDNAME",
        injectionKeyDescription = "DimensionLookup.Injection.KEY_STREAM_FIELDNAME")
    private String name;

    /** Fields in the dimension to use for lookup */
    @HopMetadataProperty(
        injectionKey = "KEY_DATABASE_FIELDNAME",
        injectionKeyDescription = "DimensionLookup.Injection.KEY_DATABASE_FIELDNAME")
    private String lookup;

    public DLKey() {}

    public DLKey(DLKey k) {
      this.name = k.name;
      this.lookup = k.lookup;
    }

    /**
     * Gets name
     *
     * @return value of name
     */
    public String getName() {
      return name;
    }

    /**
     * Sets name
     *
     * @param name value of name
     */
    public void setName(String name) {
      this.name = name;
    }

    /**
     * Gets lookup
     *
     * @return value of lookup
     */
    public String getLookup() {
      return lookup;
    }

    /**
     * Sets lookup
     *
     * @param lookup value of lookup
     */
    public void setLookup(String lookup) {
      this.lookup = lookup;
    }
  }

  public static class DLDate {
    /** The field to use for date range lookup in the dimension */
    @HopMetadataProperty(
        injectionKey = "STREAM_DATE_FIELD",
        injectionKeyDescription = "DimensionLookup.Injection.STREAM_DATE_FIELD")
    private String name;

    /** The 'from' field of the date range in the dimension */
    @HopMetadataProperty(
        injectionKey = "DATE_RANGE_START_FIELD",
        injectionKeyDescription = "DimensionLookup.Injection.DATE_RANGE_START_FIELD")
    private String from;

    /** The 'to' field of the date range in the dimension */
    @HopMetadataProperty(
        injectionKey = "DATE_RANGE_END_FIELD",
        injectionKeyDescription = "DimensionLookup.Injection.DATE_RANGE_END_FIELD")
    private String to;

    public DLDate() {}

    public DLDate(DLDate d) {
      this.name = d.name;
      this.from = d.from;
      this.to = d.to;
    }

    /**
     * Gets name
     *
     * @return value of name
     */
    public String getName() {
      return name;
    }

    /**
     * Sets name
     *
     * @param name value of name
     */
    public void setName(String name) {
      this.name = name;
    }

    /**
     * Gets from
     *
     * @return value of from
     */
    public String getFrom() {
      return from;
    }

    /**
     * Sets from
     *
     * @param from value of from
     */
    public void setFrom(String from) {
      this.from = from;
    }

    /**
     * Gets to
     *
     * @return value of to
     */
    public String getTo() {
      return to;
    }

    /**
     * Sets to
     *
     * @param to value of to
     */
    public void setTo(String to) {
      this.to = to;
    }
  }

  /**
   * Gets schema name
   *
   * @return value of schemaName
   */
  public String getSchemaName() {
    return schemaName;
  }

  /**
   * Sets schemaName
   *
   * @param schemaName value of schemaName
   */
  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  /**
   * Gets table name
   *
   * @return value of tableName
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Sets table name
   *
   * @param tableName value of tableName
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * Get connection name
   *
   * @return
   */
  public String getConnection() {
    return connection;
  }

  /**
   * Set connection name
   *
   * @param connection
   */
  public void setConnection(String connection) {
    this.connection = connection;
  }

  /**
   * Gets update
   *
   * @return value of update
   */
  public boolean isUpdate() {
    return update;
  }

  /**
   * Sets update
   *
   * @param update value of update
   */
  public void setUpdate(boolean update) {
    this.update = update;
  }

  /**
   * Gets fields
   *
   * @return value of fields
   */
  public DLFields getFields() {
    return fields;
  }

  /**
   * Sets fields
   *
   * @param fields value of fields
   */
  public void setFields(DLFields fields) {
    this.fields = fields;
  }

  /**
   * Gets sequenceName
   *
   * @return value of sequenceName
   */
  public String getSequenceName() {
    return sequenceName;
  }

  /**
   * Sets sequenceName
   *
   * @param sequenceName value of sequenceName
   */
  public void setSequenceName(String sequenceName) {
    this.sequenceName = sequenceName;
  }

  /**
   * Gets commitSize
   *
   * @return value of commitSize
   */
  public int getCommitSize() {
    return commitSize;
  }

  /**
   * Sets commitSize
   *
   * @param commitSize value of commitSize
   */
  public void setCommitSize(int commitSize) {
    this.commitSize = commitSize;
  }

  /**
   * Gets useBatchUpdate
   *
   * @return value of useBatchUpdate
   */
  public boolean isUseBatchUpdate() {
    return useBatchUpdate;
  }

  /**
   * Sets useBatchUpdate
   *
   * @param useBatchUpdate value of useBatchUpdate
   */
  public void setUseBatchUpdate(boolean useBatchUpdate) {
    this.useBatchUpdate = useBatchUpdate;
  }

  /**
   * Gets minYear
   *
   * @return value of minYear
   */
  public int getMinYear() {
    return minYear;
  }

  /**
   * Sets minYear
   *
   * @param minYear value of minYear
   */
  public void setMinYear(int minYear) {
    this.minYear = minYear;
  }

  /**
   * Gets maxYear
   *
   * @return value of maxYear
   */
  public int getMaxYear() {
    return maxYear;
  }

  /**
   * Sets maxYear
   *
   * @param maxYear value of maxYear
   */
  public void setMaxYear(int maxYear) {
    this.maxYear = maxYear;
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
   * Sets cacheSize
   *
   * @param cacheSize value of cacheSize
   */
  public void setCacheSize(int cacheSize) {
    this.cacheSize = cacheSize;
  }

  /**
   * Gets usingStartDateAlternative
   *
   * @return value of usingStartDateAlternative
   */
  public boolean isUsingStartDateAlternative() {
    return usingStartDateAlternative;
  }

  /**
   * Sets usingStartDateAlternative
   *
   * @param usingStartDateAlternative value of usingStartDateAlternative
   */
  public void setUsingStartDateAlternative(boolean usingStartDateAlternative) {
    this.usingStartDateAlternative = usingStartDateAlternative;
  }

  /**
   * Gets startDateAlternative
   *
   * @return value of startDateAlternative
   */
  public StartDateAlternative getStartDateAlternative() {
    return startDateAlternative;
  }

  /**
   * Sets startDateAlternative
   *
   * @param startDateAlternative value of startDateAlternative
   */
  public void setStartDateAlternative(StartDateAlternative startDateAlternative) {
    this.startDateAlternative = startDateAlternative;
  }

  /**
   * Gets startDateFieldName
   *
   * @return value of startDateFieldName
   */
  public String getStartDateFieldName() {
    return startDateFieldName;
  }

  /**
   * Sets startDateFieldName
   *
   * @param startDateFieldName value of startDateFieldName
   */
  public void setStartDateFieldName(String startDateFieldName) {
    this.startDateFieldName = startDateFieldName;
  }

  /**
   * Gets preloadingCache
   *
   * @return value of preloadingCache
   */
  public boolean isPreloadingCache() {
    return preloadingCache;
  }

  /**
   * Sets preloadingCache
   *
   * @param preloadingCache value of preloadingCache
   */
  public void setPreloadingCache(boolean preloadingCache) {
    this.preloadingCache = preloadingCache;
  }
}
