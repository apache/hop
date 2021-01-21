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

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.IProvidesModelerMeta;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.AfterInjection;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.injection.InjectionTypeConverter;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * @author Matt
 * @since on 14-may-2003
 *     <p>WANTED: Interconnected Dynamic Lookups -->
 *     http://www.datawarehouse.com/article/?articleId=5354
 *     <p>The idea is here to create a central 'dimension' cache process, seperated from the other
 *     Hop processes. Hop then connects over a socket to this daemon-like process to check wether a
 *     certain dimension entry is present. Perhaps a more general caching service should be
 *     considered.
 */
@Transform(
    id = "DimensionLookup",
    image = "dimensionlookup.svg",
    name = "i18n::BaseTransform.TypeLongDesc.DimensionUpdate",
    description = "i18n::BaseTransform.TypeTooltipDesc.DimensionUpdate",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.DataWarehouse",
    documentationUrl =
        "https://hop.apache.org/manual/latest/plugins/transforms/dimensionlookup.html")
@InjectionSupported(
    localizationPrefix = "DimensionLookup.Injection.",
    groups = {"KEYS", "FIELDS"})
public class DimensionLookupMeta extends BaseTransformMeta
    implements ITransformMeta<DimensionLookup, DimensionLookupData>, IProvidesModelerMeta {
  private static final Class<?> PKG = DimensionLookupMeta.class; // For Translator

  public static final int TYPE_UPDATE_DIM_INSERT = 0;
  public static final int TYPE_UPDATE_DIM_UPDATE = 1;
  public static final int TYPE_UPDATE_DIM_PUNCHTHROUGH = 2;
  public static final int TYPE_UPDATE_DATE_INSUP = 3;
  public static final int TYPE_UPDATE_DATE_INSERTED = 4;
  public static final int TYPE_UPDATE_DATE_UPDATED = 5;
  public static final int TYPE_UPDATE_LAST_VERSION = 6;

  public static final String[] typeDesc = {
    BaseMessages.getString(PKG, "DimensionLookupMeta.TypeDesc.Insert"),
    BaseMessages.getString(PKG, "DimensionLookupMeta.TypeDesc.Update"),
    BaseMessages.getString(PKG, "DimensionLookupMeta.TypeDesc.PunchThrough"),
    BaseMessages.getString(PKG, "DimensionLookupMeta.TypeDesc.DateInsertedOrUpdated"),
    BaseMessages.getString(PKG, "DimensionLookupMeta.TypeDesc.DateInserted"),
    BaseMessages.getString(PKG, "DimensionLookupMeta.TypeDesc.DateUpdated"),
    BaseMessages.getString(PKG, "DimensionLookupMeta.TypeDesc.LastVersion"),
  };

  public static final String[] typeCodes = { // for serialization
    "Insert",
    "Update",
    "Punch through",
    "DateInsertedOrUpdated",
    "DateInserted",
    "DateUpdated",
    "LastVersion",
  };

  public static final String[] typeDescLookup = ValueMetaFactory.getValueMetaNames();

  public static final int START_DATE_ALTERNATIVE_NONE = 0;
  public static final int START_DATE_ALTERNATIVE_SYSDATE = 1;
  public static final int START_DATE_ALTERNATIVE_START_OF_PIPELINE = 2;
  public static final int START_DATE_ALTERNATIVE_NULL = 3;
  public static final int START_DATE_ALTERNATIVE_COLUMN_VALUE = 4;

  private static final String[] startDateAlternativeCodes = {
    "none", "sysdate", "pipeline_start", "null", "column_value",
  };

  private static final String[] startDateAlternativeDescs = {
    BaseMessages.getString(PKG, "DimensionLookupMeta.StartDateAlternative.None.Label"),
    BaseMessages.getString(PKG, "DimensionLookupMeta.StartDateAlternative.Sysdate.Label"),
    BaseMessages.getString(PKG, "DimensionLookupMeta.StartDateAlternative.PipelineStart.Label"),
    BaseMessages.getString(PKG, "DimensionLookupMeta.StartDateAlternative.Null.Label"),
    BaseMessages.getString(PKG, "DimensionLookupMeta.StartDateAlternative.ColumnValue.Label"),
  };

  /** The lookup schema name */
  @Injection(name = "TARGET_SCHEMA")
  private String schemaName;

  /** The lookup table */
  @Injection(name = "TARGET_TABLE")
  private String tableName;

  private IHopMetadataProvider metadataProvider;

  /** The database connection */
  private DatabaseMeta databaseMeta;

  /** Update the dimension or just lookup? */
  @Injection(name = "UPDATE_DIMENSION")
  private boolean update;

  /** Fields used to look up a value in the dimension */
  @Injection(name = "KEY_STREAM_FIELDNAME", group = "KEYS")
  private String[] keyStream;

  /** Fields in the dimension to use for lookup */
  @Injection(name = "KEY_DATABASE_FIELDNAME", group = "KEYS")
  private String[] keyLookup;

  /** The field to use for date range lookup in the dimension */
  @Injection(name = "STREAM_DATE_FIELD")
  private String dateField;

  /** The 'from' field of the date range in the dimension */
  @Injection(name = "DATE_RANGE_START_FIELD")
  private String dateFrom;

  /** The 'to' field of the date range in the dimension */
  @Injection(name = "DATE_RANGE_END_FIELD")
  private String dateTo;

  /** Fields containing the values in the input stream to update the dimension with */
  @Injection(name = "STREAM_FIELDNAME", group = "FIELDS")
  private String[] fieldStream;

  /** Fields in the dimension to update or retrieve */
  @Injection(name = "DATABASE_FIELDNAME", group = "FIELDS")
  private String[] fieldLookup;

  /** The type of update to perform on the fields: insert, update, punch-through */
  @Injection(name = "UPDATE_TYPE", group = "FIELDS", converter = UpdateTypeCodeConverter.class)
  private int[] fieldUpdate;

  @Injection(
      name = "TYPE_OF_RETURN_FIELD",
      group = "FIELDS",
      converter = ReturnTypeCodeConverter.class)
  private int[] returnType = {};

  /** Name of the technical key (surrogate key) field to return from the dimension */
  @Injection(name = "TECHNICAL_KEY_FIELD")
  private String keyField;

  /** New name of the technical key field */
  @Injection(name = "TECHNICAL_KEY_NEW_NAME")
  private String keyRename;

  /** Use auto increment field as TK */
  private boolean autoIncrement;

  /** The name of the version field */
  @Injection(name = "VERSION_FIELD")
  private String versionField;

  /** Sequence name to get the sequence from */
  @Injection(name = "TECHNICAL_KEY_SEQUENCE")
  private String sequenceName;

  /** The number of rows between commits */
  @Injection(name = "COMMIT_SIZE")
  private int commitSize;

  /** Flag to indicate the use of batch updates, default disabled for backward compatibility */
  private boolean useBatchUpdate;

  /** The year to use as minus infinity in the dimensions date range */
  @Injection(name = "MIN_YEAR")
  private int minYear;

  /** The year to use as plus infinity in the dimensions date range */
  @Injection(name = "MAX_YEAR")
  private int maxYear;

  /** Which method to use for the creation of the tech key */
  @Injection(name = "TECHNICAL_KEY_CREATION")
  private String techKeyCreation = null;

  public static String CREATION_METHOD_AUTOINC = "autoinc";
  public static String CREATION_METHOD_SEQUENCE = "sequence";
  public static String CREATION_METHOD_TABLEMAX = "tablemax";

  /** The size of the cache in ROWS : -1 means: not set, 0 means: cache all */
  @Injection(name = "CACHE_SIZE")
  private int cacheSize;

  /** Flag to indicate we're going to use an alternative start date */
  @Injection(name = "USE_ALTERNATIVE_START_DATE")
  private boolean usingStartDateAlternative;

  /** The type of alternative */
  @Injection(name = "ALTERNATIVE_START_OPTION", converter = StartDateCodeConverter.class)
  private int startDateAlternative;

  /** The field name in case we select the column value option as an alternative start date */
  @Injection(name = "ALTERNATIVE_START_COLUMN")
  private String startDateFieldName;

  @Injection(name = "PRELOAD_CACHE")
  private boolean preloadingCache;

  public DimensionLookupMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the tablename. */
  @Override
  public String getTableName() {
    return tableName;
  }

  /** @param tableName The tablename to set. */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /** @return Returns the database. */
  @Override
  public DatabaseMeta getDatabaseMeta() {
    return databaseMeta;
  }

  /** @param database The database to set. */
  public void setDatabaseMeta(DatabaseMeta database) {
    this.databaseMeta = database;
  }

  @Injection(name = "CONNECTION_NAME")
  public void setConnection(String connectionName) {
    try {
      databaseMeta = DatabaseMeta.loadDatabase(metadataProvider, connectionName);
    } catch (Exception e) {
      throw new RuntimeException(
          "Error loading relational database connection '" + connectionName + "'", e);
    }
  }

  /** @return Returns the update. */
  public boolean isUpdate() {
    return update;
  }

  /** @param update The update to set. */
  public void setUpdate(boolean update) {
    this.update = update;
  }

  /** @return Returns the autoIncrement. */
  public boolean isAutoIncrement() {
    return autoIncrement;
  }

  /** @param autoIncrement The autoIncrement to set. */
  public void setAutoIncrement(boolean autoIncrement) {
    this.autoIncrement = autoIncrement;
  }

  /**
   * Set the way how the technical key field should be created.
   *
   * @param techKeyCreation which method to use for the creation of the technical key.
   */
  public void setTechKeyCreation(String techKeyCreation) {
    this.techKeyCreation = techKeyCreation;
  }

  /**
   * Get the way how the technical key field should be created.
   *
   * @return creation way for the technical key.
   */
  public String getTechKeyCreation() {
    return this.techKeyCreation;
  }

  /** @return Returns the commitSize. */
  public int getCommitSize() {
    return commitSize;
  }

  /** @param commitSize The commitSize to set. */
  public void setCommitSize(int commitSize) {
    this.commitSize = commitSize;
  }

  /** @return Returns the dateField. */
  public String getDateField() {
    return dateField;
  }

  /** @param dateField The dateField to set. */
  public void setDateField(String dateField) {
    this.dateField = dateField;
  }

  /** @return Returns the dateFrom. */
  public String getDateFrom() {
    return dateFrom;
  }

  /** @param dateFrom The dateFrom to set. */
  public void setDateFrom(String dateFrom) {
    this.dateFrom = dateFrom;
  }

  /** @return Returns the dateTo. */
  public String getDateTo() {
    return dateTo;
  }

  /** @param dateTo The dateTo to set. */
  public void setDateTo(String dateTo) {
    this.dateTo = dateTo;
  }

  /** @return Fields in the dimension to update or retrieve. */
  public String[] getFieldLookup() {
    return fieldLookup;
  }

  /** @param fieldLookup sets the fields in the dimension to update or retrieve. */
  public void setFieldLookup(String[] fieldLookup) {
    this.fieldLookup = fieldLookup;
  }

  /** @return Fields containing the values in the input stream to update the dimension with. */
  public String[] getFieldStream() {
    return fieldStream;
  }

  /**
   * @param fieldStream The fields containing the values in the input stream to update the dimension
   *     with.
   */
  public void setFieldStream(String[] fieldStream) {
    this.fieldStream = fieldStream;
  }

  /** @return Returns the fieldUpdate. */
  public int[] getFieldUpdate() {
    return fieldUpdate;
  }

  /** @param fieldUpdate The fieldUpdate to set. */
  public void setFieldUpdate(int[] fieldUpdate) {
    this.fieldUpdate = fieldUpdate;
  }

  /** @return Returns the returnType. */
  public int[] getReturnType() {
    return returnType;
  }

  /** @param returnType The returnType to set. */
  public void setReturnType(int[] returnType) {
    this.returnType = returnType;
  }

  /** @return Returns the keyField. */
  public String getKeyField() {
    return keyField;
  }

  /** @param keyField The keyField to set. */
  public void setKeyField(String keyField) {
    this.keyField = keyField;
  }

  /** @return Returns the keyLookup. */
  public String[] getKeyLookup() {
    return keyLookup;
  }

  /** @param keyLookup The keyLookup to set. */
  public void setKeyLookup(String[] keyLookup) {
    this.keyLookup = keyLookup;
  }

  /** @return Returns the keyRename. */
  public String getKeyRename() {
    return keyRename;
  }

  /** @param keyRename The keyRename to set. */
  public void setKeyRename(String keyRename) {
    this.keyRename = keyRename;
  }

  /** @return Returns the keyStream. */
  public String[] getKeyStream() {
    return keyStream;
  }

  /** @param keyStream The keyStream to set. */
  public void setKeyStream(String[] keyStream) {
    this.keyStream = keyStream;
  }

  /** @return Returns the maxYear. */
  public int getMaxYear() {
    return maxYear;
  }

  /** @param maxYear The maxYear to set. */
  public void setMaxYear(int maxYear) {
    this.maxYear = maxYear;
  }

  /** @return Returns the minYear. */
  public int getMinYear() {
    return minYear;
  }

  /** @param minYear The minYear to set. */
  public void setMinYear(int minYear) {
    this.minYear = minYear;
  }

  /** @return Returns the sequenceName. */
  public String getSequenceName() {
    return sequenceName;
  }

  /** @param sequenceName The sequenceName to set. */
  public void setSequenceName(String sequenceName) {
    this.sequenceName = sequenceName;
  }

  /** @return Returns the versionField. */
  public String getVersionField() {
    return versionField;
  }

  /** @param versionField The versionField to set. */
  public void setVersionField(String versionField) {
    this.versionField = versionField;
  }

  public void actualizeWithInjectedValues() {
    if (!update && returnType.length > 0) {
      fieldUpdate = returnType;
    }
    normalizeAllocationFields();
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode, metadataProvider);
  }

  public void allocate(int nrkeys, int nrFields) {
    keyStream = new String[nrkeys];
    keyLookup = new String[nrkeys];

    fieldStream = new String[nrFields];
    fieldLookup = new String[nrFields];
    fieldUpdate = new int[nrFields];
    returnType = new int[nrFields];
  }

  @Override
  public Object clone() {
    DimensionLookupMeta retval = (DimensionLookupMeta) super.clone();

    int nrkeys = keyStream.length;
    int nrFields = fieldStream.length;

    retval.allocate(nrkeys, nrFields);

    System.arraycopy(keyStream, 0, retval.keyStream, 0, nrkeys);
    System.arraycopy(keyLookup, 0, retval.keyLookup, 0, nrkeys);

    System.arraycopy(fieldStream, 0, retval.fieldStream, 0, nrFields);
    System.arraycopy(fieldLookup, 0, retval.fieldLookup, 0, nrFields);
    System.arraycopy(fieldUpdate, 0, retval.fieldUpdate, 0, nrFields);
    System.arraycopy(returnType, 0, retval.returnType, 0, nrFields);

    return retval;
  }

  public static final int getUpdateType(boolean upd, String ty) {
    if (upd) {
      for (int i = 0; i < typeCodes.length; i++) {
        if (typeCodes[i].equalsIgnoreCase(ty)) {
          return i;
        }
      }
      // for compatibility:
      for (int i = 0; i < typeDesc.length; i++) {
        if (typeDesc[i].equalsIgnoreCase(ty)) {
          return i;
        }
      }
      if ("Y".equalsIgnoreCase(ty)) {
        return TYPE_UPDATE_DIM_PUNCHTHROUGH;
      }

      return TYPE_UPDATE_DIM_INSERT; // INSERT is the default: don't lose information.
    } else {
      int retval = ValueMetaFactory.getIdForValueMeta(ty);
      if (retval == IValueMeta.TYPE_NONE) {
        retval = IValueMeta.TYPE_STRING;
      }
      return retval;
    }
  }

  public static final String getUpdateType(boolean upd, int t) {
    if (!upd) {
      return ValueMetaFactory.getValueMetaName(t);
    } else {
      return typeDesc[t];
    }
  }

  public static final String getUpdateTypeCode(boolean upd, int t) {
    if (!upd) {
      return ValueMetaFactory.getValueMetaName(t);
    } else {
      return typeCodes[t];
    }
  }

  public static final int getStartDateAlternative(String string) {
    for (int i = 0; i < startDateAlternativeCodes.length; i++) {
      if (startDateAlternativeCodes[i].equalsIgnoreCase(string)) {
        return i;
      }
    }
    for (int i = 0; i < startDateAlternativeDescs.length; i++) {
      if (startDateAlternativeDescs[i].equalsIgnoreCase(string)) {
        return i;
      }
    }
    return START_DATE_ALTERNATIVE_NONE;
  }

  public static final String getStartDateAlternativeCode(int alternative) {
    return startDateAlternativeCodes[alternative];
  }

  public static final String getStartDateAlternativeDesc(int alternative) {
    return startDateAlternativeDescs[alternative];
  }

  public static final String[] getStartDateAlternativeCodes() {
    return startDateAlternativeCodes;
  }

  public static final String[] getStartDateAlternativeDescriptions() {
    return startDateAlternativeDescs;
  }

  public static final boolean isUpdateTypeWithoutArgument(boolean update, int type) {
    if (!update) {
      return false; // doesn't apply
    }

    switch (type) {
      case TYPE_UPDATE_DATE_INSUP:
      case TYPE_UPDATE_DATE_INSERTED:
      case TYPE_UPDATE_DATE_UPDATED:
      case TYPE_UPDATE_LAST_VERSION:
        return true;
      default:
        return false;
    }
  }

  @Override
  public void setDefault() {
    int nrkeys, nrFields;

    schemaName = "";
    tableName = BaseMessages.getString(PKG, "DimensionLookupMeta.DefualtTableName");
    databaseMeta = null;
    commitSize = 100;
    update = true;

    nrkeys = 0;
    nrFields = 0;

    allocate(nrkeys, nrFields);

    // Read keys to dimension
    for (int i = 0; i < nrkeys; i++) {
      keyStream[i] = "key" + i;
      keyLookup[i] = "keylookup" + i;
    }

    for (int i = 0; i < nrFields; i++) {
      fieldStream[i] = "field" + i;
      fieldLookup[i] = "lookup" + i;
      fieldUpdate[i] = DimensionLookupMeta.TYPE_UPDATE_DIM_INSERT;
    }

    // Only one date is supported
    // No datefield: use system date...
    dateField = "";
    dateFrom = "date_from";
    dateTo = "date_to";

    minYear = Const.MIN_YEAR;
    maxYear = Const.MAX_YEAR;

    keyField = "";
    keyRename = "";
    autoIncrement = false;
    versionField = "version";

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

    if (Utils.isEmpty(keyField)) {
      String message =
          BaseMessages.getString(PKG, "DimensionLookupMeta.Error.NoTechnicalKeySpecified");

      logError(message);
      throw new HopTransformException(message);
    }

    IValueMeta v = new ValueMetaInteger(keyField);
    if (keyRename != null && keyRename.length() > 0) {
      v.setName(keyRename);
    }

    v.setLength(9);
    v.setPrecision(0);
    v.setOrigin(name);
    row.addValueMeta(v);

    // retrieve extra fields on lookup?
    // Don't bother if there are no return values specified.
    if (!update && fieldLookup.length > 0) {
      Database db = null;
      try {
        // Get the rows from the table...
        if (databaseMeta != null) {
          db = createDatabaseObject(variables);

          IRowMeta extraFields = getDatabaseTableFields(db, schemaName, tableName);

          for (int i = 0; i < fieldLookup.length; i++) {
            v = extraFields.searchValueMeta(fieldLookup[i]);
            if (v == null) {
              String message =
                  BaseMessages.getString(
                      PKG, "DimensionLookupMeta.Exception.UnableToFindReturnField", fieldLookup[i]);
              logError(message);
              throw new HopTransformException(message);
            }

            // If the field needs to be renamed, rename
            if (fieldStream[i] != null && fieldStream[i].length() > 0) {
              v.setName(fieldStream[i]);
            }
            v.setOrigin(name);
            row.addValueMeta(v);
          }
        } else {
          String message =
              BaseMessages.getString(
                  PKG, "DimensionLookupMeta.Exception.UnableToRetrieveDataTypeOfReturnField");
          logError(message);
          throw new HopTransformException(message);
        }
      } catch (Exception e) {
        String message =
            BaseMessages.getString(
                PKG, "DimensionLookupMeta.Exception.UnableToRetrieveDataTypeOfReturnField2");
        logError(message);
        throw new HopTransformException(message, e);
      } finally {
        if (db != null) {
          db.disconnect();
        }
      }
    }
  }

  @Override
  public String getXml() {
    actualizeWithInjectedValues();
    StringBuilder retval = new StringBuilder(512);

    retval.append("      ").append(XmlHandler.addTagValue("schema", schemaName));
    retval.append("      ").append(XmlHandler.addTagValue("table", tableName));
    retval
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                "connection", databaseMeta == null ? "" : databaseMeta.getName()));
    retval.append("      ").append(XmlHandler.addTagValue("commit", commitSize));
    retval.append("      ").append(XmlHandler.addTagValue("update", update));

    retval.append("      <fields>").append(Const.CR);
    for (int i = 0; i < keyStream.length; i++) {
      retval.append("        <key>").append(Const.CR);
      retval.append("          ").append(XmlHandler.addTagValue("name", keyStream[i]));
      retval.append("          ").append(XmlHandler.addTagValue("lookup", keyLookup[i]));
      retval.append("        </key>").append(Const.CR);
    }

    retval.append("        <date>").append(Const.CR);
    retval.append("          ").append(XmlHandler.addTagValue("name", dateField));
    retval.append("          ").append(XmlHandler.addTagValue("from", dateFrom));
    retval.append("          ").append(XmlHandler.addTagValue("to", dateTo));
    retval.append("        </date>").append(Const.CR);

    if (fieldStream != null) {
      for (int i = 0; i < fieldStream.length; i++) {
        retval.append("        <field>").append(Const.CR);
        retval
            .append("          ")
            .append(XmlHandler.addTagValue("name", Const.NVL(fieldStream[i], "")));
        retval
            .append("          ")
            .append(XmlHandler.addTagValue("lookup", Const.NVL(fieldLookup[i], "")));
        retval
            .append("          ")
            .append(XmlHandler.addTagValue("update", getUpdateTypeCode(update, fieldUpdate[i])));
        retval.append("        </field>").append(Const.CR);
      }
    }

    retval.append("        <return>").append(Const.CR);
    retval.append("          ").append(XmlHandler.addTagValue("name", keyField));
    retval.append("          ").append(XmlHandler.addTagValue("rename", keyRename));
    retval.append("          ").append(XmlHandler.addTagValue("creation_method", techKeyCreation));
    retval.append("          ").append(XmlHandler.addTagValue("use_autoinc", autoIncrement));
    retval.append("          ").append(XmlHandler.addTagValue("version", versionField));
    retval.append("        </return>").append(Const.CR);

    retval.append("      </fields>").append(Const.CR);

    // If sequence is empty: use auto-increment field!
    retval.append("      ").append(XmlHandler.addTagValue("sequence", sequenceName));
    retval.append("      ").append(XmlHandler.addTagValue("min_year", minYear));
    retval.append("      ").append(XmlHandler.addTagValue("max_year", maxYear));

    retval.append("      ").append(XmlHandler.addTagValue("cache_size", cacheSize));
    retval.append("      ").append(XmlHandler.addTagValue("preload_cache", preloadingCache));

    retval
        .append("      ")
        .append(XmlHandler.addTagValue("use_start_date_alternative", usingStartDateAlternative));
    retval
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                "start_date_alternative", getStartDateAlternativeCode(startDateAlternative)));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("start_date_field_name", startDateFieldName));
    retval.append("      ").append(XmlHandler.addTagValue("useBatch", useBatchUpdate));

    return retval.toString();
  }

  private void readData(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    this.metadataProvider = metadataProvider;
    try {
      String upd;
      int nrkeys, nrFields;
      String commit;
      this.databases = databases;
      schemaName = XmlHandler.getTagValue(transformNode, "schema");
      tableName = XmlHandler.getTagValue(transformNode, "table");
      String con = XmlHandler.getTagValue(transformNode, "connection");
      databaseMeta = DatabaseMeta.loadDatabase(metadataProvider, con);
      commit = XmlHandler.getTagValue(transformNode, "commit");
      commitSize = Const.toInt(commit, 0);

      upd = XmlHandler.getTagValue(transformNode, "update");
      if (upd.equalsIgnoreCase("Y")) {
        update = true;
      } else {
        update = false;
      }

      Node fields = XmlHandler.getSubNode(transformNode, "fields");

      nrkeys = XmlHandler.countNodes(fields, "key");
      nrFields = XmlHandler.countNodes(fields, "field");

      allocate(nrkeys, nrFields);

      // Read keys to dimension
      for (int i = 0; i < nrkeys; i++) {
        Node knode = XmlHandler.getSubNodeByNr(fields, "key", i);

        keyStream[i] = XmlHandler.getTagValue(knode, "name");
        keyLookup[i] = XmlHandler.getTagValue(knode, "lookup");
      }

      // Only one date is supported
      // No datefield: use system date...
      Node dnode = XmlHandler.getSubNode(fields, "date");
      dateField = XmlHandler.getTagValue(dnode, "name");
      dateFrom = XmlHandler.getTagValue(dnode, "from");
      dateTo = XmlHandler.getTagValue(dnode, "to");

      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

        fieldStream[i] = XmlHandler.getTagValue(fnode, "name");
        fieldLookup[i] = XmlHandler.getTagValue(fnode, "lookup");
        upd = XmlHandler.getTagValue(fnode, "update");
        fieldUpdate[i] = getUpdateType(update, upd);
      }

      if (update) {
        // If this is empty: use auto-increment field!
        sequenceName = XmlHandler.getTagValue(transformNode, "sequence");
      }

      maxYear = Const.toInt(XmlHandler.getTagValue(transformNode, "max_year"), Const.MAX_YEAR);
      minYear = Const.toInt(XmlHandler.getTagValue(transformNode, "min_year"), Const.MIN_YEAR);

      keyField = XmlHandler.getTagValue(fields, "return", "name");
      keyRename = XmlHandler.getTagValue(fields, "return", "rename");
      autoIncrement =
          !"N".equalsIgnoreCase(XmlHandler.getTagValue(fields, "return", "use_autoinc"));
      versionField = XmlHandler.getTagValue(fields, "return", "version");

      setTechKeyCreation(XmlHandler.getTagValue(fields, "return", "creation_method"));

      cacheSize = Const.toInt(XmlHandler.getTagValue(transformNode, "cache_size"), -1);
      preloadingCache =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "preload_cache"));
      useBatchUpdate = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "useBatch"));

      usingStartDateAlternative =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "use_start_date_alternative"));
      startDateAlternative =
          getStartDateAlternative(XmlHandler.getTagValue(transformNode, "start_date_alternative"));
      startDateFieldName = XmlHandler.getTagValue(transformNode, "start_date_field_name");
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(
              PKG, "DimensionLookupMeta.Exception.UnableToLoadTransformMetaFromXML"),
          e);
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
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    if (update) {
      checkUpdate(variables, remarks, transformMeta, prev);
    } else {
      checkLookup(variables, remarks, transformMeta, prev);
    }

    if (techKeyCreation != null) {
      // post 2.2 version
      if (!(CREATION_METHOD_AUTOINC.equals(techKeyCreation)
          || CREATION_METHOD_SEQUENCE.equals(techKeyCreation)
          || CREATION_METHOD_TABLEMAX.equals(techKeyCreation))) {
        String errorMessage =
            BaseMessages.getString(PKG, "DimensionLookupMeta.CheckResult.ErrorTechKeyCreation")
                + ": "
                + techKeyCreation
                + "!";
        CheckResult cr =
            new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      }
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "DimensionLookupMeta.CheckResult.TransformReceiveInfoOK"),
              transformMeta);
      remarks.add(cr);
    } else {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "DimensionLookupMeta.CheckResult.NoInputReceiveFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    }
  }

  private void checkUpdate(
      IVariables variables,
      List<ICheckResult> remarks,
      TransformMeta transforminfo,
      IRowMeta prev) {
    CheckResult cr;
    String errorMessage = "";

    if (databaseMeta != null) {
      Database db = createDatabaseObject(variables);

      try {
        db.connect();
        if (!Utils.isEmpty(tableName)) {
          boolean first = true;
          boolean errorFound = false;
          errorMessage = "";

          IRowMeta r = db.getTableFieldsMeta(schemaName, tableName);
          if (r != null) {
            for (int i = 0; i < fieldLookup.length; i++) {
              String lufield = fieldLookup[i];
              logDebug(
                  BaseMessages.getString(PKG, "DimensionLookupMeta.Log.CheckLookupField")
                      + i
                      + " --> "
                      + lufield
                      + " in lookup table...");
              IValueMeta v = r.searchValueMeta(lufield);
              if (v == null) {
                if (first) {
                  first = false;
                  errorMessage +=
                      BaseMessages.getString(
                              PKG,
                              "DimensionLookupMeta.CheckResult.MissingCompareFieldsInTargetTable")
                          + Const.CR;
                }
                errorFound = true;
                errorMessage += "\t\t" + lufield + Const.CR;
              }
            }
            if (errorFound) {
              cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo);
            } else {
              cr =
                  new CheckResult(
                      ICheckResult.TYPE_RESULT_OK,
                      BaseMessages.getString(
                          PKG, "DimensionLookupMeta.CheckResult.AllLookupFieldFound"),
                      transforminfo);
            }
            remarks.add(cr);

            /* Also, check the fields: tk, version, from-to, ... */
            if (keyField != null && keyField.length() > 0) {
              if (r.indexOfValue(keyField) < 0) {
                errorMessage =
                    BaseMessages.getString(
                            PKG, "DimensionLookupMeta.CheckResult.TechnicalKeyNotFound", keyField)
                        + Const.CR;
                cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo);
              } else {
                errorMessage =
                    BaseMessages.getString(
                            PKG, "DimensionLookupMeta.CheckResult.TechnicalKeyFound", keyField)
                        + Const.CR;
                cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transforminfo);
              }
              remarks.add(cr);
            } else {
              errorMessage =
                  BaseMessages.getString(
                          PKG, "DimensionLookupMeta.CheckResult.TechnicalKeyRequired")
                      + Const.CR;
              remarks.add(
                  new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo));
            }

            if (versionField != null && versionField.length() > 0) {
              if (r.indexOfValue(versionField) < 0) {
                errorMessage =
                    BaseMessages.getString(
                            PKG,
                            "DimensionLookupMeta.CheckResult.VersionFieldNotFound",
                            versionField)
                        + Const.CR;
                cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo);
              } else {
                errorMessage =
                    BaseMessages.getString(
                            PKG, "DimensionLookupMeta.CheckResult.VersionFieldFound", versionField)
                        + Const.CR;
                cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transforminfo);
              }
              remarks.add(cr);
            } else {
              errorMessage =
                  BaseMessages.getString(PKG, "DimensionLookupMeta.CheckResult.VersionKeyRequired")
                      + Const.CR;
              remarks.add(
                  new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo));
            }

            if (dateFrom != null && dateFrom.length() > 0) {
              if (r.indexOfValue(dateFrom) < 0) {
                errorMessage =
                    BaseMessages.getString(
                            PKG,
                            "DimensionLookupMeta.CheckResult.StartPointOfDaterangeNotFound",
                            dateFrom)
                        + Const.CR;
                cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo);
              } else {
                errorMessage =
                    BaseMessages.getString(
                            PKG,
                            "DimensionLookupMeta.CheckResult.StartPointOfDaterangeFound",
                            dateFrom)
                        + Const.CR;
                cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transforminfo);
              }
              remarks.add(cr);
            } else {
              errorMessage =
                  BaseMessages.getString(PKG, "DimensionLookupMeta.CheckResult.StartKeyRequired")
                      + Const.CR;
              remarks.add(
                  new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo));
            }

            if (dateTo != null && dateTo.length() > 0) {
              if (r.indexOfValue(dateTo) < 0) {
                errorMessage =
                    BaseMessages.getString(
                            PKG,
                            "DimensionLookupMeta.CheckResult.EndPointOfDaterangeNotFound",
                            dateTo)
                        + Const.CR;
                cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo);
              } else {
                errorMessage =
                    BaseMessages.getString(
                            PKG, "DimensionLookupMeta.CheckResult.EndPointOfDaterangeFound", dateTo)
                        + Const.CR;
                cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transforminfo);
              }
              remarks.add(cr);
            } else {
              errorMessage =
                  BaseMessages.getString(PKG, "DimensionLookupMeta.CheckResult.EndKeyRequired")
                      + Const.CR;
              remarks.add(
                  new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo));
            }
          } else {
            errorMessage =
                BaseMessages.getString(
                    PKG, "DimensionLookupMeta.CheckResult.CouldNotReadTableInfo");
            cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo);
            remarks.add(cr);
          }
        }

        // Look up fields in the input stream <prev>
        if (prev != null && prev.size() > 0) {
          boolean first = true;
          errorMessage = "";
          boolean errorFound = false;

          for (int i = 0; i < fieldStream.length; i++) {
            logDebug(
                BaseMessages.getString(
                    PKG, "DimensionLookupMeta.Log.CheckField", i + " --> " + fieldStream[i]));
            IValueMeta v = prev.searchValueMeta(fieldStream[i]);
            if (v == null) {
              if (first) {
                first = false;
                errorMessage +=
                    BaseMessages.getString(PKG, "DimensionLookupMeta.CheckResult.MissongFields")
                        + Const.CR;
              }
              errorFound = true;
              errorMessage += "\t\t" + fieldStream[i] + Const.CR;
            }
          }
          if (errorFound) {
            cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo);
          } else {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(PKG, "DimensionLookupMeta.CheckResult.AllFieldsFound"),
                    transforminfo);
          }
          remarks.add(cr);
        } else {
          errorMessage =
              BaseMessages.getString(
                      PKG,
                      "DimensionLookupMeta.CheckResult.CouldNotReadFieldsFromPreviousTransform")
                  + Const.CR;
          cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo);
          remarks.add(cr);
        }

        // Check sequence
        if (databaseMeta.supportsSequences()
            && CREATION_METHOD_SEQUENCE.equals(getTechKeyCreation())
            && sequenceName != null
            && sequenceName.length() != 0) {
          if (db.checkSequenceExists(sequenceName)) {
            errorMessage =
                BaseMessages.getString(
                    PKG, "DimensionLookupMeta.CheckResult.SequenceExists", sequenceName);
            cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transforminfo);
            remarks.add(cr);
          } else {
            errorMessage +=
                BaseMessages.getString(
                    PKG, "DimensionLookupMeta.CheckResult.SequenceCouldNotFound", sequenceName);
            cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo);
            remarks.add(cr);
          }
        }
      } catch (HopException e) {
        errorMessage =
            BaseMessages.getString(PKG, "DimensionLookupMeta.CheckResult.CouldNotConectToDB")
                + e.getMessage();
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo);
        remarks.add(cr);
      }
    } else {
      errorMessage =
          BaseMessages.getString(PKG, "DimensionLookupMeta.CheckResult.InvalidConnectionName");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo);
      remarks.add(cr);
    }
  }

  private void checkLookup(
      IVariables variables,
      List<ICheckResult> remarks,
      TransformMeta transforminfo,
      IRowMeta prev) {
    int i;
    boolean errorFound = false;
    String errorMessage = "";
    boolean first;
    CheckResult cr;

    if (databaseMeta != null) {
      Database db = createDatabaseObject(variables);

      try {
        db.connect();

        if (!Utils.isEmpty(tableName)) {
          IRowMeta tableFields = db.getTableFieldsMeta(schemaName, tableName);
          if (tableFields != null) {
            if (prev != null && prev.size() > 0) {
              // Start at the top, see if the key fields exist:
              first = true;
              boolean warning_found = false;
              for (i = 0; i < keyStream.length; i++) {
                // Does the field exist in the input stream?
                String strfield = keyStream[i];
                IValueMeta strvalue = prev.searchValueMeta(strfield); //
                if (strvalue == null) {
                  if (first) {
                    first = false;
                    errorMessage +=
                        BaseMessages.getString(PKG, "DimensionLookupMeta.CheckResult.KeyhasProblem")
                            + Const.CR;
                  }
                  errorFound = true;
                  errorMessage +=
                      "\t\t"
                          + keyField
                          + BaseMessages.getString(
                              PKG, "DimensionLookupMeta.CheckResult.KeyNotPresentInStream")
                          + Const.CR;
                } else {
                  // does the field exist in the dimension table?
                  String dimfield = keyLookup[i];
                  IValueMeta dimvalue = tableFields.searchValueMeta(dimfield);
                  if (dimvalue == null) {
                    if (first) {
                      first = false;
                      errorMessage +=
                          BaseMessages.getString(
                                  PKG, "DimensionLookupMeta.CheckResult.KeyhasProblem2")
                              + Const.CR;
                    }
                    errorFound = true;
                    errorMessage +=
                        "\t\t"
                            + dimfield
                            + BaseMessages.getString(
                                PKG,
                                "DimensionLookupMeta.CheckResult.KeyNotPresentInDimensiontable")
                            + databaseMeta.getQuotedSchemaTableCombination(
                                variables, schemaName, tableName)
                            + ")"
                            + Const.CR;
                  } else {
                    // Is the streamvalue of the same type as the dimension value?
                    if (strvalue.getType() != dimvalue.getType()) {
                      if (first) {
                        first = false;
                        errorMessage +=
                            BaseMessages.getString(
                                    PKG, "DimensionLookupMeta.CheckResult.KeyhasProblem3")
                                + Const.CR;
                      }
                      warning_found = true;
                      errorMessage +=
                          "\t\t"
                              + strfield
                              + " ("
                              + strvalue.getOrigin()
                              + BaseMessages.getString(
                                  PKG, "DimensionLookupMeta.CheckResult.KeyNotTheSameTypeAs")
                              + dimfield
                              + " ("
                              + databaseMeta.getQuotedSchemaTableCombination(
                                  variables, schemaName, tableName)
                              + ")"
                              + Const.CR;
                      errorMessage +=
                          BaseMessages.getString(
                              PKG, "DimensionLookupMeta.CheckResult.WarningInfoInDBConversion");
                    }
                  }
                }
              }
              if (errorFound) {
                cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo);
              } else if (warning_found) {
                cr = new CheckResult(ICheckResult.TYPE_RESULT_WARNING, errorMessage, transforminfo);
              } else {
                cr =
                    new CheckResult(
                        ICheckResult.TYPE_RESULT_OK,
                        BaseMessages.getString(
                            PKG, "DimensionLookupMeta.CheckResult.AllKeysFieldsFound"),
                        transforminfo);
              }
              remarks.add(cr);

              // In case of lookup, the first column of the UpIns dialog table contains the table
              // field
              errorFound = false;
              for (i = 0; i < fieldLookup.length; i++) {
                String lufield = fieldLookup[i];
                if (lufield != null && lufield.length() > 0) {
                  // Checking compare field: lufield
                  IValueMeta v = tableFields.searchValueMeta(lufield);
                  if (v == null) {
                    if (first) {
                      first = false;
                      errorMessage +=
                          BaseMessages.getString(
                                  PKG,
                                  "DimensionLookupMeta.CheckResult.FieldsToRetrieveNotExistInDimension")
                              + Const.CR;
                    }
                    errorFound = true;
                    errorMessage += "\t\t" + lufield + Const.CR;
                  }
                }
              }
              if (errorFound) {
                cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo);
              } else {
                cr =
                    new CheckResult(
                        ICheckResult.TYPE_RESULT_OK,
                        BaseMessages.getString(
                            PKG, "DimensionLookupMeta.CheckResult.AllFieldsToRetrieveFound"),
                        transforminfo);
              }
              remarks.add(cr);

              /* Also, check the fields: tk, version, from-to, ... */
              if (tableFields.indexOfValue(keyField) < 0) {
                errorMessage =
                    BaseMessages.getString(
                            PKG, "DimensionLookupMeta.CheckResult.TechnicalKeyNotFound", keyField)
                        + Const.CR;
                cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo);
              } else {
                errorMessage =
                    BaseMessages.getString(
                            PKG, "DimensionLookupMeta.CheckResult.TechnicalKeyFound", keyField)
                        + Const.CR;
                cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transforminfo);
              }
              remarks.add(cr);

              if (tableFields.indexOfValue(versionField) < 0) {
                errorMessage =
                    BaseMessages.getString(
                            PKG,
                            "DimensionLookupMeta.CheckResult.VersionFieldNotFound",
                            versionField)
                        + Const.CR;
                cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo);
              } else {
                errorMessage =
                    BaseMessages.getString(
                            PKG, "DimensionLookupMeta.CheckResult.VersionFieldFound", versionField)
                        + Const.CR;
                cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transforminfo);
              }
              remarks.add(cr);

              if (tableFields.indexOfValue(dateFrom) < 0) {
                errorMessage =
                    BaseMessages.getString(
                            PKG,
                            "DimensionLookupMeta.CheckResult.StartOfDaterangeFieldNotFound",
                            dateFrom)
                        + Const.CR;
                cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo);
              } else {
                errorMessage =
                    BaseMessages.getString(
                            PKG,
                            "DimensionLookupMeta.CheckResult.StartOfDaterangeFieldFound",
                            dateFrom)
                        + Const.CR;
                cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transforminfo);
              }
              remarks.add(cr);

              if (tableFields.indexOfValue(dateTo) < 0) {
                errorMessage =
                    BaseMessages.getString(
                            PKG,
                            "DimensionLookupMeta.CheckResult.EndOfDaterangeFieldNotFound",
                            dateTo)
                        + Const.CR;
                cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo);
              } else {
                errorMessage =
                    BaseMessages.getString(
                        PKG, "DimensionLookupMeta.CheckResult.EndOfDaterangeFieldFound", dateTo);
                cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transforminfo);
              }
              remarks.add(cr);
            } else {
              errorMessage =
                  BaseMessages.getString(
                          PKG,
                          "DimensionLookupMeta.CheckResult.CouldNotReadFieldsFromPreviousTransform")
                      + Const.CR;
              cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo);
              remarks.add(cr);
            }
          } else {
            errorMessage =
                BaseMessages.getString(
                    PKG, "DimensionLookupMeta.CheckResult.CouldNotReadTableInfo");
            cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo);
            remarks.add(cr);
          }
        }
      } catch (HopException e) {
        errorMessage =
            BaseMessages.getString(PKG, "DimensionLookupMeta.CheckResult.CouldNotConnectDB")
                + e.getMessage();
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo);
        remarks.add(cr);
      }
    } else {
      errorMessage =
          BaseMessages.getString(PKG, "DimensionLookupMeta.CheckResult.InvalidConnection");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo);
      remarks.add(cr);
    }
  }

  @Override
  public IRowMeta getTableFields(IVariables variables) {
    IRowMeta fields = null;
    if (databaseMeta != null) {
      Database db = createDatabaseObject(variables);
      try {
        db.connect();
        fields = db.getTableFieldsMeta(schemaName, tableName);
      } catch (HopDatabaseException dbe) {
        logError(
            BaseMessages.getString(PKG, "DimensionLookupMeta.Log.DatabaseErrorOccurred")
                + dbe.getMessage());
      } finally {
        db.disconnect();
      }
    }
    return fields;
  }

  @Override
  public SqlStatement getSqlStatements(
      IVariables variables,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      IHopMetadataProvider metadataProvider) {
    SqlStatement retval =
        new SqlStatement(transformMeta.getName(), databaseMeta, null); // default: nothing to do!

    if (update) { // Only bother in case of update, not lookup!
      logDebug(BaseMessages.getString(PKG, "DimensionLookupMeta.Log.Update"));
      if (databaseMeta != null) {
        if (prev != null && prev.size() > 0) {
          String schemaTable =
              databaseMeta.getQuotedSchemaTableCombination(variables, schemaName, tableName);
          if (!Utils.isEmpty(schemaTable)) {
            Database db = createDatabaseObject(variables);
            try {
              db.connect();

              String sql = "";

              // How does the table look like?
              //
              IRowMeta fields = new RowMeta();

              // First the technical key
              //
              IValueMeta vkeyfield = new ValueMetaInteger(keyField);
              vkeyfield.setLength(10);
              fields.addValueMeta(vkeyfield);

              // The the version
              //
              IValueMeta vversion = new ValueMetaInteger(versionField);
              vversion.setLength(5);
              fields.addValueMeta(vversion);

              // The date from
              //
              IValueMeta vdatefrom = new ValueMetaDate(dateFrom);
              fields.addValueMeta(vdatefrom);

              // The date to
              //
              IValueMeta vdateto = new ValueMetaDate(dateTo);
              fields.addValueMeta(vdateto);

              String errors = "";

              // Then the keys
              //
              for (int i = 0; i < keyLookup.length; i++) {
                IValueMeta vprev = prev.searchValueMeta(keyStream[i]);
                if (vprev != null) {
                  IValueMeta field = vprev.clone();
                  field.setName(keyLookup[i]);
                  fields.addValueMeta(field);
                } else {
                  if (errors.length() > 0) {
                    errors += ", ";
                  }
                  errors += keyStream[i];
                }
              }

              //
              // Then the fields to update...
              //
              for (int i = 0; i < fieldLookup.length; i++) {
                IValueMeta vprev = prev.searchValueMeta(fieldStream[i]);
                if (vprev != null) {
                  IValueMeta field = vprev.clone();
                  field.setName(fieldLookup[i]);
                  fields.addValueMeta(field);
                } else {
                  if (errors.length() > 0) {
                    errors += ", ";
                  }
                  errors += fieldStream[i];
                }
              }

              // Finally, the special update fields...
              //
              for (int i = 0; i < fieldUpdate.length; i++) {
                IValueMeta valueMeta = null;
                switch (fieldUpdate[i]) {
                  case TYPE_UPDATE_DATE_INSUP:
                  case TYPE_UPDATE_DATE_INSERTED:
                  case TYPE_UPDATE_DATE_UPDATED:
                    valueMeta = new ValueMetaDate(fieldLookup[i]);
                    break;
                  case TYPE_UPDATE_LAST_VERSION:
                    valueMeta = new ValueMetaBoolean(fieldLookup[i]);
                    break;
                  default:
                    break;
                }
                if (valueMeta != null) {
                  fields.addValueMeta(valueMeta);
                }
              }

              if (errors.length() > 0) {
                retval.setError(
                    BaseMessages.getString(
                            PKG, "DimensionLookupMeta.ReturnValue.UnableToFindFields")
                        + errors);
              }

              logDebug(
                  BaseMessages.getString(PKG, "DimensionLookupMeta.Log.GetDDLForTable")
                      + schemaTable
                      + "] : "
                      + fields.toStringMeta());

              sql +=
                  db.getDDL(
                      schemaTable,
                      fields,
                      (sequenceName != null && sequenceName.length() != 0) ? null : keyField,
                      autoIncrement,
                      null,
                      true);

              logDebug("sql =" + sql);

              String[] idxFields = null;

              // Key lookup dimensions...
              if (!Utils.isEmpty(keyLookup)) {
                idxFields = new String[keyLookup.length];
                for (int i = 0; i < keyLookup.length; i++) {
                  idxFields[i] = keyLookup[i];
                }
              } else {
                retval.setError(
                    BaseMessages.getString(
                        PKG, "DimensionLookupMeta.ReturnValue.NoKeyFieldsSpecified"));
              }

              if (!Utils.isEmpty(idxFields) && !db.checkIndexExists(schemaTable, idxFields)) {
                String indexname = "idx_" + tableName + "_lookup";
                sql +=
                    db.getCreateIndexStatement(
                        schemaTable, indexname, idxFields, false, false, false, true);
              }

              // (Bitmap) index on technical key
              idxFields = new String[] {keyField};
              if (!Utils.isEmpty(keyField)) {
                if (!db.checkIndexExists(schemaTable, idxFields)) {
                  String indexname = "idx_" + tableName + "_tk";
                  sql +=
                      db.getCreateIndexStatement(
                          schemaTable, indexname, idxFields, true, false, true, true);
                }
              } else {
                retval.setError(
                    BaseMessages.getString(
                        PKG, "DimensionLookupMeta.ReturnValue.TechnicalKeyFieldRequired"));
              }

              // The optional Oracle sequence
              if (CREATION_METHOD_SEQUENCE.equals(getTechKeyCreation())
                  && !Utils.isEmpty(sequenceName)) {
                if (!db.checkSequenceExists(schemaName, sequenceName)) {
                  sql += db.getCreateSequenceStatement(schemaName, sequenceName, 1L, 1L, -1L, true);
                }
              }

              if (sql.length() == 0) {
                retval.setSql(null);
              } else {
                retval.setSql(variables.resolve(sql));
              }
            } catch (HopDatabaseException dbe) {
              retval.setError(
                  BaseMessages.getString(PKG, "DimensionLookupMeta.ReturnValue.ErrorOccurred")
                      + dbe.getMessage());
            } finally {
              db.disconnect();
            }
          } else {
            retval.setError(
                BaseMessages.getString(
                    PKG, "DimensionLookupMeta.ReturnValue.NoTableDefinedOnConnection"));
          }
        } else {
          retval.setError(
              BaseMessages.getString(PKG, "DimensionLookupMeta.ReturnValue.NotReceivingAnyFields"));
        }
      } else {
        retval.setError(
            BaseMessages.getString(
                PKG, "DimensionLookupMeta.ReturnValue.NoConnectionDefiendInTransform"));
      }
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
      IHopMetadataProvider metadataProvider) {
    if (prev != null) {
      if (!update) {
        // Lookup: we do a lookup on the natural keys + the return fields!
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
                  v == null ? "" : "Type = " + v.toStringMeta());
          impact.add(ii);
        }

        // Return fields...
        for (int i = 0; i < fieldLookup.length; i++) {
          IValueMeta v = prev.searchValueMeta(fieldStream[i]);

          DatabaseImpact ii =
              new DatabaseImpact(
                  DatabaseImpact.TYPE_IMPACT_READ,
                  pipelineMeta.getName(),
                  transformMeta.getName(),
                  databaseMeta.getDatabaseName(),
                  tableName,
                  fieldLookup[i],
                  fieldLookup[i],
                  v == null ? "" : v != null ? v.getOrigin() : "?",
                  "",
                  v == null ? "" : "Type = " + v.toStringMeta());
          impact.add(ii);
        }
      } else {
        // Update: insert/update on all specified fields...
        // Lookup: we do a lookup on the natural keys + the return fields!
        for (int i = 0; i < keyLookup.length; i++) {
          IValueMeta v = prev.searchValueMeta(keyStream[i]);

          DatabaseImpact ii =
              new DatabaseImpact(
                  DatabaseImpact.TYPE_IMPACT_READ_WRITE,
                  pipelineMeta.getName(),
                  transformMeta.getName(),
                  databaseMeta.getDatabaseName(),
                  tableName,
                  keyLookup[i],
                  keyStream[i],
                  v == null ? "" : v.getOrigin(),
                  "",
                  v == null ? "" : "Type = " + v.toStringMeta());
          impact.add(ii);
        }

        // Return fields...
        for (int i = 0; i < fieldLookup.length; i++) {
          IValueMeta v = prev.searchValueMeta(fieldStream[i]);

          DatabaseImpact ii =
              new DatabaseImpact(
                  DatabaseImpact.TYPE_IMPACT_READ_WRITE,
                  pipelineMeta.getName(),
                  transformMeta.getName(),
                  databaseMeta.getDatabaseName(),
                  tableName,
                  fieldLookup[i],
                  fieldLookup[i],
                  v == null ? "" : v.getOrigin(),
                  "",
                  v == null ? "" : "Type = " + v.toStringMeta());
          impact.add(ii);
        }
      }
    }
  }

  @Override
  public DimensionLookup createTransform(
      TransformMeta transformMeta,
      DimensionLookupData data,
      int cnr,
      PipelineMeta tr,
      Pipeline pipeline) {
    return new DimensionLookup(transformMeta, this, data, cnr, tr, pipeline);
  }

  @Override
  public DimensionLookupData getTransformData() {
    return new DimensionLookupData();
  }

  @Override
  public DatabaseMeta[] getUsedDatabaseConnections() {
    if (databaseMeta != null) {
      return new DatabaseMeta[] {databaseMeta};
    } else {
      return super.getUsedDatabaseConnections();
    }
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

  /** @return the cacheSize */
  public int getCacheSize() {
    return cacheSize;
  }

  /** @param cacheSize the cacheSize to set */
  public void setCacheSize(int cacheSize) {
    this.cacheSize = cacheSize;
  }

  /** @return the usingStartDateAlternative */
  public boolean isUsingStartDateAlternative() {
    return usingStartDateAlternative;
  }

  /** @param usingStartDateAlternative the usingStartDateAlternative to set */
  public void setUsingStartDateAlternative(boolean usingStartDateAlternative) {
    this.usingStartDateAlternative = usingStartDateAlternative;
  }

  /** @return the startDateAlternative */
  public int getStartDateAlternative() {
    return startDateAlternative;
  }

  /** @param startDateAlternative the startDateAlternative to set */
  public void setStartDateAlternative(int startDateAlternative) {
    this.startDateAlternative = startDateAlternative;
  }

  /** @return the startDateFieldName */
  public String getStartDateFieldName() {
    return startDateFieldName;
  }

  /** @param startDateFieldName the startDateFieldName to set */
  public void setStartDateFieldName(String startDateFieldName) {
    this.startDateFieldName = startDateFieldName;
  }

  /** @return the preloadingCache */
  public boolean isPreloadingCache() {
    return preloadingCache;
  }

  /** @param preloadingCache the preloadingCache to set */
  public void setPreloadingCache(boolean preloadingCache) {
    this.preloadingCache = preloadingCache;
  }

  /** @return the useBatchUpdate */
  public boolean useBatchUpdate() {
    return useBatchUpdate;
  }

  /** @param useBatchUpdate the useBatchUpdate to set */
  public void setUseBatchUpdate(boolean useBatchUpdate) {
    this.useBatchUpdate = useBatchUpdate;
  }

  protected IRowMeta getDatabaseTableFields(Database db, String schemaName, String tableName)
      throws HopDatabaseException {
    // First try without connecting to the database... (can be S L O W)
    IRowMeta extraFields = db.getTableFieldsMeta(schemaName, tableName);
    if (extraFields == null) { // now we need to connect
      db.connect();
      extraFields = db.getTableFieldsMeta(schemaName, tableName);
    }
    return extraFields;
  }

  Database createDatabaseObject(IVariables variables) {
    return new Database(loggingObject, variables, databaseMeta );
  }

  @Override
  public RowMeta getRowMeta( IVariables variables, final ITransformData transformData ) {
    try {
      return (RowMeta) getDatabaseTableFields(createDatabaseObject(variables), schemaName, tableName);
    } catch (HopDatabaseException e) {
      log.logError("", e);
      return new RowMeta();
    }
  }

  @Override
  public List<String> getDatabaseFields() {
    ArrayList<String> fields = new ArrayList<>(fieldLookup.length + keyLookup.length);
    fields.addAll(Arrays.asList(fieldLookup));
    fields.addAll(Arrays.asList(keyLookup));
    return fields;
  }

  @Override
  public List<String> getStreamFields() {
    ArrayList<String> fields = new ArrayList<>(fieldLookup.length + keyLookup.length);
    fields.addAll(Arrays.asList(fieldStream));
    fields.addAll(Arrays.asList(keyStream));
    return fields;
  }

  public void normalizeAllocationFields() {
    if (keyLookup != null) {
      int keysGroupSize = keyLookup.length;
      keyStream = normalizeAllocation(keyStream, keysGroupSize);
    }
    if (fieldLookup != null) {
      int fieldsGroupSize = fieldLookup.length;
      fieldStream = normalizeAllocation(fieldStream, fieldsGroupSize);
      fieldUpdate = normalizeAllocation(fieldUpdate, fieldsGroupSize);
      returnType = normalizeAllocation(returnType, fieldsGroupSize);
    }
  }

  private String[] normalizeAllocation(String[] oldAllocation, int length) {
    String[] newAllocation = null;
    if (oldAllocation.length < length) {
      newAllocation = new String[length];
      for (int i = 0; i < oldAllocation.length; i++) {
        newAllocation[i] = oldAllocation[i];
      }
    } else {
      newAllocation = oldAllocation;
    }
    return newAllocation;
  }

  private int[] normalizeAllocation(int[] oldAllocation, int length) {
    int[] newAllocation = null;
    if (oldAllocation.length < length) {
      newAllocation = new int[length];
      for (int i = 0; i < oldAllocation.length; i++) {
        newAllocation[i] = oldAllocation[i];
      }
    } else {
      newAllocation = oldAllocation;
    }
    return newAllocation;
  }

  public static class StartDateCodeConverter extends InjectionTypeConverter {

    @Override
    public int string2intPrimitive(String v) throws HopValueException {
      return DimensionLookupMeta.getStartDateAlternative(v);
    }
  }

  public static class UpdateTypeCodeConverter extends InjectionTypeConverter {

    @Override
    public int string2intPrimitive(String v) throws HopValueException {
      return DimensionLookupMeta.getUpdateType(true, v);
    }
  }

  public static class ReturnTypeCodeConverter extends InjectionTypeConverter {
    @Override
    public int string2intPrimitive(String v) throws HopValueException {
      return DimensionLookupMeta.getUpdateType(false, v);
    }
  }

  /**
   * If we use injection we can have different arrays lengths. We need synchronize them for
   * consistency behavior with UI
   */
  @AfterInjection
  public void afterInjectionSynchronization() {
    int nrStream = (keyStream == null) ? -1 : keyStream.length;
    if (nrStream > 0) {
      String[][] rtn = Utils.normalizeArrays(nrStream, keyLookup);
      keyLookup = rtn[0];
    }
    int nrFields = (fieldStream == null) ? -1 : fieldStream.length;
    if (nrFields <= 0) {
      return;
    }
    String[][] rtnFieldLookup = Utils.normalizeArrays(nrFields, fieldLookup);
    fieldLookup = rtnFieldLookup[0];

    int[][] rtnFieldUpdate = Utils.normalizeArrays(nrFields, fieldUpdate);
    fieldUpdate = rtnFieldUpdate[0];
  }
}
