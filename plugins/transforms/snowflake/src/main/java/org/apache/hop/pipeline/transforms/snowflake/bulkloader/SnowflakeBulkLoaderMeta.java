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

package org.apache.hop.pipeline.transforms.snowflake.bulkloader;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "SnowflakeBulkLoader",
    image = "snowflakebulkloader.svg",
    name = "i18n::SnowflakeBulkLoader.Name",
    description = "i18n::SnowflakeBulkLoader.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Bulk",
    documentationUrl = "/pipeline/transforms/snowflakebulkloader.html",
    keywords = "i18n::SnowflakeBulkLoader.Keyword",
    classLoaderGroup = "snowflake",
    isIncludeJdbcDrivers = true,
    actionTransformTypes = {ActionTransformType.RDBMS, ActionTransformType.OUTPUT})
public class SnowflakeBulkLoaderMeta
    extends BaseTransformMeta<SnowflakeBulkLoader, SnowflakeBulkLoaderData> {

  private static final Class<?> PKG =
      SnowflakeBulkLoaderMeta.class; // for i18n purposes, needed by Translator2!!

  protected static final String DEBUG_MODE_VAR = "${SNOWFLAKE_DEBUG_MODE}";

  /*
   * Static constants used for the bulk loader when creating temp files.
   */
  public static final String CSV_DELIMITER = ",";
  public static final String CSV_RECORD_DELIMITER = "\n";
  public static final String CSV_ESCAPE_CHAR = "\\";
  public static final String ENCLOSURE = "\"";
  public static final String DATE_FORMAT_STRING = "yyyy-MM-dd";
  public static final String TIMESTAMP_FORMAT_STRING = "YYYY-MM-DD HH24:MI:SS.FF3";

  /** The valid location type codes */
  public static final String[] LOCATION_TYPE_CODES = {"user", "table", "internal_stage"};

  public static final int LOCATION_TYPE_USER = 0;
  public static final int LOCATION_TYPE_TABLE = 1;
  public static final int LOCATION_TYPE_INTERNAL_STAGE = 2;

  /** The valid on error codes */
  public static final String[] ON_ERROR_CODES = {
    "continue", "skip_file", "skip_file_percent", "abort"
  };

  public static final int ON_ERROR_CONTINUE = 0;
  public static final int ON_ERROR_SKIP_FILE = 1;
  public static final int ON_ERROR_SKIP_FILE_PERCENT = 2;
  public static final int ON_ERROR_ABORT = 3;

  /** The valid data type codes */
  public static final String[] DATA_TYPE_CODES = {"csv", "json"};

  public static final int DATA_TYPE_CSV = 0;
  public static final int DATA_TYPE_JSON = 1;

  /** The date appended to the filenames */
  private String fileDate;

  /** The database connection to use */
  @HopMetadataProperty(
      key = "connection",
      injectionKeyDescription = "",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String connection;

  /** The schema to use */
  @HopMetadataProperty(
      key = "target_schema",
      injectionKeyDescription = "",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_SCHEMA)
  private String targetSchema;

  /** The table to load */
  @HopMetadataProperty(
      key = "target_table",
      injectionKeyDescription = "",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
  private String targetTable;

  /** The location type (user, table, internal_stage) */
  @HopMetadataProperty(key = "location_type", injectionKeyDescription = "")
  private String locationType;

  /** If location type = Internal stage, the stage name to use */
  @HopMetadataProperty(key = "stage_name", injectionKeyDescription = "")
  private String stageName;

  /** The work directory to use when writing temp files */
  @HopMetadataProperty(key = "work_directory", injectionKeyDescription = "")
  private String workDirectory;

  /** What to do when an error is encountered (continue, skip_file, skip_file_percent, abort) */
  @HopMetadataProperty(key = "on_error", injectionKeyDescription = "")
  private String onError;

  /**
   * When On Error = Skip File, the number of error rows before skipping the file, if 0 skip
   * immediately. When On Error = Skip File Percent, the percentage of the file to error before
   * skipping the file.
   */
  @HopMetadataProperty(key = "error_limit", injectionKeyDescription = "")
  private String errorLimit;

  /** The size to split the data at to enable faster bulk loading */
  @HopMetadataProperty(key = "split_size", injectionKeyDescription = "")
  private String splitSize;

  /** Should the files loaded to the staging location be removed */
  @HopMetadataProperty(key = "remove_files", injectionKeyDescription = "")
  private boolean removeFiles;

  /** The target transform for bulk loader output */
  @HopMetadataProperty(key = "output_target_transform", injectionKeyDescription = "")
  private String outputTargetTransform;

  /** The data type of the data (csv, json) */
  @HopMetadataProperty(key = "data_type", injectionKeyDescription = "")
  private String dataType;

  /** CSV: Trim whitespace */
  @HopMetadataProperty(key = "trim_whitespace", injectionKeyDescription = "")
  private boolean trimWhitespace;

  /** CSV: Convert column value to null if */
  @HopMetadataProperty(key = "null_if", injectionKeyDescription = "")
  private String nullIf;

  /**
   * CSV: Should the load fail if the column count in the row does not match the column count in the
   * table
   */
  @HopMetadataProperty(key = "error_column_mismatch", injectionKeyDescription = "")
  private boolean errorColumnMismatch;

  /** JSON: Strip nulls from JSON */
  @HopMetadataProperty(key = "strip_null", injectionKeyDescription = "")
  private boolean stripNull;

  /** JSON: Ignore UTF8 Errors */
  @HopMetadataProperty(key = "ignore_utf8", injectionKeyDescription = "")
  private boolean ignoreUtf8;

  /** JSON: Allow duplicate elements */
  @HopMetadataProperty(key = "allow_duplicate_elements", injectionKeyDescription = "")
  private boolean allowDuplicateElements;

  /** JSON: Enable Octal number parsing */
  @HopMetadataProperty(key = "enable_octal", injectionKeyDescription = "")
  private boolean enableOctal;

  /** CSV: Specify field to table mapping */
  @HopMetadataProperty(key = "specify_fields", injectionKeyDescription = "")
  private boolean specifyFields;

  /** JSON: JSON field name */
  @HopMetadataProperty(key = "JSON_FIELD", injectionKeyDescription = "")
  private String jsonField;

  /** CSV: The field mapping from the Stream to the database */
  @HopMetadataProperty(
      groupKey = "fields",
      key = "field",
      injectionKey = "FIELD",
      injectionGroupKey = "FIELDS",
      injectionKeyDescription = "",
      injectionGroupDescription = "")
  private List<SnowflakeBulkLoaderField> snowflakeBulkLoaderFields;

  /** Default initializer */
  public SnowflakeBulkLoaderMeta() {
    super();
    snowflakeBulkLoaderFields = new ArrayList<>();
  }

  /**
   * @return The metadata of the database connection to use when bulk loading
   */
  public String getConnection() {
    return connection;
  }

  /**
   * Set the database connection to use
   *
   * @param connection The database connection name
   */
  public void setConnection(String connection) {
    this.connection = connection;
  }

  /**
   * @return The schema to load
   */
  public String getTargetSchema() {
    return targetSchema;
  }

  /**
   * Set the schema to load
   *
   * @param targetSchema The schema name
   */
  public void setTargetSchema(String targetSchema) {
    this.targetSchema = targetSchema;
  }

  /**
   * @return The table name to load
   */
  public String getTargetTable() {
    return targetTable;
  }

  /**
   * Set the table name to load
   *
   * @param targetTable The table name
   */
  public void setTargetTable(String targetTable) {
    this.targetTable = targetTable;
  }

  /**
   * @return The location type code for the files to load
   */
  public String getLocationType() {
    return locationType;
  }

  /**
   * Set the location type code to use
   *
   * @param locationType The location type code from @LOCATION_TYPE_CODES
   * @throws HopException Invalid location type
   */
  public void setLocationType(String locationType) throws HopException {
    for (String LOCATION_TYPE_CODE : LOCATION_TYPE_CODES) {
      if (LOCATION_TYPE_CODE.equals(locationType)) {
        this.locationType = locationType;
        return;
      }
    }

    // No matching location type, the location type is invalid
    throw new HopException("Invalid location type " + locationType);
  }

  /**
   * @return The ID of the location type
   */
  public int getLocationTypeId() {
    for (int i = 0; i < LOCATION_TYPE_CODES.length; i++) {
      if (LOCATION_TYPE_CODES[i].equals(locationType)) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Takes an ID for the location type and sets the location type
   *
   * @param locationTypeId The location type id
   */
  public void setLocationTypeById(int locationTypeId) {
    locationType = LOCATION_TYPE_CODES[locationTypeId];
  }

  /**
   * Ignored unless the location_type is internal_stage
   *
   * @return The name of the Snowflake stage
   */
  public String getStageName() {
    return stageName;
  }

  /**
   * Ignored unless the location_type is internal_stage, sets the name of the Snowflake stage
   *
   * @param stageName The name of the Snowflake stage
   */
  public void setStageName(String stageName) {
    this.stageName = stageName;
  }

  /**
   * @return The local work directory to store temporary files
   */
  public String getWorkDirectory() {
    return workDirectory;
  }

  /**
   * Set the local word directory to store temporary files. The directory must exist.
   *
   * @param workDirectory The path to the work directory
   */
  public void setWorkDirectory(String workDirectory) {
    this.workDirectory = workDirectory;
  }

  /**
   * @return The code from @ON_ERROR_CODES to use when an error occurs during the load
   */
  public String getOnError() {
    return onError;
  }

  /**
   * Set the behavior for what to do when an error occurs during the load
   *
   * @param onError The error code from @ON_ERROR_CODES
   * @throws HopException
   */
  public void setOnError(String onError) throws HopException {
    for (String ON_ERROR_CODE : ON_ERROR_CODES) {
      if (ON_ERROR_CODE.equals(onError)) {
        this.onError = onError;
        return;
      }
    }

    // No matching on error codes, we have a problem
    throw new HopException("Invalid on error code " + onError);
  }

  /**
   * Gets the ID for the onError method being used
   *
   * @return The ID for the onError method being used
   */
  public int getOnErrorId() {
    for (int i = 0; i < ON_ERROR_CODES.length; i++) {
      if (ON_ERROR_CODES[i].equals(onError)) {
        return i;
      }
    }
    return -1;
  }

  /**
   * @param onErrorId The ID of the error method
   */
  public void setOnErrorById(int onErrorId) {
    onError = ON_ERROR_CODES[onErrorId];
  }

  /**
   * Ignored if onError is not skip_file or skip_file_percent
   *
   * @return The limit at which to fail
   */
  public String getErrorLimit() {
    return errorLimit;
  }

  /**
   * Ignored if onError is not skip_file or skip_file_percent, the limit at which Snowflake should
   * skip loading the file
   *
   * @param errorLimit The limit at which Snowflake should skip loading the file. 0 = no limit
   */
  public void setErrorLimit(String errorLimit) {
    this.errorLimit = errorLimit;
  }

  /**
   * @return The number of rows at which the files should be split
   */
  public String getSplitSize() {
    return splitSize;
  }

  /**
   * Set the number of rows at which to split files
   *
   * @param splitSize The size to split at in number of rows
   */
  public void setSplitSize(String splitSize) {
    this.splitSize = splitSize;
  }

  /**
   * @return Should the files be removed from the Snowflake internal storage after they are loaded
   */
  public boolean isRemoveFiles() {
    return removeFiles;
  }

  /**
   * Set if the files should be removed from the Snowflake internal storage after they are loaded
   *
   * @param removeFiles true/false
   */
  public void setRemoveFiles(boolean removeFiles) {
    this.removeFiles = removeFiles;
  }

  /**
   * @return The transform to direct the output data to.
   */
  public String getOutputTargetTransform() {
    return outputTargetTransform;
  }

  /**
   * Set the transform to direct bulk loader output to.
   *
   * @param outputTargetTransform The transform name
   */
  public void setOutputTargetTransform(String outputTargetTransform) {
    this.outputTargetTransform = outputTargetTransform;
  }

  /**
   * @return The data type code being loaded from @DATA_TYPE_CODES
   */
  public String getDataType() {
    return dataType;
  }

  /**
   * Set the data type
   *
   * @param dataType The data type code from @DATA_TYPE_CODES
   * @throws HopException Invalid value
   */
  public void setDataType(String dataType) throws HopException {
    for (String DATA_TYPE_CODE : DATA_TYPE_CODES) {
      if (DATA_TYPE_CODE.equals(dataType)) {
        this.dataType = dataType;
        return;
      }
    }

    // No matching data type
    throw new HopException("Invalid data type " + dataType);
  }

  /**
   * Gets the data type ID, which is equivalent to the location of the data type code within the
   * (DATA_TYPE_CODES) array
   *
   * @return The ID of the data type
   */
  public int getDataTypeId() {
    for (int i = 0; i < DATA_TYPE_CODES.length; i++) {
      if (DATA_TYPE_CODES[i].equals(dataType)) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Takes the ID of the data type and sets the data type code to the equivalent location within the
   * DATA_TYPE_CODES array
   *
   * @param dataTypeId The ID of the data type
   */
  public void setDataTypeById(int dataTypeId) {
    dataType = DATA_TYPE_CODES[dataTypeId];
  }

  /**
   * CSV:
   *
   * @return Should whitespace in the fields be trimmed
   */
  public boolean isTrimWhitespace() {
    return trimWhitespace;
  }

  /**
   * CSV: Set if the whitespace in the files should be trimmmed
   *
   * @param trimWhitespace true/false
   */
  public void setTrimWhitespace(boolean trimWhitespace) {
    this.trimWhitespace = trimWhitespace;
  }

  /**
   * CSV:
   *
   * @return Comma delimited list of strings to convert to Null
   */
  public String getNullIf() {
    return nullIf;
  }

  /**
   * CSV: Set the string constants to convert to Null
   *
   * @param nullIf Comma delimited list of constants
   */
  public void setNullIf(String nullIf) {
    this.nullIf = nullIf;
  }

  /**
   * CSV:
   *
   * @return Should the load error if the number of columns in the table and in the CSV do not match
   */
  public boolean isErrorColumnMismatch() {
    return errorColumnMismatch;
  }

  /**
   * CSV: Set if the load should error if the number of columns in the table and in the CSV do not
   * match
   *
   * @param errorColumnMismatch true/false
   */
  public void setErrorColumnMismatch(boolean errorColumnMismatch) {
    this.errorColumnMismatch = errorColumnMismatch;
  }

  /**
   * JSON:
   *
   * @return Should null values be stripped out of the JSON
   */
  public boolean isStripNull() {
    return stripNull;
  }

  /**
   * JSON: Set if null values should be stripped out of the JSON
   *
   * @param stripNull true/false
   */
  public void setStripNull(boolean stripNull) {
    this.stripNull = stripNull;
  }

  /**
   * JSON:
   *
   * @return Should UTF8 errors be ignored
   */
  public boolean isIgnoreUtf8() {
    return ignoreUtf8;
  }

  /**
   * JSON: Set if UTF8 errors should be ignored
   *
   * @param ignoreUtf8 true/false
   */
  public void setIgnoreUtf8(boolean ignoreUtf8) {
    this.ignoreUtf8 = ignoreUtf8;
  }

  /**
   * JSON:
   *
   * @return Should duplicate element names in the JSON be allowed. If true the last value for the
   *     name is used.
   */
  public boolean isAllowDuplicateElements() {
    return allowDuplicateElements;
  }

  /**
   * JSON: Set if duplicate element names in the JSON be allowed. If true the last value for the
   * name is used.
   *
   * @param allowDuplicateElements true/false
   */
  public void setAllowDuplicateElements(boolean allowDuplicateElements) {
    this.allowDuplicateElements = allowDuplicateElements;
  }

  /**
   * JSON: Should processing of octal based numbers be enabled?
   *
   * @return Is octal number parsing enabled?
   */
  public boolean isEnableOctal() {
    return enableOctal;
  }

  /**
   * JSON: Set if processing of octal based numbers should be enabled
   *
   * @param enableOctal true/false
   */
  public void setEnableOctal(boolean enableOctal) {
    this.enableOctal = enableOctal;
  }

  /**
   * CSV: Is the mapping of stream fields to table fields being specified?
   *
   * @return Are fields being specified?
   */
  public boolean isSpecifyFields() {
    return specifyFields;
  }

  /**
   * CSV: Set if the mapping of stream fields to table fields is being specified
   *
   * @param specifyFields true/false
   */
  public void setSpecifyFields(boolean specifyFields) {
    this.specifyFields = specifyFields;
  }

  /**
   * JSON: The stream field containing the JSON string.
   *
   * @return The stream field containing the JSON
   */
  public String getJsonField() {
    return jsonField;
  }

  /**
   * JSON: Set the input stream field containing the JSON string.
   *
   * @param jsonField The stream field containing the JSON
   */
  public void setJsonField(String jsonField) {
    this.jsonField = jsonField;
  }

  /**
   * CSV: Get the array containing the Stream to Table field mapping
   *
   * @return The array containing the stream to table field mapping
   */
  public List<SnowflakeBulkLoaderField> getSnowflakeBulkLoaderFields() {
    return snowflakeBulkLoaderFields;
  }

  /**
   * CSV: Set the array containing the Stream to Table field mapping
   *
   * @param snowflakeBulkLoaderFields The array containing the stream to table field mapping
   */
  public void setSnowflakeBulkLoaderFields(
      List<SnowflakeBulkLoaderField> snowflakeBulkLoaderFields) {
    this.snowflakeBulkLoaderFields = snowflakeBulkLoaderFields;
  }

  /**
   * Get the file date that is appended in the file names
   *
   * @return The file date that is appended in the file names
   */
  public String getFileDate() {
    return fileDate;
  }

  /**
   * Clones the transform so that it can be copied and used in clusters
   *
   * @return A copy of the transform
   */
  @Override
  public Object clone() {
    return super.clone();
  }

  /** Sets the default values for all metadata attributes. */
  @Override
  public void setDefault() {
    locationType = LOCATION_TYPE_CODES[LOCATION_TYPE_USER];
    workDirectory = "${java.io.tmpdir}";
    onError = ON_ERROR_CODES[ON_ERROR_ABORT];
    removeFiles = true;

    dataType = DATA_TYPE_CODES[DATA_TYPE_CSV];
    trimWhitespace = false;
    errorColumnMismatch = true;
    stripNull = false;
    ignoreUtf8 = false;
    allowDuplicateElements = false;
    enableOctal = false;
    splitSize = "20000";

    specifyFields = false;
  }

  /**
   * Builds a filename for a temporary file The filename is in
   * tableName_date_time_transformnr_partnr_splitnr.gz format
   *
   * @param variables The variables currently set
   * @param transformNumber The transform number. Used when multiple copies of the transform are
   *     started.
   * @param partNumber The partition number. Used when the pipeline is executed clustered, the
   *     number of the partition.
   * @param splitNumber The split number. Used when the file is split into multiple chunks.
   * @return The filename to use
   */
  public String buildFilename(
      IVariables variables, int transformNumber, String partNumber, int splitNumber) {
    SimpleDateFormat daf = new SimpleDateFormat();

    // Replace possible environment variables...
    String realWorkDirectory = variables.resolve(workDirectory);

    // Files are always gzipped
    String extension = ".gz";

    StringBuilder returnValue = new StringBuilder(realWorkDirectory);
    if (!realWorkDirectory.endsWith("/") && !realWorkDirectory.endsWith("\\")) {
      returnValue.append(Const.FILE_SEPARATOR);
    }

    returnValue.append(targetTable).append("_");

    if (fileDate == null) {

      Date now = new Date();

      daf.applyPattern("yyyyMMdd_HHmmss");
      fileDate = daf.format(now);
    }
    returnValue.append(fileDate).append("_");

    returnValue.append(transformNumber).append("_");
    returnValue.append(partNumber).append("_");
    returnValue.append(splitNumber);
    returnValue.append(extension);

    return returnValue.toString();
  }

  /**
   * Check the transform to make sure it is valid. This is what is run when the user presses the
   * check pipeline button in Hop
   *
   * @param remarks The list of remarks to add to
   * @param pipelineMeta The pipeline metadata
   * @param transformMeta The transform metadata
   * @param prev The metadata about the input stream
   * @param input The input fields
   * @param output The output fields
   * @param info The metadata about the info stream
   * @param variables The variable space
   * @param metadataProvider The metastore
   */
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

    // Check output fields
    if (prev != null && prev.size() > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "SnowflakeBulkLoadMeta.CheckResult.FieldsReceived", "" + prev.size()),
              transformMeta);
      remarks.add(cr);

      String errorMessage = "";
      boolean errorFound = false;

      // Starting from selected fields in ...
      for (SnowflakeBulkLoaderField snowflakeBulkLoaderField : snowflakeBulkLoaderFields) {
        int idx = prev.indexOfValue(snowflakeBulkLoaderField.getStreamField());
        if (idx < 0) {
          errorMessage += "\t\t" + snowflakeBulkLoaderField.getStreamField() + Const.CR;
          errorFound = true;
        }
      }
      if (errorFound) {
        errorMessage =
            BaseMessages.getString(
                PKG, "SnowflakeBulkLoadMeta.CheckResult.FieldsNotFound", errorMessage);
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "SnowflakeBulkLoadMeta.CheckResult.AllFieldsFound"),
                transformMeta);
        remarks.add(cr);
      }
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SnowflakeBulkLoadMeta.CheckResult.ExpectedInputOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SnowflakeBulkLoadMeta.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    }

    for (SnowflakeBulkLoaderField snowflakeBulkLoaderField : snowflakeBulkLoaderFields) {
      try {
        snowflakeBulkLoaderField.validate();
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG,
                    "SnowflakeBulkLoadMeta.CheckResult.MappingValid",
                    snowflakeBulkLoaderField.getStreamField()),
                transformMeta);
        remarks.add(cr);
      } catch (HopException ex) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG,
                    "SnowflakeBulkLoadMeta.CheckResult.MappingNotValid",
                    snowflakeBulkLoaderField.getStreamField()),
                transformMeta);
        remarks.add(cr);
      }
    }
  }

  /**
   * Gets a list of fields in the database table
   *
   * @param variables The variable space
   * @return The metadata about the fields in the table.
   * @throws HopException
   */
  @Override
  public IRowMeta getRequiredFields(IVariables variables) throws HopException {
    String realTableName = variables.resolve(targetTable);
    String realSchemaName = variables.resolve(targetSchema);

    if (connection != null) {
      DatabaseMeta databaseMeta =
          getParentTransformMeta().getParentPipelineMeta().findDatabase(connection, variables);

      Database db = new Database(loggingObject, variables, databaseMeta);
      try {
        db.connect();

        if (!StringUtils.isEmpty(realTableName)) {
          String schemaTable =
              databaseMeta.getQuotedSchemaTableCombination(
                  variables, realSchemaName, realTableName);

          // Check if this table exists...
          if (db.checkTableExists(realSchemaName, realTableName)) {
            return db.getTableFields(schemaTable);
          } else {
            throw new HopException(
                BaseMessages.getString(PKG, "SnowflakeBulkLoaderMeta.Exception.TableNotFound"));
          }
        } else {
          throw new HopException(
              BaseMessages.getString(PKG, "SnowflakeBulkLoaderMeta.Exception.TableNotSpecified"));
        }
      } catch (Exception e) {
        throw new HopException(
            BaseMessages.getString(PKG, "SnowflakeBulkLoaderMeta.Exception.ErrorGettingFields"), e);
      } finally {
        db.disconnect();
      }
    } else {
      throw new HopException(
          BaseMessages.getString(PKG, "SnowflakeBulkLoaderMeta.Exception.ConnectionNotDefined"));
    }
  }

  /**
   * Gets the transform data
   *
   * @return The transform data
   */
  public ITransformData getTransformData() {
    return new SnowflakeBulkLoaderData();
  }

  /**
   * Gets the Snowflake stage name based on the configured metadata
   *
   * @param variables The variable space
   * @return The Snowflake stage name to use
   */
  public String getStage(IVariables variables) {
    if (locationType.equals(LOCATION_TYPE_CODES[LOCATION_TYPE_USER])) {
      return "@~/" + variables.resolve(targetTable);
    } else if (locationType.equals(LOCATION_TYPE_CODES[LOCATION_TYPE_TABLE])) {
      if (!StringUtils.isEmpty(variables.resolve(targetSchema))) {
        return "@" + variables.resolve(targetSchema) + ".%" + variables.resolve(targetTable);
      } else {
        return "@%" + variables.resolve(targetTable);
      }
    } else if (locationType.equals(LOCATION_TYPE_CODES[LOCATION_TYPE_INTERNAL_STAGE])) {
      if (!StringUtils.isEmpty(variables.resolve(targetSchema))) {
        return "@" + variables.resolve(targetSchema) + "." + variables.resolve(stageName);
      } else {
        return "@" + variables.resolve(stageName);
      }
    }
    return null;
  }

  /**
   * Creates the copy statement used to load data into Snowflake
   *
   * @param variables The variable space
   * @param filenames A list of filenames to load
   * @return The copy statement to load data into Snowflake
   * @throws HopFileException
   */
  public String getCopyStatement(IVariables variables, List<String> filenames)
      throws HopFileException {
    StringBuilder returnValue = new StringBuilder();
    returnValue.append("COPY INTO ");

    // Schema
    if (!StringUtils.isEmpty(variables.resolve(targetSchema))) {
      returnValue.append(variables.resolve(targetSchema)).append(".");
    }

    // Table
    returnValue.append(variables.resolve(targetTable)).append(" ");

    // Location
    returnValue.append("FROM ").append(getStage(variables)).append("/ ");
    returnValue.append("FILES = (");
    boolean first = true;
    for (String filename : filenames) {
      String shortFile = HopVfs.getFileObject(filename, variables).getName().getBaseName();
      if (first) {
        returnValue.append("'");
        first = false;
      } else {
        returnValue.append(",'");
      }
      returnValue.append(shortFile).append("' ");
    }
    returnValue.append(") ");

    // FILE FORMAT
    returnValue.append("FILE_FORMAT = ( TYPE = ");

    // CSV
    if (dataType.equals(DATA_TYPE_CODES[DATA_TYPE_CSV])) {
      returnValue.append("'CSV' FIELD_DELIMITER = ',' RECORD_DELIMITER = '\\n' ESCAPE = '\\\\' ");
      returnValue.append("ESCAPE_UNENCLOSED_FIELD = '\\\\' FIELD_OPTIONALLY_ENCLOSED_BY='\"' ");
      returnValue.append("SKIP_HEADER = 0 DATE_FORMAT = '").append(DATE_FORMAT_STRING).append("' ");
      returnValue.append("TIMESTAMP_FORMAT = '").append(TIMESTAMP_FORMAT_STRING).append("' ");
      returnValue.append("TRIM_SPACE = ").append(trimWhitespace).append(" ");
      if (!StringUtils.isEmpty(nullIf)) {
        returnValue.append("NULL_IF = (");
        String[] nullIfStrings = variables.resolve(nullIf).split(",");
        boolean firstNullIf = true;
        for (String nullIfString : nullIfStrings) {
          nullIfString = nullIfString.replaceAll("'", "''");
          if (firstNullIf) {
            firstNullIf = false;
            returnValue.append("'");
          } else {
            returnValue.append(", '");
          }
          returnValue.append(nullIfString).append("'");
        }
        returnValue.append(" ) ");
      }
      returnValue
          .append("ERROR_ON_COLUMN_COUNT_MISMATCH = ")
          .append(errorColumnMismatch)
          .append(" ");
      returnValue.append("COMPRESSION = 'GZIP' ");

    } else if (dataType.equals(DATA_TYPE_CODES[DATA_TYPE_JSON])) {
      returnValue.append("'JSON' COMPRESSION = 'GZIP' STRIP_OUTER_ARRAY = FALSE ");
      returnValue.append("ENABLE_OCTAL = ").append(enableOctal).append(" ");
      returnValue.append("ALLOW_DUPLICATE = ").append(allowDuplicateElements).append(" ");
      returnValue.append("STRIP_NULL_VALUES = ").append(stripNull).append(" ");
      returnValue.append("IGNORE_UTF8_ERRORS = ").append(ignoreUtf8).append(" ");
    }
    returnValue.append(") ");

    returnValue.append("ON_ERROR = ");
    if (onError.equals(ON_ERROR_CODES[ON_ERROR_ABORT])) {
      returnValue.append("'ABORT_STATEMENT' ");
    } else if (onError.equals(ON_ERROR_CODES[ON_ERROR_CONTINUE])) {
      returnValue.append("'CONTINUE' ");
    } else if (onError.equals(ON_ERROR_CODES[ON_ERROR_SKIP_FILE])
        || onError.equals(ON_ERROR_CODES[ON_ERROR_SKIP_FILE_PERCENT])) {
      if (Const.toDouble(variables.resolve(errorLimit), 0) <= 0) {
        returnValue.append("'SKIP_FILE' ");
      } else {
        returnValue.append("'SKIP_FILE_").append(Const.toInt(variables.resolve(errorLimit), 0));
      }
      if (onError.equals(ON_ERROR_CODES[ON_ERROR_SKIP_FILE_PERCENT])) {
        returnValue.append("%' ");
      } else {
        returnValue.append("' ");
      }
    }

    if (!Boolean.getBoolean(variables.resolve(DEBUG_MODE_VAR))) {
      returnValue.append("PURGE = ").append(removeFiles).append(" ");
    }

    returnValue.append(";");

    return returnValue.toString();
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

    SqlStatement retval =
        new SqlStatement(transformMeta.getName(), databaseMeta, null); // default: nothing to do!

    if (databaseMeta != null) {
      if (prev != null && prev.size() > 0) {

        if (!Utils.isEmpty(targetTable)) {
          Database db = new Database(loggingObject, variables, databaseMeta);
          try {
            db.connect();

            String schemaTable =
                databaseMeta.getQuotedSchemaTableCombination(variables, targetSchema, targetTable);
            String crTable = db.getDDL(schemaTable, prev, null, false, null);

            // Empty string means: nothing to do: set it to null...
            if (crTable == null || crTable.length() == 0) {
              crTable = null;
            }

            retval.setSql(crTable);
          } catch (HopDatabaseException dbe) {
            retval.setError(
                BaseMessages.getString(
                    PKG, "TableOutputMeta.Error.ErrorConnecting", dbe.getMessage()));
          } finally {
            db.disconnect();
          }
        } else {
          retval.setError(BaseMessages.getString(PKG, "TableOutputMeta.Error.NoTable"));
        }
      } else {
        retval.setError(
            BaseMessages.getString(PKG, "SnowflakeBulkLoaderMeta.GetSQL.NotReceivingAnyFields"));
      }
    } else {
      retval.setError(
          BaseMessages.getString(PKG, "SnowflakeBulkLoaderMeta.GetSQL.NoConnectionDefined"));
    }

    return retval;
  }
}
