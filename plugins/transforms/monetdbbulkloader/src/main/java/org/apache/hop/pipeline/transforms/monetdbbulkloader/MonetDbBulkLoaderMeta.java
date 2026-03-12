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
package org.apache.hop.pipeline.transforms.monetdbbulkloader;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
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
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.databases.monetdb.MonetDBDatabaseMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "MonetDBBulkLoader",
    image = "monetdbbulkloader.svg",
    name = "i18n::MonetDBBulkLoaderDialog.TypeLongDesc.MonetDBBulkLoader",
    description = "i18n::MonetDBBulkLoaderDialog.TypeTooltipDesc.MonetDBBulkLoader",
    documentationUrl = "/pipeline/transforms/monetdbbulkloader.html",
    keywords = "i18n::MonetDbBulkLoaderMeta.keyword",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Bulk",
    isIncludeJdbcDrivers = true,
    classLoaderGroup = "monetdb",
    actionTransformTypes = {ActionTransformType.RDBMS, ActionTransformType.OUTPUT})
@Getter
@Setter
public class MonetDbBulkLoaderMeta
    extends BaseTransformMeta<MonetDbBulkLoader, MonetDbBulkLoaderData> {
  private static final Class<?> PKG =
      MonetDbBulkLoaderMeta.class; // for i18n purposes, needed by Translator2!!
  public static final String CONST_SPACES = "        ";

  /** Inline class representing a single field mapping from stream to table column. */
  @Getter
  @Setter
  public static class MonetDbField {
    @HopMetadataProperty(key = "stream_name", injectionKey = "TARGETFIELDS")
    private String fieldTable;

    @HopMetadataProperty(key = "field_name", injectionKey = "SOURCEFIELDS")
    private String fieldStream;

    @HopMetadataProperty(key = "field_format_ok", injectionKey = "FIELDFORMATOK")
    private boolean fieldFormatOk;

    public MonetDbField() {}

    public MonetDbField(String fieldTable, String fieldStream, boolean fieldFormatOk) {
      this.fieldTable = fieldTable;
      this.fieldStream = fieldStream;
      this.fieldFormatOk = fieldFormatOk;
    }
  }

  /** The database connection name * */
  @HopMetadataProperty(
      key = "connection",
      injectionKey = "CONNECTIONNAME",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String dbConnectionName;

  /** what's the schema for the target? */
  @HopMetadataProperty(key = "schema", injectionKey = "SCHEMANAME")
  private String schemaName;

  /** what's the table for the target? */
  @HopMetadataProperty(key = "table", injectionKey = "SCHEMANAME")
  private String tableName;

  /** Path to the log file */
  @HopMetadataProperty(key = "log_file", injectionKey = "LOGFILE")
  private String logFile;

  @HopMetadataProperty(
      key = "mapping",
      inlineListTags = {"stream_name", "field_name", "field_format_ok"})
  private List<MonetDbField> fields;

  /** Field separator character or string used to delimit fields */
  @HopMetadataProperty(key = "field_separator", injectionKey = "SEPARATOR")
  private String fieldSeparator;

  /**
   * Specifies which character surrounds each field's data. i.e. double quotes, single quotes or
   * something else
   */
  @HopMetadataProperty(key = "field_enclosure", injectionKey = "FIELDENCLOSURE")
  private String fieldEnclosure;

  /**
   * How are NULLs represented as text to the MonetDB API or mclient i.e. can be an empty string or
   * something else the value is written out as text to the API and MonetDB is able to interpret it
   * to the correct representation of NULL in the database for the given column type.
   */
  @HopMetadataProperty(key = "null_representation", injectionKey = "NULLVALUE")
  private String nullRepresentation;

  /** Encoding to use */
  @HopMetadataProperty(key = "encoding", injectionKey = "ENCODING")
  private String encoding;

  /** Truncate table? */
  @HopMetadataProperty(key = "truncate", injectionKey = "TRUNCATE")
  private boolean truncate = false;

  /** Fully Quote SQL used in the transform? */
  @HopMetadataProperty(key = "fully_quote_sql", injectionKey = "QUOTEFIELDS")
  private boolean fullyQuoteSQL;

  /** Auto adjust the table structure? */
  private boolean autoSchema = false;

  /** Auto adjust strings that are too long? */
  private boolean autoStringWidths = false;

  /**
   * The number of rows to buffer before passing them over to MonetDB. This number should be
   * non-zero since we need to specify the number of rows we pass.
   */
  @HopMetadataProperty(key = "buffer_size")
  private String bufferSize;

  /**
   * The indicator defines that it is used the version of <i>MonetBD Jan2014-SP2</i> or later if it
   * is <code>true</code>. <code>False</code> indicates about using all MonetDb versions before this
   * one.
   */
  private boolean compatibilityDbVersionMode = false;

  @Override
  public void setDefault() {
    fields = new ArrayList<>();
    schemaName = "";
    tableName = BaseMessages.getString(PKG, "MonetDBBulkLoaderMeta.DefaultTableName");
    bufferSize = "100000";
    logFile = "";
    truncate = false;
    fullyQuoteSQL = true;
    fieldSeparator = "|";
    fieldEnclosure = "\"";
    nullRepresentation = "";
    encoding = "UTF-8";
  }

  @Override
  public void getFields(
      IRowMeta r,
      String name,
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
    StringBuilder erroMessage = new StringBuilder();

    DatabaseMeta databaseMeta = null;

    try {
      databaseMeta =
          metadataProvider
              .getSerializer(DatabaseMeta.class)
              .load(variables.resolve(dbConnectionName));
    } catch (HopException e) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG,
                  "TableInputMeta.CheckResult.DatabaseMetaError",
                  variables.resolve(dbConnectionName)),
              transformMeta);
      remarks.add(cr);
    }

    if (databaseMeta != null) {
      Database db = new Database(loggingObject, variables, databaseMeta);
      try {
        db.connect();

        if (!Utils.isEmpty(tableName)) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(PKG, "MonetDBBulkLoaderMeta.CheckResult.TableNameOK"),
                  transformMeta);
          remarks.add(cr);

          boolean first = true;
          boolean errorFound = false;

          // Check fields in table
          String schemaTable =
              databaseMeta.getQuotedSchemaTableCombination(variables, schemaName, tableName);
          IRowMeta r = db.getTableFields(schemaTable);
          if (r != null) {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(PKG, "MonetDBBulkLoaderMeta.CheckResult.TableExists"),
                    transformMeta);
            remarks.add(cr);

            // How about the fields to insert/dateMask in the table?
            first = true;
            errorFound = false;

            for (MonetDbField field : fields) {
              IValueMeta v = r.searchValueMeta(field.fieldTable);
              if (v == null) {
                if (first) {
                  first = false;
                  erroMessage
                      .append(
                          BaseMessages.getString(
                              PKG,
                              "MonetDBBulkLoaderMeta.CheckResult.MissingFieldsToLoadInTargetTable"))
                      .append(Const.CR);
                }
                errorFound = true;
                erroMessage.append("\t\t").append(field).append(Const.CR);
              }
            }
            if (errorFound) {
              cr =
                  new CheckResult(
                      ICheckResult.TYPE_RESULT_ERROR, erroMessage.toString(), transformMeta);
            } else {
              cr =
                  new CheckResult(
                      ICheckResult.TYPE_RESULT_OK,
                      BaseMessages.getString(
                          PKG, "MonetDBBulkLoaderMeta.CheckResult.AllFieldsFoundInTargetTable"),
                      transformMeta);
            }
          } else {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_ERROR,
                    BaseMessages.getString(
                        PKG, "MonetDBBulkLoaderMeta.CheckResult.CouldNotReadTableInfo"),
                    transformMeta);
          }
          remarks.add(cr);
        }

        // Look up fields in the input stream <prev>
        if (prev != null && !prev.isEmpty()) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(
                      PKG,
                      "MonetDBBulkLoaderMeta.CheckResult.TransformReceivingDatas",
                      prev.size() + ""),
                  transformMeta);
          remarks.add(cr);

          boolean first = true;
          erroMessage.setLength(0);
          boolean errorFound = false;

          for (MonetDbField s : fields) {
            IValueMeta v = prev.searchValueMeta(s.fieldStream);
            if (v == null) {
              if (first) {
                first = false;
                erroMessage
                    .append(
                        BaseMessages.getString(
                            PKG, "MonetDBBulkLoaderMeta.CheckResult.MissingFieldsInInput"))
                    .append(Const.CR);
              }
              errorFound = true;
              erroMessage.append("\t\t").append(s).append(Const.CR);
            }
          }
          if (errorFound) {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_ERROR, erroMessage.toString(), transformMeta);
          } else {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(
                        PKG, "MonetDBBulkLoaderMeta.CheckResult.AllFieldsFoundInInput"),
                    transformMeta);
          }
          remarks.add(cr);
        } else {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(
                          PKG, "MonetDBBulkLoaderMeta.CheckResult.MissingFieldsInInput3")
                      + Const.CR,
                  transformMeta);
          remarks.add(cr);
        }
      } catch (HopException e) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                        PKG, "MonetDBBulkLoaderMeta.CheckResult.DatabaseErrorOccurred")
                    + e.getMessage(),
                transformMeta);
        remarks.add(cr);
      } finally {
        db.disconnect();
      }
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "MonetDBBulkLoaderMeta.CheckResult.InvalidConnection"),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "MonetDBBulkLoaderMeta.CheckResult.TransformReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "MonetDBBulkLoaderMeta.CheckResult.NoInputError"),
              transformMeta);
      remarks.add(cr);
    }
  }

  public SqlStatement getTableDdl(
      IVariables variables,
      PipelineMeta pipelineMeta,
      String transformName,
      boolean autoSchema,
      MonetDbBulkLoaderData data,
      boolean safeMode)
      throws HopException {

    TransformMeta transformMeta =
        new TransformMeta(
            BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.transformMeta.Title"),
            transformName,
            this);
    IRowMeta prev = pipelineMeta.getPrevTransformFields(variables, transformName);

    return getSqlStatements(variables, transformMeta, prev, autoSchema, data, safeMode);
  }

  public IRowMeta updateFields(
      IVariables variables,
      PipelineMeta pipelineMeta,
      String transformName,
      MonetDbBulkLoaderData data)
      throws HopTransformException {

    IRowMeta prev = pipelineMeta.getPrevTransformFields(variables, transformName);
    return updateFields(prev, data);
  }

  public IRowMeta updateFields(IRowMeta prev, MonetDbBulkLoaderData data) {
    // update the field table from the fields coming from the previous transforms
    IRowMeta tableFields = new RowMeta();
    List<IValueMeta> prevFields = prev.getValueMetaList();
    int idx = 0;
    for (IValueMeta field : prevFields) {
      IValueMeta tableField = field.clone();
      tableFields.addValueMeta(tableField);
      fields.add(new MonetDbField(field.getName(), field.getName(), true));
      idx++;
    }

    data.keynrs = new int[fields.size()];
    for (int i = 0; i < data.keynrs.length; i++) {
      data.keynrs[i] = i;
    }
    return tableFields;
  }

  public SqlStatement getSqlStatements(
      IVariables variables,
      TransformMeta transformMeta,
      IRowMeta prev,
      boolean autoSchema,
      MonetDbBulkLoaderData data,
      boolean safeMode) {

    DatabaseMeta databaseMeta =
        getParentTransformMeta().getParentPipelineMeta().findDatabase(dbConnectionName, variables);
    SqlStatement retval =
        new SqlStatement(transformMeta.getName(), databaseMeta, null); // default: nothing to do!

    if (databaseMeta != null) {
      if (prev != null && !prev.isEmpty()) {
        // Copy the row
        IRowMeta tableFields;

        if (autoSchema) {
          tableFields = updateFields(prev, data);
        } else {
          tableFields = new RowMeta();
          // Now change the field names
          for (int i = 0; i < fields.size(); i++) {
            IValueMeta v = prev.searchValueMeta(fields.get(i).fieldStream);
            if (v != null) {
              IValueMeta tableField = v.clone();
              tableField.setName(fields.get(i).fieldTable);
              tableFields.addValueMeta(tableField);
            }
          }
        }

        if (!Utils.isEmpty(tableName)) {
          Database db = new Database(loggingObject, variables, databaseMeta);
          try {
            db.connect();

            String schemaTable =
                databaseMeta.getQuotedSchemaTableCombination(variables, schemaName, tableName);
            MonetDBDatabaseMeta.safeModeLocal.set(safeMode);
            String createTable = db.getDDL(schemaTable, tableFields, null, false, null, true);

            String sql = createTable;
            if (sql.isEmpty()) {
              retval.setSql(null);
            } else {
              retval.setSql(sql);
            }
          } catch (HopException e) {
            retval.setError(
                BaseMessages.getString(PKG, "MonetDBBulkLoaderMeta.GetSQL.ErrorOccurred")
                    + e.getMessage());
          } finally {
            db.disconnect();
            MonetDBDatabaseMeta.safeModeLocal.remove();
          }
        } else {
          retval.setError(
              BaseMessages.getString(
                  PKG, "MonetDBBulkLoaderMeta.GetSQL.NoTableDefinedOnConnection"));
        }
      } else {
        retval.setError(
            BaseMessages.getString(PKG, "MonetDBBulkLoaderMeta.GetSQL.NotReceivingAnyFields"));
      }
    } else {
      retval.setError(
          BaseMessages.getString(PKG, "MonetDBBulkLoaderMeta.GetSQL.NoConnectionDefined"));
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
    DatabaseMeta databaseMeta =
        getParentTransformMeta().getParentPipelineMeta().findDatabase(dbConnectionName, variables);

    if (prev != null) {
      /* DEBUG CHECK THIS */
      // Insert dateMask fields : read/write
      for (MonetDbField field : fields) {
        IValueMeta v = prev.searchValueMeta(field.fieldStream);

        DatabaseImpact ii =
            new DatabaseImpact(
                DatabaseImpact.TYPE_IMPACT_READ_WRITE,
                pipelineMeta.getName(),
                transformMeta.getName(),
                databaseMeta.getDatabaseName(),
                variables.resolve(tableName),
                field.fieldTable,
                field.fieldStream,
                v != null ? v.getOrigin() : "?",
                "",
                "Type = " + v.toStringMeta());
        impact.add(ii);
      }
    }
  }

  @Override
  public IRowMeta getRequiredFields(IVariables variables) throws HopException {
    String realTableName = variables.resolve(tableName);
    String realSchemaName = variables.resolve(schemaName);

    DatabaseMeta databaseMeta =
        getParentTransformMeta().getParentPipelineMeta().findDatabase(dbConnectionName, variables);

    if (databaseMeta != null) {
      Database db = new Database(loggingObject, variables, databaseMeta);
      try {
        db.connect();

        if (!Utils.isEmpty(realTableName)) {
          String schemaTable =
              databaseMeta.getQuotedSchemaTableCombination(
                  variables, realSchemaName, realTableName);

          // Check if this table exists...
          if (db.checkTableExists(realSchemaName, realTableName)) {
            return db.getTableFields(schemaTable);
          } else {
            throw new HopException(
                BaseMessages.getString(PKG, "MonetDBBulkLoaderMeta.Exception.TableNotFound"));
          }
        } else {
          throw new HopException(
              BaseMessages.getString(PKG, "MonetDBBulkLoaderMeta.Exception.TableNotSpecified"));
        }
      } catch (Exception e) {
        throw new HopException(
            BaseMessages.getString(PKG, "MonetDBBulkLoaderMeta.Exception.ErrorGettingFields"), e);
      } finally {
        db.disconnect();
      }
    } else {
      throw new HopException(
          BaseMessages.getString(PKG, "MonetDBBulkLoaderMeta.Exception.ConnectionNotDefined"));
    }
  }

  /**
   * Returns the version of MonetDB that is used.
   *
   * @return The version of MonetDB
   * @throws HopException if an error occurs
   */
  private MonetDbVersion getMonetDBVersion(IVariables variables) throws HopException {
    Database db;

    DatabaseMeta databaseMeta =
        getParentTransformMeta().getParentPipelineMeta().findDatabase(dbConnectionName, variables);

    db = new Database(loggingObject, variables, databaseMeta);
    try {
      db.connect();
      return new MonetDbVersion(db.getDatabaseMetaData().getDatabaseProductVersion());
    } catch (Exception e) {
      throw new HopException(e);
    } finally {
      if (db != null) {
        db.disconnect();
      }
    }
  }

  /**
   * Returns <code>true</code> if used the version of MonetBD Jan2014-SP2 or later, <code>false
   * </code> otherwise.
   *
   * @return the compatibilityDbVersionMode
   */
  public boolean isCompatibilityDbVersionMode() {
    return compatibilityDbVersionMode;
  }

  /**
   * Defines and sets <code>true</code> if it is used the version of <i>MonetBD Jan2014-SP2</i> or
   * later, <code>false</code> otherwise. Sets also <code>false</code> if it's impossible to define
   * which version of db is used.
   */
  public void setCompatibilityDbVersionMode(IVariables variables) {

    MonetDbVersion monetDBVersion;
    try {
      monetDBVersion = getMonetDBVersion(variables);
      this.compatibilityDbVersionMode =
          monetDBVersion.compareTo(MonetDbVersion.JAN_2014_SP2_DB_VERSION) >= 0;
      if (isDebug() && this.compatibilityDbVersionMode) {
        logDebug(
            BaseMessages.getString(
                PKG,
                "MonetDBVersion.Info.UsingCompatibilityMode",
                MonetDbVersion.JAN_2014_SP2_DB_VERSION));
      }
    } catch (HopException e) {
      if (isDebug()) {
        logDebug(
            BaseMessages.getString(
                PKG, "MonetDBBulkLoaderMeta.Exception.ErrorOnGettingDbVersion", e.getMessage()));
      }
    }
  }
}
