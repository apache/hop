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

import java.util.List;
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
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.databases.monetdb.MonetDBDatabaseMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

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
@InjectionSupported(localizationPrefix = "MonetDBBulkLoaderDialog.Injection.")
public class MonetDbBulkLoaderMeta
    extends BaseTransformMeta<MonetDbBulkLoader, MonetDbBulkLoaderData> {
  private static final Class<?> PKG =
      MonetDbBulkLoaderMeta.class; // for i18n purposes, needed by Translator2!!
  public static final String CONST_SPACES = "        ";

  /** The database connection name * */
  @Injection(name = "CONNECTIONNAME")
  private String dbConnectionName;

  /** what's the schema for the target? */
  @Injection(name = "SCHEMANAME")
  private String schemaName;

  /** what's the table for the target? */
  @Injection(name = "TABLENAME")
  private String tableName;

  /** Path to the log file */
  @Injection(name = "LOGFILE")
  private String logFile;

  /** database connection */
  private DatabaseMeta databaseMeta;

  /** Field name of the target table */
  @Injection(name = "TARGETFIELDS")
  private String[] fieldTable;

  /** Field name in the stream */
  @Injection(name = "SOURCEFIELDS")
  private String[] fieldStream;

  /** flag to indicate that the format is OK for MonetDB */
  @Injection(name = "FIELDFORMATOK")
  private boolean[] fieldFormatOk;

  /** Field separator character or string used to delimit fields */
  @Injection(name = "SEPARATOR")
  private String fieldSeparator;

  /**
   * Specifies which character surrounds each field's data. i.e. double quotes, single quotes or
   * something else
   */
  @Injection(name = "FIELDENCLOSURE")
  private String fieldEnclosure;

  /**
   * How are NULLs represented as text to the MonetDB API or mclient i.e. can be an empty string or
   * something else the value is written out as text to the API and MonetDB is able to interpret it
   * to the correct representation of NULL in the database for the given column type.
   */
  @Injection(name = "NULLVALUE")
  private String nullRepresentation;

  /** Encoding to use */
  @Injection(name = "ENCODING")
  private String encoding;

  /** Truncate table? */
  @Injection(name = "TRUNCATE")
  private boolean truncate = false;

  /** Fully Quote SQL used in the transform? */
  @Injection(name = "QUOTEFIELDS")
  private boolean fullyQuoteSQL;

  /** Auto adjust the table structure? */
  private boolean autoSchema = false;

  /** Auto adjust strings that are too long? */
  private boolean autoStringWidths = false;

  public boolean isAutoStringWidths() {
    return autoStringWidths;
  }

  public boolean isTruncate() {
    return truncate;
  }

  public void setTruncate(boolean truncate) {
    this.truncate = truncate;
  }

  public boolean isFullyQuoteSQL() {
    return fullyQuoteSQL;
  }

  public void setFullyQuoteSQL(boolean fullyQuoteSQLbool) {
    this.fullyQuoteSQL = fullyQuoteSQLbool;
  }

  public boolean isAutoSchema() {
    return autoSchema;
  }

  public void setAutoSchema(boolean autoSchema) {
    this.autoSchema = autoSchema;
  }

  /**
   * The number of rows to buffer before passing them over to MonetDB. This number should be
   * non-zero since we need to specify the number of rows we pass.
   */
  private String bufferSize;

  /**
   * The indicator defines that it is used the version of <i>MonetBD Jan2014-SP2</i> or later if it
   * is <code>true</code>. <code>False</code> indicates about using all MonetDb versions before this
   * one.
   */
  private boolean compatibilityDbVersionMode = false;

  /**
   * @return Returns the database.
   */
  public DatabaseMeta getDatabaseMeta() {
    return databaseMeta;
  }

  /**
   * @return Returns the database.
   */
  public DatabaseMeta getDatabaseMeta(MonetDbBulkLoader loader) {
    return databaseMeta;
  }

  /**
   * @param database The database to set.
   */
  public void setDatabaseMeta(DatabaseMeta database) {
    this.databaseMeta = database;
  }

  /**
   * @return Returns the tableName.
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName The tableName to set.
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * @return Returns the fieldTable.
   */
  public String[] getFieldTable() {
    return fieldTable;
  }

  /**
   * @param fieldTable The fieldTable to set.
   */
  public void setFieldTable(String[] fieldTable) {
    this.fieldTable = fieldTable;
  }

  /**
   * @return Returns the fieldStream.
   */
  public String[] getFieldStream() {
    return fieldStream;
  }

  /**
   * @param fieldStream The fieldStream to set.
   */
  public void setFieldStream(String[] fieldStream) {
    this.fieldStream = fieldStream;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode, metadataProvider);
  }

  public void allocate(int nrvalues) {
    fieldTable = new String[nrvalues];
    fieldStream = new String[nrvalues];
    fieldFormatOk = new boolean[nrvalues];
  }

  @Override
  public Object clone() {
    MonetDbBulkLoaderMeta retval = (MonetDbBulkLoaderMeta) super.clone();
    int nrvalues = fieldTable.length;

    retval.allocate(nrvalues);

    System.arraycopy(fieldTable, 0, retval.fieldTable, 0, nrvalues);
    System.arraycopy(fieldStream, 0, retval.fieldStream, 0, nrvalues);
    System.arraycopy(fieldFormatOk, 0, retval.fieldFormatOk, 0, nrvalues);
    return retval;
  }

  private void readData(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      dbConnectionName = XmlHandler.getTagValue(transformNode, "connection");
      databaseMeta = DatabaseMeta.loadDatabase(metadataProvider, dbConnectionName);

      schemaName = XmlHandler.getTagValue(transformNode, "schema");
      tableName = XmlHandler.getTagValue(transformNode, "table");
      bufferSize = XmlHandler.getTagValue(transformNode, "buffer_size");
      logFile = XmlHandler.getTagValue(transformNode, "log_file");
      truncate = "Y".equals(XmlHandler.getTagValue(transformNode, "truncate"));

      // New in January 2013 Updates - For compatibility we set default values according to the old
      // version of the transform.
      //

      // This expression will only be true if a yes answer was previously recorded.
      fullyQuoteSQL = "Y".equals(XmlHandler.getTagValue(transformNode, "fully_quote_sql"));

      fieldSeparator = XmlHandler.getTagValue(transformNode, "field_separator");
      if (fieldSeparator == null) {
        fieldSeparator = "|";
      }
      fieldEnclosure = XmlHandler.getTagValue(transformNode, "field_enclosure");
      if (fieldEnclosure == null) {
        fieldEnclosure = "\"";
      }
      nullRepresentation = XmlHandler.getTagValue(transformNode, "null_representation");
      if (nullRepresentation == null) {
        nullRepresentation = "null";
      }
      encoding = XmlHandler.getTagValue(transformNode, "encoding");
      if (encoding == null) {
        encoding = "UTF-8";
      }

      int nrvalues = XmlHandler.countNodes(transformNode, "mapping");
      allocate(nrvalues);

      for (int i = 0; i < nrvalues; i++) {
        Node vnode = XmlHandler.getSubNodeByNr(transformNode, "mapping", i);

        fieldTable[i] = XmlHandler.getTagValue(vnode, "stream_name");
        fieldStream[i] = XmlHandler.getTagValue(vnode, "field_name");
        if (fieldStream[i] == null) {
          fieldStream[i] = fieldTable[i]; // default: the same name!
        }
        fieldFormatOk[i] = "Y".equalsIgnoreCase(XmlHandler.getTagValue(vnode, "field_format_ok"));
      }
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(
              PKG, "MonetDBBulkLoaderMeta.Exception.UnableToReadTransformInfoFromXML"),
          e);
    }
  }

  @Override
  public void setDefault() {
    fieldTable = null;
    databaseMeta = null;
    schemaName = "";
    tableName = BaseMessages.getString(PKG, "MonetDBBulkLoaderMeta.DefaultTableName");
    bufferSize = "100000";
    logFile = "";
    truncate = false;
    fullyQuoteSQL = true;

    // MonetDB safe defaults.
    fieldSeparator = "|";
    fieldEnclosure = "\"";
    nullRepresentation = "";
    encoding = "UTF-8";
    allocate(0);
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(300);

    // General Settings Tab
    retval.append("    ").append(XmlHandler.addTagValue("connection", dbConnectionName));
    retval.append("    ").append(XmlHandler.addTagValue("buffer_size", bufferSize));
    retval.append("    ").append(XmlHandler.addTagValue("schema", schemaName));
    retval.append("    ").append(XmlHandler.addTagValue("table", tableName));
    retval.append("    ").append(XmlHandler.addTagValue("log_file", logFile));
    retval.append("    ").append(XmlHandler.addTagValue("truncate", truncate));
    retval.append("    ").append(XmlHandler.addTagValue("fully_quote_sql", fullyQuoteSQL));

    // MonetDB Settings Tab
    retval.append("    ").append(XmlHandler.addTagValue("field_separator", fieldSeparator));
    retval.append("    ").append(XmlHandler.addTagValue("field_enclosure", fieldEnclosure));
    retval.append("    ").append(XmlHandler.addTagValue("null_representation", nullRepresentation));
    retval.append("    ").append(XmlHandler.addTagValue("encoding", encoding));

    // Output Fields Tab
    for (int i = 0; i < fieldTable.length; i++) {
      Boolean fieldFormat = false;
      if (fieldFormatOk.length == fieldTable.length) {
        fieldFormat = fieldFormatOk[i];
      }
      retval.append("      <mapping>").append(Const.CR);
      retval.append(CONST_SPACES).append(XmlHandler.addTagValue("stream_name", fieldTable[i]));
      retval.append(CONST_SPACES).append(XmlHandler.addTagValue("field_name", fieldStream[i]));
      retval.append(CONST_SPACES).append(XmlHandler.addTagValue("field_format_ok", fieldFormat));
      retval.append("      </mapping>").append(Const.CR);
    }

    return retval.toString();
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
    String erroMessage = "";

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
          erroMessage = "";

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
            erroMessage = "";

            for (String field : fieldTable) {
              IValueMeta v = r.searchValueMeta(field);
              if (v == null) {
                if (first) {
                  first = false;
                  erroMessage +=
                      BaseMessages.getString(
                              PKG,
                              "MonetDBBulkLoaderMeta.CheckResult.MissingFieldsToLoadInTargetTable")
                          + Const.CR;
                }
                errorFound = true;
                erroMessage += "\t\t" + field + Const.CR;
              }
            }
            if (errorFound) {
              cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, erroMessage, transformMeta);
            } else {
              cr =
                  new CheckResult(
                      ICheckResult.TYPE_RESULT_OK,
                      BaseMessages.getString(
                          PKG, "MonetDBBulkLoaderMeta.CheckResult.AllFieldsFoundInTargetTable"),
                      transformMeta);
            }
          } else {
            erroMessage =
                BaseMessages.getString(
                    PKG, "MonetDBBulkLoaderMeta.CheckResult.CouldNotReadTableInfo");
            cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, erroMessage, transformMeta);
          }
          remarks.add(cr);
        }

        // Look up fields in the input stream <prev>
        if (prev != null && prev.size() > 0) {
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
          erroMessage = "";
          boolean errorFound = false;

          for (String s : fieldStream) {
            IValueMeta v = prev.searchValueMeta(s);
            if (v == null) {
              if (first) {
                first = false;
                erroMessage +=
                    BaseMessages.getString(
                            PKG, "MonetDBBulkLoaderMeta.CheckResult.MissingFieldsInInput")
                        + Const.CR;
              }
              errorFound = true;
              erroMessage += "\t\t" + s + Const.CR;
            }
          }
          if (errorFound) {
            cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, erroMessage, transformMeta);
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
          erroMessage =
              BaseMessages.getString(PKG, "MonetDBBulkLoaderMeta.CheckResult.MissingFieldsInInput3")
                  + Const.CR;
          cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, erroMessage, transformMeta);
          remarks.add(cr);
        }
      } catch (HopException e) {
        erroMessage =
            BaseMessages.getString(PKG, "MonetDBBulkLoaderMeta.CheckResult.DatabaseErrorOccurred")
                + e.getMessage();
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, erroMessage, transformMeta);
        remarks.add(cr);
      } finally {
        db.disconnect();
      }
    } else {
      erroMessage =
          BaseMessages.getString(PKG, "MonetDBBulkLoaderMeta.CheckResult.InvalidConnection");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, erroMessage, transformMeta);
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
    List<IValueMeta> fields = prev.getValueMetaList();
    fieldTable = new String[fields.size()];
    fieldStream = new String[fields.size()];
    fieldFormatOk = new boolean[fields.size()];
    int idx = 0;
    for (IValueMeta field : fields) {
      IValueMeta tableField = field.clone();
      tableFields.addValueMeta(tableField);
      fieldTable[idx] = field.getName();
      fieldStream[idx] = field.getName();
      fieldFormatOk[idx] = true;
      idx++;
    }

    data.keynrs = new int[getFieldStream().length];
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
    SqlStatement retval =
        new SqlStatement(transformMeta.getName(), databaseMeta, null); // default: nothing to do!

    if (databaseMeta != null) {
      if (prev != null && prev.size() > 0) {
        // Copy the row
        IRowMeta tableFields;

        if (autoSchema) {
          tableFields = updateFields(prev, data);
        } else {
          tableFields = new RowMeta();
          // Now change the field names
          for (int i = 0; i < fieldTable.length; i++) {
            IValueMeta v = prev.searchValueMeta(fieldStream[i]);
            if (v != null) {
              IValueMeta tableField = v.clone();
              tableField.setName(fieldTable[i]);
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
            if (sql.length() == 0) {
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
    if (prev != null) {
      /* DEBUG CHECK THIS */
      // Insert dateMask fields : read/write
      for (int i = 0; i < fieldTable.length; i++) {
        IValueMeta v = prev.searchValueMeta(fieldStream[i]);

        DatabaseImpact ii =
            new DatabaseImpact(
                DatabaseImpact.TYPE_IMPACT_READ_WRITE,
                pipelineMeta.getName(),
                transformMeta.getName(),
                databaseMeta.getDatabaseName(),
                variables.resolve(tableName),
                fieldTable[i],
                fieldStream[i],
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
   * @return the schemaName
   */
  public String getSchemaName() {
    return schemaName;
  }

  /**
   * @param schemaName the schemaName to set
   */
  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public String getLogFile() {
    return logFile;
  }

  public void setLogFile(String logFile) {
    this.logFile = logFile;
  }

  public String getFieldSeparator() {
    return fieldSeparator;
  }

  public void setFieldSeparator(String fieldSeparatorStr) {
    this.fieldSeparator = fieldSeparatorStr;
  }

  public String getFieldEnclosure() {
    return fieldEnclosure;
  }

  public void setFieldEnclosure(String fieldEnclosureStr) {
    this.fieldEnclosure = fieldEnclosureStr;
  }

  public String getNullRepresentation() {
    return nullRepresentation;
  }

  public void setNullRepresentation(String nullRepresentationString) {
    this.nullRepresentation = nullRepresentationString;
  }

  public String getEncoding() {
    return encoding;
  }

  public void setEncoding(String encoding) {
    this.encoding = encoding;
  }

  /**
   * @return the bufferSize
   */
  public String getBufferSize() {
    return bufferSize;
  }

  /**
   * @param bufferSize the bufferSize to set
   */
  public void setBufferSize(String bufferSize) {
    this.bufferSize = bufferSize;
  }

  /**
   * @return the fieldFormatOk
   */
  public boolean[] getFieldFormatOk() {
    return fieldFormatOk;
  }

  /**
   * @param fieldFormatOk the fieldFormatOk to set
   */
  public void setFieldFormatOk(boolean[] fieldFormatOk) {
    this.fieldFormatOk = fieldFormatOk;
  }

  /**
   * @param dbConnectionName connection name to set
   */
  public void setDbConnectionName(String dbConnectionName) {
    this.dbConnectionName = dbConnectionName;
  }

  /**
   * @return the database connection name
   */
  public String getDbConnectionName() {
    return this.dbConnectionName;
  }

  /**
   * Returns the version of MonetDB that is used.
   *
   * @return The version of MonetDB
   * @throws HopException if an error occurs
   */
  private MonetDbVersion getMonetDBVersion(IVariables variables) throws HopException {
    Database db = null;

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
