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

package org.apache.hop.pipeline.transforms.mysqlbulkloader;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

@Transform(
    id = "MySqlBulkLoader",
    image = "mysqlbulkloader.svg",
    name = "i18n::MySqlBulkLoader.Name",
    description = "i18n::MySqlBulkLoader.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Bulk",
    keywords = "i18n::MySqlBulkLoader.keyword",
    documentationUrl = "/pipeline/transforms/mysqlbulkloader.html",
    isIncludeJdbcDrivers = true,
    classLoaderGroup = "mysql-db")
@Getter
@Setter
public class MySqlBulkLoaderMeta extends BaseTransformMeta<MySqlBulkLoader, MySqlBulkLoaderData> {
  private static final Class<?> PKG = MySqlBulkLoaderMeta.class;

  public static final int FIELD_FORMAT_TYPE_OK = 0;
  public static final int FIELD_FORMAT_TYPE_DATE = 1;
  public static final int FIELD_FORMAT_TYPE_TIMESTAMP = 2;
  public static final int FIELD_FORMAT_TYPE_NUMBER = 3;
  public static final int FIELD_FORMAT_TYPE_STRING_ESCAPE = 4;

  private static final String[] fieldFormatTypeCodes = {
    "OK", "DATE", "TIMESTAMP", "NUMBER", "STRING_ESC"
  };
  private static final String[] fieldFormatTypeDescriptions = {
    BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.FieldFormatType.OK.Description"),
    BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.FieldFormatType.Date.Description"),
    BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.FieldFormatType.Timestamp.Description"),
    BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.FieldFormatType.Number.Description"),
    BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.FieldFormatType.StringEscape.Description"),
  };

  @HopMetadataProperty(key = "schema")
  private String schemaName;

  @HopMetadataProperty(key = "table")
  private String tableName;

  @HopMetadataProperty(key = "fifo_file_name")
  private String fifoFileName;

  @HopMetadataProperty(key = "connection")
  private String connection;

  @HopMetadataProperty(key = "field", groupKey = "fields")
  private List<Field> fields;

  @HopMetadataProperty(key = "encoding")
  private String encoding;

  @HopMetadataProperty(key = "loadCharSet")
  private String loadCharSet;

  @HopMetadataProperty(key = "replace")
  private boolean replacingData;

  @HopMetadataProperty(key = "ignore")
  private boolean ignoringErrors;

  @HopMetadataProperty(key = "local")
  private boolean localFile;

  @HopMetadataProperty(key = "delimiter")
  private String delimiter;

  @HopMetadataProperty(key = "enclosure")
  private String enclosure;

  @HopMetadataProperty(key = "escape_char")
  private String escapeChar;

  @HopMetadataProperty(key = "bulk_size")
  private String bulkSize;

  /**
   * @deprecated Keep for backwards compatibility
   * @param transformNode xml transform node
   * @param metadataProvider metadata provider
   * @throws HopXmlException thrown when unable to read XML
   */
  @Override
  @Deprecated(since = "2.13")
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    super.loadXml(transformNode, metadataProvider);
    try {
      int nrvalues = XmlHandler.countNodes(transformNode, "mapping");
      for (int i = 0; i < nrvalues; i++) {
        Node vnode = XmlHandler.getSubNodeByNr(transformNode, "mapping", i);
        Field field = new Field();
        field.setFieldStream(XmlHandler.getTagValue(vnode, "field_name"));
        field.setFieldTable(XmlHandler.getTagValue(vnode, "stream_name"));
        field.setFieldFormatType(XmlHandler.getTagValue(vnode, "field_format_ok"));
        fields.add(field);
      }
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(
              PKG, "MySqlBulkLoaderMeta.Exception.UnableToReadTransformInfoFromXML"),
          e);
    }
  }

  @Override
  public void setDefault() {
    connection = "";
    schemaName = "";
    tableName = BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.DefaultTableName");
    encoding = "UTF8";
    loadCharSet = "UTF8MB4";
    fifoFileName = "/tmp/fifo";
    delimiter = "\t";
    enclosure = "\"";
    escapeChar = "\\";
    replacingData = false;
    ignoringErrors = false;
    localFile = true;
    bulkSize = null;
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

    if (connection != null) {
      DatabaseMeta databaseMeta =
          getParentTransformMeta().getParentPipelineMeta().findDatabase(connection, variables);
      try (Database db = new Database(loggingObject, variables, databaseMeta); ) {
        db.connect();

        if (!Utils.isEmpty(tableName)) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.CheckResult.TableNameOK"),
                  transformMeta);
          remarks.add(cr);

          boolean first = true;
          boolean errorFound = false;
          errorMessage = "";

          // Check fields in table
          String schemaTable =
              databaseMeta.getQuotedSchemaTableCombination(
                  variables, variables.resolve(schemaName), variables.resolve(tableName));
          IRowMeta r = db.getTableFields(schemaTable);
          if (r != null) {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.CheckResult.TableExists"),
                    transformMeta);
            remarks.add(cr);

            // How about the fields to insert/dateMask in the table?
            first = true;
            errorFound = false;
            errorMessage = "";

            for (Field value : fields) {
              String field = value.getFieldTable();

              IValueMeta v = r.searchValueMeta(field);
              if (v == null) {
                if (first) {
                  first = false;
                  errorMessage +=
                      BaseMessages.getString(
                              PKG,
                              "MySqlBulkLoaderMeta.CheckResult.MissingFieldsToLoadInTargetTable")
                          + Const.CR;
                }
                errorFound = true;
                errorMessage += "\t\t" + field + Const.CR;
              }
            }
            if (errorFound) {
              cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
            } else {
              cr =
                  new CheckResult(
                      ICheckResult.TYPE_RESULT_OK,
                      BaseMessages.getString(
                          PKG, "MySqlBulkLoaderMeta.CheckResult.AllFieldsFoundInTargetTable"),
                      transformMeta);
            }
            remarks.add(cr);
          } else {
            errorMessage =
                BaseMessages.getString(
                    PKG, "MySqlBulkLoaderMeta.CheckResult.CouldNotReadTableInfo");
            cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
            remarks.add(cr);
          }
        }

        // Look up fields in the input stream <prev>
        if (prev != null && !prev.isEmpty()) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(
                      PKG,
                      "MySqlBulkLoaderMeta.CheckResult.TransformReceivingDatas",
                      prev.size() + ""),
                  transformMeta);
          remarks.add(cr);

          boolean first = true;
          errorMessage = "";
          boolean errorFound = false;

          for (Field field : fields) {
            IValueMeta v = prev.searchValueMeta(field.getFieldStream());
            if (v == null) {
              if (first) {
                first = false;
                errorMessage +=
                    BaseMessages.getString(
                            PKG, "MySqlBulkLoaderMeta.CheckResult.MissingFieldsInInput")
                        + Const.CR;
              }
              errorFound = true;
              errorMessage += "\t\t" + field.getFieldStream() + Const.CR;
            }
          }
          if (errorFound) {
            cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
          } else {
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString(
                        PKG, "MySqlBulkLoaderMeta.CheckResult.AllFieldsFoundInInput"),
                    transformMeta);
          }
          remarks.add(cr);
        } else {
          errorMessage =
              BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.CheckResult.MissingFieldsInInput3")
                  + Const.CR;
          cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
          remarks.add(cr);
        }
      } catch (HopException e) {
        errorMessage =
            BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.CheckResult.DatabaseErrorOccurred")
                + e.getMessage();
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      }
    } else {
      errorMessage =
          BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.CheckResult.InvalidConnection");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    }

    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "MySqlBulkLoaderMeta.CheckResult.TransformReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.CheckResult.NoInputError"),
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
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    DatabaseMeta databaseMeta =
        getParentTransformMeta().getParentPipelineMeta().findDatabase(connection, variables);

    SqlStatement retval =
        new SqlStatement(transformMeta.getName(), databaseMeta, null); // default: nothing to do!

    if (databaseMeta != null) {
      if (prev != null && !prev.isEmpty()) {
        // Copy the row
        IRowMeta tableFields = new RowMeta();

        // Now change the field names
        for (Field field : fields) {
          IValueMeta v = prev.searchValueMeta(field.getFieldStream());
          if (v != null) {
            IValueMeta tableField = v.clone();
            tableField.setName(field.getFieldStream());
            tableFields.addValueMeta(tableField);
          } else {
            throw new HopTransformException(
                "Unable to find field [" + field.getFieldStream() + "] in the input rows");
          }
        }

        if (!Utils.isEmpty(tableName)) {
          Database db = new Database(loggingObject, variables, databaseMeta);
          try {
            db.connect();

            String schemaTable =
                databaseMeta.getQuotedSchemaTableCombination(
                    variables, variables.resolve(schemaName), variables.resolve(tableName));
            String crTable = db.getDDL(schemaTable, tableFields, null, false, null, true);

            String sql = crTable;
            if (sql.isEmpty()) {
              retval.setSql(null);
            } else {
              retval.setSql(sql);
            }
          } catch (HopException e) {
            retval.setError(
                BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.GetSQL.ErrorOccurred")
                    + e.getMessage());
          }
        } else {
          retval.setError(
              BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.GetSQL.NoTableDefinedOnConnection"));
        }
      } else {
        retval.setError(
            BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.GetSQL.NotReceivingAnyFields"));
      }
    } else {
      retval.setError(
          BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.GetSQL.NoConnectionDefined"));
    }

    return retval;
  }

  @Override
  public IRowMeta getRequiredFields(IVariables variables) throws HopException {
    String realTableName = variables.resolve(tableName);
    String realSchemaName = variables.resolve(schemaName);

    DatabaseMeta databaseMeta =
        getParentTransformMeta().getParentPipelineMeta().findDatabase(connection, variables);
    if (databaseMeta != null) {
      Database db = new Database(loggingObject, variables, databaseMeta);
      try {
        db.connect();

        if (!Utils.isEmpty(realTableName)) {
          String schemaTable =
              databaseMeta.getQuotedSchemaTableCombination(
                  variables, realSchemaName, realTableName);

          // Check if this table exists...
          if (db.checkTableExists(realSchemaName, schemaTable)) {
            return db.getTableFields(schemaTable);
          } else {
            throw new HopException(
                BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.Exception.TableNotFound"));
          }
        } else {
          throw new HopException(
              BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.Exception.TableNotSpecified"));
        }
      } catch (Exception e) {
        throw new HopException(
            BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.Exception.ErrorGettingFields"), e);
      } finally {
        db.disconnect();
      }
    } else {
      throw new HopException(
          BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.Exception.ConnectionNotDefined"));
    }
  }

  public static String[] getFieldFormatTypeDescriptions() {
    return fieldFormatTypeDescriptions;
  }

  public static String getFieldFormatTypeCode(int type) {
    return fieldFormatTypeCodes[type];
  }

  public static String getFieldFormatTypeDescription(int type) {
    return fieldFormatTypeDescriptions[type];
  }

  public static int getFieldFormatType(String codeOrDescription) {
    for (int i = 0; i < fieldFormatTypeCodes.length; i++) {
      if (fieldFormatTypeCodes[i].equalsIgnoreCase(codeOrDescription)) {
        return i;
      }
    }
    for (int i = 0; i < fieldFormatTypeDescriptions.length; i++) {
      if (fieldFormatTypeDescriptions[i].equalsIgnoreCase(codeOrDescription)) {
        return i;
      }
    }
    return FIELD_FORMAT_TYPE_OK;
  }

  @Getter
  @Setter
  public static final class Field {

    @HopMetadataProperty(key = "stream_name")
    private String fieldTable;

    @HopMetadataProperty(key = "field_name")
    private String fieldStream;

    @HopMetadataProperty(key = "field_format_ok")
    private String fieldFormatType;
  }
}
