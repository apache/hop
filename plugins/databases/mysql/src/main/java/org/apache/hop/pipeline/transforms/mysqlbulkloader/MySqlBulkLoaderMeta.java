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

import java.util.ArrayList;
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
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
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

  public MySqlBulkLoaderMeta() {
    fields = new ArrayList<>();
  }

  @Override
  public void setDefault() {
    connection = "";
    schemaName = "";
    tableName = BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.DefaultTableName");
    encoding = "UTF8";
    loadCharSet = "UTF8MB4";
    fifoFileName = "${java.io.tmpdir}/fifo.${Internal.Transform.CopyNr}";
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
    StringBuilder errorMessage = new StringBuilder();

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
          errorMessage = new StringBuilder();

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
            errorMessage = new StringBuilder();

            for (Field value : fields) {
              String field = value.getFieldTable();

              IValueMeta v = r.searchValueMeta(field);
              if (v == null) {
                if (first) {
                  first = false;
                  errorMessage
                      .append(
                          BaseMessages.getString(
                              PKG,
                              "MySqlBulkLoaderMeta.CheckResult.MissingFieldsToLoadInTargetTable"))
                      .append(Const.CR);
                }
                errorFound = true;
                errorMessage.append("\t\t").append(field).append(Const.CR);
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
                          PKG, "MySqlBulkLoaderMeta.CheckResult.AllFieldsFoundInTargetTable"),
                      transformMeta);
            }
            remarks.add(cr);
          } else {
            errorMessage =
                new StringBuilder(
                    BaseMessages.getString(
                        PKG, "MySqlBulkLoaderMeta.CheckResult.CouldNotReadTableInfo"));
            cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_ERROR, errorMessage.toString(), transformMeta);
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
          errorMessage = new StringBuilder();
          boolean errorFound = false;

          for (Field field : fields) {
            IValueMeta v = prev.searchValueMeta(field.getFieldStream());
            if (v == null) {
              if (first) {
                first = false;
                errorMessage
                    .append(
                        BaseMessages.getString(
                            PKG, "MySqlBulkLoaderMeta.CheckResult.MissingFieldsInInput"))
                    .append(Const.CR);
              }
              errorFound = true;
              errorMessage.append("\t\t").append(field.getFieldStream()).append(Const.CR);
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
                        PKG, "MySqlBulkLoaderMeta.CheckResult.AllFieldsFoundInInput"),
                    transformMeta);
          }
          remarks.add(cr);
        } else {
          errorMessage =
              new StringBuilder(
                  BaseMessages.getString(
                          PKG, "MySqlBulkLoaderMeta.CheckResult.MissingFieldsInInput3")
                      + Const.CR);
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR, errorMessage.toString(), transformMeta);
          remarks.add(cr);
        }
      } catch (HopException e) {
        errorMessage =
            new StringBuilder(
                BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.CheckResult.DatabaseErrorOccurred")
                    + e.getMessage());
        cr =
            new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage.toString(), transformMeta);
        remarks.add(cr);
      }
    } else {
      errorMessage =
          new StringBuilder(
              BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.CheckResult.InvalidConnection"));
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage.toString(), transformMeta);
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

    try {
      DatabaseMeta databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connection));

      SqlStatement sqlStatement =
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
            try (Database db = new Database(loggingObject, variables, databaseMeta)) {
              db.connect();

              String schemaTable =
                  databaseMeta.getQuotedSchemaTableCombination(
                      variables, variables.resolve(schemaName), variables.resolve(tableName));
              String sql = db.getDDL(schemaTable, tableFields, null, false, null, true);
              if (sql.isEmpty()) {
                sqlStatement.setSql(null);
              } else {
                sqlStatement.setSql(sql);
              }
            } catch (HopException e) {
              sqlStatement.setError(
                  BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.GetSQL.ErrorOccurred")
                      + e.getMessage());
            }
          } else {
            sqlStatement.setError(
                BaseMessages.getString(
                    PKG, "MySqlBulkLoaderMeta.GetSQL.NoTableDefinedOnConnection"));
          }
        } else {
          sqlStatement.setError(
              BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.GetSQL.NotReceivingAnyFields"));
        }
      } else {
        sqlStatement.setError(
            BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.GetSQL.NoConnectionDefined"));
      }

      return sqlStatement;
    } catch (Exception e) {
      throw new HopTransformException("Error generating SQL statement", e);
    }
  }

  @Override
  public IRowMeta getRequiredFields(IVariables variables) throws HopException {
    String realTableName = variables.resolve(tableName);
    String realSchemaName = variables.resolve(schemaName);

    DatabaseMeta databaseMeta =
        getParentTransformMeta().getParentPipelineMeta().findDatabase(connection, variables);
    if (databaseMeta != null) {
      try (Database db = new Database(loggingObject, variables, databaseMeta)) {
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
      }
    } else {
      throw new HopException(
          BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.Exception.ConnectionNotDefined"));
    }
  }

  public static String[] getFieldFormatTypeDescriptions() {
    return IEnumHasCodeAndDescription.getDescriptions(FieldFormatType.class);
  }

  public static String getFieldFormatTypeCode(FieldFormatType type) {
    if (type == null) return "";
    return type.getCode();
  }

  public static String getFieldFormatTypeDescription(FieldFormatType type) {
    if (type == null) return "";
    return type.getDescription();
  }

  public static FieldFormatType getFieldFormatType(String codeOrDescription) {
    FieldFormatType type = IEnumHasCode.lookupCode(FieldFormatType.class, codeOrDescription, null);
    if (type != null) {
      return type;
    }
    return IEnumHasCodeAndDescription.lookupDescription(
        FieldFormatType.class, codeOrDescription, FieldFormatType.OK);
  }

  @Getter
  public enum FieldFormatType implements IEnumHasCodeAndDescription {
    OK("OK", BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.FieldFormatType.OK.Description")),
    DATE(
        "DATE",
        BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.FieldFormatType.Date.Description")),
    TIMESTAMP(
        "TIMESTAMP",
        BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.FieldFormatType.Timestamp.Description")),
    TIMESTAMP_MS(
        "TIMESTAMP_MS",
        BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.FieldFormatType.TimestampMs.Description")),
    TIMESTAMP_IS(
        "TIMESTAMP_IS",
        BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.FieldFormatType.TimestampIs.Description")),
    TIMESTAMP_NS(
        "TIMESTAMP_NS",
        BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.FieldFormatType.TimestampNs.Description")),
    NUMBER(
        "NUMBER",
        BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.FieldFormatType.Number.Description")),
    STRING_ESCAPE(
        "STRING_ESC",
        BaseMessages.getString(
            PKG, "MySqlBulkLoaderMeta.FieldFormatType.StringEscape.Description")),
    AUTO_FORMAT(
        "AUTO_FORMAT",
        BaseMessages.getString(PKG, "MySqlBulkLoaderMeta.FieldFormatType.AutoFormat.Description")),
    ;
    private final String code;
    private final String description;

    FieldFormatType(String code, String description) {
      this.code = code;
      this.description = description;
    }
  }

  @Getter
  @Setter
  public static final class Field {

    @HopMetadataProperty(key = "stream_name")
    private String fieldTable;

    @HopMetadataProperty(key = "field_name")
    private String fieldStream;

    @HopMetadataProperty(key = "field_format_ok", storeWithCode = true)
    private FieldFormatType fieldFormatType;

    public void setFieldFormatTypeWithCode(String code) {
      fieldFormatType = IEnumHasCode.lookupCode(FieldFormatType.class, code, FieldFormatType.OK);
    }

    public void setFieldFormatTypeWithDescription(String description) {
      fieldFormatType =
          IEnumHasCodeAndDescription.lookupDescription(
              FieldFormatType.class, description, FieldFormatType.OK);
    }

    /**
     * A missing or empty {@code <field_format_ok>} (legacy, injection-generated or hand-edited
     * fields) must behave as {@link FieldFormatType#OK}, matching the pre-@HopMetadataProperty
     * String default. Returning {@code null} caused a dialog NPE on open and changed the bytes
     * written to the bulk-load file at runtime.
     */
    public FieldFormatType getFieldFormatType() {
      return fieldFormatType == null ? FieldFormatType.OK : fieldFormatType;
    }
  }

  /** Added for backwards compatibility with older XML "mapping" blocks. */
  @Override
  public void convertLegacyXml(Node node) throws HopException {
    if (node == null) {
      return;
    }

    if (fields == null) {
      fields = new java.util.ArrayList<>();
    }

    int nrvalues = XmlHandler.countNodes(node, "mapping");
    for (int i = 0; i < nrvalues; i++) {
      Node vnode = XmlHandler.getSubNodeByNr(node, "mapping", i);
      if (vnode == null) {
        continue;
      }
      Field field = new Field();
      field.setFieldStream(XmlHandler.getTagValue(vnode, "field_name"));
      field.setFieldTable(XmlHandler.getTagValue(vnode, "stream_name"));
      field.setFieldFormatTypeWithCode(XmlHandler.getTagValue(vnode, "field_format_ok"));
      fields.add(field);
    }
  }
}
