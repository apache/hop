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

package org.apache.hop.pipeline.transforms.mssqlbulkloader;

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
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lineage.api.RelationalLineage;
import org.apache.hop.lineage.model.RelationalIoOperation;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Streams pipeline rows straight into a SQL Server table with the JDBC driver's {@code
 * SQLServerBulkCopy}, without staging them in a file first.
 */
@Transform(
    id = "MsSqlServerBulkLoader",
    image = "MssqlBulkLoader.svg",
    name = "i18n::BaseTransform.TypeLongDesc.MsSqlServerBulkLoaderMessage",
    description = "i18n::BaseTransform.TypeTooltipDesc.MsSqlServerBulkLoaderMessage",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Bulk",
    keywords = "i18n::MsSqlServerBulkLoaderMeta.keyword",
    documentationUrl = "/pipeline/transforms/mssqlbulkloader.html",
    isIncludeJdbcDrivers = true,
    classLoaderGroup = "mssqlnative-db",
    actionTransformTypes = {ActionTransformType.RDBMS, ActionTransformType.OUTPUT})
@Getter
@Setter
@RelationalLineage(operation = RelationalIoOperation.WRITE)
public class MsSqlServerBulkLoaderMeta
    extends BaseTransformMeta<MsSqlServerBulkLoader, MsSqlServerBulkLoaderData> {
  private static final Class<?> PKG = MsSqlServerBulkLoaderMeta.class;

  public static final String DEFAULT_BATCH_SIZE = "100000";

  @HopMetadataProperty(
      key = "connection",
      injectionKey = "CONNECTIONNAME",
      injectionKeyDescription = "MsSqlServerBulkLoader.Injection.CONNECTIONNAME",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String connection;

  @HopMetadataProperty(
      key = "schema",
      injectionKey = "SCHEMANAME",
      injectionKeyDescription = "MsSqlServerBulkLoader.Injection.SCHEMANAME",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_SCHEMA)
  private String schemaName;

  @HopMetadataProperty(
      key = "table",
      injectionKey = "TABLENAME",
      injectionKeyDescription = "MsSqlServerBulkLoader.Injection.TABLENAME",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
  private String tableName;

  /** Number of rows buffered before they are handed to the driver as one bulk copy batch. */
  @HopMetadataProperty(
      key = "batch_size",
      injectionKey = "BATCHSIZE",
      injectionKeyDescription = "MsSqlServerBulkLoader.Injection.BATCHSIZE")
  private String batchSize;

  @HopMetadataProperty(
      key = "truncate",
      injectionKey = "TRUNCATE_TABLE",
      injectionKeyDescription = "MsSqlServerBulkLoader.Injection.TRUNCATE_TABLE",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TRUNCATE)
  private boolean truncateTable;

  /** Only truncate when at least one row arrives, so an empty stream leaves the table alone. */
  @HopMetadataProperty(
      key = "only_when_have_rows",
      injectionKey = "ONLY_WHEN_HAVE_ROWS",
      injectionKeyDescription = "MsSqlServerBulkLoader.Injection.ONLY_WHEN_HAVE_ROWS")
  private boolean onlyWhenHaveRows;

  /** When false every input field is matched to the target table by name. */
  @HopMetadataProperty(
      key = "specify_fields",
      injectionKey = "SPECIFY_FIELDS",
      injectionKeyDescription = "MsSqlServerBulkLoader.Injection.SPECIFY_FIELDS")
  private boolean specifyFields;

  @HopMetadataProperty(
      groupKey = "fields",
      key = "field",
      injectionGroupKey = "FIELDS",
      injectionGroupDescription = "MsSqlServerBulkLoader.Injection.FIELDS")
  private List<Field> fields;

  @HopMetadataProperty(
      key = "table_lock",
      injectionKey = "TABLE_LOCK",
      injectionKeyDescription = "MsSqlServerBulkLoader.Injection.TABLE_LOCK")
  private boolean tableLock;

  @HopMetadataProperty(
      key = "keep_identity",
      injectionKey = "KEEP_IDENTITY",
      injectionKeyDescription = "MsSqlServerBulkLoader.Injection.KEEP_IDENTITY")
  private boolean keepIdentity;

  @HopMetadataProperty(
      key = "keep_nulls",
      injectionKey = "KEEP_NULLS",
      injectionKeyDescription = "MsSqlServerBulkLoader.Injection.KEEP_NULLS")
  private boolean keepNulls;

  @HopMetadataProperty(
      key = "check_constraints",
      injectionKey = "CHECK_CONSTRAINTS",
      injectionKeyDescription = "MsSqlServerBulkLoader.Injection.CHECK_CONSTRAINTS")
  private boolean checkConstraints;

  @HopMetadataProperty(
      key = "fire_triggers",
      injectionKey = "FIRE_TRIGGERS",
      injectionKeyDescription = "MsSqlServerBulkLoader.Injection.FIRE_TRIGGERS")
  private boolean fireTriggers;

  /** Bulk copy timeout in seconds, 0 meaning no timeout. */
  @HopMetadataProperty(
      key = "bulk_copy_timeout",
      injectionKey = "BULK_COPY_TIMEOUT",
      injectionKeyDescription = "MsSqlServerBulkLoader.Injection.BULK_COPY_TIMEOUT")
  private String bulkCopyTimeout;

  /** Needed to load into columns protected by Always Encrypted. */
  @HopMetadataProperty(
      key = "allow_encrypted_value_modifications",
      injectionKey = "ALLOW_ENCRYPTED_VALUE_MODIFICATIONS",
      injectionKeyDescription =
          "MsSqlServerBulkLoader.Injection.ALLOW_ENCRYPTED_VALUE_MODIFICATIONS")
  private boolean allowEncryptedValueModifications;

  public MsSqlServerBulkLoaderMeta() {
    super();
    fields = new ArrayList<>();
  }

  @Override
  public void setDefault() {
    connection = "";
    schemaName = "";
    tableName = "";
    batchSize = DEFAULT_BATCH_SIZE;
    bulkCopyTimeout = "0";
    truncateTable = false;
    onlyWhenHaveRows = false;
    specifyFields = false;
    tableLock = true;
    keepIdentity = false;
    keepNulls = false;
    checkConstraints = false;
    fireTriggers = false;
    allowEncryptedValueModifications = false;
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

    if (input.length > 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "MsSqlServerBulkLoaderMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "MsSqlServerBulkLoaderMeta.CheckResult.NoInputReceived"),
              transformMeta));
    }

    if (Utils.isEmpty(connection)) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "MsSqlServerBulkLoaderMeta.CheckResult.InvalidConnection"),
              transformMeta));
      return;
    }

    DatabaseMeta databaseMeta;
    try {
      databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connection));
    } catch (HopException e) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                      PKG, "MsSqlServerBulkLoaderMeta.CheckResult.DatabaseErrorOccurred")
                  + e.getMessage(),
              transformMeta));
      return;
    }

    if (databaseMeta == null) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "MsSqlServerBulkLoaderMeta.CheckResult.InvalidConnection"),
              transformMeta));
      return;
    }

    if (Utils.isEmpty(tableName)) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "MsSqlServerBulkLoaderMeta.CheckResult.NoTableName"),
              transformMeta));
      return;
    }

    try (Database db = new Database(loggingObject, variables, databaseMeta)) {
      db.connect();

      String realSchemaName = variables.resolve(schemaName);
      String realTableName = variables.resolve(tableName);
      String schemaTable =
          databaseMeta.getQuotedSchemaTableCombination(variables, realSchemaName, realTableName);

      IRowMeta tableFields = db.getTableFields(schemaTable);
      if (tableFields == null) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG, "MsSqlServerBulkLoaderMeta.CheckResult.CouldNotReadTableInfo"),
                transformMeta));
        return;
      }

      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "MsSqlServerBulkLoaderMeta.CheckResult.TableAccessible", schemaTable),
              transformMeta));

      checkFieldsAgainstTable(remarks, transformMeta, prev, tableFields);
    } catch (HopException e) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                      PKG, "MsSqlServerBulkLoaderMeta.CheckResult.DatabaseErrorOccurred")
                  + e.getMessage(),
              transformMeta));
    }
  }

  /**
   * Every column the transform will write has to exist in the target table, and every stream field
   * it reads has to exist in the incoming row. Both lists are reported separately so that a mapping
   * mistake points at the side it came from.
   */
  private void checkFieldsAgainstTable(
      List<ICheckResult> remarks,
      TransformMeta transformMeta,
      IRowMeta prev,
      IRowMeta tableFields) {

    StringBuilder missingInTable = new StringBuilder();
    StringBuilder missingInStream = new StringBuilder();

    if (specifyFields) {
      for (Field field : fields) {
        if (tableFields.searchValueMeta(field.getFieldTable()) == null) {
          missingInTable.append("\t\t").append(field.getFieldTable()).append(Const.CR);
        }
        if (prev != null && prev.searchValueMeta(field.getFieldStream()) == null) {
          missingInStream.append("\t\t").append(field.getFieldStream()).append(Const.CR);
        }
      }
    } else if (prev != null) {
      for (int i = 0; i < prev.size(); i++) {
        IValueMeta v = prev.getValueMeta(i);
        if (tableFields.searchValueMeta(v.getName()) == null) {
          missingInTable.append("\t\t").append(v.getName()).append(Const.CR);
        }
      }
    }

    if (missingInTable.length() > 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                      PKG, "MsSqlServerBulkLoaderMeta.CheckResult.MissingFieldsInTable")
                  + Const.CR
                  + missingInTable,
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "MsSqlServerBulkLoaderMeta.CheckResult.AllFieldsFoundInTable"),
              transformMeta));
    }

    if (missingInStream.length() > 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                      PKG, "MsSqlServerBulkLoaderMeta.CheckResult.MissingFieldsInInput")
                  + Const.CR
                  + missingInStream,
              transformMeta));
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

      SqlStatement sqlStatement = new SqlStatement(transformMeta.getName(), databaseMeta, null);

      if (databaseMeta == null) {
        sqlStatement.setError(
            BaseMessages.getString(PKG, "MsSqlServerBulkLoaderMeta.GetSQL.NoConnectionDefined"));
        return sqlStatement;
      }
      if (prev == null || prev.isEmpty()) {
        sqlStatement.setError(
            BaseMessages.getString(PKG, "MsSqlServerBulkLoaderMeta.GetSQL.NotReceivingAnyFields"));
        return sqlStatement;
      }
      if (Utils.isEmpty(tableName)) {
        sqlStatement.setError(
            BaseMessages.getString(PKG, "MsSqlServerBulkLoaderMeta.GetSQL.NoTableDefined"));
        return sqlStatement;
      }

      // The DDL describes the table as it would have to look to accept what this transform writes,
      // so it is built from the target column names carrying the incoming field types.
      IRowMeta tableFields = new RowMeta();
      if (specifyFields) {
        for (Field field : fields) {
          IValueMeta v = prev.searchValueMeta(field.getFieldStream());
          if (v == null) {
            throw new HopTransformException(
                BaseMessages.getString(
                    PKG,
                    "MsSqlServerBulkLoaderMeta.Exception.FieldNotFoundInStream",
                    field.getFieldStream()));
          }
          IValueMeta tableField = v.clone();
          tableField.setName(field.getFieldTable());
          tableFields.addValueMeta(tableField);
        }
      } else {
        tableFields = prev.clone();
      }

      try (Database db = new Database(loggingObject, variables, databaseMeta)) {
        db.connect();
        String schemaTable =
            databaseMeta.getQuotedSchemaTableCombination(
                variables, variables.resolve(schemaName), variables.resolve(tableName));
        String sql = db.getDDL(schemaTable, tableFields, null, false, null, true);
        sqlStatement.setSql(Utils.isEmpty(sql) ? null : sql);
      } catch (HopException e) {
        sqlStatement.setError(
            BaseMessages.getString(PKG, "MsSqlServerBulkLoaderMeta.GetSQL.ErrorOccurred")
                + e.getMessage());
      }

      return sqlStatement;
    } catch (HopTransformException e) {
      throw e;
    } catch (Exception e) {
      throw new HopTransformException("Error generating the SQL statement", e);
    }
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

    if (prev == null) {
      return;
    }
    try {
      DatabaseMeta databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connection));

      for (int i = 0; i < prev.size(); i++) {
        IValueMeta v = prev.getValueMeta(i);
        impact.add(
            new DatabaseImpact(
                DatabaseImpact.TYPE_IMPACT_WRITE,
                pipelineMeta.getName(),
                transformMeta.getName(),
                databaseMeta == null ? "" : databaseMeta.getDatabaseName(),
                variables.resolve(tableName),
                v.getName(),
                v.getName(),
                v.getOrigin() == null ? "?" : v.getOrigin(),
                "",
                "Type = " + v.toStringMeta()));
      }
    } catch (HopException e) {
      throw new HopTransformException(
          "Unable to get the database connection: " + Const.CR + variables.resolve(connection), e);
    }
  }

  @Override
  public IRowMeta getRequiredFields(IVariables variables) throws HopException {
    String realTableName = variables.resolve(tableName);
    String realSchemaName = variables.resolve(schemaName);

    DatabaseMeta databaseMeta =
        getParentTransformMeta().getParentPipelineMeta().findDatabase(connection, variables);
    if (databaseMeta == null) {
      throw new HopException(
          BaseMessages.getString(PKG, "MsSqlServerBulkLoaderMeta.Exception.ConnectionNotDefined"));
    }
    if (Utils.isEmpty(realTableName)) {
      throw new HopException(
          BaseMessages.getString(PKG, "MsSqlServerBulkLoaderMeta.Exception.TableNotSpecified"));
    }

    try (Database db = new Database(loggingObject, variables, databaseMeta)) {
      db.connect();
      if (!db.checkTableExists(realSchemaName, realTableName)) {
        throw new HopException(
            BaseMessages.getString(PKG, "MsSqlServerBulkLoaderMeta.Exception.TableNotFound"));
      }
      return db.getTableFields(
          databaseMeta.getQuotedSchemaTableCombination(variables, realSchemaName, realTableName));
    } catch (HopException e) {
      throw new HopException(
          BaseMessages.getString(PKG, "MsSqlServerBulkLoaderMeta.Exception.ErrorGettingFields"), e);
    }
  }

  public static String[] getOrderHintDescriptions() {
    return IEnumHasCodeAndDescription.getDescriptions(OrderHint.class);
  }

  public static OrderHint lookupOrderHint(String codeOrDescription) {
    OrderHint hint = IEnumHasCode.lookupCode(OrderHint.class, codeOrDescription, null);
    if (hint != null) {
      return hint;
    }
    return IEnumHasCodeAndDescription.lookupDescription(
        OrderHint.class, codeOrDescription, OrderHint.NONE);
  }

  /**
   * Tells SQL Server that the incoming stream is already sorted on a column, which lets it skip a
   * sort when that column backs the clustered index.
   */
  @Getter
  public enum OrderHint implements IEnumHasCodeAndDescription {
    NONE(
        "NONE",
        BaseMessages.getString(PKG, "MsSqlServerBulkLoaderMeta.OrderHint.None.Description")),
    ASCENDING(
        "ASC",
        BaseMessages.getString(PKG, "MsSqlServerBulkLoaderMeta.OrderHint.Ascending.Description")),
    DESCENDING(
        "DESC",
        BaseMessages.getString(PKG, "MsSqlServerBulkLoaderMeta.OrderHint.Descending.Description"));

    private final String code;
    private final String description;

    OrderHint(String code, String description) {
      this.code = code;
      this.description = description;
    }
  }

  @Getter
  @Setter
  public static final class Field {

    @HopMetadataProperty(
        key = "table_field",
        injectionKey = "FIELDTABLE",
        injectionKeyDescription = "MsSqlServerBulkLoader.Injection.FIELDTABLE",
        hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_COLUMN)
    private String fieldTable;

    @HopMetadataProperty(
        key = "stream_field",
        injectionKey = "FIELDSTREAM",
        injectionKeyDescription = "MsSqlServerBulkLoader.Injection.FIELDSTREAM",
        hopMetadataPropertyType = HopMetadataPropertyType.STREAM_FIELD)
    private String fieldStream;

    @HopMetadataProperty(
        key = "order_hint",
        storeWithCode = true,
        injectionKey = "ORDERHINT",
        injectionKeyDescription = "MsSqlServerBulkLoader.Injection.ORDERHINT")
    private OrderHint orderHint;

    public Field() {
      // Needed for metadata deserialization.
    }

    public Field(String fieldTable, String fieldStream, OrderHint orderHint) {
      this.fieldTable = fieldTable;
      this.fieldStream = fieldStream;
      this.orderHint = orderHint;
    }

    /**
     * A missing or empty {@code <order_hint>} - injected, hand-edited or written before the option
     * existed - has to read as {@link OrderHint#NONE} rather than null, which would NPE the dialog
     * and the bulk copy setup alike.
     */
    public OrderHint getOrderHint() {
      return orderHint == null ? OrderHint.NONE : orderHint;
    }

    public void setOrderHintWithDescription(String description) {
      orderHint =
          IEnumHasCodeAndDescription.lookupDescription(
              OrderHint.class, description, OrderHint.NONE);
    }
  }
}
