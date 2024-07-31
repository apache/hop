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

package org.apache.hop.pipeline.transforms.gettablenames;

import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@InjectionSupported(
    localizationPrefix = "GetTableNames.Injection.",
    groups = {"FIELDS", "SETTINGS", "OUTPUT"})
@Transform(
    id = "GetTableNames",
    image = "gettablenames.svg",
    name = "i18n::GetTableNames.Name",
    description = "i18n::GetTableNames.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "i18n::GetTableNamesMeta.keyword",
    documentationUrl = "/pipeline/transforms/gettablenames.html")
public class GetTableNamesMeta extends BaseTransformMeta<GetTableNames, GetTableNamesData> {
  private static final Class<?> PKG = GetTableNamesMeta.class;

  /** The database connection */
  @HopMetadataProperty(
      key = "connection",
      injectionKey = "CONNECTIONNAME",
      injectionKeyDescription = "GetTableNames.Injection.CONNECTION_NAME")
  private String connection;

  @HopMetadataProperty(
      key = "schemaname",
      injectionKey = "SCHEMANAME",
      injectionKeyDescription = "GetTableNames.Injection.SCHEMA_NAME")
  private String schemaName;

  /** function result: new value name */
  @HopMetadataProperty(
      key = "tablenamefieldname",
      injectionKey = "TABLENAMEFIELDNAME",
      injectionKeyDescription = "GetTableNames.Injection.TABLE_NAME_FIELD_NAME")
  private String tableNameFieldName;

  @HopMetadataProperty(
      key = "sqlcreationfieldname",
      injectionKey = "SQLCREATIONFIELDNAME",
      injectionKeyDescription = "GetTableNames.Injection.SQL_CREATION_FIELD_NAME")
  private String sqlCreationFieldName;

  @HopMetadataProperty(
      key = "objecttypefieldname",
      injectionKey = "OBJECTTYPEFIELDNAME",
      injectionKeyDescription = "GetTableNames.Injection.OBJECT_TYPE_FIELD_NAME")
  private String objectTypeFieldName;

  @HopMetadataProperty(
      key = "issystemobjectfieldname",
      injectionKey = "ISSYSTEMOBJECTFIELDNAME",
      injectionKeyDescription = "GetTableNames.Injection.IS_SYSTEM_OBJECT_FIELD_NAME")
  private String isSystemObjectFieldName;

  @HopMetadataProperty(
      key = "includeCatalog",
      injectionKey = "INCLUDECATALOG",
      injectionKeyDescription = "GetTableNames.Injection.INCLUDE_CATALOG")
  private boolean includeCatalog;

  @HopMetadataProperty(
      key = "includeSchema",
      injectionKey = "INCLUDESCHEMA",
      injectionKeyDescription = "GetTableNames.Injection.INCLUDE_SCHEMA")
  private boolean includeSchema;

  @HopMetadataProperty(
      key = "includeTable",
      injectionKey = "INCLUDETABLE",
      injectionKeyDescription = "GetTableNames.Injection.INCLUDE_TABLE")
  private boolean includeTable;

  @HopMetadataProperty(
      key = "includeView",
      injectionKey = "INCLUDEVIEW",
      injectionKeyDescription = "GetTableNames.Injection.INCLUDE_VIEW")
  private boolean includeView;

  @HopMetadataProperty(
      key = "includeProcedure",
      injectionKey = "INCLUDEPROCEDURE",
      injectionKeyDescription = "GetTableNames.Injection.INCLUDE_PROCEDURE")
  private boolean includeProcedure;

  @HopMetadataProperty(
      key = "includeSynonym",
      injectionKey = "INCLUDESYNONYM",
      injectionKeyDescription = "GetTableNames.Injection.INCLUDE_SYNONYM")
  private boolean includeSynonym;

  @HopMetadataProperty(
      key = "addSchemaInOutput",
      injectionKey = "ADDSCHEMAINOUTPUT",
      injectionKeyDescription = "GetTableNames.Injection.ADD_SCHEMA_IN_OUTPUT")
  private boolean addSchemaInOutput;

  @HopMetadataProperty(
      key = "dynamicSchema",
      injectionKey = "DYNAMICSCHEMA",
      injectionKeyDescription = "GetTableNames.Injection.DYNAMIC_SCHEMA")
  private boolean dynamicSchema;

  @HopMetadataProperty(
      key = "schemaNameField",
      injectionKey = "SCHEMANAMEFIELD",
      injectionKeyDescription = "GetTableNames.Injection.SCHEMA_NAME_FIELD")
  private String schemaNameField;

  public GetTableNamesMeta() {
    super();
  }

  public GetTableNamesMeta(GetTableNamesMeta m) {
    this();
    this.connection = m.connection;
    this.schemaName = m.schemaName;
    this.tableNameFieldName = m.tableNameFieldName;
    this.sqlCreationFieldName = m.sqlCreationFieldName;
    this.objectTypeFieldName = m.objectTypeFieldName;
    this.isSystemObjectFieldName = m.isSystemObjectFieldName;
    this.includeCatalog = m.includeCatalog;
    this.includeSchema = m.includeSchema;
    this.includeTable = m.includeTable;
    this.includeView = m.includeView;
    this.includeProcedure = m.includeProcedure;
    this.includeSynonym = m.includeSynonym;
    this.addSchemaInOutput = m.addSchemaInOutput;
    this.dynamicSchema = m.dynamicSchema;
    this.schemaNameField = m.schemaNameField;
  }

  @Override
  public GetTableNamesMeta clone() {
    return new GetTableNamesMeta(this);
  }

  @Override
  public void setDefault() {
    connection = null;
    schemaName = null;
    includeCatalog = false;
    includeSchema = false;
    includeTable = true;
    includeProcedure = true;
    includeView = true;
    includeSynonym = true;
    addSchemaInOutput = false;
    tableNameFieldName = "tableName";
    sqlCreationFieldName = null;
    objectTypeFieldName = "type";
    isSystemObjectFieldName = "isSystem";
    dynamicSchema = false;
    schemaNameField = null;
  }

  @Override
  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    String realTableName = variables.resolve(tableNameFieldName);
    if (StringUtils.isNotEmpty(realTableName)) {
      IValueMeta v = new ValueMetaString(realTableName);
      v.setLength(500);
      v.setPrecision(-1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }

    String realObjectType = variables.resolve(objectTypeFieldName);
    if (StringUtils.isNotEmpty(realObjectType)) {
      IValueMeta v = new ValueMetaString(realObjectType);
      v.setLength(500);
      v.setPrecision(-1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    String systemObject = variables.resolve(isSystemObjectFieldName);
    if (StringUtils.isNotEmpty(systemObject)) {
      IValueMeta v = new ValueMetaBoolean(systemObject);
      v.setOrigin(name);
      r.addValueMeta(v);
    }

    String realSqlCreation = variables.resolve(sqlCreationFieldName);
    if (StringUtils.isNotEmpty(realSqlCreation)) {
      IValueMeta v = new ValueMetaString(realSqlCreation);
      v.setLength(500);
      v.setPrecision(-1);
      v.setOrigin(name);
      r.addValueMeta(v);
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

    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(connection, variables);
    if (databaseMeta == null) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG,
                  "GetTableNamesMeta.CheckResult.InvalidConnection",
                  variables.resolve(connection)),
              transformMeta);
      remarks.add(cr);
    }
    if (Utils.isEmpty(tableNameFieldName)) {
      errorMessage =
          BaseMessages.getString(PKG, "GetTableNamesMeta.CheckResult.TableNameFieldNameMissing");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage =
          BaseMessages.getString(PKG, "GetTableNamesMeta.CheckResult.TableNameFieldNameOK");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }

    if (!isIncludeCatalog()
        && !isIncludeSchema()
        && !isIncludeTable()
        && !isIncludeView()
        && !isIncludeProcedure()
        && !isIncludeSynonym()) {
      errorMessage =
          BaseMessages.getString(PKG, "GetTableNamesMeta.CheckResult.IncludeAtLeastOneType");
      remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta));
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0 && !isDynamicSchema()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "GetTableNamesMeta.CheckResult.NoInputReceived"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "GetTableNamesMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
    }
    remarks.add(cr);
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  public String getConnection() {
    return connection;
  }

  public void setConnection(String connection) {
    this.connection = connection;
  }

  /**
   * @return Returns the resultName.
   */
  public String getTableNameFieldName() {
    return tableNameFieldName;
  }

  /**
   * @param tablenamefieldname The tablenamefieldname to set.
   */
  public void setTableNameFieldName(String tablenamefieldname) {
    this.tableNameFieldName = tablenamefieldname;
  }

  /**
   * @return Returns the resultName.
   */
  public String getSqlCreationFieldName() {
    return sqlCreationFieldName;
  }

  /**
   * @param sqlcreationfieldname The sqlcreationfieldname to set.
   */
  public void setSqlCreationFieldName(String sqlcreationfieldname) {
    this.sqlCreationFieldName = sqlcreationfieldname;
  }

  /**
   * @return Returns the resultName.
   */
  public String getSchemaName() {
    return schemaName;
  }

  /**
   * @param schemaname The schemaname to set.
   */
  public void setSchemaName(String schemaname) {
    this.schemaName = schemaname;
  }

  /**
   * @param objecttypefieldname The objecttypefieldname to set.
   */
  public void setObjectTypeFieldName(String objecttypefieldname) {
    this.objectTypeFieldName = objecttypefieldname;
  }

  /**
   * Gets isSystemObjectFieldName
   *
   * @return value of isSystemObjectFieldName
   */
  public String getIsSystemObjectFieldName() {
    return isSystemObjectFieldName;
  }

  /**
   * Sets isSystemObjectFieldName
   *
   * @param isSystemObjectFieldName value of isSystemObjectFieldName
   */
  public void setIsSystemObjectFieldName(String isSystemObjectFieldName) {
    this.isSystemObjectFieldName = isSystemObjectFieldName;
  }

  /**
   * @return Returns the objecttypefieldname.
   */
  public String getObjectTypeFieldName() {
    return objectTypeFieldName;
  }

  /**
   * @return Returns the issystemobjectfieldname.
   */
  public String isSystemObjectFieldName() {
    return isSystemObjectFieldName;
  }

  /**
   * @return Returns the schema name field.
   */
  public String getSchemaNameField() {
    return schemaNameField;
  }

  /**
   * @param schemaNameField The schema name field to set.
   */
  public void setSchemaNameField(String schemaNameField) {
    this.schemaNameField = schemaNameField;
  }

  public void setIncludeTable(boolean includetable) {
    this.includeTable = includetable;
  }

  public boolean isIncludeTable() {
    return this.includeTable;
  }

  public void setIncludeSchema(boolean includeSchema) {
    this.includeSchema = includeSchema;
  }

  public boolean isIncludeSchema() {
    return this.includeSchema;
  }

  public void setIncludeCatalog(boolean includeCatalog) {
    this.includeCatalog = includeCatalog;
  }

  public boolean isIncludeCatalog() {
    return this.includeCatalog;
  }

  public void setIncludeView(boolean includeView) {
    this.includeView = includeView;
  }

  public boolean isIncludeView() {
    return this.includeView;
  }

  public void setIncludeProcedure(boolean includeProcedure) {
    this.includeProcedure = includeProcedure;
  }

  public boolean isIncludeProcedure() {
    return this.includeProcedure;
  }

  public void setIncludeSynonym(boolean includeSynonym) {
    this.includeSynonym = includeSynonym;
  }

  public boolean isIncludeSynonym() {
    return this.includeSynonym;
  }

  public void setDynamicSchema(boolean dynamicSchema) {
    this.dynamicSchema = dynamicSchema;
  }

  public boolean isDynamicSchema() {
    return this.dynamicSchema;
  }

  public void setAddSchemaInOutput(boolean addSchemaInOutput) {
    this.addSchemaInOutput = addSchemaInOutput;
  }

  public boolean isAddSchemaInOutput() {
    return this.addSchemaInOutput;
  }
}
