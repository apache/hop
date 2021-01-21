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

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 03-Juin-2008
 *
 */
@InjectionSupported(
    localizationPrefix = "GetTableNames.Injection.",
    groups = {"FIELDS", "SETTINGS", "OUTPUT"})
@Transform(
    id = "GetTableNames",
    image = "gettablenames.svg",
    name = "i18n::BaseTransform.TypeLongDesc.GetTableNames",
    description = "i18n::BaseTransform.TypeTooltipDesc.GetTableNames",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/gettablenames.html")
public class GetTableNamesMeta extends BaseTransformMeta
    implements ITransformMeta<GetTableNames, GetTableNamesData> {
  private static final Class<?> PKG = GetTableNamesMeta.class; // For Translator

  /** database connection */
  private DatabaseMeta database;

  @Injection(name = "SCHEMANAME", group = "FIELDS")
  private String schemaname;

  @Injection(name = "TABLENAMEFIELDNAME", group = "OUTPUT")
  /** function result: new value name */
  private String tablenamefieldname;

  @Injection(name = "SQLCREATIONFIELDNAME", group = "OUTPUT")
  private String sqlcreationfieldname;

  @Injection(name = "OBJECTTYPEFIELDNAME", group = "OUTPUT")
  private String objecttypefieldname;

  @Injection(name = "ISSYSTEMOBJECTFIELDNAME", group = "OUTPUT")
  private String issystemobjectfieldname;

  @Injection(name = "INCLUDECATALOG", group = "SETTINGS")
  private boolean includeCatalog;

  @Injection(name = "INCLUDESCHEMA", group = "SETTINGS")
  private boolean includeSchema;

  @Injection(name = "INCLUDETABLE", group = "SETTINGS")
  private boolean includeTable;

  @Injection(name = "INCLUDEVIEW", group = "SETTINGS")
  private boolean includeView;

  @Injection(name = "INCLUDEPROCEDURE", group = "SETTINGS")
  private boolean includeProcedure;

  @Injection(name = "INCLUDESYNONYM", group = "SETTINGS")
  private boolean includeSynonym;

  @Injection(name = "ADDSCHEMAINOUTPUT", group = "SETTINGS")
  private boolean addSchemaInOutput;

  @Injection(name = "DYNAMICSCHEMA", group = "FIELDS")
  private boolean dynamicSchema;

  @Injection(name = "SCHEMANAMEFIELD", group = "FIELDS")
  private String schemaNameField;

  private IHopMetadataProvider metadataProvider;

  public GetTableNamesMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the database. */
  public DatabaseMeta getDatabase() {
    return database;
  }

  /** @param database The database to set. */
  public void setDatabase(DatabaseMeta database) {
    this.database = database;
  }

  /** @return Returns the resultName. */
  public String getTablenameFieldName() {
    return tablenamefieldname;
  }

  /** @param tablenamefieldname The tablenamefieldname to set. */
  public void setTablenameFieldName(String tablenamefieldname) {
    this.tablenamefieldname = tablenamefieldname;
  }

  /** @return Returns the resultName. */
  public String getSqlCreationFieldName() {
    return sqlcreationfieldname;
  }

  /** @param sqlcreationfieldname The sqlcreationfieldname to set. */
  public void setSqlCreationFieldName(String sqlcreationfieldname) {
    this.sqlcreationfieldname = sqlcreationfieldname;
  }

  /** @return Returns the resultName. */
  public String getSchemaName() {
    return schemaname;
  }

  /** @param schemaname The schemaname to set. */
  public void setSchemaName(String schemaname) {
    this.schemaname = schemaname;
  }

  /** @param objecttypefieldname The objecttypefieldname to set. */
  public void setObjectTypeFieldName(String objecttypefieldname) {
    this.objecttypefieldname = objecttypefieldname;
  }

  /** @param issystemobjectfieldname The issystemobjectfieldname to set. */
  // TODO deprecate one of these
  public void setIsSystemObjectFieldName(String issystemobjectfieldname) {
    this.issystemobjectfieldname = issystemobjectfieldname;
  }

  public void setSystemObjectFieldName(String issystemobjectfieldname) {
    this.issystemobjectfieldname = issystemobjectfieldname;
  }

  /** @return Returns the objecttypefieldname. */
  public String getObjectTypeFieldName() {
    return objecttypefieldname;
  }

  /** @return Returns the issystemobjectfieldname. */
  public String isSystemObjectFieldName() {
    return issystemobjectfieldname;
  }

  /** @return Returns the schenameNameField. */
  public String getSchemaFieldName() {
    return schemaNameField;
  }

  /** @param schemaNameField The schemaNameField to set. */
  public void setSchemaFieldName(String schemaNameField) {
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

  public void setAddSchemaInOut(boolean addSchemaInOutput) {
    this.addSchemaInOutput = addSchemaInOutput;
  }

  public boolean isAddSchemaInOut() {
    return this.addSchemaInOutput;
  }

  @Injection(name = "CONNECTIONNAME")
  public void setConnection(String connectionName) {
    try {
      database = DatabaseMeta.loadDatabase(metadataProvider, connectionName);
    } catch (Exception e) {
      throw new RuntimeException("Unable to load connection '" + connectionName + "'", e);
    }
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode, metadataProvider);
  }

  public Object clone() {
    GetTableNamesMeta retval = (GetTableNamesMeta) super.clone();

    return retval;
  }

  public void setDefault() {
    database = null;
    schemaname = null;
    includeCatalog = false;
    includeSchema = false;
    includeTable = true;
    includeProcedure = true;
    includeView = true;
    includeSynonym = true;
    addSchemaInOutput = false;
    tablenamefieldname = "tablename";
    sqlcreationfieldname = null;
    objecttypefieldname = "type";
    issystemobjectfieldname = "is system";
    dynamicSchema = false;
    schemaNameField = null;
  }

  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    String realtablename = variables.resolve(tablenamefieldname);
    if (!Utils.isEmpty(realtablename)) {
      IValueMeta v = new ValueMetaString(realtablename);
      v.setLength(500);
      v.setPrecision(-1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }

    String realObjectType = variables.resolve(objecttypefieldname);
    if (!Utils.isEmpty(realObjectType)) {
      IValueMeta v = new ValueMetaString(realObjectType);
      v.setLength(500);
      v.setPrecision(-1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    String sysobject = variables.resolve(issystemobjectfieldname);
    if (!Utils.isEmpty(sysobject)) {
      IValueMeta v = new ValueMetaBoolean(sysobject);
      v.setOrigin(name);
      r.addValueMeta(v);
    }

    String realSqlCreation = variables.resolve(sqlcreationfieldname);
    if (!Utils.isEmpty(realSqlCreation)) {
      IValueMeta v = new ValueMetaString(realSqlCreation);
      v.setLength(500);
      v.setPrecision(-1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append(
        "    " + XmlHandler.addTagValue("connection", database == null ? "" : database.getName()));
    retval.append("    " + XmlHandler.addTagValue("schemaname", schemaname));
    retval.append("    " + XmlHandler.addTagValue("tablenamefieldname", tablenamefieldname));
    retval.append("    " + XmlHandler.addTagValue("objecttypefieldname", objecttypefieldname));
    retval.append(
        "    " + XmlHandler.addTagValue("issystemobjectfieldname", issystemobjectfieldname));
    retval.append("    " + XmlHandler.addTagValue("sqlcreationfieldname", sqlcreationfieldname));

    retval.append("    " + XmlHandler.addTagValue("includeCatalog", includeCatalog));
    retval.append("    " + XmlHandler.addTagValue("includeSchema", includeSchema));
    retval.append("    " + XmlHandler.addTagValue("includeTable", includeTable));
    retval.append("    " + XmlHandler.addTagValue("includeView", includeView));
    retval.append("    " + XmlHandler.addTagValue("includeProcedure", includeProcedure));
    retval.append("    " + XmlHandler.addTagValue("includeSynonym", includeSynonym));
    retval.append("    " + XmlHandler.addTagValue("addSchemaInOutput", addSchemaInOutput));
    retval.append("    " + XmlHandler.addTagValue("dynamicSchema", dynamicSchema));
    retval.append("    " + XmlHandler.addTagValue("schemaNameField", schemaNameField));

    return retval.toString();
  }

  private void readData(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      this.metadataProvider = metadataProvider;
      String con = XmlHandler.getTagValue(transformNode, "connection");
      database = DatabaseMeta.loadDatabase(metadataProvider, con);
      schemaname = XmlHandler.getTagValue(transformNode, "schemaname");
      tablenamefieldname = XmlHandler.getTagValue(transformNode, "tablenamefieldname");
      objecttypefieldname = XmlHandler.getTagValue(transformNode, "objecttypefieldname");
      sqlcreationfieldname = XmlHandler.getTagValue(transformNode, "sqlcreationfieldname");

      issystemobjectfieldname = XmlHandler.getTagValue(transformNode, "issystemobjectfieldname");
      includeCatalog =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "includeCatalog"));
      includeSchema = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "includeSchema"));
      includeTable = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "includeTable"));
      includeView = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "includeView"));
      includeProcedure =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "includeProcedure"));
      includeSynonym =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "includeSynonym"));
      addSchemaInOutput =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "addSchemaInOutput"));
      dynamicSchema = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "dynamicSchema"));
      schemaNameField = XmlHandler.getTagValue(transformNode, "schemaNameField");

      if (XmlHandler.getTagValue(transformNode, "schenameNameField") != null) {
        /*
         * Fix for wrong field name in the 7.0. Can be removed if we don't want to keep backward compatibility with 7.0
         * tranformations.
         */
        schemaNameField = XmlHandler.getTagValue(transformNode, "schenameNameField");
      }
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "GetTableNamesMeta.Exception.UnableToReadTransformMeta"), e);
    }
  }

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

    if (database == null) {
      errorMessage = BaseMessages.getString(PKG, "GetTableNamesMeta.CheckResult.InvalidConnection");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    }
    if (Utils.isEmpty(tablenamefieldname)) {
      errorMessage =
          BaseMessages.getString(PKG, "GetTableNamesMeta.CheckResult.TablenameFieldNameMissing");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage =
          BaseMessages.getString(PKG, "GetTableNamesMeta.CheckResult.TablenameFieldNameOK");
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0 && !isDynamicSchema()) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "GetTableNamesMeta.CheckResult.NoInpuReceived"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "GetTableNamesMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
    }
    remarks.add(cr);
  }

  public GetTableNames createTransform(
      TransformMeta transformMeta,
      GetTableNamesData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new GetTableNames(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  public GetTableNamesData getTransformData() {
    return new GetTableNamesData();
  }

  public DatabaseMeta[] getUsedDatabaseConnections() {
    if (database != null) {
      return new DatabaseMeta[] {database};
    } else {
      return super.getUsedDatabaseConnections();
    }
  }

  public boolean supportsErrorHandling() {
    return true;
  }
}
