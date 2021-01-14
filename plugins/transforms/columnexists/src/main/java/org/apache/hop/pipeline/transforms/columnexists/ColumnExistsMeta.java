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

package org.apache.hop.pipeline.transforms.columnexists;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
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

@Transform(
    id = "ColumnExists",
    image = "columnexists.svg",
    name = "i18n::ColumnExists.Name",
    description = "i18n::ColumnExists.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Lookup",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/columnexists.html")
public class ColumnExistsMeta extends BaseTransformMeta
    implements ITransformMeta<ColumnExists, ColumnExistsData> {

  private static final Class<?> PKG = ColumnExistsMeta.class; // For Translator

  /** database connection */
  private DatabaseMeta database;

  private String schemaname;

  private String tableName;

  /** dynamic tablename */
  private String tablenamefield;

  /** dynamic columnname */
  private String columnnamefield;

  /** function result: new value name */
  private String resultfieldname;

  private boolean istablenameInfield;

  public ColumnExistsMeta() {
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

  /** @return Returns the tablenamefield. */
  public String getDynamicTablenameField() {
    return tablenamefield;
  }

  /** @return Returns the tablename. */
  public String getTablename() {
    return tableName;
  }

  /** @param tableName The tablename to set. */
  public void setTablename(String tableName) {
    this.tableName = tableName;
  }

  /** @return Returns the schemaname. */
  public String getSchemaname() {
    return schemaname;
  }

  /** @param schemaname The schemaname to set. */
  public void setSchemaname(String schemaname) {
    this.schemaname = schemaname;
  }

  /** @param tablenamefield The tablenamefield to set. */
  public void setDynamicTablenameField(String tablenamefield) {
    this.tablenamefield = tablenamefield;
  }

  /** @return Returns the columnnamefield. */
  public String getDynamicColumnnameField() {
    return columnnamefield;
  }

  /** @param columnnamefield The columnnamefield to set. */
  public void setDynamicColumnnameField(String columnnamefield) {
    this.columnnamefield = columnnamefield;
  }

  /** @return Returns the resultName. */
  public String getResultFieldName() {
    return resultfieldname;
  }

  /** @param resultfieldname The resultfieldname to set. */
  public void setResultFieldName(String resultfieldname) {
    this.resultfieldname = resultfieldname;
  }

  public boolean isTablenameInField() {
    return istablenameInfield;
  }

  /** @param isTablenameInField the isTablenameInField to set */
  public void setTablenameInField(boolean isTablenameInField) {
    this.istablenameInfield = isTablenameInField;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode, metadataProvider);
  }

  public Object clone() {
    ColumnExistsMeta retval = (ColumnExistsMeta) super.clone();

    return retval;
  }

  @Override
  public void setDefault() {
    database = null;
    schemaname = null;
    tableName = null;
    istablenameInfield = false;
    resultfieldname = "result";
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // Output field (String)
    if (!Utils.isEmpty(resultfieldname)) {
      IValueMeta v = new ValueMetaBoolean(variables.resolve(resultfieldname));
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder();
    retval.append(
        "    " + XmlHandler.addTagValue("connection", database == null ? "" : database.getName()));
    retval.append("    " + XmlHandler.addTagValue("tablename", tableName));
    retval.append("    " + XmlHandler.addTagValue("schemaname", schemaname));
    retval.append("    " + XmlHandler.addTagValue("istablenameInfield", istablenameInfield));
    retval.append("    " + XmlHandler.addTagValue("tablenamefield", tablenamefield));
    retval.append("    " + XmlHandler.addTagValue("columnnamefield", columnnamefield));
    retval.append("      " + XmlHandler.addTagValue("resultfieldname", resultfieldname));
    return retval.toString();
  }

  private void readData(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      String con = XmlHandler.getTagValue(transformNode, "connection");
      database = DatabaseMeta.loadDatabase(metadataProvider, con);
      tableName = XmlHandler.getTagValue(transformNode, "tablename");
      schemaname = XmlHandler.getTagValue(transformNode, "schemaname");
      istablenameInfield =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "istablenameInfield"));
      tablenamefield = XmlHandler.getTagValue(transformNode, "tablenamefield");
      columnnamefield = XmlHandler.getTagValue(transformNode, "columnnamefield");
      resultfieldname =
          XmlHandler.getTagValue(transformNode, "resultfieldname"); // Optional, can be null
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "ColumnExistsMeta.Exception.UnableToReadTransformMeta"), e);
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

    if (database == null) {
      errorMessage = BaseMessages.getString(PKG, "ColumnExistsMeta.CheckResult.InvalidConnection");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    }
    if (Utils.isEmpty(resultfieldname)) {
      errorMessage = BaseMessages.getString(PKG, "ColumnExistsMeta.CheckResult.ResultFieldMissing");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
    } else {
      errorMessage = BaseMessages.getString(PKG, "ColumnExistsMeta.CheckResult.ResultFieldOK");
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
    }
    remarks.add(cr);
    if (istablenameInfield) {
      if (Utils.isEmpty(tablenamefield)) {
        errorMessage =
            BaseMessages.getString(PKG, "ColumnExistsMeta.CheckResult.TableFieldMissing");
        cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      } else {
        errorMessage = BaseMessages.getString(PKG, "ColumnExistsMeta.CheckResult.TableFieldOK");
        cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      }
      remarks.add(cr);
    } else {
      if (Utils.isEmpty(tableName)) {
        errorMessage = BaseMessages.getString(PKG, "ColumnExistsMeta.CheckResult.TablenameMissing");
        cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      } else {
        errorMessage = BaseMessages.getString(PKG, "ColumnExistsMeta.CheckResult.TablenameOK");
        cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      }
      remarks.add(cr);
    }

    if (Utils.isEmpty(columnnamefield)) {
      errorMessage =
          BaseMessages.getString(PKG, "ColumnExistsMeta.CheckResult.ColumnNameFieldMissing");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
    } else {
      errorMessage = BaseMessages.getString(PKG, "ColumnExistsMeta.CheckResult.ColumnNameFieldOK");
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
    }
    remarks.add(cr);

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "ColumnExistsMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ColumnExistsMeta.CheckResult.NoInpuReceived"),
              transformMeta);
    }
    remarks.add(cr);
  }

  @Override
  public ColumnExists createTransform(
      TransformMeta transformMeta,
      ColumnExistsData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new ColumnExists(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  @Override
  public ColumnExistsData getTransformData() {
    return new ColumnExistsData();
  }

  @Override
  public DatabaseMeta[] getUsedDatabaseConnections() {
    if (database != null) {
      return new DatabaseMeta[] {database};
    } else {
      return super.getUsedDatabaseConnections();
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
