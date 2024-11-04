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

import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "ColumnExists",
    image = "columnexists.svg",
    name = "i18n::ColumnExists.Name",
    description = "i18n::ColumnExists.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Lookup",
    keywords = "i18n::ColumnExistsMeta.keyword",
    documentationUrl = "/pipeline/transforms/columnexists.html",
    actionTransformTypes = {ActionTransformType.RDBMS})
public class ColumnExistsMeta extends BaseTransformMeta<ColumnExists, ColumnExistsData> {

  private static final Class<?> PKG = ColumnExistsMeta.class;

  /** database connection */
  @HopMetadataProperty(
      key = "connection",
      injectionKeyDescription = "ColumnExists.Injection.ConnectionName",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String databaseName;

  @HopMetadataProperty(
      key = "schemaname",
      injectionKeyDescription = "ColumnExists.Injection.SchemaName",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_SCHEMA)
  private String schemaname;

  @HopMetadataProperty(
      key = "tablename",
      injectionKeyDescription = "ColumnExists.Injection.TableName",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
  private String tableName;

  /** dynamic tablename */
  @HopMetadataProperty(
      key = "tablenamefield",
      injectionKeyDescription = "ColumnExists.Injection.TableNameField",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
  private String tablenamefield;

  /** dynamic columnname */
  @HopMetadataProperty(
      key = "columnnamefield",
      injectionKeyDescription = "ColumnExists.Injection.ColumnNameField",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_COLUMN)
  private String columnnamefield;

  /** function result: new value name */
  @HopMetadataProperty(
      key = "resultfieldname",
      injectionKeyDescription = "ColumnExists.Injection.ResultFieldName")
  private String resultfieldname;

  @HopMetadataProperty(
      key = "istablenameInfield",
      injectionKeyDescription = "ColumnExists.Injection.TableNameInField")
  private boolean tablenameInfield;

  public ColumnExistsMeta() {
    super(); // allocate BaseTransformMeta
  }

  /**
   * @return Returns the tablenamefield.
   */
  public String getTablenamefield() {
    return tablenamefield;
  }

  /**
   * @return Returns the tablename.
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName The tablename to set.
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * @return Returns the schemaname.
   */
  public String getSchemaname() {
    return schemaname;
  }

  /**
   * @param schemaname The schemaname to set.
   */
  public void setSchemaname(String schemaname) {
    this.schemaname = schemaname;
  }

  /**
   * @param tablenamefield The tablenamefield to set.
   */
  public void setTablenamefield(String tablenamefield) {
    this.tablenamefield = tablenamefield;
  }

  /**
   * @return Returns the columnnamefield.
   */
  public String getColumnnamefield() {
    return columnnamefield;
  }

  /**
   * @param columnnamefield The columnnamefield to set.
   */
  public void setColumnnamefield(String columnnamefield) {
    this.columnnamefield = columnnamefield;
  }

  /**
   * @return Returns the resultName.
   */
  public String getResultfieldname() {
    return resultfieldname;
  }

  /**
   * @param resultfieldname The resultfieldname to set.
   */
  public void setResultfieldname(String resultfieldname) {
    this.resultfieldname = resultfieldname;
  }

  public boolean isTablenameInfield() {
    return tablenameInfield;
  }

  /**
   * @param tablenameInfield the isTablenameInField to set
   */
  public void setTablenameInfield(boolean tablenameInfield) {
    this.tablenameInfield = tablenameInfield;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  @Override
  public Object clone() {
    ColumnExistsMeta retval = (ColumnExistsMeta) super.clone();

    return retval;
  }

  @Override
  public void setDefault() {
    databaseName = null;
    schemaname = null;
    tableName = null;
    tablenameInfield = false;
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

    if (databaseName == null) {
      errorMessage = BaseMessages.getString(PKG, "ColumnExistsMeta.CheckResult.InvalidConnection");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    }
    if (Utils.isEmpty(resultfieldname)) {
      errorMessage = BaseMessages.getString(PKG, "ColumnExistsMeta.CheckResult.ResultFieldMissing");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
    } else {
      errorMessage = BaseMessages.getString(PKG, "ColumnExistsMeta.CheckResult.ResultFieldOK");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
    }
    remarks.add(cr);
    if (tablenameInfield) {
      if (Utils.isEmpty(tablenamefield)) {
        errorMessage =
            BaseMessages.getString(PKG, "ColumnExistsMeta.CheckResult.TableFieldMissing");
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      } else {
        errorMessage = BaseMessages.getString(PKG, "ColumnExistsMeta.CheckResult.TableFieldOK");
        cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      }
      remarks.add(cr);
    } else {
      if (Utils.isEmpty(tableName)) {
        errorMessage = BaseMessages.getString(PKG, "ColumnExistsMeta.CheckResult.TablenameMissing");
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      } else {
        errorMessage = BaseMessages.getString(PKG, "ColumnExistsMeta.CheckResult.TablenameOK");
        cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      }
      remarks.add(cr);
    }

    if (Utils.isEmpty(columnnamefield)) {
      errorMessage =
          BaseMessages.getString(PKG, "ColumnExistsMeta.CheckResult.ColumnNameFieldMissing");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
    } else {
      errorMessage = BaseMessages.getString(PKG, "ColumnExistsMeta.CheckResult.ColumnNameFieldOK");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
    }
    remarks.add(cr);

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "ColumnExistsMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ColumnExistsMeta.CheckResult.NoInpuReceived"),
              transformMeta);
    }
    remarks.add(cr);
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
