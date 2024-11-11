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

package org.apache.hop.pipeline.transforms.tableexists;

import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
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
    id = "TableExists",
    image = "tableexists.svg",
    name = "i18n::TableExists.Name",
    description = "i18n::TableExists.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Lookup",
    keywords = "i18n::TableExistsMeta.keyword",
    documentationUrl = "/pipeline/transforms/tableexists.html",
    actionTransformTypes = {ActionTransformType.RDBMS})
public class TableExistsMeta extends BaseTransformMeta<TableExists, TableExistsData> {
  private static final Class<?> PKG = TableExistsMeta.class;

  /** database connection */
  @HopMetadataProperty(
      injectionKeyDescription = "TableExistsMeta.Injection.Connection",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String connection;

  /** dynamic tablename */
  @HopMetadataProperty(
      key = "tablenamefield",
      injectionKeyDescription = "TableExistsMeta.Injection.TableNameField",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
  private String tableNameField;

  /** function result: new value name */
  @HopMetadataProperty(
      key = "resultfieldname",
      injectionKeyDescription = "TableExistsMeta.Injection.ResultFieldName")
  private String resultFieldName;

  @HopMetadataProperty(
      key = "schemaname",
      injectionKeyDescription = "TableExistsMeta.Injection.SchemaName",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_SCHEMA)
  private String schemaName;

  public TableExistsMeta() {
    super(); // allocate BaseTransformMeta
  }

  public String getConnection() {
    return connection;
  }

  public void setConnection(String connection) {
    this.connection = connection;
  }

  /**
   * @return Returns the tablenamefield.
   */
  public String getTableNameField() {
    return tableNameField;
  }

  /**
   * @param tablenamefield The tablenamefield to set.
   */
  public void setTableNameField(String tablenamefield) {
    this.tableNameField = tablenamefield;
  }

  /**
   * @return Returns the resultName.
   */
  public String getResultFieldName() {
    return resultFieldName;
  }

  /**
   * @param name The resultfieldname to set.
   */
  public void setResultFieldName(String name) {
    this.resultFieldName = name;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public void setSchemaName(String name) {
    this.schemaName = name;
  }

  @Override
  public void setDefault() {
    connection = null;
    schemaName = null;
    resultFieldName = "result";
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
    if (!Utils.isEmpty(resultFieldName)) {
      IValueMeta v = new ValueMetaBoolean(variables.resolve(resultFieldName));
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
    Database database = null;

    try {
      DatabaseMeta databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(variables.resolve(connection));

      if (databaseMeta != null) {

        database = new Database(loggingObject, variables, databaseMeta);
        database.connect();
        if (database == null) {
          errorMessage =
              BaseMessages.getString(PKG, "TableExistsMeta.CheckResult.InvalidConnection");
          cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
          remarks.add(cr);
        }

        if (Utils.isEmpty(resultFieldName)) {
          errorMessage =
              BaseMessages.getString(PKG, "TableExistsMeta.CheckResult.ResultFieldMissing");
          cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
          remarks.add(cr);
        } else {
          errorMessage = BaseMessages.getString(PKG, "TableExistsMeta.CheckResult.ResultFieldOK");
          cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
          remarks.add(cr);
        }
        if (Utils.isEmpty(tableNameField)) {
          errorMessage =
              BaseMessages.getString(PKG, "TableExistsMeta.CheckResult.TableFieldMissing");
          cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
          remarks.add(cr);
        } else {
          errorMessage = BaseMessages.getString(PKG, "TableExistsMeta.CheckResult.TableFieldOK");
          cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
          remarks.add(cr);
        }
        // See if we have input streams leading to this transform!
        if (input.length > 0) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(
                      PKG, "TableExistsMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
                  transformMeta);
          remarks.add(cr);
        } else {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(PKG, "TableExistsMeta.CheckResult.NoInpuReceived"),
                  transformMeta);
          remarks.add(cr);
        }
      }
    } catch (HopException e) {
      errorMessage =
          BaseMessages.getString(PKG, "TableExistsMeta.CheckResult.DatabaseErrorOccurred")
              + e.getMessage();
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } finally {
      database.disconnect();
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
