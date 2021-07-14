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

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.List;

@Transform(
    id = "TableExists",
    image = "tableexists.svg",
    name = "i18n::TableExists.Name",
    description = "i18n::TableExists.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Lookup",
    documentationUrl = "https://hop.apache.org/manual/latest/pipeline/transforms/tableexists.html")
public class TableExistsMeta extends BaseTransformMeta
    implements ITransformMeta<TableExists, TableExistsData> {
  private static final Class<?> PKG = TableExistsMeta.class; // For Translator

  /** database connection */
  @HopMetadataProperty(
      key = "connection",
      storeWithName = true,
      injectionKeyDescription = "TableExistsMeta.Injection.Connection")
  private DatabaseMeta database;

  /** dynamic tablename */
  @HopMetadataProperty(
      key = "tablenamefield",
      injectionKeyDescription = "TableExistsMeta.Injection.TableNameField")
  private String tableNameField;

  /** function result: new value name */
  @HopMetadataProperty(
      key = "resultfieldname",
      injectionKeyDescription = "TableExistsMeta.Injection.ResultFieldName")
  private String resultFieldName;

  @HopMetadataProperty(
      key = "schemaname",
      injectionKeyDescription = "TableExistsMeta.Injection.SchemaName")
  private String schemaName;

  public TableExistsMeta() {
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
  public String getTableNameField() {
    return tableNameField;
  }

  /** @param tablenamefield The tablenamefield to set. */
  public void setTableNameField(String tablenamefield) {
    this.tableNameField = tablenamefield;
  }

  /** @return Returns the resultName. */
  public String getResultFieldName() {
    return resultFieldName;
  }

  /** @param name The resultfieldname to set. */
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
  public TableExists createTransform(
      TransformMeta transformMeta,
      TableExistsData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new TableExists(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public void setDefault() {
    database = null;
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

    if (database == null) {
      errorMessage = BaseMessages.getString(PKG, "TableExistsMeta.CheckResult.InvalidConnection");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    }
    if (Utils.isEmpty(resultFieldName)) {
      errorMessage = BaseMessages.getString(PKG, "TableExistsMeta.CheckResult.ResultFieldMissing");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage = BaseMessages.getString(PKG, "TableExistsMeta.CheckResult.ResultFieldOK");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }
    if (Utils.isEmpty(tableNameField)) {
      errorMessage = BaseMessages.getString(PKG, "TableExistsMeta.CheckResult.TableFieldMissing");
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

  public TableExistsData getTransformData() {
    return new TableExistsData();
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
