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

/*
 * Created on 03-Juin-2008
 *
 */

@Transform(
    id = "TableExists",
    image = "tableexists.svg",
    name = "i18n::BaseTransform.TypeLongDesc.TableExists",
    description = "i18n::BaseTransform.TypeTooltipDesc.TableExists",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Lookup",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/tableexists.html")
public class TableExistsMeta extends BaseTransformMeta
    implements ITransformMeta<TableExists, TableExistsData> {
  private static final Class<?> PKG = TableExistsMeta.class; // For Translator

  /** database connection */
  private DatabaseMeta database;

  /** dynamuc tablename */
  private String tablenamefield;

  /** function result: new value name */
  private String resultfieldname;

  private String schemaname;

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
  public String getDynamicTablenameField() {
    return tablenamefield;
  }

  /** @param tablenamefield The tablenamefield to set. */
  public void setDynamicTablenameField(String tablenamefield) {
    this.tablenamefield = tablenamefield;
  }

  /** @return Returns the resultName. */
  public String getResultFieldName() {
    return resultfieldname;
  }

  /** @param resultfieldname The resultfieldname to set. */
  public void setResultFieldName(String resultfieldname) {
    this.resultfieldname = resultfieldname;
  }

  public String getSchemaname() {
    return schemaname;
  }

  public void setSchemaname(String schemaname) {
    this.schemaname = schemaname;
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode, metadataProvider);
  }

  public Object clone() {
    TableExistsMeta retval = (TableExistsMeta) super.clone();

    return retval;
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

  public void setDefault() {
    database = null;
    schemaname = null;
    resultfieldname = "result";
  }

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

  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append(
        "    " + XmlHandler.addTagValue("connection", database == null ? "" : database.getName()));
    retval.append("    " + XmlHandler.addTagValue("tablenamefield", tablenamefield));
    retval.append("    " + XmlHandler.addTagValue("resultfieldname", resultfieldname));
    retval.append("    " + XmlHandler.addTagValue("schemaname", schemaname));

    return retval.toString();
  }

  private void readData(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      String con = XmlHandler.getTagValue(transformNode, "connection");
      database = DatabaseMeta.loadDatabase(metadataProvider, con);
      tablenamefield = XmlHandler.getTagValue(transformNode, "tablenamefield");
      resultfieldname = XmlHandler.getTagValue(transformNode, "resultfieldname");
      schemaname = XmlHandler.getTagValue(transformNode, "schemaname");

    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "TableExistsMeta.Exception.UnableToReadTransformMeta"), e);
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
      errorMessage = BaseMessages.getString(PKG, "TableExistsMeta.CheckResult.InvalidConnection");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    }
    if (Utils.isEmpty(resultfieldname)) {
      errorMessage = BaseMessages.getString(PKG, "TableExistsMeta.CheckResult.ResultFieldMissing");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage = BaseMessages.getString(PKG, "TableExistsMeta.CheckResult.ResultFieldOK");
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }
    if (Utils.isEmpty(tablenamefield)) {
      errorMessage = BaseMessages.getString(PKG, "TableExistsMeta.CheckResult.TableFieldMissing");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage = BaseMessages.getString(PKG, "TableExistsMeta.CheckResult.TableFieldOK");
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }
    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "TableExistsMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "TableExistsMeta.CheckResult.NoInpuReceived"),
              transformMeta);
      remarks.add(cr);
    }
  }

  public TableExistsData getTransformData() {
    return new TableExistsData();
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
