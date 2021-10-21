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

package org.apache.hop.pipeline.transforms.datagrid;

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;
import java.util.List;

@Transform(
    id = "DataGrid",
    image = "datagrid.svg",
    name = "i18n::BaseTransform.TypeLongDesc.DataGrid",
    description = "i18n::BaseTransform.TypeTooltipDesc.DataGrid",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "i18n::DataGridMeta.keyword",
    documentationUrl = "/pipeline/transforms/datagrid.html")
public class DataGridMeta extends BaseTransformMeta
    implements ITransformMeta<DataGrid, DataGridData> {

  @HopMetadataProperty(
      groupKey = "fields",
      key = "field",
      injectionGroupDescription = "DataGridDialog.Meta.Label")
  private List<DataGridFieldMeta> dataGridFields;

  @HopMetadataProperty(
      groupKey = "data",
      key = "line",
      injectionGroupDescription = "DataGridDialog.Data.Label")
  private List<DataGridDataMeta> dataLines;

  public DataGridMeta() {
    dataGridFields = new ArrayList<>();
    dataLines = new ArrayList<>();
  }

  public DataGridMeta(DataGridMeta m) {
    this.dataGridFields = m.dataGridFields;
    this.dataLines = m.dataLines;
  }

  @Override
  public DataGridMeta clone() {
    return new DataGridMeta(this);
  }

  public List<DataGridDataMeta> getDataLines() {
    return dataLines;
  }

  public void setDataLines(List<DataGridDataMeta> dataLines) {
    this.dataLines = dataLines;
  }

  public List<DataGridFieldMeta> getDataGridFields() {
    return dataGridFields;
  }

  public void setDataGridFields(List<DataGridFieldMeta> dataGridFields) {
    this.dataGridFields = dataGridFields;
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    for (int i = 0; i < dataGridFields.size(); i++) {
      try {
        if (!Utils.isEmpty(dataGridFields.get(i).getName())) {
          int type = ValueMetaFactory.getIdForValueMeta(dataGridFields.get(i).getType());
          if (type == IValueMeta.TYPE_NONE) {
            type = IValueMeta.TYPE_STRING;
          }
          IValueMeta v = ValueMetaFactory.createValueMeta(dataGridFields.get(i).getName(), type);
          v.setLength(dataGridFields.get(i).getLenght());
          v.setPrecision(dataGridFields.get(i).getPrecision());
          v.setOrigin(name);
          v.setConversionMask(dataGridFields.get(i).getFormat());
          v.setCurrencySymbol(dataGridFields.get(i).getCurrency());
          v.setGroupingSymbol(dataGridFields.get(i).getGroup());
          v.setDecimalSymbol(dataGridFields.get(i).getDecimal());

          rowMeta.addValueMeta(v);
        }
      } catch (Exception e) {
        throw new HopTransformException(
            "Unable to create value of type " + dataGridFields.get(i).getType(), e);
      }
    }
  }

  @Override
  public DataGrid createTransform(
      TransformMeta transformMeta,
      DataGridData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new DataGrid(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  @Override
  public DataGridData getTransformData() {
    return new DataGridData();
  }
}
