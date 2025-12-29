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

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "DataGrid",
    image = "datagrid.svg",
    name = "i18n::DataGrid.Name",
    description = "i18n::DataGrid.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "i18n::DataGridMeta.keyword",
    documentationUrl = "/pipeline/transforms/datagrid.html")
public class DataGridMeta extends BaseTransformMeta<DataGrid, DataGridData> {

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
    for (DataGridFieldMeta dataGridField : dataGridFields) {
      try {
        if (!Utils.isEmpty(dataGridField.getName())) {
          int type = ValueMetaFactory.getIdForValueMeta(dataGridField.getType());
          if (type == IValueMeta.TYPE_NONE) {
            type = IValueMeta.TYPE_STRING;
          }
          IValueMeta v = ValueMetaFactory.createValueMeta(dataGridField.getName(), type);
          v.setLength(dataGridField.getLenght());
          v.setPrecision(dataGridField.getPrecision());
          v.setOrigin(name);
          v.setConversionMask(dataGridField.getFormat());
          v.setCurrencySymbol(dataGridField.getCurrency());
          v.setGroupingSymbol(dataGridField.getGroup());
          v.setDecimalSymbol(dataGridField.getDecimal());

          rowMeta.addValueMeta(v);
        }
      } catch (Exception e) {
        throw new HopTransformException(
            "Unable to create value of type " + dataGridField.getType(), e);
      }
    }
  }
}
