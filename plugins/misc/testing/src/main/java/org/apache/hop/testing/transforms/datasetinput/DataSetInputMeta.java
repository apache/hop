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

package org.apache.hop.testing.transforms.datasetinput;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.testing.DataSet;

@Getter
@Setter
@Transform(
    id = "DataSetInput",
    description = "Read static data from a data set defined in the metadata",
    name = "Data Set Input",
    image = "dataset.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "i18n::DataSetInputMeta.keyword",
    documentationUrl = "/pipeline/transforms/datasetinput.html")
public class DataSetInputMeta extends BaseTransformMeta<DataSetInput, DataSetInputData> {
  @HopMetadataProperty private String dataSetName;

  public DataSetInputMeta() {
    super();
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

    inputRowMeta.clear();

    String realDataSetName = variables.resolve(dataSetName);
    try {
      IHopMetadataSerializer<DataSet> serializer = metadataProvider.getSerializer(DataSet.class);
      DataSet dataSet = serializer.load(realDataSetName);
      inputRowMeta.addRowMeta(dataSet.getSetRowMeta());
    } catch (Exception e) {
      throw new HopTransformException(
          "Error getting row metadata from dataset " + realDataSetName, e);
    }
  }
}
