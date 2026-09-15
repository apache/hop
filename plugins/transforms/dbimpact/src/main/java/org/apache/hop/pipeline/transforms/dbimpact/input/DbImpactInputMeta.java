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

package org.apache.hop.pipeline.transforms.dbimpact.input;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMetaBuilder;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "DbImpactInput",
    image = "ui/images/database.svg",
    name = "i18n::DbImpactInputMeta.Name",
    description = "i18n::DbImpactInputMeta.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "i18n::DbImpactInputMeta.keyword",
    documentationUrl = "/pipeline/transforms/dbimpactinput.html")
@Getter
@Setter
public class DbImpactInputMeta extends BaseTransformMeta<DbImpactInput, DbImpactInputData> {

  @HopMetadataProperty(
      key = "filename-field",
      injectionKeyDescription = "DbImpactInputMeta.FileNameField.Label")
  private String fileNameField;

  public DbImpactInputMeta() {}

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    // Always start with an empty row
    //
    rowMeta.clear();

    // Reflect the output from the DatabaseImpact class
    //
    RowMetaBuilder rowBuilder = new RowMetaBuilder();
    rowBuilder
        .addString("Type")
        .addString("PipelineName")
        .addString("PipelineFileName")
        .addString("TransformName")
        .addString("DatabaseName")
        .addString("DatabaseTable")
        .addString("TableField")
        .addString("FieldName")
        .addString("FieldOrigin")
        .addString("SQL")
        .addString("Remark");
    rowMeta.addRowMeta(rowBuilder.build());
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
