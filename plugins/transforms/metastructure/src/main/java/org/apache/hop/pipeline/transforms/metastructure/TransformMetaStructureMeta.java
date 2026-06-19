/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.metastructure;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Getter
@Setter
@Transform(
    id = "TransformMetaStructure",
    name = "i18n::TransformMetaStructure.Transform.Name",
    description = "i18n::TransformMetaStructure.Transform.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Utility",
    keywords = "i18n::TransformMetaStructureMeta.keyword",
    documentationUrl = "/pipeline/transforms/metastructure.html",
    image = "metastructure.svg")
public class TransformMetaStructureMeta
    extends BaseTransformMeta<TransformMetaStructure, TransformMetaStructureData> {
  // needed by Translator
  private static final Class<?> PKG = TransformMetaStructureMeta.class;

  @HopMetadataProperty(defaultBoolean = true)
  private boolean includePositionField;

  @HopMetadataProperty(key = "positionFieldname")
  private String positionFieldName;

  @HopMetadataProperty(key = "includeFieldnameField", defaultBoolean = true)
  private boolean includeFieldNameField;

  @HopMetadataProperty(key = "fieldFieldname")
  private String fieldFieldName;

  @HopMetadataProperty(defaultBoolean = true)
  private boolean includeCommentsField;

  @HopMetadataProperty(key = "commentsFieldname")
  private String commentsFieldName;

  @HopMetadataProperty(defaultBoolean = true)
  private boolean includeTypeField;

  @HopMetadataProperty(key = "typeFieldname")
  private String typeFieldName;

  @HopMetadataProperty(defaultBoolean = true)
  private boolean includeLengthField;

  @HopMetadataProperty(key = "lengthFieldname")
  private String lengthFieldName;

  @HopMetadataProperty(defaultBoolean = true)
  private boolean includePrecisionField;

  @HopMetadataProperty(key = "precisionFieldname")
  private String precisionFieldName;

  @HopMetadataProperty(defaultBoolean = true)
  private boolean includeMaskField;

  @HopMetadataProperty(key = "maskFieldname")
  private String maskFieldName;

  @HopMetadataProperty(defaultBoolean = true)
  private boolean includeOriginField;

  @HopMetadataProperty(key = "originFieldname")
  private String originFieldName;

  @HopMetadataProperty(defaultBoolean = true)
  private boolean outputRowcount;

  @HopMetadataProperty private String rowcountField;

  public TransformMetaStructureMeta() {
    includePositionField = true;
    positionFieldName = BaseMessages.getString(PKG, "TransformMetaStructureMeta.PositionName");
    includeFieldNameField = true;
    fieldFieldName = BaseMessages.getString(PKG, "TransformMetaStructureMeta.FieldName");
    includeCommentsField = true;
    commentsFieldName = BaseMessages.getString(PKG, "TransformMetaStructureMeta.CommentsName");
    includeTypeField = true;
    typeFieldName = BaseMessages.getString(PKG, "TransformMetaStructureMeta.TypeName");
    includeLengthField = true;
    lengthFieldName = BaseMessages.getString(PKG, "TransformMetaStructureMeta.LengthName");
    includePrecisionField = true;
    precisionFieldName = BaseMessages.getString(PKG, "TransformMetaStructureMeta.PrecisionName");
    includeMaskField = true;
    maskFieldName = BaseMessages.getString(PKG, "TransformMetaStructureMeta.MaskName");
    includeOriginField = true;
    originFieldName = BaseMessages.getString(PKG, "TransformMetaStructureMeta.OriginName");
  }

  @Override
  public TransformMetaStructureMeta clone() {
    return (TransformMetaStructureMeta) super.clone();
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
    cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, "Not implemented", transformMeta);
    remarks.add(cr);
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
    // we create a new output row structure - clear r
    inputRowMeta.clear();

    this.setDefault();
    // create the new fields
    // Position
    if (includePositionField) {
      IValueMeta positionFieldValue = new ValueMetaInteger(positionFieldName);
      positionFieldValue.setOrigin(name);
      inputRowMeta.addValueMeta(positionFieldValue);
    }
    // field name
    if (includeFieldNameField) {
      IValueMeta nameFieldValue = new ValueMetaString(fieldFieldName);
      nameFieldValue.setOrigin(name);
      inputRowMeta.addValueMeta(nameFieldValue);
    }
    // comments
    if (includeCommentsField) {
      IValueMeta commentsFieldValue = new ValueMetaString(commentsFieldName);
      commentsFieldValue.setOrigin(name);
      inputRowMeta.addValueMeta(commentsFieldValue);
    }
    // Type
    if (includeTypeField) {
      IValueMeta typeFieldValue = new ValueMetaString(typeFieldName);
      typeFieldValue.setOrigin(name);
      inputRowMeta.addValueMeta(typeFieldValue);
    }
    // Length
    if (includeLengthField) {
      IValueMeta lengthFieldValue = new ValueMetaInteger(lengthFieldName);
      lengthFieldValue.setOrigin(name);
      inputRowMeta.addValueMeta(lengthFieldValue);
    }
    // Precision
    if (includePrecisionField) {
      IValueMeta precisionFieldValue = new ValueMetaInteger(precisionFieldName);
      precisionFieldValue.setOrigin(name);
      inputRowMeta.addValueMeta(precisionFieldValue);
    }
    // Mask
    if (includeMaskField) {
      IValueMeta maskFieldValue = new ValueMetaString(maskFieldName);
      maskFieldValue.setOrigin(name);
      inputRowMeta.addValueMeta(maskFieldValue);
    }
    // Origin
    if (includeOriginField) {
      IValueMeta originFieldValue = new ValueMetaString(originFieldName);
      originFieldValue.setOrigin(name);
      inputRowMeta.addValueMeta(originFieldValue);
    }

    if (isOutputRowcount()) {
      // RowCount
      IValueMeta v = new ValueMetaInteger(this.getRowcountField());
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }
  }
}
