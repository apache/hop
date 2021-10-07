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
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.List;

@Transform(
    id = "TransformMetaStructure",
    name = "i18n::TransformMetaStructure.Transform.Name",
    description = "i18n::TransformMetaStructure.Transform.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Utility",
        keywords = "i18n::TransformMetaStructureMeta.keyword",
    documentationUrl = "/pipeline/transforms/metastructure.html",
    image = "MetaStructure.svg")
public class TransformMetaStructureMeta extends BaseTransformMeta
    implements ITransformMeta<TransformMetaStructure, TransformMetaStructureData> {

  private static Class<?> PKG = TransformMetaStructureMeta.class; // needed by Translator

  @HopMetadataProperty(defaultBoolean = true)
  private boolean includePositionField;

  @HopMetadataProperty private String positionFieldname;

  @HopMetadataProperty(defaultBoolean = true)
  private boolean includeFieldnameField;

  @HopMetadataProperty private String fieldFieldname;

  @HopMetadataProperty(defaultBoolean = true)
  private boolean includeCommentsField;

  @HopMetadataProperty private String commentsFieldname;

  @HopMetadataProperty(defaultBoolean = true)
  private boolean includeTypeField;

  @HopMetadataProperty private String typeFieldname;

  @HopMetadataProperty(defaultBoolean = true)
  private boolean includeLengthField;

  @HopMetadataProperty private String lengthFieldname;

  @HopMetadataProperty(defaultBoolean = true)
  private boolean includePrecisionField;

  @HopMetadataProperty private String precisionFieldname;

  @HopMetadataProperty(defaultBoolean = true)
  private boolean includeOriginField;

  @HopMetadataProperty private String originFieldname;

  @HopMetadataProperty(defaultBoolean = true)
  private boolean outputRowcount;

  @HopMetadataProperty private String rowcountField;

  public TransformMetaStructureMeta() {
    includePositionField = true;
    positionFieldname = BaseMessages.getString(PKG, "TransformMetaStructureMeta.PositionName");
    includeFieldnameField = true;
    fieldFieldname = BaseMessages.getString(PKG, "TransformMetaStructureMeta.FieldName");
    includeCommentsField = true;
    commentsFieldname = BaseMessages.getString(PKG, "TransformMetaStructureMeta.CommentsName");
    includeTypeField = true;
    typeFieldname = BaseMessages.getString(PKG, "TransformMetaStructureMeta.TypeName");
    includeLengthField = true;
    lengthFieldname = BaseMessages.getString(PKG, "TransformMetaStructureMeta.LengthName");
    includePrecisionField = true;
    precisionFieldname = BaseMessages.getString(PKG, "TransformMetaStructureMeta.PrecisionName");
    includeOriginField = true;
    originFieldname = BaseMessages.getString(PKG, "TransformMetaStructureMeta.OriginName");
  }

  @Override
  public TransformMetaStructureMeta clone() {
    return (TransformMetaStructureMeta) super.clone();
  }

  @Override
  public TransformMetaStructure createTransform(
      TransformMeta transformMeta,
      TransformMetaStructureData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new TransformMetaStructure(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public TransformMetaStructureData getTransformData() {
    return new TransformMetaStructureData();
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
      IValueMeta positionFieldValue = new ValueMetaInteger(positionFieldname);
      positionFieldValue.setOrigin(name);
      inputRowMeta.addValueMeta(positionFieldValue);
    }
    // field name
    if (includeFieldnameField) {
      IValueMeta nameFieldValue = new ValueMetaString(fieldFieldname);
      nameFieldValue.setOrigin(name);
      inputRowMeta.addValueMeta(nameFieldValue);
    }
    // comments
    if (includeCommentsField) {
      IValueMeta commentsFieldValue = new ValueMetaString(commentsFieldname);
      commentsFieldValue.setOrigin(name);
      inputRowMeta.addValueMeta(commentsFieldValue);
    }
    // Type
    if (includeTypeField) {
      IValueMeta typeFieldValue = new ValueMetaString(typeFieldname);
      typeFieldValue.setOrigin(name);
      inputRowMeta.addValueMeta(typeFieldValue);
    }
    // Length
    if (includeLengthField) {
      IValueMeta lengthFieldValue = new ValueMetaInteger(lengthFieldname);
      lengthFieldValue.setOrigin(name);
      inputRowMeta.addValueMeta(lengthFieldValue);
    }
    // Precision
    if (includePrecisionField) {
      IValueMeta precisionFieldValue = new ValueMetaInteger(precisionFieldname);
      precisionFieldValue.setOrigin(name);
      inputRowMeta.addValueMeta(precisionFieldValue);
    }
    // Origin
    if (includeOriginField) {
      IValueMeta originFieldValue = new ValueMetaString(originFieldname);
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

  public boolean isOutputRowcount() {
    return outputRowcount;
  }

  public void setOutputRowcount(boolean outputRowcount) {
    this.outputRowcount = outputRowcount;
  }

  public String getRowcountField() {
    return rowcountField;
  }

  public void setRowcountField(String rowcountField) {
    this.rowcountField = rowcountField;
  }

  public String getFieldFieldname() {
    return fieldFieldname;
  }

  public void setFieldFieldname(String fieldFieldname) {
    this.fieldFieldname = fieldFieldname;
  }

  public String getCommentsFieldname() {
    return commentsFieldname;
  }

  public void setCommentsFieldname(String commentsFieldname) {
    this.commentsFieldname = commentsFieldname;
  }

  public String getTypeFieldname() {
    return typeFieldname;
  }

  public void setTypeFieldname(String typeFieldname) {
    this.typeFieldname = typeFieldname;
  }

  public String getPositionFieldname() {
    return positionFieldname;
  }

  public void setPositionFieldname(String positionFieldname) {
    this.positionFieldname = positionFieldname;
  }

  public String getLengthFieldname() {
    return lengthFieldname;
  }

  public void setLengthFieldname(String lengthFieldname) {
    this.lengthFieldname = lengthFieldname;
  }

  public String getPrecisionFieldname() {
    return precisionFieldname;
  }

  public void setPrecisionFieldname(String precisionFieldname) {
    this.precisionFieldname = precisionFieldname;
  }

  public String getOriginFieldname() {
    return originFieldname;
  }

  public void setOriginFieldname(String originFieldname) {
    this.originFieldname = originFieldname;
  }

  public boolean isIncludePositionField() {
    return includePositionField;
  }

  public void setIncludePositionField(boolean includePositionField) {
    this.includePositionField = includePositionField;
  }

  public boolean isIncludeFieldnameField() {
    return includeFieldnameField;
  }

  public void setIncludeFieldnameField(boolean includeFieldnameField) {
    this.includeFieldnameField = includeFieldnameField;
  }

  public boolean isIncludeCommentsField() {
    return includeCommentsField;
  }

  public void setIncludeCommentsField(boolean includeCommentsField) {
    this.includeCommentsField = includeCommentsField;
  }

  public boolean isIncludeTypeField() {
    return includeTypeField;
  }

  public void setIncludeTypeField(boolean includeTypeField) {
    this.includeTypeField = includeTypeField;
  }

  public boolean isIncludeLengthField() {
    return includeLengthField;
  }

  public void setIncludeLengthField(boolean includeLengthField) {
    this.includeLengthField = includeLengthField;
  }

  public boolean isIncludePrecisionField() {
    return includePrecisionField;
  }

  public void setIncludePrecisionField(boolean includePrecisionField) {
    this.includePrecisionField = includePrecisionField;
  }

  public boolean isIncludeOriginField() {
    return includeOriginField;
  }

  public void setIncludeOriginField(boolean includeOriginField) {
    this.includeOriginField = includeOriginField;
  }
}
