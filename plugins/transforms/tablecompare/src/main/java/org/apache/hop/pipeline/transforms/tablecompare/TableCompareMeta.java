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

package org.apache.hop.pipeline.transforms.tablecompare;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
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
    id = "TableCompare",
    image = "tablecompare.svg",
    description = "i18n::BaseTransform.TypeTooltipDesc.TableCompare",
    name = "i18n::BaseTransform.TypeLongDesc.TableCompare",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Flow",
    keywords = "i18n::TableCompareMeta.keyword",
    documentationUrl = "/pipeline/transforms/tablecompare.html",
    actionTransformTypes = {ActionTransformType.RDBMS})
@Getter
@Setter
public class TableCompareMeta extends BaseTransformMeta<TableCompare, TableCompareData> {
  private static final Class<?> PKG = TableCompare.class;

  @HopMetadataProperty(
      key = "reference_connection",
      injectionKeyDescription = "TableCompareMeta.Injection.ReferenceConnection",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String referenceConnection;

  @HopMetadataProperty(
      key = "reference_schema_field",
      injectionKeyDescription = "TableCompareMeta.Injection.ReferenceSchemaField",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_SCHEMA)
  private String referenceSchemaField;

  @HopMetadataProperty(
      key = "reference_table_field",
      injectionKeyDescription = "TableCompareMeta.Injection.ReferenceTableField",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
  private String referenceTableField;

  @HopMetadataProperty(
      key = "compare_connection",
      injectionKeyDescription = "TableCompareMeta.Injection.CompareConnection",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String compareConnection;

  @HopMetadataProperty(
      key = "compare_schema_field",
      injectionKeyDescription = "TableCompareMeta.Injection.CompareSchemaField",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_SCHEMA)
  private String compareSchemaField;

  @HopMetadataProperty(
      key = "compare_table_field",
      injectionKeyDescription = "TableCompareMeta.Injection.CompareTableField",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_TABLE)
  private String compareTableField;

  @HopMetadataProperty(
      key = "key_fields_field",
      injectionKeyDescription = "TableCompareMeta.Injection.KeyFieldsField")
  private String keyFieldsField;

  @HopMetadataProperty(
      key = "exclude_fields_field",
      injectionKeyDescription = "TableCompareMeta.Injection.ExcludeFieldsField")
  private String excludeFieldsField;

  @HopMetadataProperty(
      key = "nr_errors_field",
      injectionKeyDescription = "TableCompareMeta.Injection.NrErrorsField")
  private String nrErrorsField;

  @HopMetadataProperty(
      key = "nr_records_reference_field",
      injectionKeyDescription = "TableCompareMeta.Injection.NrRecordsReferenceField")
  private String nrRecordsReferenceField;

  @HopMetadataProperty(
      key = "nr_records_compare_field",
      injectionKeyDescription = "TableCompareMeta.Injection.NrRecordsCompareField")
  private String nrRecordsCompareField;

  @HopMetadataProperty(
      key = "nr_errors_left_join_field",
      injectionKeyDescription = "TableCompareMeta.Injection.NrErrorsLeftJoinField")
  private String nrErrorsLeftJoinField;

  @HopMetadataProperty(
      key = "nr_errors_inner_join_field",
      injectionKeyDescription = "TableCompareMeta.Injection.NrErrorsInnerJoinField")
  private String nrErrorsInnerJoinField;

  @HopMetadataProperty(
      key = "nr_errors_right_join_field",
      injectionKeyDescription = "TableCompareMeta.Injection.NrErrorsRightJoinField")
  private String nrErrorsRightJoinField;

  @HopMetadataProperty(
      key = "key_description_field",
      injectionKeyDescription = "TableCompareMeta.Injection.KeyDescriptionField")
  private String keyDescriptionField;

  @HopMetadataProperty(
      key = "value_reference_field",
      injectionKeyDescription = "TableCompareMeta.Injection.ValueReferenceField")
  private String valueReferenceField;

  @HopMetadataProperty(
      key = "value_compare_field",
      injectionKeyDescription = "TableCompareMeta.Injection.ValueCompareField")
  private String valueCompareField;

  @HopMetadataProperty(key = "subject_field_name")
  private String subjectFieldName;

  public TableCompareMeta() {
    super(); // allocate BaseTransformMeta
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    if (Utils.isEmpty(nrErrorsField)) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "TableCompareMeta.Exception.NrErrorsFieldIsNotSpecified"));
    }
    if (Utils.isEmpty(nrRecordsReferenceField)) {
      throw new HopTransformException(
          BaseMessages.getString(
              PKG, "TableCompareMeta.Exception.NrRecordsReferenceFieldNotSpecified"));
    }
    if (Utils.isEmpty(nrRecordsCompareField)) {
      throw new HopTransformException(
          BaseMessages.getString(
              PKG, "TableCompareMeta.Exception.NrRecordsCompareFieldNotSpecified"));
    }
    if (Utils.isEmpty(nrErrorsLeftJoinField)) {
      throw new HopTransformException(
          BaseMessages.getString(
              PKG, "TableCompareMeta.Exception.NrErrorsLeftJoinFieldNotSpecified"));
    }
    if (Utils.isEmpty(nrErrorsInnerJoinField)) {
      throw new HopTransformException(
          BaseMessages.getString(
              PKG, "TableCompareMeta.Exception.NrErrorsInnerJoinFieldNotSpecified"));
    }
    if (Utils.isEmpty(nrErrorsRightJoinField)) {
      throw new HopTransformException(
          BaseMessages.getString(
              PKG, "TableCompareMeta.Exception.NrErrorsRightJoinFieldNotSpecified"));
    }

    IValueMeta nrErrorsValueMeta = new ValueMetaInteger(nrErrorsField);
    nrErrorsValueMeta.setLength(9);
    nrErrorsValueMeta.setOrigin(origin);
    inputRowMeta.addValueMeta(nrErrorsValueMeta);

    IValueMeta nrRecordsReference = new ValueMetaInteger(nrRecordsReferenceField);
    nrRecordsReference.setLength(9);
    nrRecordsReference.setOrigin(origin);
    inputRowMeta.addValueMeta(nrRecordsReference);

    IValueMeta nrRecordsCompare = new ValueMetaInteger(nrRecordsCompareField);
    nrRecordsCompare.setLength(9);
    nrRecordsCompare.setOrigin(origin);
    inputRowMeta.addValueMeta(nrRecordsCompare);

    IValueMeta nrErrorsLeft = new ValueMetaInteger(nrErrorsLeftJoinField);
    nrErrorsLeft.setLength(9);
    nrErrorsLeft.setOrigin(origin);
    inputRowMeta.addValueMeta(nrErrorsLeft);

    IValueMeta nrErrorsInner = new ValueMetaInteger(nrErrorsInnerJoinField);
    nrErrorsInner.setLength(9);
    nrErrorsInner.setOrigin(origin);
    inputRowMeta.addValueMeta(nrErrorsInner);

    IValueMeta nrErrorsRight = new ValueMetaInteger(nrErrorsRightJoinField);
    nrErrorsRight.setLength(9);
    nrErrorsRight.setOrigin(origin);
    inputRowMeta.addValueMeta(nrErrorsRight);
  }

  @Override
  public void setDefault() {
    nrErrorsField = "nrErrors";
    nrRecordsReferenceField = "nrRecordsReferenceTable";
    nrRecordsCompareField = "nrRecordsCompareTable";
    nrErrorsLeftJoinField = "nrErrorsLeftJoin";
    nrErrorsInnerJoinField = "nrErrorsInnerJoin";
    nrErrorsRightJoinField = "nrErrorsRightJoin";
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
    if (prev == null || prev.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "IfNullMeta.CheckResult.NotReceivingFields"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "IfNullMeta.CheckResult.TransformRecevingData", prev.size() + ""),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "IfNullMeta.CheckResult.TransformRecevingData2"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "IfNullMeta.CheckResult.NoInputReceivedFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
