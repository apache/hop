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
package org.apache.hop.pipeline.transforms.vcardoutput;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.vcard.VCardFieldMapping;
import org.apache.hop.pipeline.transforms.vcard.VCardMapper;
import org.apache.hop.pipeline.transforms.vcard.VCardVersionOption;

@Transform(
    id = "VCardOutput",
    image = "vcard.svg",
    name = "i18n::VCardOutput.name",
    description = "i18n::VCardOutput.description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    documentationUrl = "/pipeline/transforms/vcardoutput.html",
    keywords = "i18n::VCardOutput.keyword")
@Getter
@Setter
public class VCardOutputMeta extends BaseTransformMeta<VCardOutput, VCardOutputData> {

  private static final Class<?> PKG = VCardOutputMeta.class;

  @Getter
  public enum OperationType implements IEnumHasCodeAndDescription {
    OUTPUT_VALUE(
        "outputvalue", BaseMessages.getString(PKG, "VCardOutputMeta.OperationType.OutputValue")),
    WRITE_TO_FILE(
        "writetofile", BaseMessages.getString(PKG, "VCardOutputMeta.OperationType.WriteToFile")),
    BOTH("both", BaseMessages.getString(PKG, "VCardOutputMeta.OperationType.Both"));

    private final String code;
    private final String description;

    OperationType(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(OperationType.class);
    }

    public static OperationType lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(
          OperationType.class, description, WRITE_TO_FILE);
    }
  }

  @HopMetadataProperty(key = "operation_type", storeWithCode = true)
  private OperationType operationType;

  @HopMetadataProperty(key = "vcard_version", storeWithCode = true)
  private VCardVersionOption vcardVersion;

  @HopMetadataProperty(key = "output_field")
  private String outputField;

  @HopMetadataProperty(key = "pass_input_fields")
  private boolean passInputFields;

  @HopMetadataProperty(key = "add_prodid")
  private boolean addProdId;

  @HopMetadataProperty(key = "add_revision")
  private boolean addRevision;

  @HopMetadataProperty(key = "file_name_in_field")
  private boolean fileNameInField;

  @HopMetadataProperty(key = "file_name_field")
  private String fileNameField;

  @HopMetadataProperty(key = "encoding")
  private String encoding;

  @HopMetadataProperty(key = "file")
  private VCardFileSettings fileSettings;

  @HopMetadataProperty(key = "field")
  private List<VCardFieldMapping> fieldMappings;

  @HopMetadataProperty(key = "add_to_result_filenames")
  private boolean addingToResult;

  public VCardOutputMeta() {
    operationType = OperationType.BOTH;
    vcardVersion = VCardVersionOption.V3_0;
    outputField = "vcard_text";
    passInputFields = true;
    addProdId = true;
    addRevision = true;
    fileNameInField = true;
    fileNameField = "vcard_filename";
    encoding = Const.UTF_8;
    fileSettings = new VCardFileSettings();
    fieldMappings = new ArrayList<>();
    addingToResult = true;
  }

  public boolean writesToFile() {
    return operationType == OperationType.WRITE_TO_FILE || operationType == OperationType.BOTH;
  }

  public boolean outputsValue() {
    return operationType == OperationType.OUTPUT_VALUE || operationType == OperationType.BOTH;
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    if (outputsValue()) {
      IValueMeta valueMeta = new ValueMetaString(outputField);
      valueMeta.setOrigin(name);
      rowMeta.addValueMeta(valueMeta);
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
    try {
      VCardMapper.validateMappings(fieldMappings);
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "VCardOutputMeta.CheckResult.MappingsOK"),
              transformMeta));
    } catch (Exception e) {
      remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, e.getMessage(), transformMeta));
    }

    if (outputsValue() && Utils.isEmpty(outputField)) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "VCardOutputMeta.CheckResult.OutputFieldMissing"),
              transformMeta));
    }

    if (writesToFile()) {
      if (fileNameInField) {
        if (Utils.isEmpty(fileNameField)) {
          remarks.add(
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(PKG, "VCardOutputMeta.CheckResult.FileNameFieldMissing"),
                  transformMeta));
        } else if (prev != null && prev.indexOfValue(fileNameField) < 0) {
          remarks.add(
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(
                      PKG, "VCardOutputMeta.CheckResult.FileNameFieldNotFound", fileNameField),
                  transformMeta));
        }
      } else if (Utils.isEmpty(fileSettings.getFileName())) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "VCardOutputMeta.CheckResult.FileNameMissing"),
                transformMeta));
      }
    }

    if (input.length > 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "VCardOutputMeta.CheckResult.ReceivingRows"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "VCardOutputMeta.CheckResult.NoInput"),
              transformMeta));
    }
  }
}
