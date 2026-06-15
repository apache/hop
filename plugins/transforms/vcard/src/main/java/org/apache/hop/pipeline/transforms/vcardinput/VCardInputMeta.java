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
package org.apache.hop.pipeline.transforms.vcardinput;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.file.BaseFileInput;
import org.apache.hop.pipeline.transforms.vcard.VCardFieldMapping;
import org.apache.hop.pipeline.transforms.vcard.VCardMapper;
import org.apache.hop.resource.ResourceReference;

@Transform(
    id = "VCardInput",
    image = "vcard.svg",
    name = "i18n::VCardInput.name",
    description = "i18n::VCardInput.description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    documentationUrl = "/pipeline/transforms/vcardinput.html",
    keywords = "i18n::VCardInput.keyword")
@Getter
@Setter
public class VCardInputMeta extends BaseTransformMeta<VCardInput, VCardInputData> {

  private static final Class<?> PKG = VCardInputMeta.class;

  public static final String[] REQUIRED_FILES_DESC =
      new String[] {
        BaseMessages.getString("System.Combo.No"), BaseMessages.getString("System.Combo.Yes")
      };

  @HopMetadataProperty(key = "include_filename")
  private boolean includeFilename;

  @HopMetadataProperty(key = "filename_field")
  private String filenameField;

  @HopMetadataProperty(key = "ignore_empty_file")
  private boolean ignoringEmptyFile;

  @HopMetadataProperty(key = "do_not_fail_if_no_file")
  private boolean doNotFailIfNoFile;

  @HopMetadataProperty(key = "encoding")
  private String encoding;

  @HopMetadataProperty(
      key = "file",
      inline = true,
      childKeysToIgnore = {
        "accept_filenames",
        "accept_transform_name",
        "passing_through_fields",
        "accept_field",
        "add_to_result_filenames",
      })
  private BaseFileInput fileInput;

  @HopMetadataProperty(key = "field")
  private List<VCardFieldMapping> fieldMappings;

  public VCardInputMeta() {
    filenameField = "vcard_filename";
    encoding = Const.UTF_8;
    fileInput = new BaseFileInput();
    fieldMappings = new ArrayList<>();
  }

  public FileInputList getFileInputList(IVariables variables) {
    return FileInputList.createFileList(variables, fileInput.getInputFiles());
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    if (!(fileInput.isAcceptingFilenames() && fileInput.isPassingThruFields())) {
      rowMeta.clear();
    }
    for (VCardFieldMapping mapping : fieldMappings) {
      if (mapping == null || Utils.isEmpty(mapping.getHopField())) {
        continue;
      }
      IValueMeta valueMeta = new ValueMetaString(mapping.getHopField());
      valueMeta.setOrigin(name);
      rowMeta.addValueMeta(valueMeta);
    }
    if (includeFilename && !Utils.isEmpty(filenameField)) {
      IValueMeta valueMeta = new ValueMetaString(variables.resolve(filenameField));
      valueMeta.setOrigin(name);
      rowMeta.addValueMeta(valueMeta);
    }
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, TransformMeta transformMeta) {
    if (fileInput == null) {
      return List.of();
    }
    return fileInput.getResourceDependencies(variables, transformMeta);
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
    if (fileInput.isAcceptingFilenames()) {
      if (Utils.isEmpty(fileInput.getAcceptingField())) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "VCardInputMeta.CheckResult.AcceptFieldMissing"),
                transformMeta));
      } else if (prev != null && prev.indexOfValue(fileInput.getAcceptingField()) < 0) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG,
                    "VCardInputMeta.CheckResult.AcceptFieldNotFound",
                    fileInput.getAcceptingField()),
                transformMeta));
      }
      if (input.length == 0) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "VCardInputMeta.CheckResult.NoInput"),
                transformMeta));
      }
    } else {
      FileInputList files = getFileInputList(variables);
      if (files.nrOfFiles() == 0) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "VCardInputMeta.CheckResult.NoFiles"),
                transformMeta));
      } else {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG, "VCardInputMeta.CheckResult.FilesOK", files.nrOfFiles()),
                transformMeta));
      }
    }

    try {
      VCardMapper.validateMappings(fieldMappings);
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "VCardInputMeta.CheckResult.MappingsOK"),
              transformMeta));
    } catch (Exception e) {
      remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, e.getMessage(), transformMeta));
    }
  }
}
