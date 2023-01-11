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

package org.apache.hop.pipeline.transforms.file;

import com.google.common.base.Preconditions;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.ResourceReference;

import java.util.List;

/** Base meta for file-based input transforms. */
public abstract class BaseFileInputMeta<
        Main extends ITransform,
        Data extends ITransformData,
        A extends BaseFileInputAdditionalField,
        I extends BaseFileInputFiles,
        F extends BaseFileField>
    extends BaseTransformMeta<Main, Data> {
  private static final Class<?> PKG = BaseFileInputMeta.class; // For Translator

  public static final String[] RequiredFilesCode = new String[] {"N", "Y"};

  public static final String NO = "N";

  public static final String YES = "Y";

  public static final String[] RequiredFilesDesc =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };

  @HopMetadataProperty(key = "file")
  public I inputFiles;

  /** The fields to import... */
  @HopMetadataProperty(groupKey = "fields", key = "field")
  public F[] inputFields;

  @HopMetadataProperty(inline = true)
  public BaseFileErrorHandling errorHandling = new BaseFileErrorHandling();

  @HopMetadataProperty(inline = true)
  public A additionalOutputFields;

  @Override
  public Object clone() {
    BaseFileInputMeta<
            BaseFileInputTransform,
            BaseFileInputTransformData,
            BaseFileInputAdditionalField,
            BaseFileInputFiles,
            BaseFileField>
        retval =
            (BaseFileInputMeta<
                    BaseFileInputTransform,
                    BaseFileInputTransformData,
                    BaseFileInputAdditionalField,
                    BaseFileInputFiles,
                    BaseFileField>)
                super.clone();

    retval.inputFiles = (BaseFileInputFiles) inputFiles.clone();
    retval.errorHandling = (BaseFileErrorHandling) errorHandling.clone();
    retval.additionalOutputFields = (BaseFileInputAdditionalField) additionalOutputFields.clone();

    return retval;
  }

  /**
   * @param fileRequiredin The fileRequired to set.
   */
  public void inputFiles_fileRequired(List<String> fileRequiredin) {
    for (int i = 0; i < fileRequiredin.size(); i++) {
      inputFiles.fileRequired.add(getRequiredFilesCode(fileRequiredin.get(i)));
    }
  }

  public List<String> inputFiles_includeSubFolders() {
    return inputFiles.includeSubFolders;
  }

  public void inputFiles_includeSubFolders(List<String> includeSubFoldersin) {
    for (int i = 0; i < includeSubFoldersin.size(); i++) {
      inputFiles.includeSubFolders.add(getRequiredFilesCode(includeSubFoldersin.get(i)));
    }
  }

  public static String getRequiredFilesCode(String tt) {
    if (tt == null) {
      return RequiredFilesCode[0];
    }
    if (tt.equals(RequiredFilesDesc[1])) {
      return RequiredFilesCode[1];
    } else {
      return RequiredFilesCode[0];
    }
  }

  public FileInputList getFileInputList(IVariables variables) {
    inputFiles.normalizeAllocation(inputFiles.fileName);
    return FileInputList.createFileList(
        variables,
        inputFiles.fileName.toArray(new String[0]),
        inputFiles.fileMask.toArray(new String[0]),
        inputFiles.excludeFileMask.toArray(new String[0]),
        inputFiles.fileRequired.toArray(new String[0]),
        inputFiles.includeSubFolderBoolean());
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, TransformMeta transformMeta) {
    return inputFiles.getResourceDependencies(variables, transformMeta);
  }

  public abstract String getEncoding();

  public boolean isAcceptingFilenames() {
    Preconditions.checkNotNull(inputFiles);
    return inputFiles.acceptingFilenames;
  }

  public String getAcceptingTransformName() {
    return inputFiles == null ? null : inputFiles.acceptingTransformName;
  }

  public String getAcceptingField() {
    return inputFiles == null ? null : inputFiles.acceptingField;
  }

  public String[] getFilePaths(IVariables variables, final boolean showSamples) {
    final TransformMeta parentTransformMeta = getParentTransformMeta();
    if (parentTransformMeta != null) {
      final PipelineMeta parentPipelineMeta = parentTransformMeta.getParentPipelineMeta();
      if (parentPipelineMeta != null) {
        final FileInputList inputList = getFileInputList(variables);
        if (inputList != null) {
          return inputList.getFileStrings();
        }
      }
    }
    return new String[] {};
  }

  public I getInputFiles() {
    return inputFiles;
  }

  public void setInputFiles(I inputFiles) {
    this.inputFiles = inputFiles;
  }

  public F[] getInputFields() {
    return inputFields;
  }

  public void setInputFields(F[] inputFields) {
    this.inputFields = inputFields;
  }

  public BaseFileErrorHandling getErrorHandling() {
    return errorHandling;
  }

  public void setErrorHandling(BaseFileErrorHandling errorHandling) {
    this.errorHandling = errorHandling;
  }

  public A getAdditionalOutputFields() {
    return additionalOutputFields;
  }

  public void setAdditionalOutputFields(A additionalOutputFields) {
    this.additionalOutputFields = additionalOutputFields;
  }
}
