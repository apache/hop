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
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.fileinput.InputFile;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

/** Base meta for file-based input transforms. */
@Getter
@Setter
public abstract class BaseFileInputMeta<
        Main extends ITransform, Data extends ITransformData, I extends BaseFileInput>
    extends BaseTransformMeta<Main, Data> {
  private static final Class<?> PKG = BaseFileInputMeta.class;

  public static final String[] REQUIRED_FILES_CODE = new String[] {"N", "Y"};

  public static final String NO = "N";

  public static final String YES = "Y";

  public static final String[] REQUIRED_FILES_DESC =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };

  protected abstract I getFileInput();

  protected abstract void setFileInput(I input);

  public BaseFileInputMeta() {}

  public BaseFileInputMeta(BaseFileInputMeta<Main, Data, I> meta) {
    this();
  }

  @Override
  public Object clone() {
    BaseFileInputMeta<BaseFileInputTransform, BaseFileInputTransformData, BaseFileInput> retval =
        (BaseFileInputMeta<BaseFileInputTransform, BaseFileInputTransformData, BaseFileInput>)
            super.clone();
    retval.setFileInput((BaseFileInput) getFileInput().clone());
    return retval;
  }

  public FileInputList getFileInputList(IVariables variables) {
    return FileInputList.createFileList(variables, getFileInput().getInputFiles());
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, TransformMeta transformMeta) {
    return getFileInput().getResourceDependencies(variables, transformMeta);
  }

  public abstract String getEncoding();

  public boolean isAcceptingFilenames() {
    Preconditions.checkNotNull(getFileInput());
    return getFileInput().isAcceptingFilenames();
  }

  public String getAcceptingTransformName() {
    return getFileInput() == null ? null : getFileInput().getAcceptingTransformName();
  }

  public String getAcceptingField() {
    return getFileInput() == null ? null : getFileInput().getAcceptingField();
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

  /** Convert old style inline file XML block contents */
  public static void convertLegacyXml(List<InputFile> inputFiles, Node node) {
    Node fileNode = XmlHandler.getSubNode(node, "file");
    int count = XmlHandler.countNodes(fileNode, "name");
    if (fileNode == null || count == 0) {
      // This is already using the new files/file structure.
      return;
    }

    inputFiles.clear();
    for (int i = 0; i < count; i++) {
      InputFile inputFile = new InputFile();
      String fileName = XmlHandler.getNodeValue(XmlHandler.getSubNodeByNr(fileNode, "name", i));
      String fileMask = XmlHandler.getNodeValue(XmlHandler.getSubNodeByNr(fileNode, "filemask", i));
      String fileExcludeMask =
          XmlHandler.getNodeValue(XmlHandler.getSubNodeByNr(fileNode, "exclude_filemask", i));
      String fileRequired =
          XmlHandler.getNodeValue(XmlHandler.getSubNodeByNr(fileNode, "file_required", i));
      inputFile.setFileName(fileName);
      inputFile.setFileMask(fileMask);
      inputFile.setExcludeFileMask(fileExcludeMask);
      inputFile.setFileRequired(Const.toBoolean(fileRequired));
      inputFiles.add(inputFile);
    }
  }
}
