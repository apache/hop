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

package org.apache.hop.pipeline.transforms.cubeoutput;

import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.gui.plugin.ITypeFilename;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataWrapper;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.cube.CubeFilename;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;

@Transform(
    id = "CubeOutput",
    image = "cubeoutput.svg",
    name = "i18n::CubeOutput.Name",
    description = "i18n::CubeOutput.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    keywords = "i18n::CubeOutputMeta.keyword",
    documentationUrl = "/pipeline/transforms/serialize-to-file.html")
@HopMetadataWrapper(tag = "file")
@GuiPlugin
@Getter
@Setter
public class CubeOutputMeta extends BaseTransformMeta<CubeOutput, CubeOutputData> {
  private static final Class<?> PKG = CubeOutputMeta.class;

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "CubeOutputDialog.File";
  public static final String WIDGET_INCLUDE_TRANSFORM_NR = "includeTransformNr";

  public static final String GROUP_FILE = "File";

  @GuiWidgetElement(
      id = "filename",
      order = "0100",
      type = GuiElementType.FILENAME,
      typeFilename = CubeFileType.class,
      label = "i18n::CubeOutputDialog.Filename.Label",
      toolTip = "i18n::CubeOutputDialog.Filename.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_FILE)
  @HopMetadataProperty(key = "name")
  private String filename;

  @GuiWidgetElement(
      id = WIDGET_INCLUDE_TRANSFORM_NR,
      order = "0200",
      type = GuiElementType.CHECKBOX,
      label = "i18n::CubeOutputDialog.IncludeTransformNr.Label",
      toolTip = "i18n::CubeOutputDialog.IncludeTransformNr.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_FILE)
  @HopMetadataProperty(key = "include_transform_nr")
  private boolean includeTransformNr;

  @GuiWidgetElement(
      id = "filenameCreatingParentFolders",
      order = "0300",
      type = GuiElementType.CHECKBOX,
      label = "i18n::CubeOutputDialog.CreatingParentFolders.Label",
      toolTip = "i18n::CubeOutputDialog.CreatingParentFolders.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_FILE)
  @HopMetadataProperty(key = "filename_create_parent_folders")
  private boolean filenameCreatingParentFolders;

  /** Flag : Do not open new file when pipeline start */
  @GuiWidgetElement(
      id = "doNotOpenNewFileInit",
      order = "0400",
      type = GuiElementType.CHECKBOX,
      label = "i18n::CubeOutputDialog.DoNotOpenNewFileInit.Label",
      toolTip = "i18n::CubeOutputDialog.DoNotOpenNewFileInit.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_FILE)
  @HopMetadataProperty(key = "do_not_open_newfile_init")
  private boolean doNotOpenNewFileInit;

  /** Flag: add the filenames to result filenames */
  @GuiWidgetElement(
      id = "addToResultFilenames",
      order = "0500",
      type = GuiElementType.CHECKBOX,
      label = "i18n::CubeOutputDialog.AddFileToResult.Label",
      toolTip = "i18n::CubeOutputDialog.AddFileToResult.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_FILE)
  @HopMetadataProperty(key = "add_to_result_filenames")
  private boolean addToResultFilenames;

  public CubeOutputMeta() {
    super();
  }

  @Override
  public void setDefault() {
    addToResultFilenames = false;
    doNotOpenNewFileInit = false;
    filenameCreatingParentFolders = false;
    includeTransformNr = false;
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

    // Check output fields
    if (prev != null && !prev.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "CubeOutputMeta.CheckResult.ReceivingFields", String.valueOf(prev.size())),
              transformMeta);
      remarks.add(cr);
    }

    cr =
        new CheckResult(
            ICheckResult.TYPE_RESULT_COMMENT,
            BaseMessages.getString(PKG, "CubeOutputMeta.CheckResult.FileSpecificationsNotChecked"),
            transformMeta);
    remarks.add(cr);
  }

  /**
   * @param variables the variable variables to use
   * @param definitions
   * @param iResourceNaming
   * @param metadataProvider the metadataProvider in which non-hop metadata could reside.
   * @return the filename of the exported resource
   */
  @Override
  public String exportResources(
      IVariables variables,
      Map<String, ResourceDefinition> definitions,
      IResourceNaming iResourceNaming,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    // The object that we're modifying here is a copy of the original.
    // Map the folder of copy 0 and keep the stored file name, so each copy still opens its own
    // file after export.
    String exported =
        CubeFilename.exportResourceName(variables, filename, includeTransformNr, iResourceNaming);
    if (exported == null) {
      return null;
    }
    filename = exported;
    return filename;
  }

  /** Browse filter for {@code *.cube} files. */
  public static class CubeFileType implements ITypeFilename {
    @Override
    public String getDefaultFileExtension() {
      return ".cube";
    }

    @Override
    public String[] getFilterExtensions() {
      return new String[] {"*.cube", "*"};
    }

    @Override
    public String[] getFilterNames() {
      return new String[] {
        BaseMessages.getString(PKG, "CubeOutputDialog.FilterNames.Options.CubeFiles"),
        BaseMessages.getString(PKG, "CubeOutputDialog.FilterNames.Options.AllFiles")
      };
    }
  }
}
