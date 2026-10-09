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

package org.apache.hop.pipeline.transforms.cubeinput;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.gui.plugin.ITypeFilename;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.cube.CubeFilename;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;

@Transform(
    id = "CubeInput",
    image = "cubeinput.svg",
    name = "i18n::CubeInput.Name",
    description = "i18n::CubeInput.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "i18n::CubeInputMeta.keyword",
    documentationUrl = "/pipeline/transforms/serialize-de-from-file.html")
@GuiPlugin
@Getter
@Setter
public class CubeInputMeta extends BaseTransformMeta<CubeInput, CubeInputData> {
  private static final Class<?> PKG = CubeInputMeta.class;

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "CubeInputDialog.File";
  public static final String WIDGET_FILENAME = "filename";
  public static final String WIDGET_INCLUDE_TRANSFORM_NR = "includeTransformNr";
  public static final String WIDGET_FILENAME_IN_FIELD = "filenameInField";
  public static final String WIDGET_FILENAME_FIELD = "filenameField";

  public static final String GROUP_FILE = "File";

  @HopMetadataProperty(key = "file")
  private CubeFile file;

  /**
   * Dialog value for the cube filename. Existing pipelines store the name on {@link CubeFile}, so
   * this field is not serialized. The accessors keep it aligned with {@link CubeFile#getName()}.
   */
  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  @GuiWidgetElement(
      id = WIDGET_FILENAME,
      order = "0100",
      type = GuiElementType.FILENAME,
      typeFilename = CubeFileType.class,
      label = "i18n::CubeInputDialog.Filename.Label",
      toolTip = "i18n::CubeInputDialog.Filename.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_FILE)
  private String filename;

  @GuiWidgetElement(
      id = WIDGET_INCLUDE_TRANSFORM_NR,
      order = "0200",
      type = GuiElementType.CHECKBOX,
      label = "i18n::CubeInputDialog.IncludeTransformNr.Label",
      toolTip = "i18n::CubeInputDialog.IncludeTransformNr.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_FILE)
  @HopMetadataProperty(key = "include_transform_nr")
  private boolean includeTransformNr;

  @GuiWidgetElement(
      id = WIDGET_FILENAME_IN_FIELD,
      order = "0300",
      type = GuiElementType.CHECKBOX,
      label = "i18n::CubeInputDialog.FilenameInField.Label",
      toolTip = "i18n::CubeInputDialog.FilenameInField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_FILE)
  @HopMetadataProperty(key = "filename_in_field")
  private boolean filenameInField;

  @GuiWidgetElement(
      id = WIDGET_FILENAME_FIELD,
      order = "0400",
      type = GuiElementType.COMBO,
      label = "i18n::CubeInputDialog.FilenameField.Label",
      toolTip = "i18n::CubeInputDialog.FilenameField.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_FILE)
  @HopMetadataProperty(key = "filename_field")
  private String filenameField;

  @GuiWidgetElement(
      id = "rowLimit",
      order = "0500",
      type = GuiElementType.TEXT,
      label = "i18n::CubeInputDialog.Limit.Label",
      toolTip = "i18n::CubeInputDialog.Limit.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_FILE)
  @HopMetadataProperty(key = "limit")
  private String rowLimit;

  @GuiWidgetElement(
      id = "addFilenameResult",
      order = "0600",
      type = GuiElementType.CHECKBOX,
      label = "i18n::CubeInputDialog.AddResult.Label",
      toolTip = "i18n::CubeInputDialog.AddResult.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP_FILE)
  @HopMetadataProperty(key = "addfilenameresult")
  private boolean addFilenameResult;

  public CubeInputMeta() {
    super();
    file = new CubeFile();
  }

  /**
   * Filename shown in the dialog and stored as {@code <file><name>}.
   *
   * @return the cube filename, or null when none is set
   */
  public String getFilename() {
    if (file != null && file.getName() != null) {
      return file.getName();
    }
    return filename;
  }

  /**
   * @param filename the cube filename. Kept on {@link CubeFile} so existing XML stays valid.
   */
  public void setFilename(String filename) {
    this.filename = filename;
    if (file == null) {
      file = new CubeFile();
    }
    file.setName(filename);
  }

  /** The copy-number suffix applies to the static filename, not to names read from a field. */
  public boolean usesTransformNrInFilename() {
    return includeTransformNr && !filenameInField;
  }

  @Override
  public boolean consumesMainInput() {
    return filenameInField;
  }

  @Override
  public boolean canStartWithoutInput() {
    return !filenameInField;
  }

  @Override
  public String getMainInputRequirementHint() {
    return BaseMessages.getString(PKG, "CubeInput.FileInField.Label");
  }

  @Override
  public void setDefault() {
    this.file = new CubeFile();
    this.filename = null;
    this.rowLimit = "0";
    this.addFilenameResult = false;
    this.includeTransformNr = false;
    this.filenameInField = false;
    this.filenameField = null;
  }

  @Override
  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    if (file == null || Utils.isEmpty(file.getName())) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "CubeInputMeta.Exception.NoFilenameSpecified"));
    }
    // The layout of a cube lives in the file itself, so we have to open it. Copy 0 is the file
    // that carries the layout when each copy has its own file. Pass the variables through so named
    // VFS connections work here the same way they do at runtime.
    String filename =
        CubeFilename.resolve(variables, file.getName(), usesTransformNrInFilename(), 0);
    try (InputStream is = HopVfs.getInputStream(filename, variables);
        GZIPInputStream fis = new GZIPInputStream(is);
        DataInputStream dis = new DataInputStream(fis)) {
      IRowMeta add = new RowMeta(dis);
      for (int i = 0; i < add.size(); i++) {
        add.getValueMeta(i).setOrigin(name);
      }
      r.mergeRowMeta(add);
    } catch (HopFileException kfe) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "CubeInputMeta.Exception.UnableToReadMetaData", filename),
          kfe);
    } catch (IOException e) {
      throw new HopTransformException(
          BaseMessages.getString(
              PKG, "CubeInputMeta.Exception.ErrorOpeningOrReadingCubeFile", filename),
          e);
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
    CheckResult cr;

    cr =
        new CheckResult(
            ICheckResult.TYPE_RESULT_COMMENT,
            BaseMessages.getString(PKG, "CubeInputMeta.CheckResult.FileSpecificationsNotChecked"),
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
    try {
      // The object that we're modifying here is a copy of the original.
      // From : ${Internal.Pipeline.Filename.Directory}/../foo/bar.data
      // To   : /home/matt/test/files/foo/bar.data
      //
      // A name that contains the copy variable is exported as copy 0, the file getFields opens.
      // The suffix is cleared so the exported name is not numbered a second time.
      String resolved =
          CubeFilename.resolve(variables, getFilename(), usesTransformNrInFilename(), 0);
      FileObject fileObject = HopVfs.getFileObject(resolved, variables);

      if (fileObject.exists()) {
        file.name = iResourceNaming.nameResource(fileObject, variables, true);
        this.filename = file.name;
        includeTransformNr = false;
        return file.name;
      }
      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
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
        BaseMessages.getString(PKG, "CubeInputDialog.FilterNames.CubeFiles"),
        BaseMessages.getString(PKG, "CubeInputDialog.FilterNames.AllFiles")
      };
    }
  }

  @Getter
  @Setter
  public static class CubeFile {
    @HopMetadataProperty private String name;

    public CubeFile() {}

    public CubeFile(String name) {
      this.name = name;
    }

    public CubeFile(CubeFile f) {
      this.name = f.name;
    }
  }
}
