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
 *
 */

package org.apache.hop.workflow.actions.documentation;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.FileExtensionSelector;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;

/** Generate documentation in MarkDown format. */
@Action(
    id = "DOCUMENTATION",
    name = "i18n::ActionDoc.Name",
    description = "i18n::ActionDoc.Description",
    image = "info.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Utility",
    keywords = "i18n::ActionDoc.keyword",
    documentationUrl = "/workflow/actions/documentation.html")
@Getter
@Setter
@GuiPlugin
public class ActionDoc extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionDoc.class;

  public static final String GUI_WIDGETS_PARENT_ID = "ActionDocDialog-GuiWidgetsParent";

  public static final String PIPELINE_EXTENSION = "hpl";
  public static final String WORKFLOW_EXTENSION = "hwf";
  public static final String DATASETS_FOLDER = "datasets";
  public static final String METADATA_FOLDER = "metadata";
  public static final String STRING_PIPELINE = "pipeline";
  public static final String STRING_WORKFLOW = "workflow";
  public static final String ASSETS_IMAGES = "assets/images/";
  public static final String STYLES_CSS = "assets/styles.css";
  public static final String EXPR_PROJECT_HOME = "${PROJECT_HOME}";
  public static final String EXPR_PROJECT_NAME = "${HOP_PROJECT_NAME}";
  public static final String INDEX_MD = "index.md";

  @GuiWidgetElement(
      id = "1000-action-doc-dialog-target-filename",
      parentId = GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.FILENAME,
      variables = true,
      label = "i18n::ActionDoc.Target.Label")
  @HopMetadataProperty
  private String targetParentFolder;

  @GuiWidgetElement(
      id = "1010-action-doc-dialog-include-parameters",
      parentId = GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      variables = false,
      label = "i18n::ActionDoc.Parameters.Label")
  @HopMetadataProperty
  private boolean includingParameters = true;

  @GuiWidgetElement(
      id = "1020-action-doc-dialog-include-notes",
      parentId = GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      variables = false,
      label = "i18n::ActionDoc.Notes.Label")
  @HopMetadataProperty
  private boolean includingNotes;

  public ActionDoc() {}

  public ActionDoc(ActionDoc other) {
    super(other.getName(), other.getDescription(), other.getPluginId());
    this.targetParentFolder = other.targetParentFolder;
    this.includingNotes = other.includingNotes;
    this.includingParameters = other.includingParameters;
  }

  @Override
  public ActionDoc clone() {
    return new ActionDoc(this);
  }

  private void buildDocumentation(Result result) {
    try {
      String projectName = resolve(EXPR_PROJECT_NAME);

      // In the target folder we should copy the folder structure of the project.
      // For every file in the project we have something to document.
      // This way we can also rely on the existence of files to create links between documents.
      //
      Toc toc = new Toc();
      try (FileObject targetParentFolder = HopVfs.getFileObject(getTargetParentFolder())) {
        // Create images/assets folders
        createImagesFolder(targetParentFolder);
        writeStylesFile(targetParentFolder);

        try (FileObject parentFolder = HopVfs.getFileObject(resolve(EXPR_PROJECT_HOME))) {
          List<FileObject> sourceFolders = getSourceFolders(parentFolder, true);
          for (FileObject sourceFolder : sourceFolders) {
            // Get the relative path of the source folder vs the parent
            String relativeName = parentFolder.getName().getRelativeName(sourceFolder.getName());
            String targetName = targetParentFolder + "/" + relativeName;
            FileObject targetFolder = HopVfs.getFileObject(targetName);
            processDocumentationFolder(
                toc, targetParentFolder, sourceFolder, targetFolder, relativeName);
          }
        }
      }
      writeToc(toc, targetParentFolder, projectName);
      logBasic("Finished generating documentation for project " + projectName);
    } catch (Exception e) {
      logError("Error building documentation", e);
      result.setResult(false);
      result.setNrErrors(1);
    }
  }

  /** Create an index.md in the root */
  private void writeToc(Toc toc, String targetParentFolder, String projectName) throws Exception {
    String targetFilename = targetParentFolder + "/" + INDEX_MD;
    // First sort the entries in the Table of contents
    //
    toc.sortEntries();

    StringBuilder index = new StringBuilder();
    // Header
    index
        .append("---")
        .append(Const.CR)
        .append("title: Project ")
        .append(projectName)
        .append(" documentation")
        .append(Const.CR)
        .append("---")
        .append(Const.CR)
        .append(Const.CR);

    // Now we can loop over the entries in the TOC
    //
    for (TocEntry entry : toc.getEntries()) {
      String path;
      if (entry.relativeFolder().equals(".")) {
        path = "";
      } else {
        path = entry.relativeFolder() + "/";
      }

      index
          .append("- ")
          .append("[")
          .append(path)
          .append(entry.type())
          .append(" : ")
          .append(entry.description())
          .append("]")
          .append("(")
          .append(entry.targetDocFile())
          .append(".html")
          .append(")")
          .append(Const.CR);
    }
    index.append(Const.CR);

    // Save the file
    logBasic("Saving index (TOC) to " + targetFilename);
    saveFile(targetFilename, index.toString());
  }

  private void writeStylesFile(FileObject targetParentFolder) throws Exception {
    String stylesFileName = targetParentFolder.getName().getPath() + "/" + STYLES_CSS;
    String css =
        """
                img {
                  border: 1px solid #555;
                }

                body {
                  background-color: #ECFFFF;
                }
              """;

    saveFile(stylesFileName, css);
  }

  private void createImagesFolder(FileObject targetParentFolder) throws Exception {
    String assetsFolderName = targetParentFolder.getName().getPath() + "/assets";
    FileObject assetsFolder = HopVfs.getFileObject(assetsFolderName);
    if (!assetsFolder.exists()) {
      assetsFolder.createFolder();
    }
    String imagesFolderName = assetsFolder.getName().getPath() + "/images";
    FileObject imagesFolder = HopVfs.getFileObject(imagesFolderName);
    if (!imagesFolder.exists()) {
      imagesFolder.createFolder();
    }
  }

  // Process the files in the given source folder
  //
  private void processDocumentationFolder(
      Toc toc,
      FileObject targetParentFolder,
      FileObject sourceFolder,
      FileObject targetFolder,
      String relativeName)
      throws Exception {
    logBasic("Processing documentation folder " + relativeName);
    if (isMetadataFolder(relativeName)) {
      processMetadataFolders(toc, sourceFolder, targetFolder, relativeName);
    } else if (isDatasetsFolder(relativeName)) {
      processDatasetsFolder(toc, sourceFolder, targetFolder, relativeName);
    } else {
      processHopFolder(toc, 1, targetParentFolder, sourceFolder, targetFolder, relativeName);
    }
  }

  public boolean isMetadataFolder(String name) {
    return METADATA_FOLDER.equals(name);
  }

  public boolean isDatasetsFolder(String name) {
    return DATASETS_FOLDER.equals(name);
  }

  public boolean isPipeline(FileObject fileObject) {
    return PIPELINE_EXTENSION.equals(fileObject.getName().getExtension());
  }

  public boolean isWorkflow(FileObject fileObject) {
    return WORKFLOW_EXTENSION.equals(fileObject.getName().getExtension());
  }

  private void processHopFolder(
      Toc toc,
      int level,
      FileObject targetParentFolder,
      FileObject sourceFolder,
      FileObject targetFolder,
      String relativeName)
      throws Exception {
    logBasic("Processing pipelines and workflows in folder " + relativeName);
    if (!targetFolder.exists()) {
      targetFolder.createFolder();
      logBasic("Created target folder " + targetFolder.getName().getPath());
    }

    // Find sub-folders
    //
    for (FileObject subSourceFolder : getSourceFolders(sourceFolder, false)) {
      String relativeSubSourceFolder =
          sourceFolder.getName().getRelativeName(subSourceFolder.getName());
      FileObject subTargetFolder =
          HopVfs.getFileObject(targetFolder.getName().getPath() + "/" + relativeSubSourceFolder);
      // Avoid going into the metadata and datasets folders. These are covered elsewhere.
      //
      if (!isMetadataFolder(relativeSubSourceFolder)
          && !isDatasetsFolder(relativeSubSourceFolder)
          && !".".equals(relativeName)) {
        processHopFolder(
            toc,
            level + 1,
            targetParentFolder,
            subSourceFolder,
            subTargetFolder,
            relativeSubSourceFolder);
      }
    }

    // Find hpl and hwf files
    //
    FileObject[] childFiles =
        sourceFolder.findFiles(new FileExtensionSelector(PIPELINE_EXTENSION, WORKFLOW_EXTENSION));
    for (FileObject childFile : childFiles) {
      // Let's not search sub-folders here.
      // The find files functions finds stuff in sub-folders.
      if (!childFile.getParent().equals(sourceFolder)) {
        continue;
      }
      if (isPipeline(childFile)) {
        new PipelineDocDelegate(this)
            .buildPipelineDocumentation(
                toc, targetParentFolder, sourceFolder, targetFolder, relativeName, childFile);
      }
      if (isWorkflow(childFile)) {
        new WorkflowDocDelegate(this)
            .buildWorkflowDocumentation(
                toc, targetParentFolder, sourceFolder, targetFolder, relativeName, childFile);
      }
    }
  }

  public void saveFile(String svgFilename, String pipelineSvg) throws Exception {
    try (OutputStream outputStream = HopVfs.getOutputStream(svgFilename, false)) {
      outputStream.write(pipelineSvg.getBytes());
      outputStream.flush();
    }
  }

  private void processDatasetsFolder(
      Toc toc, FileObject sourceFolder, FileObject targetFolder, String relativeName) {}

  private void processMetadataFolders(
      Toc toc, FileObject sourceFolder, FileObject targetFolder, String relativeName) {}

  /**
   * List all the folders in the source or our documentation subject.
   *
   * @return The list of folders
   */
  private List<FileObject> getSourceFolders(FileObject parentFolder, boolean includeParent)
      throws Exception {
    List<FileObject> sourceFolders = new ArrayList<>();
    // The parent folder is also a source for documentation!
    if (includeParent) {
      sourceFolders.add(parentFolder);
    }
    // Get all the child folders
    for (FileObject child : parentFolder.getChildren()) {
      if (child.isFolder() && !child.isHidden()) {
        sourceFolders.add(child);
      }
    }
    return sourceFolders;
  }

  /**
   * Execute this action and return the result. In this case it means, just set the result boolean
   * in the Result class.
   *
   * @param prevResult The result of the previous execution
   * @return The Result of the execution.
   */
  @Override
  public Result execute(Result prevResult, int nr) {
    buildDocumentation(prevResult);
    return prevResult;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return false;
  }
}
