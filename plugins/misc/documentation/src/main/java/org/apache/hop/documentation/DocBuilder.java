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

package org.apache.hop.documentation;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileExtensionSelector;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.ConfigPluginType;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.hop.plugin.HopSubCommand;
import org.apache.hop.hop.plugin.IHopSubCommand;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import picocli.CommandLine;

@Getter
@Setter
@CommandLine.Command(
    mixinStandardHelpOptions = true,
    name = "doc",
    description = "Generate documentation")
@HopSubCommand(id = "doc", description = "Generate documentation")
public class DocBuilder implements Runnable, IHopSubCommand, IHasHopMetadataProvider {
  public static final String PIPELINE_EXTENSION = "hpl";
  public static final String WORKFLOW_EXTENSION = "hwf";
  public static final String DATASETS_FOLDER = "datasets";
  public static final String METADATA_FOLDER = "metadata";
  public static final String STRING_PIPELINE = "pipeline";
  public static final String STRING_WORKFLOW = "workflow";
  public static final String ASSETS_IMAGES = "assets/images/";
  public static final String STYLES_CSS = "assets/styles.css";
  public static final String VAR_PROJECT_HOME = "PROJECT_HOME";
  public static final String VAR_PROJECT_NAME = "HOP_PROJECT_NAME";
  public static final String INDEX_MD = "index.md";

  private ILogChannel log;
  private CommandLine cmd;
  private IVariables variables;
  private MultiMetadataProvider metadataProvider;

  @CommandLine.Option(
      names = {"-t", "--target-folder"},
      description = "Specify the target parent folder where the documentation should end up")
  private String targetParentFolder;

  @CommandLine.Option(
      names = {"-ip", "--include-parameters"},
      description = "Include a list of parameters for each pipeline and workflow")
  private boolean includingParameters;

  @CommandLine.Option(
      names = {"-in", "--include-notes"},
      description = "List the text of any notes in alphabetical order")
  private boolean includingNotes;

  @CommandLine.Option(
      names = {"-n", "--project-name"},
      description = "The name of the project")
  private String projectName;

  @CommandLine.Option(
      names = {"-s", "--source-folder"},
      description = "The source folder to document")
  private String sourceFolder;

  public DocBuilder() {}

  public DocBuilder(
      ILogChannel log,
      IVariables variables,
      MultiMetadataProvider metadataProvider,
      String projectName,
      String projectHome,
      String targetParentFolder,
      boolean includingParameters,
      boolean includingNotes) {
    this.log = log;
    this.variables = variables;
    this.metadataProvider = metadataProvider;
    this.projectName = projectName;
    this.sourceFolder = projectHome;
    this.targetParentFolder = targetParentFolder;
    this.includingParameters = includingParameters;
    this.includingNotes = includingNotes;
  }

  @Override
  public void initialize(
      CommandLine cmd, IVariables variables, MultiMetadataProvider metadataProvider)
      throws HopException {
    this.cmd = cmd;
    this.variables = variables;
    this.metadataProvider = metadataProvider;
    this.log = new LogChannel("HopDoc");

    addRunConfigPlugins();
  }

  protected void addRunConfigPlugins() throws HopPluginException {
    // Now add configuration plugins with the RUN category.
    // The 'projects' plugin for example configures things like the project metadata provider.
    //
    List<IPlugin> configPlugins = PluginRegistry.getInstance().getPlugins(ConfigPluginType.class);
    for (IPlugin configPlugin : configPlugins) {
      // Load only the plugins of the "doc" category
      if (ConfigPlugin.CATEGORY_DOC.equals(configPlugin.getCategory())) {
        IConfigOptions configOptions =
            PluginRegistry.getInstance().loadClass(configPlugin, IConfigOptions.class);
        cmd.addMixin(configPlugin.getIds()[0], configOptions);
      }
    }
  }

  protected void handleMixinActions() throws HopException {
    // Handle the options of the configuration plugins
    //
    Map<String, Object> mixins = cmd.getMixins();
    for (String key : mixins.keySet()) {
      Object mixin = mixins.get(key);
      if (mixin instanceof IConfigOptions configOptions) {
        configOptions.handleOption(log, this, variables);
      }
    }
  }

  @Override
  public void run() {
    // Check a few variables...
    //
    try {
      handleMixinActions();

      if (StringUtils.isNotEmpty(variables.getVariable(VAR_PROJECT_NAME))) {
        projectName = variables.getVariable(VAR_PROJECT_NAME);
      }
      if (StringUtils.isNotEmpty(variables.getVariable(VAR_PROJECT_HOME))) {
        sourceFolder = variables.getVariable(VAR_PROJECT_HOME);
      }

      if (StringUtils.isEmpty(projectName)) {
        projectName = "Hop";
      }
      if (StringUtils.isEmpty(sourceFolder)) {
        log.logError("No project home folder specified: giving up.");
        System.exit(1);
      } else {
        log.logBasic("Documenting folder: " + sourceFolder + " for project " + projectName);
      }

      DocBuilder docBuilder =
          new DocBuilder(
              log,
              variables,
              metadataProvider,
              projectName,
              sourceFolder,
              targetParentFolder,
              includingParameters,
              includingNotes);
      docBuilder.buildDocumentation(new Result());
    } catch (Exception e) {
      log.logError("Error generating documentation", e);
    }
  }

  public void buildDocumentation(Result result) {
    try {
      // In the target folder we should copy the folder structure of the project.
      // For every file in the project we have something to document.
      // This way we can also rely on the existence of files to create links between documents.
      //
      Toc toc = new Toc();
      try (FileObject targetParentFolder = HopVfs.getFileObject(getTargetParentFolder())) {
        // Create images/assets folders
        createImagesFolder(targetParentFolder);
        writeStylesFile(targetParentFolder);

        try (FileObject parentFolder = HopVfs.getFileObject(sourceFolder)) {
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
      log.logBasic("Finished generating documentation for project " + projectName);
    } catch (Exception e) {
      log.logError("Error building documentation", e);
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

    index.append("**Project name**: ").append(projectName).append("  ").append(Const.CR);
    index.append("**Source folder**: ").append(sourceFolder).append("  ").append(Const.CR);
    index
        .append("**Updated**: ")
        .append(XmlHandler.date2string(new Date()))
        .append("  ")
        .append(Const.CR);
    index.append("---").append(Const.CR);

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
          .append(")")
          .append(Const.CR);
    }
    index.append(Const.CR);

    // Save the file
    log.logBasic("Saving index (TOC) to " + targetFilename);
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
    log.logBasic("Processing documentation folder " + relativeName);
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
    log.logBasic("Processing pipelines and workflows in folder " + relativeName);
    if (!targetFolder.exists()) {
      targetFolder.createFolder();
      log.logBasic("Created target folder " + targetFolder.getName().getPath());
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
}
