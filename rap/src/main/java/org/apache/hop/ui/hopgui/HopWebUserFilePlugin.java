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

package org.apache.hop.ui.hopgui;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.base.AbstractMeta;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.core.gui.plugin.menu.GuiMenuItem;
import org.apache.hop.core.security.Permission;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineSvgPainter;
import org.apache.hop.ui.core.dialog.EnterStringDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.security.HopSecurityUi;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.WorkflowSvgPainter;
import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.service.UISession;
import org.w3c.dom.Node;

/** Hop Web menu actions for opening and saving files on the user's computer. */
@GuiPlugin(name = "Hop Web user files")
public class HopWebUserFilePlugin {

  private static final Class<?> PKG = HopWebUserFilePlugin.class;

  public static final String ID_MAIN_MENU_FILE_USER = "15000-menu-file-user";

  private static final String PROJECT_EXPORT_MENU_ID = "10055-menu-file-export-to-svg";
  private static final String KETTLE_IMPORT_MENU_ID = "10060-menu-tools-import";
  private static final String SESSION_TEMP_DIRECTORY =
      HopWebUserFilePlugin.class.getName() + ".tempDirectory";
  static final String SESSION_TEMP_DIRECTORY_PREFIX = "hop-web-user-files-";
  private static final String HOP_FILE_EXTENSIONS = ".hpl,.hwf";
  private static final long MAX_HOP_FILE_SIZE = 25L * 1024 * 1024;
  private static final long MAX_COMPRESSED_ZIP_SIZE = 100L * 1024 * 1024;
  private static final long MAX_UNCOMPRESSED_ZIP_SIZE = 250L * 1024 * 1024;
  private static final long MAX_UNCOMPRESSED_ZIP_ENTRY_SIZE = 64L * 1024 * 1024;
  private static final long MIN_COMPRESSION_RATIO_SIZE = 1024L * 1024;
  private static final int MAX_COMPRESSION_RATIO = 100;
  private static final int MAX_ZIP_ENTRIES = 10_000;
  private static final int MAX_ZIP_ENTRY_DEPTH = 32;
  private static final int MAX_ZIP_ENTRY_NAME_LENGTH = 4096;

  private final Map<IHopFileTypeHandler, String> userFileNames = new IdentityHashMap<>();
  private UserFileTransfer transfer;
  private boolean sessionCleanupRegistered;

  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = ID_MAIN_MENU_FILE_USER,
      label = "i18n::HopGui.Menu.File.User",
      parentId = HopGui.ID_MAIN_MENU)
  public void menuFileUser() {
    // Category only.
  }

  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = HopGui.ID_MAIN_MENU_FILE_USER_NEW,
      label = "i18n::HopGui.Menu.File.New",
      image = "ui/images/add.svg",
      parentId = ID_MAIN_MENU_FILE_USER)
  public void newFile() {
    HopGui.getInstance().menuFileNew();
  }

  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = HopGui.ID_MAIN_MENU_FILE_USER_OPEN,
      label = "i18n::HopGui.Menu.File.Open",
      image = "ui/images/open.svg",
      parentId = ID_MAIN_MENU_FILE_USER)
  public void openFile() {
    if (!HopSecurityUi.check(Permission.FILE_VIEW)) {
      return;
    }
    try {
      transfer()
          .open(
              HOP_FILE_EXTENSIONS,
              MAX_HOP_FILE_SIZE,
              new UserFileTransfer.UploadListener() {
                @Override
                public void uploaded(String filename, Path uploadedFile) {
                  openUploadedFile(filename, uploadedFile);
                }

                @Override
                public void error(Exception exception) {
                  showError("HopGui.FileBrowser.Error.Upload", exception);
                }
              });
    } catch (Exception e) {
      showError("HopGui.FileBrowser.Error.Upload", e);
    }
  }

  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = HopGui.ID_MAIN_MENU_FILE_USER_SAVE,
      label = "i18n::HopGui.Menu.File.Save",
      image = "ui/images/save.svg",
      parentId = ID_MAIN_MENU_FILE_USER)
  public void saveFile() {
    downloadActiveFile(false);
  }

  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = HopGui.ID_MAIN_MENU_FILE_USER_SAVE_AS,
      label = "i18n::HopGui.Menu.File.SaveAs",
      image = "ui/images/save-as.svg",
      parentId = ID_MAIN_MENU_FILE_USER)
  public void saveFileAs() {
    downloadActiveFile(true);
  }

  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = HopGui.ID_MAIN_MENU_FILE_USER_EXPORT_TO_SVG,
      label = "i18n::HopGui.Menu.File.ExportToSVG",
      image = "ui/images/image.svg",
      parentId = ID_MAIN_MENU_FILE_USER)
  public void exportToSvg() {
    if (!HopSecurityUi.check(Permission.FILE_EXPORT)) {
      return;
    }
    try {
      HopGuiPipelineGraph pipelineGraph = HopGui.getActivePipelineGraph();
      if (pipelineGraph != null) {
        String name = safeFilename(pipelineGraph.getPipelineMeta().getName(), ".svg");
        String svg =
            PipelineSvgPainter.generatePipelineSvg(
                pipelineGraph.getPipelineMeta(), 1.0f, pipelineGraph.getVariables());
        transfer().download(name, "image/svg+xml", svg.getBytes(StandardCharsets.UTF_8));
        return;
      }

      HopGuiWorkflowGraph workflowGraph = HopGui.getActiveWorkflowGraph();
      if (workflowGraph != null) {
        String name = safeFilename(workflowGraph.getWorkflowMeta().getName(), ".svg");
        String svg =
            WorkflowSvgPainter.generateWorkflowSvg(
                workflowGraph.getWorkflowMeta(), 1.0f, workflowGraph.getVariables());
        transfer().download(name, "image/svg+xml", svg.getBytes(StandardCharsets.UTF_8));
        return;
      }
      // The menu item is hidden until a pipeline or workflow is active. Keep this guard quiet as
      // well in case a stale keyboard shortcut or menu event reaches the action.
      return;
    } catch (Exception e) {
      showError("HopGui.FileBrowser.Error.ExportSvg", e);
    }
  }

  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = HopGui.ID_MAIN_MENU_FILE_USER_EXPORT_PROJECT,
      label = "i18n::HopGui.Menu.File.ExportProjectZip",
      image = "ui/images/zipfile.svg",
      parentId = ID_MAIN_MENU_FILE_USER)
  public void exportProjectZip() {
    if (!HopSecurityUi.check(Permission.FILE_EXPORT)) {
      return;
    }
    Path zipFile = null;
    try {
      zipFile = Files.createTempFile(getSessionTempDirectory(), "hop-project-", ".zip");
      Files.deleteIfExists(zipFile);
      invokeGuiPlugin(
          PROJECT_EXPORT_MENU_ID,
          "exportProject",
          new Class<?>[] {String.class, boolean.class},
          zipFile.toString(),
          false);
      if (!Files.exists(zipFile)) {
        return;
      }

      String projectName =
          HopGui.getInstance().getVariables().getVariable("HOP_PROJECT_NAME", "hop-project");
      transfer().download(safeFilename(projectName, ".zip"), "application/zip", zipFile);
    } catch (Exception e) {
      showError("HopGui.FileBrowser.Error.ExportProject", e);
    } finally {
      if (zipFile != null) {
        try {
          Files.deleteIfExists(zipFile);
        } catch (IOException ignored) {
          // Best effort after the browser download has been prepared.
        }
      }
    }
  }

  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = HopGui.ID_MAIN_MENU_FILE_USER_IMPORT_KETTLE,
      label = "i18n::HopGui.Menu.File.ImportKettleZip",
      image = "ui/images/kettle-logo.svg",
      parentId = ID_MAIN_MENU_FILE_USER)
  public void importFromKettleZip() {
    if (!HopSecurityUi.check(Permission.FILE_CREATE)
        || !HopSecurityUi.check(Permission.METADATA_WRITE)) {
      return;
    }
    try {
      transfer()
          .open(
              ".zip",
              MAX_COMPRESSED_ZIP_SIZE,
              new UserFileTransfer.UploadListener() {
                @Override
                public void uploaded(String filename, Path uploadedFile) {
                  openKettleImport(filename, uploadedFile);
                }

                @Override
                public void error(Exception exception) {
                  showError("HopGui.FileBrowser.Error.ImportKettle", exception);
                }
              });
    } catch (Exception e) {
      showError("HopGui.FileBrowser.Error.ImportKettle", e);
    }
  }

  private UserFileTransfer transfer() throws IOException {
    if (transfer == null || transfer.isDisposed()) {
      transfer = new UserFileTransfer(HopGui.getInstance().getShell(), getSessionTempDirectory());
      if (!sessionCleanupRegistered) {
        RWT.getUISession().addUISessionListener(event -> userFileNames.clear());
        sessionCleanupRegistered = true;
      }
    }
    return transfer;
  }

  private void openUploadedFile(String filename, Path uploadedFile) {
    try {
      if (!HopSecurityUi.check(Permission.FILE_VIEW)) {
        return;
      }
      String safeName = safeFilename(filename, "");
      HopGui hopGui = HopGui.getInstance();
      AbstractMeta model =
          readUploadedFile(
              safeName, uploadedFile, hopGui.getMetadataProvider(), hopGui.getVariables());
      // The editor must never see the upload path: tab state, recent files and file watchers
      // otherwise retain a session temporary file as if it were a saved server document.
      IHopFileTypeHandler handler =
          model instanceof PipelineMeta pipeline
              ? HopGui.getExplorerPerspective().addPipeline(pipeline)
              : HopGui.getExplorerPerspective().addWorkflow((WorkflowMeta) model);
      userFileNames.put(handler, safeName);
      handler.updateGui();
      HopGui.getExplorerPerspective().activate();
      ExtensionPointHandler.callExtensionPoint(
          hopGui.getLog(),
          hopGui.getVariables(),
          model instanceof PipelineMeta
              ? HopExtensionPoint.PipelineAfterOpen.id
              : HopExtensionPoint.WorkflowAfterOpen.id,
          model);
    } catch (Exception e) {
      showError("HopGui.FileBrowser.Error.Open", e);
    }
  }

  static AbstractMeta readUploadedFile(
      String filename,
      Path uploadedFile,
      IHopMetadataProvider metadataProvider,
      IVariables variables)
      throws IOException, HopException {
    String extension = FilenameUtils.getExtension(filename);
    boolean pipeline = "hpl".equalsIgnoreCase(extension);
    if (!pipeline && !"hwf".equalsIgnoreCase(extension)) {
      throw new HopException("Only Hop pipeline and workflow files can be opened here.");
    }
    try (InputStream input = Files.newInputStream(uploadedFile)) {
      Node root = XmlHandler.loadXmlFile(input, null, false, false).getDocumentElement();
      String expectedTag = pipeline ? PipelineMeta.XML_TAG : WorkflowMeta.XML_TAG;
      if (root == null || !expectedTag.equals(root.getNodeName())) {
        throw new HopException("The uploaded file does not contain the selected Hop file type.");
      }
      AbstractMeta model;
      if (pipeline) {
        PipelineMeta pipelineMeta = new PipelineMeta();
        pipelineMeta.loadXml(root, null, metadataProvider, variables);
        model = pipelineMeta;
      } else {
        model = new WorkflowMeta(root, metadataProvider, variables);
      }
      if (StringUtils.isBlank(model.getName())) {
        model.setName(FilenameUtils.getBaseName(filename));
      }
      model.setFilename(null);
      // Like a new document, the uploaded copy has no permanent server location yet.
      model.setChanged();
      return model;
    }
  }

  private void openKettleImport(String filename, Path uploadedFile) {
    Path extractionDirectory = null;
    try {
      if (!HopSecurityUi.check(Permission.FILE_CREATE)
          || !HopSecurityUi.check(Permission.METADATA_WRITE)) {
        return;
      }
      if (!"zip".equalsIgnoreCase(FilenameUtils.getExtension(filename))) {
        throw new HopException("Select a ZIP file containing the Kettle/PDI project.");
      }

      extractionDirectory =
          Files.createDirectory(
              getSessionTempDirectory().resolve("kettle-import-" + UUID.randomUUID()));
      Path sourceFolder = extractZip(uploadedFile, extractionDirectory);
      invokeGuiPlugin(
          KETTLE_IMPORT_MENU_ID,
          "menuToolsImport",
          new Class<?>[] {String.class},
          sourceFolder.toString());
    } catch (Exception e) {
      showError("HopGui.FileBrowser.Error.ImportKettle", e);
    } finally {
      if (extractionDirectory != null) {
        deleteTree(extractionDirectory);
      }
    }
  }

  private void downloadActiveFile(boolean saveAs) {
    if (!HopSecurityUi.check(Permission.FILE_SAVE)) {
      return;
    }
    try {
      IHopFileTypeHandler handler = HopGui.getInstance().getActiveFileTypeHandler();
      if (handler == null) {
        showError(
            "HopGui.FileBrowser.Error.NoActiveFile",
            new HopException("No active pipeline or workflow"));
        return;
      }

      byte[] content = serialize(handler);
      String extension = handler.getFileType().getDefaultFileExtension();
      String name = suggestedFilename(handler, extension);
      if (saveAs) {
        name =
            new EnterStringDialog(
                    HopGui.getInstance().getActiveShell(),
                    name,
                    BaseMessages.getString(PKG, "HopGui.FileBrowser.SaveAs.Title"),
                    BaseMessages.getString(PKG, "HopGui.FileBrowser.SaveAs.Label"))
                .open();
        if (StringUtils.isBlank(name)) {
          return;
        }
      }

      String downloadName = safeFilename(name, extension);
      transfer()
          .download(
              downloadName,
              "application/xml",
              content,
              () -> {
                // A tab can be closed or edited while its HTTP download is in flight.
                if (HopGui.getExplorerPerspective().getItems().stream()
                    .noneMatch(item -> item.getTypeHandler() == handler)) {
                  return;
                }
                try {
                  if (saveAs) {
                    userFileNames.put(handler, downloadName);
                  }
                  markDownloaded(handler, content);
                } catch (HopException e) {
                  showError("HopGui.FileBrowser.Error.Save", e);
                }
              });
    } catch (Exception e) {
      showError("HopGui.FileBrowser.Error.Save", e);
    }
  }

  static void markDownloaded(IHopFileTypeHandler handler, byte[] downloadedContent)
      throws HopException {
    // A browser copy must not mark an existing server file as saved. For an untitled document,
    // acknowledge only the exact revision sent, leaving any edits made during the transfer dirty.
    if (StringUtils.isBlank(handler.getFilename())
        && Arrays.equals(downloadedContent, serialize(handler))) {
      if (handler.getSubject() instanceof AbstractMeta model) {
        model.clearChanged();
        handler.updateGui();
      }
    }
  }

  private String suggestedFilename(IHopFileTypeHandler handler, String extension) {
    String name =
        userFileNames.getOrDefault(
            handler,
            StringUtils.isNotBlank(handler.getFilename())
                ? FilenameUtils.getName(handler.getFilename().replace('\\', '/'))
                : handler.getName());
    return safeFilename(name, extension);
  }

  static byte[] serialize(IHopFileTypeHandler handler) throws HopException {
    String xml;
    if (handler.getSubject() instanceof PipelineMeta pipelineMeta) {
      xml = pipelineMeta.getXml(handler.getVariables());
    } else if (handler.getSubject() instanceof WorkflowMeta workflowMeta) {
      xml = workflowMeta.getXml(handler.getVariables());
    } else {
      throw new HopException("Only pipelines and workflows can currently be saved as user files.");
    }
    return (XmlHandler.getXmlHeader(Const.UTF_8) + xml).getBytes(StandardCharsets.UTF_8);
  }

  static String safeFilename(String name, String extension) {
    String safeName = FilenameUtils.getName(StringUtils.defaultIfBlank(name, "hop-file"));
    safeName = safeName.replaceAll("[\\p{Cntrl}\\\\/:*?\"<>|]", "_");
    if (StringUtils.isNotBlank(extension)
        && !safeName.toLowerCase().endsWith(extension.toLowerCase())) {
      safeName += extension;
    }
    return safeName;
  }

  static Path extractZip(Path zipFile, Path directory) throws IOException, HopException {
    long compressedSize = Files.size(zipFile);
    if (compressedSize <= 0 || compressedSize > MAX_COMPRESSED_ZIP_SIZE) {
      throw new HopException("The ZIP file is empty or exceeds the compressed size limit.");
    }

    Path extractionRoot = directory.toAbsolutePath().normalize();
    int entries = 0;
    int fileEntries = 0;
    long totalSize = 0;
    byte[] buffer = new byte[8192];
    Set<Path> extractedPaths = new HashSet<>();

    try (CountingInputStream compressed = new CountingInputStream(Files.newInputStream(zipFile));
        ZipInputStream zip = new ZipInputStream(compressed)) {
      ZipEntry entry;
      while ((entry = zip.getNextEntry()) != null) {
        if (++entries > MAX_ZIP_ENTRIES) {
          throw new HopException("The ZIP file contains too many entries.");
        }

        String entryName = entry.getName().replace('\\', '/');
        if (entryName.isBlank()
            || entryName.length() > MAX_ZIP_ENTRY_NAME_LENGTH
            || entryName.startsWith("/")
            || entryName.matches("^[A-Za-z]:.*")
            || entryName.indexOf('\0') >= 0) {
          throw new HopException("The ZIP file contains an invalid entry path.");
        }
        Path relativeEntry = Path.of(entryName).normalize();
        Path target = extractionRoot.resolve(relativeEntry).normalize();
        if (relativeEntry.getNameCount() > MAX_ZIP_ENTRY_DEPTH
            || !target.startsWith(extractionRoot)
            || !extractedPaths.add(target)) {
          throw new HopException("The ZIP file contains an invalid or duplicate entry path.");
        }

        if (entry.isDirectory()) {
          Files.createDirectories(target);
        } else {
          fileEntries++;
          Files.createDirectories(target.getParent());
          try (OutputStream output = Files.newOutputStream(target)) {
            long entrySize = 0;
            int length;
            while ((length = zip.read(buffer)) != -1) {
              if (length == 0) {
                continue;
              }
              entrySize += length;
              totalSize += length;
              if (entrySize > MAX_UNCOMPRESSED_ZIP_ENTRY_SIZE) {
                throw new HopException("A ZIP entry exceeds the uncompressed size limit.");
              }
              if (totalSize > MAX_UNCOMPRESSED_ZIP_SIZE) {
                throw new HopException("The uncompressed ZIP file is larger than 250 MiB.");
              }
              if (totalSize > MIN_COMPRESSION_RATIO_SIZE
                  && compressed.getByteCount() > 0
                  && totalSize / compressed.getByteCount() > MAX_COMPRESSION_RATIO) {
                throw new HopException("The ZIP file exceeds the allowed compression ratio.");
              }
              output.write(buffer, 0, length);
            }
          }
        }
        zip.closeEntry();
      }
    }

    if (entries == 0 || fileEntries == 0) {
      throw new HopException("The selected ZIP file is empty.");
    }

    try (Stream<Path> children = Files.list(extractionRoot)) {
      List<Path> paths = children.toList();
      if (paths.size() == 1 && Files.isDirectory(paths.get(0))) {
        return paths.get(0);
      }
    }
    return extractionRoot;
  }

  private static Object invokeGuiPlugin(
      String menuId, String methodName, Class<?>[] parameterTypes, Object... arguments)
      throws Exception {
    GuiRegistry guiRegistry = GuiRegistry.getInstance();
    GuiMenuItem menuItem = guiRegistry.findGuiMenuItem(HopGui.ID_MAIN_MENU, menuId);
    if (menuItem == null) {
      throw new HopException("The required Hop plugin is not installed.");
    }

    HopGui hopGui = HopGui.getInstance();
    String listenerClassName = menuItem.getListenerClassName();
    String instanceId = hopGui.getMainMenuWidgets().getInstanceId();
    Object plugin = guiRegistry.findGuiPluginObject(hopGui.getId(), listenerClassName, instanceId);
    Class<?> pluginClass = menuItem.getClassLoader().loadClass(listenerClassName);
    if (plugin == null) {
      plugin = pluginClass.getConstructor().newInstance();
      guiRegistry.registerGuiPluginObject(hopGui.getId(), listenerClassName, instanceId, plugin);
    }
    try {
      return pluginClass.getMethod(methodName, parameterTypes).invoke(plugin, arguments);
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof Exception cause) {
        throw cause;
      }
      throw e;
    }
  }

  private static Path getSessionTempDirectory() throws IOException {
    UISession session = RWT.getUISession();
    Path directory = (Path) session.getAttribute(SESSION_TEMP_DIRECTORY);
    if (directory != null) {
      return directory;
    }

    directory = Files.createTempDirectory(SESSION_TEMP_DIRECTORY_PREFIX);
    session.setAttribute(SESSION_TEMP_DIRECTORY, directory);
    Path sessionDirectory = directory;
    session.addUISessionListener(event -> deleteSessionTempDirectory(sessionDirectory));
    return directory;
  }

  private static void deleteSessionTempDirectory(Path directory) {
    deleteTree(directory);
  }

  private static void deleteTree(Path directory) {
    try (Stream<Path> files = Files.walk(directory)) {
      files
          .sorted(Comparator.reverseOrder())
          .forEach(
              file -> {
                try {
                  Files.deleteIfExists(file);
                } catch (IOException ignored) {
                  // Best effort during session teardown.
                }
              });
    } catch (IOException ignored) {
      // Best effort during session teardown.
    }
  }

  private static void showError(String messageKey, Exception cause) {
    String title = BaseMessages.getString(PKG, "HopGui.FileBrowser.Error.Title");
    String message = BaseMessages.getString(PKG, messageKey);
    HopGui.getInstance().getLog().logError(message, cause);
    new ErrorDialog(
        HopGui.getInstance().getActiveShell(), title, message, new HopException(message));
  }

  private static final class CountingInputStream extends FilterInputStream {
    private long byteCount;

    private CountingInputStream(InputStream input) {
      super(input);
    }

    @Override
    public int read() throws IOException {
      int value = super.read();
      if (value >= 0) {
        byteCount++;
      }
      return value;
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
      int count = super.read(buffer, offset, length);
      if (count > 0) {
        byteCount += count;
      }
      return count;
    }

    private long getByteCount() {
      return byteCount;
    }
  }
}
