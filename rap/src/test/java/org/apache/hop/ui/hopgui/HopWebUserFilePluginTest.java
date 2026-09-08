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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.workflow.WorkflowMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HopWebUserFilePluginTest {

  @TempDir Path tempDirectory;

  @BeforeAll
  static void initializeLogging() {
    HopLogStore.init();
  }

  @Test
  void uploadedPipelineIsUnsavedAndIndependentOfTemporaryFile() throws Exception {
    Path upload = tempDirectory.resolve("pipeline with spaces #1.hpl");
    Files.writeString(upload, "<pipeline><info><name>Uploaded pipeline</name></info></pipeline>");

    PipelineMeta model =
        (PipelineMeta)
            HopWebUserFilePlugin.readUploadedFile(
                upload.getFileName().toString(),
                upload,
                new MemoryMetadataProvider(),
                new Variables());
    Files.delete(upload);

    assertNull(model.getFilename());
    assertEquals("Uploaded pipeline", model.getName());
    assertTrue(model.hasChanged());
    assertTrue(model.getXml(new Variables()).contains("Uploaded pipeline"));
  }

  @Test
  void uploadedWorkflowIsUnsavedAndIndependentOfTemporaryFile() throws Exception {
    Path upload = tempDirectory.resolve("workflow.hwf");
    Files.writeString(upload, "<workflow><name>Uploaded workflow</name></workflow>");

    WorkflowMeta model =
        (WorkflowMeta)
            HopWebUserFilePlugin.readUploadedFile(
                "workflow.hwf", upload, new MemoryMetadataProvider(), new Variables());
    Files.delete(upload);

    assertNull(model.getFilename());
    assertEquals("Uploaded workflow", model.getName());
    assertTrue(model.hasChanged());
    assertTrue(model.getXml(new Variables()).contains("Uploaded workflow"));
  }

  @Test
  void rejectsWrongXmlTypeAndMalformedUpload() throws Exception {
    Path upload = tempDirectory.resolve("invalid.hpl");
    Files.writeString(upload, "<workflow><name>Wrong type</name></workflow>");
    assertThrows(
        HopException.class,
        () ->
            HopWebUserFilePlugin.readUploadedFile(
                "invalid.hpl", upload, new MemoryMetadataProvider(), new Variables()));

    Files.writeString(upload, "<pipeline>");
    assertThrows(
        HopException.class,
        () ->
            HopWebUserFilePlugin.readUploadedFile(
                "invalid.hpl", upload, new MemoryMetadataProvider(), new Variables()));
  }

  @Test
  void successfulDownloadClearsOnlyTheUploadedRevision() throws Exception {
    PipelineMeta model = new PipelineMeta();
    model.setName("Downloaded pipeline");
    model.setChanged();
    IHopFileTypeHandler handler = handler(model);

    HopWebUserFilePlugin.markDownloaded(handler, HopWebUserFilePlugin.serialize(handler));

    assertFalse(model.hasChanged());
    assertNull(model.getFilename());
    verify(handler).updateGui();
  }

  @Test
  void successfulWorkflowDownloadClearsItsChangedFlag() throws Exception {
    WorkflowMeta model = new WorkflowMeta();
    model.setName("Downloaded workflow");
    model.setChanged();
    IHopFileTypeHandler handler = mock(IHopFileTypeHandler.class);
    when(handler.getSubject()).thenReturn(model);
    when(handler.getVariables()).thenReturn(new Variables());

    HopWebUserFilePlugin.markDownloaded(handler, HopWebUserFilePlugin.serialize(handler));

    assertFalse(model.hasChanged());
    verify(handler).updateGui();
  }

  @Test
  void downloadDoesNotDiscardEditsMadeWhileTransferWasPending() throws Exception {
    PipelineMeta model = new PipelineMeta();
    model.setName("Before download");
    IHopFileTypeHandler handler = handler(model);
    byte[] snapshot = HopWebUserFilePlugin.serialize(handler);
    model.setName("Edited during download");
    model.setChanged();

    HopWebUserFilePlugin.markDownloaded(handler, snapshot);

    assertTrue(model.hasChanged());
    verify(handler, never()).updateGui();
  }

  @Test
  void downloadingCopyDoesNotMarkServerFileAsSaved() throws Exception {
    PipelineMeta model = new PipelineMeta();
    model.setName("Server pipeline");
    model.setChanged();
    IHopFileTypeHandler handler = handler(model);
    when(handler.getFilename()).thenReturn("project/server.hpl");

    HopWebUserFilePlugin.markDownloaded(handler, HopWebUserFilePlugin.serialize(handler));

    assertTrue(model.hasChanged());
    verify(handler, never()).updateGui();
  }

  private static IHopFileTypeHandler handler(PipelineMeta model) {
    IHopFileTypeHandler handler = mock(IHopFileTypeHandler.class);
    when(handler.getSubject()).thenReturn(model);
    when(handler.getVariables()).thenReturn(new Variables());
    return handler;
  }

  @Test
  void browserActionsAreRegisteredInsideTheFileUserMenu() throws Exception {
    assertEquals(
        HopGui.ID_MAIN_MENU,
        HopWebUserFilePlugin.class
            .getDeclaredMethod("menuFileUser")
            .getAnnotation(GuiMenuElement.class)
            .parentId());
    assertUserFileMenuAction("newFile", "15010-menu-file-user-new");
    assertUserFileMenuAction("openFile", "15020-menu-file-user-open");
    assertUserFileMenuAction("saveFile", HopGui.ID_MAIN_MENU_FILE_USER_SAVE);
    assertUserFileMenuAction("saveFileAs", HopGui.ID_MAIN_MENU_FILE_USER_SAVE_AS);
    assertUserFileMenuAction("exportToSvg", HopGui.ID_MAIN_MENU_FILE_USER_EXPORT_TO_SVG);
    assertUserFileMenuAction("exportProjectZip", HopGui.ID_MAIN_MENU_FILE_USER_EXPORT_PROJECT);
    assertUserFileMenuAction("importFromKettleZip", HopGui.ID_MAIN_MENU_FILE_USER_IMPORT_KETTLE);
    assertEquals(
        "ui/images/kettle-logo.svg",
        HopWebUserFilePlugin.class
            .getDeclaredMethod("importFromKettleZip")
            .getAnnotation(GuiMenuElement.class)
            .image());
  }

  @Test
  void extractsSingleProjectFolder() throws Exception {
    Path zip = zip("my-project/example.ktr", "kettle");
    Path extractionDirectory = Files.createDirectory(tempDirectory.resolve("extract-project"));

    Path source = HopWebUserFilePlugin.extractZip(zip, extractionDirectory);

    assertEquals(extractionDirectory.resolve("my-project"), source);
    assertEquals("kettle", Files.readString(source.resolve("example.ktr"), StandardCharsets.UTF_8));
  }

  @Test
  void rejectsPathsOutsideExtractionDirectory() throws Exception {
    Path zip = zip("../outside.ktr", "unsafe");
    Path extractionDirectory = Files.createDirectory(tempDirectory.resolve("extract-traversal"));

    HopException exception =
        assertThrows(
            HopException.class, () -> HopWebUserFilePlugin.extractZip(zip, extractionDirectory));
    assertFalse(Files.exists(tempDirectory.resolve("outside.ktr")));
    assertFalse(exception.getMessage().contains("outside.ktr"));
  }

  @Test
  void rejectsWindowsStylePathTraversal() throws Exception {
    Path zip = zip("..\\outside.ktr", "unsafe");
    Path extractionDirectory = Files.createDirectory(tempDirectory.resolve("extract-windows"));

    assertThrows(
        HopException.class, () -> HopWebUserFilePlugin.extractZip(zip, extractionDirectory));
    assertFalse(Files.exists(tempDirectory.resolve("outside.ktr")));
  }

  @Test
  void rejectsAbsoluteAndDriveQualifiedPaths() throws Exception {
    Path unixZip = zip("/outside.ktr", "unsafe");
    Path driveZip = zip("C:/outside.ktr", "unsafe");
    Path unixDirectory = Files.createDirectory(tempDirectory.resolve("extract-absolute"));
    Path driveDirectory = Files.createDirectory(tempDirectory.resolve("extract-drive"));

    assertThrows(HopException.class, () -> HopWebUserFilePlugin.extractZip(unixZip, unixDirectory));
    assertThrows(
        HopException.class, () -> HopWebUserFilePlugin.extractZip(driveZip, driveDirectory));
  }

  @Test
  void rejectsHighlyCompressedZipBomb() throws Exception {
    Path zip = zip("project/repeated.ktr", "0".repeat(2 * 1024 * 1024));
    Path extractionDirectory = Files.createDirectory(tempDirectory.resolve("extract-ratio"));

    assertThrows(
        HopException.class, () -> HopWebUserFilePlugin.extractZip(zip, extractionDirectory));
  }

  @Test
  void sanitizesDownloadHeaders() {
    String filename = UserFileTransfer.safeHeaderFilename("report\r\n\".svg");

    assertFalse(filename.contains("\r"));
    assertFalse(filename.contains("\n"));
    assertFalse(filename.contains("\""));
    assertTrue(UserFileTransfer.contentDisposition(filename).startsWith("attachment;"));
  }

  private Path zip(String name, String content) throws Exception {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (ZipOutputStream zip = new ZipOutputStream(output)) {
      zip.putNextEntry(new ZipEntry(name));
      zip.write(content.getBytes(StandardCharsets.UTF_8));
      zip.closeEntry();
    }
    Path zipFile = tempDirectory.resolve(UUID.randomUUID() + ".zip");
    Files.write(zipFile, output.toByteArray());
    return zipFile;
  }

  private static void assertUserFileMenuAction(String methodName, String id) throws Exception {
    Method method = HopWebUserFilePlugin.class.getDeclaredMethod(methodName);
    GuiMenuElement menu = method.getAnnotation(GuiMenuElement.class);
    assertNotNull(menu);
    assertEquals(id, menu.id());
    assertEquals(HopWebUserFilePlugin.ID_MAIN_MENU_FILE_USER, menu.parentId());
  }
}
