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

package org.apache.hop.ui.hopgui.delegates;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.history.AuditList;
import org.apache.hop.history.AuditManager;
import org.apache.hop.history.AuditState;
import org.apache.hop.history.AuditStateMap;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.HopFileTypeRegistry;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.metadata.MetadataPerspective;
import org.apache.hop.ui.util.SwtErrorHandler;
import org.eclipse.swt.SWT;

public class HopGuiAuditDelegate {

  private static final Class<?> PKG = HopGuiAuditDelegate.class;

  public static final String STATE_PROPERTY_ACTIVE = "active";

  /** Explorer perspective: pane index 0 = left, 1 = right. */
  public static final String STATE_PROPERTY_PANE = "pane";

  public static final String METADATA_FILENAME_PREFIX = "METADATA:";

  /**
   * File type name in state so the same file can be restored in different modes (e.g. pipeline vs
   * text).
   */
  public static final String STATE_PROPERTY_FILETYPE = "fileType";

  /**
   * Delimiter between file type name and filename in the audit key. Enables multiple tabs for the
   * same file. Uses a character that does not appear in paths or type names.
   */
  private static final String TAB_KEY_DELIMITER = "\u0001";

  private HopGui hopGui;

  public HopGuiAuditDelegate(HopGui hopGui) {
    this.hopGui = hopGui;
  }

  public void openLastFiles() {
    if (!hopGui.getProps().openLastFile()) {
      return;
    }

    // Collect files that fail to open
    List<String> failedFiles = new ArrayList<>();

    // Open the last files for each perspective...
    //
    List<IHopPerspective> perspectives = hopGui.getPerspectiveManager().getPerspectives();
    for (IHopPerspective perspective : perspectives) {
      List<TabItemHandler> tabItems = perspective.getItems();
      IHopFileTypeHandler activeFileTypeHandler = null;
      if (tabItems != null) {
        // This perspective has the ability to handle multiple files.
        // Let's load the files in the previously saved order...
        //
        AuditList auditList;
        try {
          auditList =
              AuditManager.getActive()
                  .retrieveList(HopNamespace.getNamespace(), perspective.getId());
        } catch (Exception e) {
          hopGui
              .getLog()
              .logError("Error reading audit list of perspective " + perspective.getId(), e);
          auditList = new AuditList();
        }

        AuditStateMap auditStateMap;
        try {
          auditStateMap =
              AuditManager.getActive()
                  .loadAuditStateMap(HopNamespace.getNamespace(), perspective.getId());
        } catch (HopException e) {
          hopGui
              .getLog()
              .logError("Error loading audit state map of perspective " + perspective.getId(), e);
          auditStateMap = new AuditStateMap();
        }

        // Restore editor split state before opening files so tabs land in the correct pane
        if (perspective instanceof ExplorerPerspective explorerPerspective) {
          explorerPerspective.applyRestoredEditorSplitState();
        }

        for (String filename : auditList.getNames()) {
          try {
            if (StringUtils.isNotEmpty(filename)) {
              if (filename.startsWith(METADATA_FILENAME_PREFIX)) {
                // Metadata tab information
                //
                int colonIndex = filename.indexOf(":", METADATA_FILENAME_PREFIX.length() + 1);
                if (colonIndex > 0) {
                  String className =
                      filename.substring(METADATA_FILENAME_PREFIX.length(), colonIndex);
                  String name = filename.substring(colonIndex + 1);
                  openMetadataObject(className, name);
                }
              } else {
                // Regular file: key may be composite "fileTypeName\u0001filename" or legacy
                // "filename"
                String tabKey = filename;
                String resolvedFilename = filename;
                IHopFileType hopFileToUse = null;
                if (filename.contains(TAB_KEY_DELIMITER)) {
                  String[] parts = filename.split(Pattern.quote(TAB_KEY_DELIMITER), 2);
                  if (parts.length >= 2) {
                    String fileTypeName = parts[0];
                    resolvedFilename = parts[1];
                    hopFileToUse =
                        HopFileTypeRegistry.getInstance().getFileTypeByName(fileTypeName);
                  }
                }
                if (hopFileToUse == null) {
                  hopFileToUse =
                      HopFileTypeRegistry.getInstance().findHopFileType(resolvedFilename);
                }

                // Set target pane for Explorer split view before opening
                if (perspective instanceof ExplorerPerspective explorerPerspective) {
                  AuditState auditState = auditStateMap.get(tabKey);
                  Object paneObj =
                      auditState != null ? auditState.getStateMap().get(STATE_PROPERTY_PANE) : null;
                  int pane = 0;
                  if (paneObj instanceof Number) {
                    pane = ((Number) paneObj).intValue();
                  } else if (paneObj != null) {
                    try {
                      pane = Integer.parseInt(paneObj.toString());
                    } catch (NumberFormatException ignored) {
                      pane = 0;
                    }
                  }
                  if (pane == 1 && explorerPerspective.getRightTabFolder() != null) {
                    perspective.setDropTargetFolder(explorerPerspective.getRightTabFolder());
                  }
                }

                IHopFileTypeHandler fileTypeHandler = null;
                if (hopFileToUse != null) {
                  fileTypeHandler =
                      hopGui.fileDelegate.fileOpenWithType(resolvedFilename, hopFileToUse, false);
                } else {
                  fileTypeHandler = hopGui.fileDelegate.fileOpen(resolvedFilename, false);
                }
                if (fileTypeHandler != null) {
                  // Restore zoom, scroll and so on
                  AuditState auditState = auditStateMap.get(tabKey);
                  if (auditState != null) {
                    fileTypeHandler.applyStateProperties(auditState.getStateMap());

                    Boolean bActive = (Boolean) auditState.getStateMap().get(STATE_PROPERTY_ACTIVE);
                    if (bActive != null && bActive) {
                      activeFileTypeHandler = fileTypeHandler;
                    }
                  }
                } else if (hopFileToUse == null) {
                  failedFiles.add(resolvedFilename);
                }
              }
            }
          } catch (Exception e) {
            // Collect failed files instead of showing error dialog immediately
            String displayName = filename;
            if (filename.contains(TAB_KEY_DELIMITER)) {
              String[] p = filename.split(Pattern.quote(TAB_KEY_DELIMITER), 2);
              if (p.length >= 2) {
                displayName = p[1];
              }
            }
            hopGui.getLog().logError("Error opening file '" + displayName + "'", e);
            failedFiles.add(displayName);
          }
        }

        // The active file in the perspective
        //
        if (activeFileTypeHandler != null) {
          perspective.setActiveFileTypeHandler(activeFileTypeHandler);
        }
      }
    }

    // Show a single dialog with all files that failed to open
    if (!failedFiles.isEmpty()) {
      StringBuilder message =
          new StringBuilder(
              BaseMessages.getString(
                  PKG, "HopGuiAuditDelegate.FilesNoLongerAvailable.Dialog.Message"));
      for (String failedFile : failedFiles) {
        message.append("  - ").append(failedFile).append("\n");
      }

      MessageBox box = new MessageBox(hopGui.getActiveShell(), SWT.OK | SWT.ICON_WARNING);
      box.setText(
          BaseMessages.getString(PKG, "HopGuiAuditDelegate.FilesNoLongerAvailable.Dialog.Header"));
      box.setMessage(message.toString());
      box.setMinimumSize(400, -1);
      box.open();
    }
  }

  private void openMetadataObject(String className, String name) throws HopException {
    try {
      IHopMetadataProvider metadataProvider = hopGui.getMetadataProvider();
      MetadataPerspective perspective = MetadataPerspective.getInstance();

      // Find the class...
      List<Class<IHopMetadata>> metadataClasses = metadataProvider.getMetadataClasses();
      for (Class<IHopMetadata> metadataClass : metadataClasses) {
        if (metadataClass.getName().equals(className)) {
          // See if the object is already open.
          // In rare cases we see doubles being saved in the audit logs.
          //
          String key = metadataClass.getAnnotation(HopMetadata.class).key();
          MetadataEditor<?> existingEditor = perspective.findEditor(key, name);
          if (existingEditor != null) {
            // We can skip this object, it's already loaded.
            continue;
          }

          // Get the serializer and open it up
          //
          IHopMetadataSerializer<IHopMetadata> serializer =
              metadataProvider.getSerializer(metadataClass);

          // Don't try to re-load removed or renamed objects...
          //
          if (serializer.exists(name)) {
            IHopMetadata metadata = serializer.load(name);
            MetadataManager<IHopMetadata> metadataManager =
                new MetadataManager<>(
                    HopGui.getInstance().getVariables(),
                    metadataProvider,
                    metadataClass,
                    hopGui.getShell());
            MetadataEditor<IHopMetadata> editor = metadataManager.createEditor(metadata);

            // We assume that all tab items are closed so we can just open up a new editor for the
            // metadata
            //
            perspective.addEditor(editor);
          }
        }
      }

    } catch (Exception e) {
      if (!SwtErrorHandler.handleException(e)) {
        throw new HopException(
            "Error opening metadata object '" + name + "' of class " + className, e);
      }
    }
  }

  /** Remember all the open files per perspective */
  public void writeLastOpenFiles() {
    // When we're re-opening files at the start of the Hop GUI, we don't need to save the open files
    // list.
    // Things get chaotic otherwise.
    //
    if (hopGui.isReOpeningFiles()) {
      return;
    }
    if (!hopGui.getProps().openLastFile()) {
      return;
    }

    List<IHopPerspective> perspectives = hopGui.getPerspectiveManager().getPerspectives();
    for (IHopPerspective perspective : perspectives) {
      IHopFileTypeHandler activeFileTypeHandler = perspective.getActiveFileTypeHandler();
      List<TabItemHandler> tabItems = perspective.getItems();
      if (tabItems != null) {
        // Use pane order for Explorer (left then right) so restore order matches split layout
        List<TabItemHandler> tabItemsToSave =
            perspective instanceof ExplorerPerspective ep
                ? ep.getTabItemHandlersInPaneOrder()
                : tabItems;

        // This perspective has the ability to handle multiple files.
        // Let's save the files in the given order...
        //
        AuditStateMap auditStateMap = new AuditStateMap();

        List<String> files = new ArrayList<>();
        for (TabItemHandler tabItem : tabItemsToSave) {
          IHopFileTypeHandler typeHandler = tabItem.getTypeHandler();
          String filename = typeHandler.getFilename();
          String name = typeHandler.getName();
          if (StringUtils.isNotEmpty(filename)) {
            // Regular filename — use composite key (fileType + filename) so same file in
            // different modes (e.g. pipeline vs text) gets separate tabs
            //
            IHopFileType fileType = typeHandler.getFileType();
            String tabKey =
                (fileType != null ? fileType.getName() : "") + TAB_KEY_DELIMITER + filename;
            files.add(tabKey);

            // Also save the state : active, zoom, pane (Explorer split), fileType, ...
            //
            Map<String, Object> stateProperties = typeHandler.getStateProperties();
            boolean active =
                activeFileTypeHandler != null
                    && activeFileTypeHandler.getFilename() != null
                    && activeFileTypeHandler.getFilename().equals(filename);
            stateProperties.put(STATE_PROPERTY_ACTIVE, active);
            if (fileType != null) {
              stateProperties.put(STATE_PROPERTY_FILETYPE, fileType.getName());
            }
            if (perspective instanceof ExplorerPerspective ep) {
              stateProperties.put(STATE_PROPERTY_PANE, ep.getPaneIndexForTab(tabItem.getTabItem()));
            }

            auditStateMap.add(new AuditState(tabKey, stateProperties));
          } else if (typeHandler instanceof MetadataEditor<?> metadataEditor
              && StringUtils.isNotEmpty(name)) {
            // Don't save new unchanged metadata objects...
            //
            // Metadata saved by name
            // We also need to store the metadata type...
            //
            IHopMetadata metadata = metadataEditor.getMetadata();
            Class<? extends IHopMetadata> metadataClass = metadata.getClass();

            // Save as METADATA:className:name
            //
            files.add(METADATA_FILENAME_PREFIX + metadataClass.getName() + ":" + name);
          }
        }
        AuditList auditList = new AuditList(files);
        try {
          AuditManager.getActive()
              .storeList(HopNamespace.getNamespace(), perspective.getId(), auditList);
          AuditManager.getActive()
              .saveAuditStateMap(HopNamespace.getNamespace(), perspective.getId(), auditStateMap);
        } catch (Exception e) {
          hopGui
              .getLog()
              .logError("Error writing audit list of perspective " + perspective.getId(), e);
        }
      }
    }
  }
}
