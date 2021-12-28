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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.history.AuditList;
import org.apache.hop.history.AuditManager;
import org.apache.hop.history.AuditState;
import org.apache.hop.history.AuditStateMap;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.perspective.metadata.MetadataPerspective;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HopGuiAuditDelegate {

  public static final String STATE_PROPERTY_ACTIVE = "active";
  public static final String METADATA_FILENAME_PREFIX = "METADATA:";

  private HopGui hopGui;

  public HopGuiAuditDelegate(HopGui hopGui) {
    this.hopGui = hopGui;
  }

  public void openLastFiles() {
    if (!hopGui.getProps().openLastFile()) {
      return;
    }

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
                // Regular filename
                IHopFileTypeHandler fileTypeHandler = hopGui.fileDelegate.fileOpen(filename);
                if (fileTypeHandler != null) {
                  // Restore zoom, scroll and so on
                  AuditState auditState = auditStateMap.get(filename);
                  if (auditState != null && fileTypeHandler != null) {
                    fileTypeHandler.applyStateProperties(auditState.getStateMap());

                    Boolean bActive = (Boolean) auditState.getStateMap().get(STATE_PROPERTY_ACTIVE);
                    if (bActive != null && bActive.booleanValue()) {
                      activeFileTypeHandler = fileTypeHandler;
                    }
                  }
                }
              }
            }
          } catch (Exception e) {
            new ErrorDialog(hopGui.getShell(), "Error", "Error opening file '" + filename + "'", e);
          }
        }

        // The active file in the perspective
        //
        if (activeFileTypeHandler != null) {
          perspective.setActiveFileTypeHandler(activeFileTypeHandler);
        }
      }
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
                    HopGui.getInstance().getVariables(), metadataProvider, metadataClass);
            MetadataEditor<IHopMetadata> editor = metadataManager.createEditor(metadata);

            // We assume that all tab items are closed so we can just open up a new editor for the
            // metadata
            //
            perspective.addEditor(editor);
          }
        }
      }

    } catch (Exception e) {
      throw new HopException(
          "Error opening metadata object '" + name + "' of class " + className, e);
    }
  }

  /** Remember all the open files per perspective */
  public void writeLastOpenFiles() {
    if (!hopGui.getProps().openLastFile()) {
      return;
    }

    List<IHopPerspective> perspectives = hopGui.getPerspectiveManager().getPerspectives();
    for (IHopPerspective perspective : perspectives) {
      IHopFileTypeHandler activeFileTypeHandler = perspective.getActiveFileTypeHandler();
      List<TabItemHandler> tabItems = perspective.getItems();
      if (tabItems != null) {
        // This perspective has the ability to handle multiple files.
        // Lets's save the files in the given order...
        //
        AuditStateMap auditStateMap = new AuditStateMap();

        List<String> files = new ArrayList<>();
        for (TabItemHandler tabItem : tabItems) {
          IHopFileTypeHandler typeHandler = tabItem.getTypeHandler();
          String filename = typeHandler.getFilename();
          String name = typeHandler.getName();
          if (StringUtils.isNotEmpty(filename)) {
            // Regular filename
            //
            files.add(filename);

            // Also save the state : active, zoom, ...
            //
            Map<String, Object> stateProperties = typeHandler.getStateProperties();
            boolean active =
                activeFileTypeHandler != null
                    && activeFileTypeHandler.getFilename() != null
                    && activeFileTypeHandler.getFilename().equals(filename);
            stateProperties.put(STATE_PROPERTY_ACTIVE, active);

            auditStateMap.add(new AuditState(filename, stateProperties));
          } else if (typeHandler instanceof MetadataEditor<?>) {
            // Don't save new unchanged metadata objects...
            //
            if (StringUtils.isNotEmpty(name)) {
              // Metadata saved by name
              // We also need to store the metadata type...
              //
              MetadataEditor<?> metadataEditor = (MetadataEditor<?>) typeHandler;
              IHopMetadata metadata = metadataEditor.getMetadata();
              Class<? extends IHopMetadata> metadataClass = metadata.getClass();

              // Save as METADATA:className:name
              //
              files.add(METADATA_FILENAME_PREFIX + metadataClass.getName() + ":" + name);
            }
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
