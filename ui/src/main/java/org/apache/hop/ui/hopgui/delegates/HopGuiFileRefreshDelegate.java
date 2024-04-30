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

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.vfs2.FileChangeEvent;
import org.apache.commons.vfs2.FileListener;
import org.apache.commons.vfs2.impl.DefaultFileMonitor;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;

public class HopGuiFileRefreshDelegate {

  private HopGui hopGui;

  private DefaultFileMonitor fileMonitor;

  // key: vfs file uri
  // value: the corresponding fileTypeHandler
  //
  private Map<String, IHopFileTypeHandler> fileHandlerMap;

  // TODO: replace it with a config option
  private static final long DELAY = 1000l;

  public HopGuiFileRefreshDelegate(HopGui hopGui) {
    this.hopGui = hopGui;
    this.fileHandlerMap = new HashMap<>();
    this.fileMonitor =
        new DefaultFileMonitor(
            new FileListener() {

              @Override
              public void fileChanged(FileChangeEvent arg0) throws Exception {
                String fileName = arg0.getFileObject().getName().getURI();
                if (fileName != null) {
                  IHopFileTypeHandler fileHandler = fileHandlerMap.get(fileName);
                  if (fileHandler != null) {
                    if (!hopGui.getDisplay().isDisposed()) {
                      hopGui.getDisplay().asyncExec(fileHandler::reload);
                    }
                  }
                }
              }

              @Override
              public void fileCreated(FileChangeEvent arg0) throws Exception {}

              @Override
              public void fileDeleted(FileChangeEvent arg0) throws Exception {}
            });
    fileMonitor.setDelay(DELAY);
    fileMonitor.start();
  }

  // A typeHandler was registered while
  // 1. The tabItems in the MetadataPerspectives, HopDataOrchestrationPerspective and
  // ExplorerPerspective were created
  // 2. If the tabItem is for a new typeFile without any file name, it'll be registered when it's
  // saved in the file system
  //
  public void register(String fileName, IHopFileTypeHandler fileTypeHandler) {
    try {
      fileMonitor.addFile(HopVfs.getFileObject(fileName));
    } catch (HopFileException e) {
      hopGui.getLog().logError("Error registering new FileObject", e);
    }
    fileHandlerMap.put(fileName, fileTypeHandler);
  }

  public void remove(String fileName) {
    fileHandlerMap.remove(fileName);
    try {
      fileMonitor.removeFile(HopVfs.getFileObject(fileName));
    } catch (HopFileException e) {
      hopGui.getLog().logError("Error removing FileObject from fileListener", e);
    }
  }
}
