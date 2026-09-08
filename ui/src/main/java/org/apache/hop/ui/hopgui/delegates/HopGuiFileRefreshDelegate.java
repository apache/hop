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

import org.apache.commons.vfs2.FileChangeEvent;
import org.apache.commons.vfs2.FileListener;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.impl.DefaultFileMonitor;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;

public class HopGuiFileRefreshDelegate {

  private final HopGui hopGui;

  private DefaultFileMonitor fileMonitor;

  private final FileRefreshHandlerMap fileHandlerMap;

  // TODO: replace it with a config option
  private static final long DELAY = 1000l;

  public HopGuiFileRefreshDelegate(HopGui hopGui) {
    this.hopGui = hopGui;
    this.fileHandlerMap = new FileRefreshHandlerMap();
    this.fileMonitor =
        new DefaultFileMonitor(
            new FileListener() {

              @Override
              public void fileChanged(FileChangeEvent arg0) throws Exception {
                String fileName = arg0.getFileObject().getName().getURI();
                if (fileName != null && !hopGui.getDisplay().isDisposed()) {
                  for (IHopFileTypeHandler fileHandler : fileHandlerMap.get(fileName)) {
                    hopGui.getDisplay().asyncExec(fileHandler::reload);
                  }
                }
              }

              @Override
              public void fileCreated(FileChangeEvent arg0) throws Exception {
                // Do nothing
              }

              @Override
              public void fileDeleted(FileChangeEvent arg0) throws Exception {
                // Do nothing
              }
            });
    fileMonitor.setDelay(DELAY);
    fileMonitor.start();
  }

  // A typeHandler was registered while
  // 1. The tabItems in the MetadataPerspectives and ExplorerPerspective were created
  // 2. If the tabItem is for a new typeFile without any file name, it'll be registered when it's
  // saved in the file system
  //
  public void register(String fileName, IHopFileTypeHandler fileTypeHandler) {
    if (fileName == null || !hopGui.getProps().isReloadingFilesOnChange()) {
      return;
    }

    try {
      FileObject file = HopVfs.getFileObject(fileName);
      String uri = file.getPublicURIString();
      boolean first = fileHandlerMap.add(uri, fileTypeHandler);
      fileHandlerMap.alias(uri, fileName);
      if (first) {
        fileMonitor.addFile(file);
      }
    } catch (HopFileException e) {
      hopGui.getLog().logError("Error registering new FileObject", e);
      fileHandlerMap.add(fileName, fileTypeHandler);
    }
  }

  public void remove(String fileName) {
    remove(fileName, null);
  }

  public void remove(String fileName, IHopFileTypeHandler fileTypeHandler) {
    if (fileName == null || !hopGui.getProps().isReloadingFilesOnChange()) {
      return;
    }
    try {
      FileObject file = HopVfs.getFileObject(fileName);
      String uri = file.getPublicURIString();
      boolean empty =
          fileTypeHandler == null
              ? fileHandlerMap.removeAll(uri)
              : fileHandlerMap.remove(uri, fileTypeHandler);
      if (fileTypeHandler != null) {
        fileHandlerMap.remove(fileName, fileTypeHandler);
      } else {
        fileHandlerMap.removeAll(fileName);
      }
      if (empty) {
        fileMonitor.removeFile(file);
      }
    } catch (HopFileException e) {
      hopGui.getLog().logError("Error removing FileObject from fileListener", e);
      if (fileTypeHandler == null) {
        fileHandlerMap.removeAll(fileName);
      } else {
        fileHandlerMap.remove(fileName, fileTypeHandler);
      }
    }
  }
}
