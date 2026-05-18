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

package org.apache.hop.git.util;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.empty.EmptyFileType;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.GenericFileType;
import org.eclipse.swt.graphics.Image;

public final class FileTypeUtils {

  private static final IHopFileType GENERIC_FILE_TYPE = new GenericFileType();

  private FileTypeUtils() {
    // Utility class
  }

  public static Image getImage(String path) throws HopException {
    ExplorerPerspective perspective = HopGui.getExplorerPerspective();
    IHopFileType fileType = perspective.getFileType(path);

    // If the file is deleted, the explorer perspective returns an EmptyFileType.
    // We use GenericFileType to find an image.
    if (fileType instanceof EmptyFileType) {
      fileType = GENERIC_FILE_TYPE;
    }

    return perspective.getFileTypeImage(fileType);
  }

  public static boolean isHopFileType(String fileName) {
    if (fileName != null) {
      try {
        ExplorerPerspective perspective = HopGui.getExplorerPerspective();
        if (perspective.getPipelineFileType().isHandledBy(fileName, false)) {
          return true;
        }
        if (perspective.getWorkflowFileType().isHandledBy(fileName, false)) {
          return true;
        }
      } catch (Exception e) {
        LogChannel.UI.logError(
            "Error checking if this file is a pipeline or workflow: " + fileName, e);
      }
    }
    return false;
  }
}
