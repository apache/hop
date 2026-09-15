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

package org.apache.hop.ui.hopgui.perspective.explorer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.pipeline.HopPipelineFileType;
import org.apache.hop.ui.hopgui.file.workflow.HopWorkflowFileType;

/** Helpers behind the "Create file / pipeline / workflow" entries of the explorer perspective. */
final class ExplorerCreateUtils {

  private ExplorerCreateUtils() {}

  /**
   * Append {@code extension} to {@code typedName} unless the name already ends with it
   * (case-insensitive). The extension may be given with or without its leading dot.
   */
  static String applyExtension(String typedName, String extension) {
    String name = typedName == null ? "" : typedName.trim();
    if (Utils.isEmpty(extension)) {
      return name;
    }
    String dotted = extension.startsWith(".") ? extension : "." + extension;
    if (name.toLowerCase(Locale.ROOT).endsWith(dotted.toLowerCase(Locale.ROOT))) {
      return name;
    }
    return name + dotted;
  }

  /** The file name without its last extension. A leading dot is not treated as a separator. */
  static String baseName(String fileName) {
    if (fileName == null) {
      return null;
    }
    int dot = fileName.lastIndexOf('.');
    if (dot <= 0) {
      return fileName;
    }
    return fileName.substring(0, dot);
  }

  /** Join a folder path and a file name with exactly one {@code /} separator. */
  static String childPath(String folderPath, String fileName) {
    return ExplorerPathUtils.normalizePath(folderPath) + "/" + fileName;
  }

  /**
   * The file types offered by the "Create file" dialog: those declaring {@link
   * IHopFileType#CAPABILITY_NEW}, minus pipelines and workflows which have their own menu entries.
   * Sorted by name so the combo is stable.
   */
  static List<IHopFileType> creatableFileTypes(List<IHopFileType> allFileTypes) {
    List<IHopFileType> creatable = new ArrayList<>();
    for (IHopFileType fileType : allFileTypes) {
      if (fileType instanceof HopPipelineFileType || fileType instanceof HopWorkflowFileType) {
        continue;
      }
      if (fileType.hasCapability(IHopFileType.CAPABILITY_NEW)) {
        creatable.add(fileType);
      }
    }
    creatable.sort(Comparator.comparing(IHopFileType::getName));
    return creatable;
  }

  static boolean fileExists(String path) throws HopFileException {
    return HopVfs.fileExists(path);
  }

  /**
   * True when the name can safely become a direct child of a folder: not blank, no path separator,
   * not a relative directory reference, and not ending with a dot (which Windows silently strips at
   * creation time, defeating the "never overwrite" guarantee). This is only the cheap first check;
   * {@link #resolvesInsideFolder(String, String)} is what actually guards against traversal.
   */
  static boolean isSimpleFileName(String typedName) {
    if (typedName == null) {
      return false;
    }
    String name = typedName.trim();
    if (name.isEmpty() || name.contains("/") || name.contains("\\") || name.endsWith(".")) {
      return false;
    }
    return !".".equals(name) && !"..".equals(name);
  }

  /**
   * True when {@code candidatePath} resolves to a direct child of {@code folderPath}. Resolving
   * both sides is what makes this safe: VFS decodes escape sequences and collapses {@code ..}
   * segments before the comparison, which inspecting the typed name alone cannot do.
   */
  static boolean resolvesInsideFolder(String folderPath, String candidatePath) {
    try {
      FileObject folder = HopVfs.getFileObject(folderPath);
      FileObject candidate = HopVfs.getFileObject(candidatePath);
      FileObject parent = candidate.getParent();
      return parent != null && parent.getName().equals(folder.getName());
    } catch (Exception e) {
      return false;
    }
  }

  /** Create an empty file. Refuses to touch an existing one. */
  static void createEmptyFile(String path) throws HopException {
    if (fileExists(path)) {
      throw new HopException("File already exists: " + path);
    }
    try {
      FileObject fileObject = HopVfs.getFileObject(path);
      fileObject.createFile();
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Unable to create file: " + path, e);
    }
  }
}
