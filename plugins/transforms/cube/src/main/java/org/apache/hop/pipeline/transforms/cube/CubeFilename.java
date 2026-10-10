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

package org.apache.hop.pipeline.transforms.cube;

import java.util.regex.Pattern;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.resource.IResourceNaming;

/**
 * Resolves a cube filename the same way at design time and at runtime.
 *
 * <p>{@code Internal.Transform.CopyNr} exists only on a running transform copy. Field lookup and
 * export run with the pipeline variables, so an unresolved token would be left in the path and the
 * file would not be found. The copy number passed in is applied even when the variable is missing,
 * nested inside another variable, written with {@code %%}, or spelled in a different case.
 */
public final class CubeFilename {

  /**
   * Quoted {@code \Q} blocks are not case-folded by {@code (?i)}, so the dots are escaped by hand.
   * The variable name has no other regex metacharacters.
   */
  private static final Pattern COPY_NR =
      Pattern.compile(
          "(?i)\\$\\{"
              + Const.INTERNAL_VARIABLE_TRANSFORM_COPYNR.replace(".", "\\.")
              + "\\}|%%"
              + Const.INTERNAL_VARIABLE_TRANSFORM_COPYNR.replace(".", "\\.")
              + "%%");

  private CubeFilename() {}

  /**
   * Resolve {@code filename} for the given 0-based copy.
   *
   * @param includeTransformNr when true, insert {@code _} and the copy number before the extension
   */
  public static String resolve(
      IVariables variables, String filename, boolean includeTransformNr, int copyNr) {
    String copy = Integer.toString(copyNr);
    Variables scoped = new Variables();
    if (variables != null) {
      scoped.initializeFrom(variables);
    }
    // Override a stale copy number inherited from a parent pipeline or the GUI.
    scoped.setVariable(Const.INTERNAL_VARIABLE_TRANSFORM_COPYNR, copy);

    String name = filename == null ? "" : filename;
    name = replaceCopyNr(name, copy);
    name = scoped.resolve(name);
    name = replaceCopyNr(name, copy);
    name = scoped.resolve(name);
    if (includeTransformNr) {
      name = insertCopyBeforeExtension(name, copy);
    }
    return name;
  }

  /**
   * Point an exported pipeline at the folder of the copy-0 file and keep the stored file name.
   *
   * <p>Copy 0 is opened only to find that folder. The stored base name is appended unchanged, so a
   * copy-number variable and the include-transform-nr option still select a file per copy. Returns
   * null when that copy-0 file does not exist.
   */
  public static String exportResourceName(
      IVariables variables, String filename, boolean includeTransformNr, IResourceNaming naming)
      throws HopException {
    try {
      String resolved = resolve(variables, filename, includeTransformNr, 0);
      FileObject fileObject = HopVfs.getFileObject(resolved, variables);
      if (!fileObject.exists()) {
        return null;
      }
      FileObject parent = fileObject.getParent();
      if (parent == null) {
        return naming.nameResource(fileObject, variables, true);
      }
      // The flag means "include the file name" in SimpleResourceNaming, despite the interface
      // calling it pathOnly. false maps the folder and leaves the name for us to append.
      String folder = naming.nameResource(parent, variables, false);
      String baseName = baseName(filename);
      if (baseName.isEmpty()) {
        baseName = fileObject.getName().getBaseName();
      }
      return appendBaseName(folder, baseName);
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  private static String baseName(String filename) {
    if (filename == null || filename.isEmpty()) {
      return "";
    }
    int slash = Math.max(filename.lastIndexOf('/'), filename.lastIndexOf('\\'));
    if (slash < 0) {
      return filename;
    }
    return filename.substring(slash + 1);
  }

  private static String appendBaseName(String folder, String baseName) {
    if (folder == null || folder.isEmpty()) {
      return baseName;
    }
    if (folder.endsWith("/") || folder.endsWith("\\")) {
      return folder + baseName;
    }
    return folder + "/" + baseName;
  }

  private static String replaceCopyNr(String name, String copy) {
    return COPY_NR.matcher(name).replaceAll(copy);
  }

  static String insertCopyBeforeExtension(String path, String copy) {
    int slash = Math.max(path.lastIndexOf('/'), path.lastIndexOf('\\'));
    int dot = path.lastIndexOf('.');
    String suffix = "_" + copy;
    if (dot > slash) {
      return path.substring(0, dot) + suffix + path.substring(dot);
    }
    return path + suffix;
  }
}
