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
package org.apache.hop.airflow.engine;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.provider.UriParser;

/**
 * The DAG id of a workflow, as the launcher on the workers names it. Hop never loads the launcher,
 * so this repeats its rules; a test checks that both give the same ids.
 */
public final class AirflowDagIds {

  private AirflowDagIds() {}

  /**
   * @param dagIdPrefix the prefix of the run configuration, empty for the default
   * @param projectName the project the bundle is made of
   * @param relativePath the workflow file relative to the project, with forward slashes
   */
  public static String dagId(String dagIdPrefix, String projectName, String relativePath) {
    String prefix = StringUtils.isBlank(dagIdPrefix) ? defaultPrefix(projectName) : dagIdPrefix;
    String withoutExtension = StringUtils.removeEnd(relativePath, ".hwf");
    return prefix + withoutExtension.replace("/", "__").replaceAll("[^A-Za-z0-9_.-]", "_");
  }

  /**
   * The path of a workflow relative to the project, as the launcher sees it. VFS keeps a '%' in a
   * file name encoded ({@code 100%25.hwf}), and other file systems can keep more encoded; the
   * launcher reads the plain file name ({@code 100%.hwf}).
   */
  public static String relativePath(FileObject project, FileObject file)
      throws FileSystemException {
    return UriParser.decode(project.getName().getRelativeName(file.getName()));
  }

  /** A path from {@link #relativePath(FileObject, FileObject)}, to resolve again with VFS. */
  public static String encode(String relativePath) {
    return UriParser.encode(relativePath);
  }

  /** The project name, with the characters Airflow does not allow in a DAG id replaced. */
  static String defaultPrefix(String projectName) {
    return projectName.replaceAll("[^A-Za-z0-9_.-]", "_") + "__";
  }
}
