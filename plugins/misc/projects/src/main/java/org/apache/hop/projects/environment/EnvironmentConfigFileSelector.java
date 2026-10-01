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

package org.apache.hop.projects.environment;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.fileinput.FileTypeFilter;
import org.apache.hop.core.fileinput.InputFile;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;

/**
 * Resolves environment configuration files from one file or from a directory.
 *
 * <p>Wildcards are regular expressions matched against the file name, using the same rules as
 * {@link FileInputList}. They are not shell globs: {@code .*\.json} matches JSON files and {@code
 * *.json} is rejected.
 */
public final class EnvironmentConfigFileSelector {

  private EnvironmentConfigFileSelector() {}

  /**
   * Files selected by an explicit list and, when a directory is set, by a directory scan.
   *
   * <p>Explicit paths stay in the given order. Directory matches are appended and a file already
   * present is not added again. A wildcard, an exclude wildcard, or subfolders without a directory
   * is an error. An empty directory argument with no extra options returns the explicit list, which
   * may be empty.
   */
  public static List<String> combine(
      IVariables variables,
      String[] explicitFiles,
      String directory,
      String wildcard,
      String excludeWildcard,
      boolean includeSubFolders)
      throws HopException {
    boolean directorySet = StringUtils.isNotEmpty(StringUtils.trimToNull(directory));
    boolean wildcardSet = StringUtils.isNotEmpty(StringUtils.trimToNull(wildcard));
    boolean excludeSet = StringUtils.isNotEmpty(StringUtils.trimToNull(excludeWildcard));
    if (!directorySet && (wildcardSet || excludeSet || includeSubFolders)) {
      throw new HopException(
          "Specify --environment-config-file-directory when using a configuration file wildcard or --environment-config-include-subfolders");
    }

    List<String> files = new ArrayList<>();
    if (explicitFiles != null) {
      for (String explicitFile : explicitFiles) {
        if (StringUtils.isEmpty(StringUtils.trimToNull(explicitFile))) {
          continue;
        }
        String trimmed = explicitFile.trim();
        if (!containsSameFile(variables, files, trimmed)) {
          files.add(trimmed);
        }
      }
    }
    if (!directorySet) {
      return files;
    }

    for (String matchedFile :
        resolve(variables, directory.trim(), wildcard, excludeWildcard, includeSubFolders)) {
      if (!containsSameFile(variables, files, matchedFile)) {
        files.add(matchedFile);
      }
    }
    return files;
  }

  /**
   * @param fileOrDirectory one configuration file, or a folder to scan
   * @param wildcard regular expression matched against the file name, or empty for every file
   * @param excludeWildcard regular expression for file names to skip, or empty
   * @param includeSubFolders also return matches from subfolders
   * @return existing file paths, in {@link FileInputList} order
   * @throws HopException when the location is missing, the expression is invalid, or nothing
   *     matches
   */
  public static List<String> resolve(
      IVariables variables,
      String fileOrDirectory,
      String wildcard,
      String excludeWildcard,
      boolean includeSubFolders)
      throws HopException {
    if (StringUtils.isEmpty(StringUtils.trimToNull(fileOrDirectory))) {
      return List.of();
    }

    IVariables space = variables == null ? new Variables() : variables;
    String location = fileOrDirectory.trim();
    String mask = StringUtils.trimToEmpty(wildcard);
    String exclude = StringUtils.trimToEmpty(excludeWildcard);
    validatePattern(space.resolve(mask), "wildcard");
    validatePattern(space.resolve(exclude), "exclude wildcard");

    InputFile inputFile = new InputFile();
    inputFile.setFileName(location);
    inputFile.setFileMask(mask);
    inputFile.setExcludeFileMask(exclude);
    inputFile.setIncludeSubFolders(includeSubFolders);
    inputFile.setFileRequired(true);
    inputFile.setFileTypeFilter(FileTypeFilter.ONLY_FILES);

    FileInputList fileInputList = FileInputList.createFileList(space, List.of(inputFile));
    if (fileInputList.nrOfFiles() == 0) {
      throw new HopException(
          "No configuration files matched '"
              + location
              + "'. The file or folder must exist. A wildcard is a regular expression matched against the file name, for example .*\\.json");
    }
    List<String> paths = new ArrayList<>();
    for (String path : fileInputList.getFileStrings()) {
      paths.add(path);
    }
    return paths;
  }

  private static void validatePattern(String pattern, String label) throws HopException {
    if (StringUtils.isEmpty(pattern)) {
      return;
    }
    try {
      Pattern.compile(pattern);
    } catch (PatternSyntaxException e) {
      throw new HopException(
          "Invalid "
              + label
              + " regular expression '"
              + pattern
              + "'. Use a regular expression such as .*\\.json, not a shell glob such as *.json",
          e);
    }
  }

  private static boolean containsSameFile(
      IVariables variables, List<String> files, String candidate) {
    for (String file : files) {
      if (sameFile(variables, file, candidate)) {
        return true;
      }
    }
    return false;
  }

  private static boolean sameFile(IVariables variables, String left, String right) {
    if (StringUtils.equals(left, right)) {
      return true;
    }
    try {
      IVariables space = variables == null ? new Variables() : variables;
      String leftName = HopVfs.getFilename(HopVfs.getFileObject(space.resolve(left), space));
      String rightName = HopVfs.getFilename(HopVfs.getFileObject(space.resolve(right), space));
      return leftName.equals(rightName);
    } catch (Exception e) {
      return false;
    }
  }
}
