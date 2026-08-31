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
package org.apache.hop.lint;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.delegates.HopGuiFileBeforeCommitExtension;

/** Extension point that performs linting before git operations in the Hop GUI Explorer. */
@ExtensionPoint(
    id = "PreCommitLintExtension",
    extensionPointId = "HopGuiFileBeforeCommit",
    description = "Runs linter checks before committing files")
public class PreCommitLintExtension implements IExtensionPoint<HopGuiFileBeforeCommitExtension> {

  private static final Class<?> PKG = PreCommitLintExtension.class; // for i18n purposes

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, HopGuiFileBeforeCommitExtension extension)
      throws HopException {
    try {
      LinterConfigPlugin config = LinterConfigPlugin.getInstance();

      if (!config.isLinterEnabled()) {
        log.logBasic("Linter disabled globally, skipping pre-commit linting");
        return;
      }

      if (!config.isPreCommitLintEnabled()) {
        log.logBasic("Pre-commit linting disabled in Hop configuration, skipping");
        return;
      }

      HopGui hopGui = HopGui.getInstance();
      if (hopGui == null) {
        log.logError("HopGui instance not available for pre-commit linting");
        return;
      }

      List<File> filesToLint = lintableFilesOf(extension, config);
      if (filesToLint.isEmpty()) {
        log.logBasic("No Hop files found for pre-commit linting");
        return;
      }

      LintSeverity.FailOn failOn =
          config.isPreCommitBlockWarnings()
              ? LintSeverity.FailOn.WARNING
              : LintSeverity.FailOn.ERROR;

      PreCommitLintService.Result result =
          PreCommitLintService.lintFiles(
              filesToLint, failOn, variables, hopGui.getMetadataProvider());

      // Keep every finding, blocking or not, so that "Show Lint Results" has the full picture
      // whichever way the commit goes.
      if (!result.getResults().isEmpty()) {
        LintResultsManager.getInstance().updateResults(result.getResults());
      }

      if (result.isBlocked()) {
        // Refuse rather than throw: the git plugin owns the dialog, and a listener which throws is
        // logged and ignored so that a broken one cannot stop people committing.
        extension.cancel(
            BaseMessages.getString(
                PKG,
                "PreCommitLintExtension.CommitBlocked.Reason",
                Integer.toString(result.getBlockingResults().size()),
                config.getPreCommitFailOnSeverity()));
        return;
      }

      log.logBasic(
          "Pre-commit linting passed. "
              + result.getResults().size()
              + " total issue(s) below blocking threshold.");

    } catch (Exception e) {
      log.logError("Error during pre-commit linting: " + e.getMessage(), e);
    }
  }

  /** The files in the commit which the linter has anything to say about. */
  private List<File> lintableFilesOf(
      HopGuiFileBeforeCommitExtension extension, LinterConfigPlugin config) {
    List<File> files = new ArrayList<>();
    for (String path : extension.getAbsoluteFilenames()) {
      File file = new File(path);
      // A commit can delete a file. There is nothing to lint then, and reading it would fail.
      if (file.isFile() && isLintableCommitFile(path, config)) {
        files.add(file);
      }
    }
    return files;
  }

  static boolean isLintableCommitFile(String path, LinterConfigPlugin config) {
    if (PreCommitLintService.isLintablePath(path)) {
      if (HopMetadataFileLoader.isMetadataJsonFile(path)) {
        return config.isPreCommitIncludeMetadata();
      }
      return true;
    }
    return false;
  }

  private List<File> findMetadataFiles(String directoryPath) {
    List<File> metadataFiles = new ArrayList<>();
    findMetadataFilesRecursive(new File(directoryPath), metadataFiles);
    return metadataFiles;
  }

  private void findMetadataFilesRecursive(File directory, List<File> metadataFiles) {
    if (directory == null || !directory.isDirectory()) {
      return;
    }
    File[] children = directory.listFiles();
    if (children == null) {
      return;
    }
    for (File child : children) {
      if (child.isDirectory()) {
        findMetadataFilesRecursive(child, metadataFiles);
      } else if (HopMetadataFileLoader.isMetadataJsonFile(child.getAbsolutePath())) {
        metadataFiles.add(child);
      }
    }
  }
}
