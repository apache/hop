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
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.lint.registry.LintConfigurationException;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/** Shared lint execution for GUI pre-commit checks and the CLI pre-commit hook. */
public final class PreCommitLintService {

  private static final ILogChannel log = LogChannel.GENERAL;

  public static final class Result {
    private final List<LintResult> results;
    private final List<LintResult> blockingResults;

    private Result(List<LintResult> results, List<LintResult> blockingResults) {
      this.results = results;
      this.blockingResults = blockingResults;
    }

    public List<LintResult> getResults() {
      return results;
    }

    public List<LintResult> getBlockingResults() {
      return blockingResults;
    }

    public boolean isBlocked() {
      return !blockingResults.isEmpty();
    }
  }

  private PreCommitLintService() {}

  public static Result lintFiles(
      List<File> files,
      LintSeverity.FailOn failOn,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    return lintFiles(files, failOn, variables, metadataProvider, false);
  }

  /**
   * @param strict fail on a hop-lint.yml that cannot be read, as the git hook does, instead of
   *     linting with the default rules, as Hop Gui's commit check does
   * @throws LintConfigurationException in strict mode, when a staged file's hop-lint.yml cannot be
   *     read
   */
  public static Result lintFiles(
      List<File> files,
      LintSeverity.FailOn failOn,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      boolean strict) {
    List<LintResult> allResults = new ArrayList<>();
    HopLinter linter = new HopLinter();

    for (File file : files) {
      if (file == null || !file.exists()) {
        continue;
      }
      try {
        if (strict) {
          // lintFile() would load the configuration again, leniently.
          linter.loadConfigurationStrictly(file);
          allResults.addAll(linter.processFile(file, metadataProvider, variables));
        } else {
          if (files.size() == 1 || allResults.isEmpty()) {
            linter.loadConfigurationForContext(file);
          }
          allResults.addAll(linter.lintFile(file.getAbsolutePath(), metadataProvider, variables));
        }
      } catch (LintConfigurationException e) {
        throw e;
      } catch (Exception e) {
        log.logError("Error linting file " + file.getAbsolutePath() + ": " + e.getMessage(), e);
        allResults.add(
            new LintResult(
                "SYSTEM-001",
                "File Processing Error",
                "ERROR",
                "Failed to process file: " + e.getMessage(),
                LintPathUtils.normalizePath(file.getAbsolutePath())));
      }
    }

    List<LintResult> blocking = new ArrayList<>();
    for (LintResult result : allResults) {
      if (LintSeverity.meetsFailOnThreshold(result.getSeverity(), failOn)) {
        blocking.add(result);
      }
    }
    return new Result(allResults, blocking);
  }

  public static List<File> readStagedFiles(String stagedFileListPath) throws HopException {
    return readStagedFiles(stagedFileListPath, null);
  }

  /**
   * The staged files to lint.
   *
   * @param baseDirectory what a relative path in the list is relative to. git lists staged files
   *     relative to the repository root, and the hop launcher changes to the Hop installation
   *     before it starts Java, so resolving them against the working directory found none of them
   *     and the hook let every commit through.
   */
  public static List<File> readStagedFiles(String stagedFileListPath, File baseDirectory)
      throws HopException {
    List<File> files = new ArrayList<>();
    File listFile = new File(stagedFileListPath);
    if (!listFile.isFile()) {
      throw new HopException("Staged file list not found: " + stagedFileListPath);
    }

    try {
      for (String line : java.nio.file.Files.readAllLines(listFile.toPath())) {
        String trimmed = line.trim();
        if (trimmed.isEmpty()) {
          continue;
        }
        File candidate = new File(trimmed);
        if (!candidate.isAbsolute() && baseDirectory != null) {
          candidate = new File(baseDirectory, trimmed);
        }
        if (!isLintablePath(candidate.getAbsolutePath())) {
          continue;
        }
        if (candidate.isFile()) {
          files.add(candidate);
        } else {
          // A staged file the hook cannot find is a hook that checks nothing, so say so.
          System.err.println("Staged file not found, not linted: " + candidate.getPath());
        }
      }
    } catch (Exception e) {
      throw new HopException("Failed to read staged file list: " + e.getMessage(), e);
    }
    return files;
  }

  public static boolean isLintablePath(String path) {
    if (path == null) {
      return false;
    }
    String lower = path.toLowerCase();
    if (lower.endsWith(".hpl") || lower.endsWith(".hwf")) {
      return true;
    }
    return HopMetadataFileLoader.isMetadataJsonFile(lower);
  }
}
