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

  /**
   * A file in the commit, and the copy of its staged version that the git hook made.
   *
   * @param file the file in the working tree, which decides the configuration, the project and the
   *     path findings are reported against
   * @param content the staged version to read, or null to read {@code file}
   */
  public record StagedFile(File file, File content) {

    /** What is linted: the staged copy when there is one, the file on disk otherwise. */
    public File source() {
      return content != null ? content : file;
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
    List<StagedFile> staged = new ArrayList<>();
    for (File file : files) {
      staged.add(new StagedFile(file, null));
    }
    return lintStagedFiles(staged, failOn, variables, metadataProvider, strict);
  }

  /**
   * Lint the staged version of each file where the git hook copied one.
   *
   * @see #lintFiles(List, LintSeverity.FailOn, IVariables, IHopMetadataProvider, boolean)
   */
  public static Result lintStagedFiles(
      List<StagedFile> stagedFiles,
      LintSeverity.FailOn failOn,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      boolean strict) {
    List<LintResult> allResults = new ArrayList<>();
    HopLinter linter = new HopLinter();

    for (StagedFile staged : stagedFiles) {
      if (staged == null || staged.file() == null || !staged.source().isFile()) {
        continue;
      }
      File file = staged.file();
      try {
        if (strict) {
          // lintFile() would load the configuration again, leniently.
          linter.loadConfigurationStrictly(file);
          allResults.addAll(
              linter.processFile(file, staged.content(), metadataProvider, variables));
        } else {
          if (stagedFiles.size() == 1 || allResults.isEmpty()) {
            linter.loadConfigurationForContext(file);
          }
          allResults.addAll(linter.lintFile(file.getAbsolutePath(), metadataProvider, variables));
        }
      } catch (LintConfigurationException e) {
        throw e;
      } catch (Exception e) {
        log.logError("Error linting file " + file.getAbsolutePath() + ": " + e.getMessage(), e);
        linter
            .systemFinding(
                HopLinter.SYSTEM_FILE_PROCESSING,
                "Failed to process file: " + e.getMessage(),
                LintPathUtils.normalizePath(file.getAbsolutePath()),
                null)
            .ifPresent(allResults::add);
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

  public static List<StagedFile> readStagedFiles(String stagedFileListPath) throws HopException {
    return readStagedFiles(stagedFileListPath, null);
  }

  /**
   * The staged files to lint.
   *
   * <p>Each line is a file, optionally followed by a tab and the copy of its staged version that
   * the git hook wrote. With a copy the file itself need not exist: it can be staged and then
   * deleted or changed on disk, and the commit still holds the staged version.
   *
   * @param baseDirectory what a relative path in the list is relative to. git lists staged files
   *     relative to the repository root, and the hop launcher changes to the Hop installation
   *     before it starts Java, so resolving them against the working directory found none of them
   *     and the hook let every commit through.
   */
  public static List<StagedFile> readStagedFiles(String stagedFileListPath, File baseDirectory)
      throws HopException {
    List<StagedFile> files = new ArrayList<>();
    File listFile = new File(stagedFileListPath);
    if (!listFile.isFile()) {
      throw new HopException("Staged file list not found: " + stagedFileListPath);
    }

    try {
      for (String line : java.nio.file.Files.readAllLines(listFile.toPath())) {
        String[] parts = line.split("\t", 2);
        String trimmed = parts[0].trim();
        if (trimmed.isEmpty()) {
          continue;
        }
        File candidate = resolve(trimmed, baseDirectory);
        if (!isLintablePath(candidate.getAbsolutePath())) {
          continue;
        }
        File content =
            parts.length > 1 && !parts[1].isBlank()
                ? resolve(parts[1].trim(), baseDirectory)
                : null;
        StagedFile staged = new StagedFile(candidate, content);
        if (staged.source().isFile()) {
          files.add(staged);
        } else {
          // A staged file the hook cannot find is a hook that checks nothing, so say so.
          System.err.println("Staged file not found, not linted: " + staged.source().getPath());
        }
      }
    } catch (Exception e) {
      throw new HopException("Failed to read staged file list: " + e.getMessage(), e);
    }
    return files;
  }

  private static File resolve(String path, File baseDirectory) {
    File file = new File(path);
    if (!file.isAbsolute() && baseDirectory != null) {
      file = new File(baseDirectory, path);
    }
    return file;
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
