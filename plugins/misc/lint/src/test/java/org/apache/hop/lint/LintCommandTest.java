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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hop.core.HopVersionProvider;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/**
 * The {@code hop lint} command line.
 *
 * @see <a href="https://github.com/apache/hop/issues/8731">#8731</a>
 */
public class LintCommandTest {

  @TempDir private Path dir;

  private static LintResult finding(String ruleId, String severity) {
    return new LintResult(ruleId, ruleId, severity, "message", "/tmp/project/a.hpl");
  }

  @Test
  public void versionComesFromHop() {
    CommandLine commandLine = new CommandLine(new LintCommand());

    assertInstanceOf(
        HopVersionProvider.class,
        commandLine.getCommandSpec().versionProvider(),
        "-V printed " + "nothing without a version provider");
  }

  @Test
  public void severityIsAMinimum() {
    LintCommand command = new LintCommand();
    command.setSeverityFilter(LintSeverity.Level.WARNING);

    List<LintResult> shown =
        command.filterForDisplay(
            List.of(finding("A-1", "ERROR"), finding("B-1", "WARNING"), finding("C-1", "INFO")));

    assertEquals(
        List.of("A-1", "B-1"), shown.stream().map(LintResult::getRuleId).toList(), "-s WARNING");
  }

  @Test
  public void quietLeavesOutTheSummary() {
    List<LintResult> results = List.of(finding("A-1", "ERROR"));

    String quiet = LintReportWriter.renderText(results, false);

    assertFalse(quiet.contains("Lint Results Summary"), quiet);
    assertTrue(quiet.contains("A-1"), quiet);
    assertTrue(LintReportWriter.renderText(results).contains("Lint Results Summary"));
  }

  /**
   * Hop logs to the stdout it saw at start-up, so piping a JSON report to jq got log lines in front
   * of the document.
   */
  @Test
  public void aJsonReportIsAloneOnStdout() throws Exception {
    Files.createDirectories(dir.resolve("project"));
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    PrintStream capture = new PrintStream(captured, true, StandardCharsets.UTF_8);

    PrintStream systemOut = System.out;
    PrintStream logOut = HopLogStore.OriginalSystemOut;
    int exitCode;
    try {
      System.setOut(capture);
      HopLogStore.OriginalSystemOut = capture;
      LogChannel.GENERAL.logBasic("A log line that must not reach stdout");
      captured.reset();

      exitCode =
          new CommandLine(new LintCommand())
              .execute("-f", "JSON", dir.resolve("project").toString());
    } finally {
      System.setOut(systemOut);
      HopLogStore.OriginalSystemOut = logOut;
    }

    String stdout = captured.toString(StandardCharsets.UTF_8);
    assertEquals(0, exitCode, stdout);
    JsonNode report = new ObjectMapper().readTree(stdout);
    assertEquals(0, report.get("summary").get("total").asInt(), stdout);
    assertTrue(stdout.trim().startsWith("{"), stdout);
  }

  // ------------------------------------------------------------------ pre-commit hook

  private static boolean canRunShell() {
    return !System.getProperty("os.name").toLowerCase().contains("win")
        && new File("/bin/sh").canExecute();
  }

  private static int run(Path workDir, Map<String, String> env, String... command)
      throws Exception {
    ProcessBuilder builder = new ProcessBuilder(command).directory(workDir.toFile());
    builder.environment().putAll(env);
    builder.redirectErrorStream(true);
    Process process = builder.start();
    process.getInputStream().readAllBytes();
    return process.waitFor();
  }

  /** A repository with staged files, and a stand-in launcher that records what it was given. */
  private Path repositoryWithStagedFiles(String... files) throws Exception {
    Path repo = dir.resolve("repo");
    Files.createDirectories(repo);
    assumeTrue(run(repo, Map.of(), "git", "init", "-q") == 0, "git is needed for this test");
    for (String file : files) {
      Path path = repo.resolve(file);
      Files.createDirectories(path.getParent());
      Files.writeString(path, "content");
    }
    assertEquals(0, run(repo, Map.of(), "git", "add", "."));

    Path hopHome = dir.resolve("hop-home");
    Files.createDirectories(hopHome);
    Path launcher = hopHome.resolve("hop");
    Files.writeString(
        launcher,
        """
        #!/bin/sh
        while [ $# -gt 0 ]; do
          if [ "$1" = "--staged-file" ]; then
            cat "$2" > "$CAPTURE"
            echo "$2" > "$CAPTURE.path"
            cut -f2 "$2" | while IFS= read -r COPY; do cat "$COPY"; echo; done > "$CAPTURE.content"
          fi
          shift
        done
        exit 1
        """);
    assertTrue(launcher.toFile().setExecutable(true));

    Files.writeString(dir.resolve("pre-commit"), new LintCommand().hookScript());
    return repo;
  }

  private Map<String, String> hookEnvironment() {
    return Map.of(
        "HOP_HOME", dir.resolve("hop-home").toString(),
        "CAPTURE", dir.resolve("capture").toString());
  }

  /**
   * git lists staged files relative to the repository root, and the launcher changes to the Hop
   * installation, so none were found and every commit passed. Metadata at the root of the
   * repository was skipped too: "metadata/rdbms/x.json" has no leading slash.
   */
  @Test
  public void theHookPassesAbsolutePathsAndBlocksTheCommit() throws Exception {
    assumeTrue(canRunShell());
    Path repo =
        repositoryWithStagedFiles("load/customers.hpl", "metadata/rdbms/crm.json", "notes.txt");

    int status = run(repo, hookEnvironment(), "/bin/sh", dir.resolve("pre-commit").toString());

    assertEquals(1, status, "a failing lint has to block the commit");
    String root = repo.toRealPath().toString();
    assertEquals(
        List.of(root + "/load/customers.hpl", root + "/metadata/rdbms/crm.json"), stagedPaths());

    String listFile = Files.readString(dir.resolve("capture.path")).trim();
    assertFalse(Files.exists(Path.of(listFile)), "the staged list is left behind: " + listFile);
  }

  /** The files the stand-in launcher was given, without the staged copies. */
  private List<String> stagedPaths() throws Exception {
    return Files.readAllLines(dir.resolve("capture")).stream()
        .map(line -> Path.of(line.split("\t")[0]).toString())
        .sorted()
        .toList();
  }

  /**
   * The hook linted the files on disk, so a password that was staged and then removed only on disk
   * was committed unchecked.
   *
   * @see <a href="https://github.com/apache/hop/issues/9071">#9071</a>
   */
  @Test
  public void theHookLintsTheStagedVersionNotTheOneOnDisk() throws Exception {
    assumeTrue(canRunShell());
    Path repo = repositoryWithStagedFiles("load/customers.hpl");
    Files.writeString(repo.resolve("load/customers.hpl"), "changed on disk only");

    run(repo, hookEnvironment(), "/bin/sh", dir.resolve("pre-commit").toString());

    assertEquals("content", Files.readString(dir.resolve("capture.content")).trim());
  }

  /** git mv lists a renamed file as R, which --diff-filter=ACM left out of the check. */
  @Test
  public void theHookLintsARenamedFile() throws Exception {
    assumeTrue(canRunShell());
    Path repo = repositoryWithStagedFiles("load/customers.hpl");
    Map<String, String> noHook = Map.of();
    assertEquals(
        0,
        run(
            repo,
            noHook,
            "git",
            "-c",
            "user.name=lint",
            "-c",
            "user.email=lint@example.org",
            "commit",
            "-q",
            "-m",
            "first"));
    assertEquals(0, run(repo, noHook, "git", "mv", "load/customers.hpl", "load/clients.hpl"));
    Files.writeString(repo.resolve("load/clients.hpl"), "content");

    int status = run(repo, hookEnvironment(), "/bin/sh", dir.resolve("pre-commit").toString());

    assertEquals(1, status, "the renamed file has to be linted");
    assertEquals(List.of(repo.toRealPath() + "/load/clients.hpl"), stagedPaths());
  }

  /** The lines the stand-in launcher was given, file and staged copy, keyed by the file name. */
  private Map<String, String[]> stagedLines() throws Exception {
    Map<String, String[]> lines = new TreeMap<>();
    for (String line : Files.readAllLines(dir.resolve("capture"))) {
      String[] parts = line.split("\t");
      lines.put(Path.of(parts[0]).getFileName().toString(), parts);
    }
    return lines;
  }

  /**
   * The staged blob of a symlink is the path it points to, which is not a pipeline. Such a file is
   * linted on disk, as it was before the hook read staged versions.
   */
  @Test
  public void aSymlinkIsLintedOnDisk() throws Exception {
    assumeTrue(canRunShell());
    Path repo = repositoryWithStagedFiles("p/customers.hpl");
    Files.createSymbolicLink(repo.resolve("link.hpl"), Path.of("p/customers.hpl"));
    assertEquals(0, run(repo, Map.of(), "git", "add", "link.hpl"));

    run(repo, hookEnvironment(), "/bin/sh", dir.resolve("pre-commit").toString());

    Map<String, String[]> lines = stagedLines();
    assertEquals(1, lines.get("link.hpl").length, "no staged copy of a symlink");
    assertEquals(2, lines.get("customers.hpl").length, "a plain file still gets one");
  }

  /**
   * A file stored with Git LFS has a pointer as its staged blob, and so does any file behind a
   * clean filter. The filter here is not configured, so the test does not depend on git-lfs.
   */
  @Test
  public void aFilteredFileIsLintedOnDisk() throws Exception {
    assumeTrue(canRunShell());
    Path repo = repositoryWithStagedFiles("lfs/customers.hpl", "plain/orders.hpl");
    Files.writeString(repo.resolve(".gitattributes"), "lfs/*.hpl filter=example\n");
    assertEquals(0, run(repo, Map.of(), "git", "add", ".gitattributes"));

    run(repo, hookEnvironment(), "/bin/sh", dir.resolve("pre-commit").toString());

    Map<String, String[]> lines = stagedLines();
    assertEquals(1, lines.get("customers.hpl").length, "no staged copy of an LFS file");
    assertEquals(2, lines.get("orders.hpl").length);
  }

  @Test
  public void theHookDoesNotStartHopWithoutHopFiles() throws Exception {
    assumeTrue(canRunShell());
    Path repo = repositoryWithStagedFiles("notes.txt");

    int status = run(repo, hookEnvironment(), "/bin/sh", dir.resolve("pre-commit").toString());

    assertEquals(0, status);
    assertFalse(Files.exists(dir.resolve("capture")), "hop was started for a text file");
  }

  @Test
  public void stagedPathsAreResolvedAgainstTheRepository() throws Exception {
    Path repo = dir.resolve("repo");
    Files.createDirectories(repo.resolve("metadata/rdbms"));
    Files.writeString(repo.resolve("customers.hpl"), "content");
    Files.writeString(repo.resolve("metadata/rdbms/crm.json"), "{}");
    Path list = dir.resolve("staged.txt");
    Files.writeString(list, "customers.hpl\nmetadata/rdbms/crm.json\nmissing.hpl\nnotes.txt\n");

    List<PreCommitLintService.StagedFile> files =
        PreCommitLintService.readStagedFiles(list.toString(), repo.toFile());

    assertEquals(
        List.of(
            new PreCommitLintService.StagedFile(repo.resolve("customers.hpl").toFile(), null),
            new PreCommitLintService.StagedFile(
                repo.resolve("metadata/rdbms/crm.json").toFile(), null)),
        files);
  }

  /** A staged file can be gone from disk; its staged copy is what the commit holds. */
  @Test
  public void aStagedCopyIsReadEvenWhenTheFileIsGone() throws Exception {
    Path repo = dir.resolve("repo");
    Files.createDirectories(repo);
    Path copy = dir.resolve("copy-1");
    Files.writeString(copy, "staged");
    Path list = dir.resolve("staged.txt");
    Files.writeString(list, repo.resolve("deleted.hpl") + "\t" + copy + "\n");

    List<PreCommitLintService.StagedFile> files =
        PreCommitLintService.readStagedFiles(list.toString(), repo.toFile());

    assertEquals(
        List.of(
            new PreCommitLintService.StagedFile(
                repo.resolve("deleted.hpl").toFile(), copy.toFile())),
        files);
    assertEquals(copy.toFile(), files.get(0).source());
  }
}
