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

package org.apache.hop.marketplace.install;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.hop.core.Const;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Runs the launchers' {@code lib/apply-pending-changes.sh} on lists written by {@link
 * PendingChanges}, so the writer and the reader agree on the format. The Windows {@code .cmd}
 * counterpart cannot run here.
 */
class ApplyPendingChangesScriptTest {

  private static final Path SCRIPT =
      Path.of(System.getProperty("basedir", System.getProperty("user.dir")))
          .resolve("../../../assemblies/static/src/main/resources/lib/apply-pending-changes.sh")
          .normalize();

  @TempDir Path hopHome;

  @BeforeEach
  void requireShell() {
    assumeFalse(Const.isWindows(), "the shell launcher helper runs on Linux and macOS");
    assumeTrue(Files.isRegularFile(SCRIPT), "script not found at " + SCRIPT);
  }

  @Test
  void appliesTheCopiesAndDeletesInTheList() throws Exception {
    write("lib/core/shared.jar", "v1");
    write("plugins/tech/example/old.jar", "old");
    PendingChanges pending = new PendingChanges(hopHome);
    pending.queueCopy(
        write("download/shared-v2.jar", "v2"), hopHome.resolve("lib/core/shared.jar"));
    pending.queueCopy(
        write("download/new.jar", "new"), hopHome.resolve("plugins/tech/my example/new 1.jar"));
    pending.queueDelete(hopHome.resolve("plugins/tech/example/old.jar"));

    Result result = run(hopHome, "");

    assertEquals(0, result.exitCode());
    assertEquals("", result.stdout(), "the Hop command's own output stays clean");
    assertTrue(result.stderr().contains("applied 3 pending plugin changes"), result.stderr());
    assertEquals("v2", read("lib/core/shared.jar"));
    assertEquals("new", read("plugins/tech/my example/new 1.jar"));
    assertFalse(Files.exists(hopHome.resolve("plugins/tech/example/old.jar")));
    assertFalse(Files.exists(hopHome.resolve(PendingChanges.PENDING_FILE)));
    try (var sources = Files.list(hopHome.resolve(PendingChanges.SOURCES_DIR))) {
      assertEquals(0, sources.count(), "applied copies are removed");
    }
    try (var leftovers = Files.list(hopHome.resolve(PluginInstaller.RECEIPTS_DIR))) {
      assertEquals(
          List.of("pending-files"),
          leftovers.map(p -> p.getFileName().toString()).toList(),
          "no temporary files are left");
    }
  }

  @Test
  void aChangeThatFailsStaysForTheNextStart() throws Exception {
    // A file where the target's folder should be: the copy fails, for root as well.
    write("plugins/blocked", "a file, not a folder");
    write("plugins/tech/example/old.jar", "old");
    PendingChanges pending = new PendingChanges(hopHome);
    pending.queueCopy(write("download/x.jar", "x"), hopHome.resolve("plugins/blocked/x.jar"));
    pending.queueDelete(hopHome.resolve("plugins/tech/example/old.jar"));

    Result first = run(hopHome, "");

    assertEquals(0, first.exitCode(), "Hop still starts");
    assertTrue(first.stderr().contains("could not write plugins/blocked/x.jar"), first.stderr());
    assertTrue(first.stderr().contains("1 still waiting"), first.stderr());
    assertFalse(Files.exists(hopHome.resolve("plugins/tech/example/old.jar")));
    List<PendingChanges.Change> left = pending.read();
    assertEquals(1, left.size());
    assertEquals("plugins/blocked/x.jar", left.get(0).target());
    assertTrue(Files.exists(hopHome.resolve(left.get(0).source())), "the copy is kept");

    Files.delete(hopHome.resolve("plugins/blocked"));
    Result second = run(hopHome, "");

    assertTrue(second.stderr().contains("applied 1 pending plugin changes"), second.stderr());
    assertEquals("x", read("plugins/blocked/x.jar"));
    assertTrue(pending.isEmpty());
  }

  @Test
  void invalidLinesAreDroppedWithoutTouchingAnything() throws Exception {
    Path outside =
        Files.writeString(hopHome.getParent().resolve(hopHome.getFileName() + "-x"), "x");
    try {
      write("plugins/tech/example/keep.jar", "keep");
      write("config/hop-config.json", "{}");
      write("plugins/.marketplace/hop-example.json", "{}");
      write("plugins/.marketplace/pending-files/a.pending", "a");
      writeList(
          "delete|../" + outside.getFileName(),
          "delete|" + outside,
          "delete|plugins/tech/example",
          "delete|plugins/tech/./example/keep.jar",
          "delete|config/hop-config.json",
          "delete|plugins/.marketplace/hop-example.json",
          "delete|plugins/tech/*",
          "copy|plugins/tech/example/keep.jar|plugins/tech/example/copy.jar",
          "copy|plugins/.marketplace/pending-files/a.pending|plugins/../config/x.jar",
          "move|plugins/tech/example/keep.jar");

      Result result = run(hopHome, "");

      assertEquals(0, result.exitCode());
      assertTrue(result.stderr().contains("ignoring invalid pending change"), result.stderr());
      assertTrue(Files.exists(outside));
      assertEquals("keep", read("plugins/tech/example/keep.jar"));
      assertTrue(Files.exists(hopHome.resolve("config/hop-config.json")));
      assertTrue(Files.exists(hopHome.resolve("plugins/.marketplace/hop-example.json")));
      assertFalse(Files.exists(hopHome.resolve("plugins/tech/example/copy.jar")));
      assertFalse(Files.exists(hopHome.resolve("config/x.jar")));
      assertFalse(
          Files.exists(hopHome.resolve(PendingChanges.PENDING_FILE)),
          "invalid lines are not tried again");
    } finally {
      Files.deleteIfExists(outside);
    }
  }

  @Test
  void aCopyWhoseSourceIsGoneIsDropped() throws Exception {
    writeList("copy|plugins/.marketplace/pending-files/gone.jar.pending|lib/core/gone.jar");

    Result result = run(hopHome, "");

    assertTrue(result.stderr().contains("is gone"), result.stderr());
    assertFalse(Files.exists(hopHome.resolve("lib/core/gone.jar")));
    assertFalse(Files.exists(hopHome.resolve(PendingChanges.PENDING_FILE)));
  }

  @Test
  void appliesTheListBesideEveryPluginBaseFolder() throws Exception {
    Path volume = hopHome.resolve("volume");
    Files.createDirectories(volume.resolve("plugins/tech/example"));
    Files.writeString(volume.resolve("plugins/tech/example/old.jar"), "old");
    new PendingChanges(volume).queueDelete(volume.resolve("plugins/tech/example/old.jar"));
    write("plugins/tech/example/old.jar", "old");
    new PendingChanges(hopHome).queueDelete(hopHome.resolve("plugins/tech/example/old.jar"));

    // Spaces and empty entries as Hop allows them; "plugins" points back to this installation.
    Result result = run(hopHome, " volume/plugins , ,plugins,");

    assertFalse(Files.exists(volume.resolve("plugins/tech/example/old.jar")));
    assertFalse(Files.exists(hopHome.resolve("plugins/tech/example/old.jar")));
    assertEquals(2, count(result.stderr(), "applied 1 pending plugin changes"), result.stderr());
  }

  @Test
  void aListWithWindowsLineEndsIsApplied() throws Exception {
    write("plugins/tech/example/old.jar", "old");
    write("plugins/.marketplace/pending-files/a.pending", "a");
    write(
        PendingChanges.PENDING_FILE,
        "# edited on Windows\r\n"
            + "delete|plugins/tech/example/old.jar\r\n"
            + "copy|plugins/.marketplace/pending-files/a.pending|lib/core/a.jar\r\n");

    Result result = run(hopHome, "");

    assertTrue(result.stderr().contains("applied 2 pending plugin changes"), result.stderr());
    assertFalse(Files.exists(hopHome.resolve("plugins/tech/example/old.jar")));
    assertEquals("a", read("lib/core/a.jar"));
    assertFalse(Files.exists(hopHome.resolve(PendingChanges.PENDING_FILE)));
  }

  @Test
  void withoutAListNothingHappens() throws Exception {
    Files.createDirectories(hopHome.resolve("plugins"));

    Result result = run(hopHome, "");

    assertEquals(0, result.exitCode());
    assertEquals("", result.stdout());
    assertEquals("", result.stderr());
  }

  @Test
  void keepsLinesItDidNotHandle() throws Exception {
    // A running Hop can add a line while a launcher applies the list, so the script removes only
    // the lines it handled and leaves the rest of the file as it is, comments included.
    write("plugins/blocked", "a file");
    PendingChanges pending = new PendingChanges(hopHome);
    pending.queueCopy(write("download/x.jar", "x"), hopHome.resolve("plugins/blocked/x.jar"));
    pending.queueDelete(hopHome.resolve("plugins/tech/a.jar"));
    String before = Files.readString(hopHome.resolve(PendingChanges.PENDING_FILE));

    run(hopHome, "");

    // The failed copy stays, the delete (nothing to delete) is done, comments survive.
    String after = Files.readString(hopHome.resolve(PendingChanges.PENDING_FILE));
    assertTrue(after.startsWith("#"), after);
    assertFalse(after.contains("delete|plugins/tech/a.jar"), after);
    assertTrue(after.contains("|plugins/blocked/x.jar"), after);
    assertEquals(
        before.lines().filter(l -> !l.startsWith("delete")).toList(), after.lines().toList());
  }

  private record Result(int exitCode, String stdout, String stderr) {}

  private static Result run(Path workingFolder, String pluginBaseFolders) throws Exception {
    List<String> command = new ArrayList<>(List.of("sh", SCRIPT.toString(), pluginBaseFolders));
    Process process = new ProcessBuilder(command).directory(workingFolder.toFile()).start();
    process.getOutputStream().close();
    CompletableFuture<String> stdout =
        CompletableFuture.supplyAsync(() -> drain(process.getInputStream()));
    CompletableFuture<String> stderr =
        CompletableFuture.supplyAsync(() -> drain(process.getErrorStream()));
    assertTrue(process.waitFor(30, TimeUnit.SECONDS), "script did not finish");
    return new Result(process.exitValue(), stdout.get(), stderr.get());
  }

  private static String drain(InputStream in) {
    try (in) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      in.transferTo(out);
      return out.toString(StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static int count(String text, String part) {
    int count = 0;
    for (int i = text.indexOf(part); i >= 0; i = text.indexOf(part, i + 1)) {
      count++;
    }
    return count;
  }

  private Path write(String relative, String content) throws Exception {
    Path file = hopHome.resolve(relative);
    Files.createDirectories(file.getParent());
    return Files.writeString(file, content, StandardCharsets.UTF_8);
  }

  private void writeList(String... lines) throws Exception {
    write(PendingChanges.PENDING_FILE, String.join("\n", lines) + "\n");
  }

  private String read(String relative) throws Exception {
    return Files.readString(hopHome.resolve(relative), StandardCharsets.UTF_8);
  }
}
