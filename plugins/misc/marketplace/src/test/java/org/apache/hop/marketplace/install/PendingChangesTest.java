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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.marketplace.install.PendingChanges.Action;
import org.apache.hop.marketplace.install.PendingChanges.Change;
import org.apache.hop.marketplace.install.PendingChanges.Effect;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PendingChangesTest {

  @TempDir Path hopHome;

  @Test
  void noListMeansNothingPending() throws Exception {
    PendingChanges pending = new PendingChanges(hopHome);

    assertTrue(pending.isEmpty());
    assertTrue(pending.describe().isEmpty());
  }

  @Test
  void queuedCopyKeepsItsOwnCopyOfTheFile() throws Exception {
    Path file = write(hopHome.resolve("plugins/.staging/example/lib/core/example-2.0.jar"), "v2");
    Path target = hopHome.resolve("lib/core/example-2.0.jar");

    new PendingChanges(hopHome).queueCopy(file, target);
    Files.delete(file);

    List<Change> changes = new PendingChanges(hopHome).read();
    assertEquals(1, changes.size());
    Change change = changes.get(0);
    assertEquals(Action.COPY, change.action());
    assertEquals("lib/core/example-2.0.jar", change.target());
    assertTrue(change.source().startsWith(PendingChanges.SOURCES_DIR + "/"));
    // The plugin registry loads every *.jar under plugins/, dot-folders included.
    assertTrue(change.source().endsWith(".jar" + PendingChanges.SOURCE_SUFFIX));
    assertEquals("v2", Files.readString(hopHome.resolve(change.source())));
  }

  @Test
  void listFormatIsOneLinePerChangeWithUnixLineEnds() throws Exception {
    Path file = write(hopHome.resolve("download/new.jar"), "new");
    PendingChanges pending = new PendingChanges(hopHome);
    pending.queueDelete(hopHome.resolve("plugins/tech/example/old.jar"));
    pending.queueCopy(file, hopHome.resolve("plugins/tech/example/new.jar"));

    String content =
        Files.readString(hopHome.resolve(PendingChanges.PENDING_FILE), StandardCharsets.UTF_8);
    assertFalse(content.contains("\r"));
    List<String> lines =
        content.lines().filter(l -> !l.startsWith("#")).filter(l -> !l.isBlank()).toList();
    assertEquals(2, lines.size());
    assertEquals("delete|plugins/tech/example/old.jar", lines.get(0));
    assertTrue(lines.get(1).startsWith("copy|plugins/.marketplace/pending-files/"));
    assertTrue(lines.get(1).endsWith("-new.jar.pending|plugins/tech/example/new.jar"));
  }

  @Test
  void laterChangeForTheSameTargetReplacesTheEarlierOne() throws Exception {
    Path target = hopHome.resolve("lib/core/example.jar");
    PendingChanges pending = new PendingChanges(hopHome);
    pending.queueCopy(write(hopHome.resolve("download/v2.jar"), "v2"), target);
    String firstSource = pending.read().get(0).source();
    pending.queueCopy(write(hopHome.resolve("download/v3.jar"), "v3"), target);

    List<Change> changes = pending.read();
    assertEquals(1, changes.size());
    assertEquals("v3", Files.readString(hopHome.resolve(changes.get(0).source())));
    assertFalse(Files.exists(hopHome.resolve(firstSource)), "the replaced copy is removed");

    pending.queueDelete(target);
    changes = pending.read();
    assertEquals(1, changes.size());
    assertEquals(Action.DELETE, changes.get(0).action());
    try (var sources = Files.list(hopHome.resolve(PendingChanges.SOURCES_DIR))) {
      assertEquals(0, sources.count(), "no copy is left behind for a target that is deleted");
    }
  }

  @Test
  void forgetDropsTheChangeAndTheEmptyList() throws Exception {
    Path target = hopHome.resolve("lib/core/example.jar");
    PendingChanges pending = new PendingChanges(hopHome);
    pending.queueCopy(write(hopHome.resolve("download/v2.jar"), "v2"), target);
    pending.queueDelete(hopHome.resolve("plugins/tech/example/old.jar"));

    pending.forget(target);
    assertEquals(1, pending.read().size());

    pending.forget(hopHome.resolve("plugins/tech/example/old.jar"));
    assertTrue(pending.isEmpty());
    assertFalse(Files.exists(hopHome.resolve(PendingChanges.PENDING_FILE)));
  }

  @Test
  void effectsSayWhatTheNextStartChanges() throws Exception {
    write(hopHome.resolve("lib/core/existing.jar"), "v1");
    PendingChanges pending = new PendingChanges(hopHome);
    pending.queueCopy(
        write(hopHome.resolve("download/a.jar"), "v2"), hopHome.resolve("lib/core/existing.jar"));
    pending.queueCopy(
        write(hopHome.resolve("download/b.jar"), "new"), hopHome.resolve("lib/core/added.jar"));
    pending.queueDelete(hopHome.resolve("plugins/tech/example/old.jar"));

    List<PendingChanges.PendingEffect> effects = pending.effects();
    assertEquals(Effect.REPLACE, effects.get(0).effect());
    assertEquals(Effect.ADD, effects.get(1).effect());
    assertEquals(Effect.REMOVE, effects.get(2).effect());
    assertEquals(
        List.of(
            "Replace lib/core/existing.jar",
            "Add lib/core/added.jar",
            "Remove plugins/tech/example/old.jar"),
        pending.describe());
  }

  @Test
  void targetsOutsideWhatTheLaunchersMayChangeAreRefused() {
    PendingChanges pending = new PendingChanges(hopHome);

    assertThrows(
        HopException.class,
        () -> pending.queueDelete(hopHome.getParent().resolve("outside.jar")),
        "outside the installation");
    assertThrows(
        HopException.class,
        () -> pending.queueDelete(hopHome.resolve("config/hop-config.json")),
        "not a plugin or library folder");
    assertThrows(
        HopException.class,
        () -> pending.queueDelete(hopHome.resolve("plugins/.marketplace/hop-example.json")),
        "the marketplace's own files");
    assertThrows(
        HopException.class,
        () -> pending.queueDelete(hopHome.resolve("plugins/tech/50%.jar")),
        "characters the Windows launcher cannot handle");
    // cmd reads the UTF-8 list in the console code page and would look for another name.
    Path nonAscii = null;
    try {
      nonAscii = hopHome.resolve("plugins/transforms/übersetzer/old-1.0.jar");
    } catch (InvalidPathException e) {
      // A JVM whose file name encoding cannot hold the name: such a file cannot be installed.
    }
    if (nonAscii != null) {
      Path target = nonAscii;
      assertThrows(HopException.class, () -> pending.queueDelete(target), "non-ASCII characters");
    }
    assertNull(
        Change.parse("delete|plugins/transforms/übersetzer/old-1.0.jar"),
        "a non-ASCII line written by hand is not read either");
  }

  @Test
  void webTargetIsAllowed() throws Exception {
    PendingChanges pending = new PendingChanges(hopHome);
    pending.queueDelete(hopHome.resolve("webapps/ROOT/WEB-INF/lib/example.jar"));

    assertEquals("webapps/ROOT/WEB-INF/lib/example.jar", pending.read().get(0).target());
  }

  @Test
  void invalidLinesAreSkipped() throws Exception {
    write(
        hopHome.resolve(PendingChanges.PENDING_FILE),
        String.join(
            "\n",
            "# a comment",
            "",
            "delete|plugins/tech/example/old.jar",
            "delete|../outside.jar",
            "delete|/etc/passwd",
            "delete|plugins/../lib/core/x.jar",
            "delete|C:/hop/lib/core/x.jar",
            "delete|plugins/tech/example",
            "delete|plugins/tech/example/",
            "delete|plugins/.marketplace/hop-example.json",
            "copy|plugins/tech/a.jar|plugins/tech/b.jar",
            "copy|plugins/.marketplace/pending-files/a.jar|plugins/tech/b.jar",
            "copy|plugins/.marketplace/pending-files/x/a.pending|plugins/tech/b.jar",
            "copy|plugins/.marketplace/pending-files/a.pending|plugins/tech/b.jar|extra",
            "move|plugins/tech/a.jar",
            "copy|plugins/.marketplace/pending-files/a.pending|lib/core/a.jar",
            ""));

    List<Change> changes = new PendingChanges(hopHome).read();

    assertEquals(3, changes.size(), changes.toString());
    assertEquals("plugins/tech/example/old.jar", changes.get(0).target());
    assertEquals("plugins/tech/example", changes.get(1).target());
    assertEquals("lib/core/a.jar", changes.get(2).target());
  }

  @Test
  void parseAndFormatRoundTrip() {
    Change copy =
        new Change(Action.COPY, "plugins/.marketplace/pending-files/a.pending", "lib/a.jar");
    Change delete = new Change(Action.DELETE, null, "plugins/x/a b.jar");

    assertEquals(copy, Change.parse(copy.toLine()));
    assertEquals(delete, Change.parse(delete.toLine()));
    assertNull(Change.parse("# comment"));
  }

  private static Path write(Path file, String content) throws Exception {
    Files.createDirectories(file.getParent());
    return Files.writeString(file, content, StandardCharsets.UTF_8);
  }
}
