/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.watchfiles;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class WatchFilesValidationTest {
  @TempDir Path temporary;

  @BeforeAll
  static void initializeHop() throws Exception {
    HopEnvironment.init();
  }

  private WatchFiles transform(WatchFilesMeta meta) {
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.setName("Validation");
    TransformMeta transform = new TransformMeta("Watch", meta);
    pipeline.addTransform(transform);
    return new WatchFiles(
        transform, meta, new WatchFilesData(), 0, pipeline, new LocalPipelineEngine(pipeline));
  }

  private WatchFilesMeta meta() throws Exception {
    WatchFilesMeta meta = new WatchFilesMeta();
    meta.setDirectory(Files.createDirectories(temporary.resolve("input")).toString());
    meta.setStateDirectory(temporary.resolve("state").toString());
    meta.setWatchId("input");
    return meta;
  }

  @Test
  void nativeRemoteRootFailsAndAutoRemoteUsesExistingVfsPolling() throws Exception {
    String uri = "ram:///watchfiles-validation-" + UUID.randomUUID();
    try (FileObject root = HopVfs.getFileObject(uri, new Variables())) {
      root.createFolder();
      WatchFilesMeta meta = meta();
      meta.setDirectory(uri);
      meta.setStrategy("NATIVE");
      WatchFiles nativeTransform = transform(meta);
      try {
        assertFalse(nativeTransform.init());
      } finally {
        nativeTransform.dispose();
      }
      meta.setStrategy("AUTO");
      WatchFiles auto = transform(meta);
      try {
        assertTrue(auto.init());
        assertNotNull(auto.getData().engine);
      } finally {
        auto.dispose();
        root.deleteAll();
      }
    }
  }

  @Test
  void stateWithinWatchedTreeIsRejected() throws Exception {
    WatchFilesMeta meta = meta();
    meta.setStateDirectory(temporary.resolve("input/state").toString());
    WatchFiles transform = transform(meta);
    try {
      assertFalse(transform.init());
    } finally {
      transform.dispose();
    }
  }

  @Test
  void missingRootAndStateDirectoryAreRejected() throws Exception {
    WatchFilesMeta meta = meta();
    meta.setDirectory(temporary.resolve("missing").toString());
    WatchFiles missing = transform(meta);
    try {
      assertFalse(missing.init());
    } finally {
      missing.dispose();
    }
    meta.setDirectory(temporary.resolve("input").toString());
    meta.setStateDirectory("");
    assertThrows(IllegalArgumentException.class, () -> meta.validate(new Variables()));
  }

  @Test
  void corruptStateIsPreservedAndInitializationReleasesNativeHandleAndLock() throws Exception {
    WatchFilesMeta meta = meta();
    Files.createDirectories(temporary.resolve("state"));
    Path corrupt = Files.writeString(temporary.resolve("state/input.json"), "{broken");
    WatchFiles transform = transform(meta);
    try {
      assertFalse(transform.init());
    } finally {
      transform.dispose();
    }
    org.junit.jupiter.api.Assertions.assertEquals("{broken", Files.readString(corrupt));
    try (JsonFileStateStore store =
        new JsonFileStateStore(temporary.resolve("state"), "input", "root", "scope", 100)) {
      assertThrows(java.io.IOException.class, store::load);
    }
  }

  @Test
  void configuredScannerLimitFailsInsteadOfReturningPartialSnapshot() throws Exception {
    Path input = Files.createDirectories(temporary.resolve("input"));
    Files.writeString(input.resolve("a.csv"), "a");
    Files.writeString(input.resolve("b.csv"), "b");
    try (FileObject root = HopVfs.getFileObject(input.toString(), new Variables())) {
      VfsFileScanner scanner = new VfsFileScanner(root, false, "", "", 1, () -> false);
      assertThrows(WatchLimitException.class, scanner::snapshot);
    }
  }

  @Test
  void emptyAndRegexAllFiltersIncludeEveryFileAndGlobStarIsRejected() throws Exception {
    WatchFilesMeta meta = meta();
    meta.setIncludeSubdirectories(true);
    meta.setIncludeWildcard("");
    meta.setExcludeWildcard("");
    meta.validate(new Variables());
    Path input = temporary.resolve("input");
    Files.writeString(input.resolve("a.csv"), "a");
    Files.writeString(input.resolve("b.txt"), "b");
    Files.writeString(Files.createDirectories(input.resolve("nested")).resolve("c.log"), "c");
    try (FileObject root = HopVfs.getFileObject(input.toString(), new Variables())) {
      var all = new VfsFileScanner(root, true, "", "", 10, () -> false).snapshot();
      org.junit.jupiter.api.Assertions.assertEquals(3, all.size());
      org.junit.jupiter.api.Assertions.assertEquals(
          all, new VfsFileScanner(root, true, ".*", "", 10, () -> false).snapshot());
    }
    meta.setIncludeWildcard("*");
    assertThrows(
        java.util.regex.PatternSyntaxException.class, () -> meta.validate(new Variables()));
  }

  @Test
  void spacesUnicodeAndFileUrisResolveToSameLocalIdentity() throws Exception {
    Path input = Files.createDirectories(temporary.resolve("árbol con espacios"));
    Files.writeString(input.resolve("datos ü.csv"), "a");
    try (FileObject pathRoot = VfsFileScanner.resolveFile(input.toString(), new Variables());
        FileObject uriRoot =
            VfsFileScanner.resolveFile(input.toUri().toString(), new Variables())) {
      org.junit.jupiter.api.Assertions.assertEquals(
          VfsFileScanner.localPath(pathRoot), VfsFileScanner.localPath(uriRoot));
      org.junit.jupiter.api.Assertions.assertEquals(
          input.toAbsolutePath(), VfsFileScanner.localPath(pathRoot));
      org.junit.jupiter.api.Assertions.assertEquals(
          new VfsFileScanner(pathRoot, false, ".*\\.csv", "", 10, () -> false).snapshot(),
          new VfsFileScanner(uriRoot, false, ".*\\.csv", "", 10, () -> false).snapshot());
    }
  }
}
