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

package org.apache.hop.databricks.deploy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.databricks.client.DatabricksJobsClient;
import org.apache.hop.databricks.client.WorkspaceFileMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HopSparkDeployHelperTest {

  @TempDir Path tempDir;

  @Test
  void normalizeSha256AcceptsChecksumOnlyAndChecksumFilename() {
    assertEquals("abc123", HopSparkDeployHelper.normalizeSha256("ABC123\n"));
    assertEquals("deadbeef", HopSparkDeployHelper.normalizeSha256("deadbeef  hop-native.jar"));
    assertEquals("cafe", HopSparkDeployHelper.normalizeSha256("  cafe\tfile  "));
  }

  @Test
  void sha256HexIsStable() throws Exception {
    Path f = tempDir.resolve("a.bin");
    Files.writeString(f, "hello-databricks");
    String h1 = HopSparkDeployHelper.sha256Hex(f);
    String h2 = HopSparkDeployHelper.sha256Hex(f);
    assertEquals(h1, h2);
    assertEquals(64, h1.length());
  }

  @Test
  void uploadFatJarSkippedWhenSizeAndSidecarMatch() throws Exception {
    Path jar = tempDir.resolve("fat.jar");
    Files.write(jar, "same-content-bytes".getBytes(StandardCharsets.UTF_8));
    long size = Files.size(jar);
    String sha = HopSparkDeployHelper.sha256Hex(jar);

    DatabricksJobsClient client = mock(DatabricksJobsClient.class);
    when(client.getFileMetadata("/Volumes/c/s/v/hop-native.jar"))
        .thenReturn(WorkspaceFileMetadata.ofFile(size));
    when(client.downloadTextIfExists("/Volumes/c/s/v/hop-native.jar.sha256"))
        .thenReturn(Optional.of(sha + "\n"));

    HopSparkDeployHelper.uploadFatJarIfNeeded(
        client, mock(ILogChannel.class), jar, "/Volumes/c/s/v/hop-native.jar");

    verify(client, never()).uploadToDbfs(any(), anyString());
    verify(client, never()).uploadText(anyString(), anyString());
  }

  @Test
  void uploadFatJarWhenSizeDiffers() throws Exception {
    Path jar = tempDir.resolve("fat.jar");
    Files.write(jar, "local-bytes".getBytes(StandardCharsets.UTF_8));
    String sha = HopSparkDeployHelper.sha256Hex(jar);

    DatabricksJobsClient client = mock(DatabricksJobsClient.class);
    when(client.getFileMetadata("/Volumes/c/s/v/hop-native.jar"))
        .thenReturn(WorkspaceFileMetadata.ofFile(999L));

    HopSparkDeployHelper.uploadFatJarIfNeeded(
        client, mock(ILogChannel.class), jar, "/Volumes/c/s/v/hop-native.jar");

    verify(client).uploadToDbfs(eq(jar), eq("/Volumes/c/s/v/hop-native.jar"));
    verify(client).uploadText(eq("/Volumes/c/s/v/hop-native.jar.sha256"), eq(sha + "\n"));
  }

  @Test
  void uploadFatJarWhenMissingSidecar() throws Exception {
    Path jar = tempDir.resolve("fat.jar");
    Files.write(jar, "local".getBytes(StandardCharsets.UTF_8));
    long size = Files.size(jar);
    String sha = HopSparkDeployHelper.sha256Hex(jar);

    DatabricksJobsClient client = mock(DatabricksJobsClient.class);
    when(client.getFileMetadata("/Volumes/c/s/v/hop-native.jar"))
        .thenReturn(WorkspaceFileMetadata.ofFile(size));
    when(client.downloadTextIfExists("/Volumes/c/s/v/hop-native.jar.sha256"))
        .thenReturn(Optional.empty());

    HopSparkDeployHelper.uploadFatJarIfNeeded(
        client, mock(ILogChannel.class), jar, "/Volumes/c/s/v/hop-native.jar");

    verify(client).uploadToDbfs(eq(jar), eq("/Volumes/c/s/v/hop-native.jar"));
    verify(client).uploadText(eq("/Volumes/c/s/v/hop-native.jar.sha256"), eq(sha + "\n"));
  }

  @Test
  void remoteJarMatchesFalseWhenHashDiffers() throws Exception {
    DatabricksJobsClient client = mock(DatabricksJobsClient.class);
    when(client.getFileMetadata("r.jar")).thenReturn(WorkspaceFileMetadata.ofFile(10L));
    when(client.downloadTextIfExists("r.jar.sha256")).thenReturn(Optional.of("aa".repeat(32)));
    assertFalse(
        HopSparkDeployHelper.remoteJarMatches(
            client, "r.jar", "r.jar.sha256", 10L, "bb".repeat(32)));
  }

  @Test
  void remoteJarMatchesTrueWhenHashMatches() throws Exception {
    String sha = "ab".repeat(32);
    DatabricksJobsClient client = mock(DatabricksJobsClient.class);
    when(client.getFileMetadata("r.jar")).thenReturn(WorkspaceFileMetadata.ofFile(10L));
    when(client.downloadTextIfExists("r.jar.sha256")).thenReturn(Optional.of(sha + "  name"));
    assertTrue(HopSparkDeployHelper.remoteJarMatches(client, "r.jar", "r.jar.sha256", 10L, sha));
  }

  @Test
  void relativePipelinePathUnderProjectHome() throws Exception {
    Path home = tempDir.resolve("proj");
    Path pipes = home.resolve("pipelines");
    Files.createDirectories(pipes);
    Path hpl = pipes.resolve("hello.hpl");
    Files.writeString(hpl, "<pipeline/>");
    String rel = HopSparkDeployHelper.relativePipelinePath(hpl.toString(), home.toString(), null);
    assertEquals("pipelines/hello.hpl", rel);
  }

  @Test
  void relativePipelinePathOutsideProjectReturnsNull() throws Exception {
    Path home = tempDir.resolve("proj2");
    Files.createDirectories(home);
    Path other = tempDir.resolve("other.hpl");
    Files.writeString(other, "<pipeline/>");
    assertEquals(
        null, HopSparkDeployHelper.relativePipelinePath(other.toString(), home.toString(), null));
  }

  @Test
  void deployWithExistingPackageAndEnvUsesNamedLaunch() throws Exception {
    Path jar = tempDir.resolve("fat.jar");
    Files.write(jar, "jar-bytes".getBytes(StandardCharsets.UTF_8));
    Path home = tempDir.resolve("home");
    Path pipes = home.resolve("pipelines");
    Files.createDirectories(pipes);
    Path hpl = pipes.resolve("entry.hpl");
    Files.writeString(hpl, "<pipeline/>");
    Path pkg = tempDir.resolve("pkg.zip");
    Files.write(pkg, "zip".getBytes(StandardCharsets.UTF_8));
    Path env = tempDir.resolve("env.json");
    Files.writeString(env, "{\"variables\":[]}");

    DatabricksJobsClient client = mock(DatabricksJobsClient.class);
    when(client.getFileMetadata(anyString())).thenReturn(WorkspaceFileMetadata.missing());

    var artifacts =
        HopSparkDeployHelper.deploy(
            client,
            null,
            null,
            mock(ILogChannel.class),
            jar.toString(),
            hpl.toString(),
            "native-rc",
            "/Volumes/c/s/v",
            new HopSparkDeployHelper.DeployOptions(
                true, home.toString(), pkg.toString(), env.toString()));

    assertEquals("/Volumes/c/s/v/hop-native.jar", artifacts.jarDbfs());
    assertEquals("/Volumes/c/s/v/hop-spark-package.zip", artifacts.projectPackageDbfs());
    assertEquals("/Volumes/c/s/v/env-config.json", artifacts.envConfigDbfs());
    assertEquals(null, artifacts.metadataDbfs());
    assertEquals("pipelines/entry.hpl", artifacts.launch().pipelinePath());
    assertTrue(artifacts.launch().hasProjectPackage());
    assertTrue(artifacts.launch().hasConfigFile());
    assertTrue(artifacts.launch().useNamedParameters());

    verify(client).uploadToDbfs(eq(pkg), eq("/Volumes/c/s/v/hop-spark-package.zip"));
    verify(client).uploadToDbfs(eq(env), eq("/Volumes/c/s/v/env-config.json"));
    // relative pipeline under package — no separate pipeline.hpl upload of entry
    verify(client, never()).uploadToDbfs(eq(hpl), anyString());
  }

  @Test
  void deployWithEnvOnlyStillUploadsPipelineAndMetadata() throws Exception {
    Path jar = tempDir.resolve("fat2.jar");
    Files.write(jar, "jar".getBytes(StandardCharsets.UTF_8));
    Path hpl = tempDir.resolve("p.hpl");
    Files.writeString(hpl, "<pipeline/>");
    Path env = tempDir.resolve("e.json");
    Files.writeString(env, "{}");

    // Minimal metadata provider
    org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider meta =
        new org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider();

    DatabricksJobsClient client = mock(DatabricksJobsClient.class);
    when(client.getFileMetadata(anyString())).thenReturn(WorkspaceFileMetadata.missing());

    var artifacts =
        HopSparkDeployHelper.deploy(
            client,
            meta,
            null,
            mock(ILogChannel.class),
            jar.toString(),
            hpl.toString(),
            "rc",
            "/Volumes/c/s/v2",
            new HopSparkDeployHelper.DeployOptions(false, null, null, env.toString()));

    assertEquals("/Volumes/c/s/v2/pipeline.hpl", artifacts.pipelineDbfs());
    assertEquals("/Volumes/c/s/v2/metadata.json", artifacts.metadataDbfs());
    assertEquals(null, artifacts.projectPackageDbfs());
    assertEquals("/Volumes/c/s/v2/env-config.json", artifacts.envConfigDbfs());
    assertTrue(artifacts.launch().useNamedParameters());
    verify(client).uploadToDbfs(eq(hpl), eq("/Volumes/c/s/v2/pipeline.hpl"));
  }
}
