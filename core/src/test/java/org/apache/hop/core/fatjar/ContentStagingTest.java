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

package org.apache.hop.core.fatjar;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ContentStagingTest {

  @TempDir Path tempDir;

  @BeforeAll
  static void init() throws Exception {
    HopLogStore.init();
    HopClientEnvironment.init();
  }

  @Test
  void stagesUnderTheContentHashOnce() throws Exception {
    Path jar = tempDir.resolve("local.jar");
    Files.write(jar, new byte[] {1, 2, 3, 4});
    Path staging = tempDir.resolve("staging");
    Files.createDirectories(staging);

    IVariables variables = new Variables();
    variables.setVariable("STAGING", staging.toString());
    ContentStaging stager =
        new ContentStaging(
            LogChannel.GENERAL,
            new VfsStagingStore(
                variables, "${STAGING}/", "http://minio:9000/hop-staging/", "no folder"));

    String url = stager.stageFatJar(jar);
    String name = "hop-native-" + FatJarCache.sha256Hex(jar).substring(0, 12) + ".jar";
    assertEquals(name, ContentStaging.fatJarName(jar));
    assertEquals("http://minio:9000/hop-staging/" + name, url);
    Path staged = staging.resolve(name);
    assertArrayEquals(Files.readAllBytes(jar), Files.readAllBytes(staged));

    // Same name and size: not uploaded again
    FileTime old = FileTime.from(Instant.now().minusSeconds(3600));
    Files.setLastModifiedTime(staged, old);
    stager.stageFatJar(jar);
    assertEquals(old, Files.getLastModifiedTime(staged));
  }

  @Test
  void withoutAClusterUrlTheClusterReadsTheStagingFolder() throws Exception {
    Path file = tempDir.resolve("hop-spark-package-abc.zip");
    Files.write(file, new byte[] {9});
    String folder = tempDir.resolve("staging").toString();
    String url =
        new ContentStaging(
                LogChannel.GENERAL, new VfsStagingStore(new Variables(), folder, "", "no folder"))
            .stage(file, file.getFileName().toString());
    assertEquals(folder + "/hop-spark-package-abc.zip", url);
  }

  @Test
  void aStagingFolderIsRequired() {
    HopException e =
        assertThrows(
            HopException.class,
            () -> new VfsStagingStore(new Variables(), "", null, "Set a staging folder"));
    assertEquals("Set a staging folder", e.getMessage().trim());
  }

  @Test
  void anyStoreCanStage() throws Exception {
    Map<String, Long> uploaded = new HashMap<>();
    IStagingStore store =
        new IStagingStore() {
          @Override
          public long size(String name) {
            return uploaded.getOrDefault(name, -1L);
          }

          @Override
          public void upload(Path localFile, String name) throws HopException {
            try {
              uploaded.put(name, Files.size(localFile));
            } catch (Exception e) {
              throw new HopException(e);
            }
          }

          @Override
          public String location(String name) {
            return "/Volumes/main/hop/" + name;
          }

          @Override
          public String clusterUrl(String name) {
            return "dbfs:/Volumes/main/hop/" + name;
          }
        };
    Path jar = tempDir.resolve("fat.jar");
    Files.write(jar, new byte[] {7, 7});
    ContentStaging staging = new ContentStaging(LogChannel.GENERAL, store);
    String url = staging.stageFatJar(jar);
    assertEquals("dbfs:/Volumes/main/hop/" + ContentStaging.fatJarName(jar), url);
    assertEquals(1, uploaded.size());
    staging.stageFatJar(jar);
    assertEquals(1, uploaded.size());
  }

  @Test
  void trimsTrailingSlashesButNotTheScheme() {
    assertEquals("s3://bucket", VfsStagingStore.trimSlash("s3://bucket//"));
    assertEquals("s3://", VfsStagingStore.trimSlash("s3://"));
  }
}
