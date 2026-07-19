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

package org.apache.hop.marketplace.env;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HopEnvironmentLoaderTest {

  @TempDir Path tempDir;

  @Test
  void loadYaml() throws Exception {
    Path file = tempDir.resolve("hop-env.yaml");
    Files.writeString(
        file,
        """
        version: "1.0"
        hopVersion: "2.19.0"
        enforceOnRun: true
        repositories:
          - id: local
            url: http://localhost:8082/artifactory/hop-plugins-local/
        plugins:
          - artifactId: hop-tech-parquet
            version: "2.19.0"
          - artifactId: hop-engines-spark
        dependencies:
          - groupId: org.postgresql
            artifactId: postgresql
            version: "42.7.3"
        """,
        StandardCharsets.UTF_8);

    HopEnvironmentSpec env = HopEnvironmentLoader.load(file);
    assertEquals("1.0", env.getVersion());
    assertEquals("2.19.0", env.getHopVersion());
    assertTrue(env.isEnforceOnRun());
    assertEquals(1, env.getRepositories().size());
    assertEquals("local", env.getRepositories().get(0).getId());
    assertEquals(2, env.getPlugins().size());
    assertEquals("hop-tech-parquet", env.getPlugins().get(0).getArtifactId());
    assertEquals(1, env.getDependencies().size());
    assertEquals("postgresql", env.getDependencies().get(0).getArtifactId());
  }

  @Test
  void loadJson() throws Exception {
    Path file = tempDir.resolve("hop-env.json");
    Files.writeString(
        file,
        """
        {
          "version": "1.0",
          "hopVersion": "2.19.0",
          "enforceOnRun": false,
          "plugins": [ { "artifactId": "hop-tech-arrow", "version": "2.19.0" } ]
        }
        """,
        StandardCharsets.UTF_8);
    HopEnvironmentSpec env = HopEnvironmentLoader.load(file);
    assertFalse(env.isEnforceOnRun());
    assertEquals(1, env.getPlugins().size());
    assertEquals("hop-tech-arrow", env.getPlugins().get(0).getArtifactId());
  }
}
