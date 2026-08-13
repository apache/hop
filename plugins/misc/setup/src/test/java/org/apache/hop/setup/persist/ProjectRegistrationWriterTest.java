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

package org.apache.hop.setup.persist;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.setup.HopEnvironmentApplyResult;
import org.apache.hop.setup.HopEnvironmentSpec;
import org.apache.hop.setup.HopSetupVariables;
import org.apache.hop.setup.OsFamily;
import org.apache.hop.setup.UserPaths;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ProjectRegistrationWriterTest {

  @TempDir Path temp;

  private static UserPaths unixPaths(Path home) {
    return new UserPaths(
        home,
        home.resolve(".local/share"),
        home.resolve(".local/state"),
        home.resolve(".config"),
        "/bin/bash");
  }

  @Test
  void createsDefaultProjectAndRegistersSamples() throws Exception {
    Path hopHome = temp.resolve("hop");
    Files.createDirectories(hopHome.resolve("plugins"));
    Files.createDirectories(hopHome.resolve("config/projects/default/metadata"));
    Files.writeString(
        hopHome
            .resolve("config/projects/default")
            .resolve(HopSetupVariables.PROJECT_CONFIG_FILENAME),
        "{ \"description\": \"template\" }");
    Files.createDirectories(hopHome.resolve("config/projects/samples"));
    Files.writeString(
        hopHome
            .resolve("config/projects/samples")
            .resolve(HopSetupVariables.PROJECT_CONFIG_FILENAME),
        "{ \"description\": \"samples\" }");

    Path configFolder = temp.resolve("user-config");
    Files.createDirectories(configFolder);
    Path defaultHome = temp.resolve("Documents/hop/default");

    HopEnvironmentSpec spec = new HopEnvironmentSpec();
    spec.setConfigFolder(configFolder.toString());
    spec.setCreateDefaultProject(true);
    spec.setDefaultProjectHome(defaultHome.toString());
    spec.setRegisterSamples(true);

    new ProjectRegistrationWriter()
        .apply(
            spec,
            OsFamily.UNIX,
            unixPaths(temp.resolve("home")),
            new HopEnvironmentApplyResult(),
            hopHome);

    assertTrue(Files.exists(defaultHome.resolve(HopSetupVariables.PROJECT_CONFIG_FILENAME)));
    String hopConfig = Files.readString(configFolder.resolve(HopSetupVariables.HOP_CONFIG_JSON));
    assertTrue(hopConfig.contains("\"projectName\" : \"default\""));
    assertTrue(hopConfig.contains(defaultHome.toString()));
    assertTrue(hopConfig.contains("\"projectName\" : \"samples\""));
    assertTrue(hopConfig.contains("config/projects/samples"));
    assertTrue(hopConfig.contains("\"defaultProject\" : \"default\""));
  }

  @Test
  void doesNotOverwriteExistingDefaultProjectConfig() throws Exception {
    Path defaultHome = temp.resolve("docs/hop/default");
    Files.createDirectories(defaultHome);
    Files.writeString(
        defaultHome.resolve(HopSetupVariables.PROJECT_CONFIG_FILENAME), "{ \"keep\": true }");
    Path configFolder = temp.resolve("cfg");
    Files.createDirectories(configFolder);

    HopEnvironmentSpec spec = new HopEnvironmentSpec();
    spec.setConfigFolder(configFolder.toString());
    spec.setCreateDefaultProject(true);
    spec.setDefaultProjectHome(defaultHome.toString());

    new ProjectRegistrationWriter()
        .apply(
            spec,
            OsFamily.UNIX,
            unixPaths(temp.resolve("home")),
            new HopEnvironmentApplyResult(),
            null);

    assertTrue(
        Files.readString(defaultHome.resolve(HopSetupVariables.PROJECT_CONFIG_FILENAME))
            .contains("keep"));
  }

  @Test
  void dryRunDoesNotWriteHopConfig() throws Exception {
    Path configFolder = temp.resolve("cfg");
    Files.createDirectories(configFolder);
    HopEnvironmentSpec spec = new HopEnvironmentSpec();
    spec.setConfigFolder(configFolder.toString());
    spec.setCreateDefaultProject(true);
    spec.setDefaultProjectHome(temp.resolve("p").toString());
    spec.setDryRun(true);

    HopEnvironmentApplyResult result = new HopEnvironmentApplyResult();
    result.setDryRun(true);
    new ProjectRegistrationWriter()
        .apply(spec, OsFamily.UNIX, unixPaths(temp.resolve("home")), result, null);

    assertFalse(Files.exists(configFolder.resolve(HopSetupVariables.HOP_CONFIG_JSON)));
    assertTrue(result.describe().contains("Would"));
  }

  @Test
  void upsertsExistingProjectEntry() throws Exception {
    Path configFolder = temp.resolve("cfg");
    Files.createDirectories(configFolder);
    Files.writeString(
        configFolder.resolve(HopSetupVariables.HOP_CONFIG_JSON),
        """
        {
          "projectsConfig": {
            "enabled": true,
            "projectConfigurations": [
              { "projectName": "default", "projectHome": "config/projects/default", "configFilename": "project-config.json" }
            ]
          }
        }
        """);
    Path defaultHome = temp.resolve("Documents/hop/default");
    HopEnvironmentSpec spec = new HopEnvironmentSpec();
    spec.setConfigFolder(configFolder.toString());
    spec.setCreateDefaultProject(true);
    spec.setDefaultProjectHome(defaultHome.toString());

    new ProjectRegistrationWriter()
        .apply(
            spec,
            OsFamily.UNIX,
            unixPaths(temp.resolve("home")),
            new HopEnvironmentApplyResult(),
            null);

    String hopConfig = Files.readString(configFolder.resolve(HopSetupVariables.HOP_CONFIG_JSON));
    assertTrue(hopConfig.contains(defaultHome.toString()));
    assertFalse(hopConfig.contains("config/projects/default"));
  }
}
