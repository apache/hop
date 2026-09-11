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

package org.apache.hop.projects.xp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataInstance;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.projects.util.Defaults;
import org.apache.hop.projects.util.ProjectsUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HopImportDbConnectionsTest {

  private static final String PROJECT_NAME = "import-connections-target";

  private Path tempRoot;
  private MultiMetadataProvider previousMetadataProvider;

  @BeforeAll
  static void beforeAll() throws Exception {
    HopLogStore.init();
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    tempRoot = Files.createTempDirectory("hop-import-db-connections");
    HopConfig.setInMemoryMode(true);
    previousMetadataProvider = HopMetadataInstance.getMetadataProvider();
  }

  @AfterEach
  void tearDown() throws Exception {
    ProjectsConfigSingleton.getConfig().removeProjectConfig(PROJECT_NAME);
    HopMetadataInstance.setMetadataProvider(previousMetadataProvider);
    HopConfig.setInMemoryMode(false);
    if (tempRoot != null && Files.exists(tempRoot)) {
      try (Stream<Path> walk = Files.walk(tempRoot)) {
        walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
      }
      tempRoot = null;
    }
  }

  @Test
  void importingConnectionsDoesNotActivateTheTargetProject() throws Exception {
    Path originalHome = tempRoot.resolve("original-home");
    Path targetHome = tempRoot.resolve("target-home");
    Files.createDirectories(originalHome);
    Files.createDirectories(targetHome);
    Files.writeString(
        targetHome.resolve(ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME),
        "{\n"
            + "  \"metadataBaseFolder\" : \"${PROJECT_HOME}/metadata\",\n"
            + "  \"unitTestsBasePath\" : \"${PROJECT_HOME}\",\n"
            + "  \"dataSetsCsvFolder\" : \"${PROJECT_HOME}/datasets\",\n"
            + "  \"enforcingExecutionInHome\" : true,\n"
            + "  \"config\" : { \"variables\" : [ ] }\n"
            + "}\n",
        StandardCharsets.UTF_8);

    ProjectsConfigSingleton.getConfig()
        .addProjectConfig(
            new ProjectConfig(
                PROJECT_NAME,
                targetHome.toString(),
                ProjectsConfig.DEFAULT_PROJECT_CONFIG_FILENAME));

    IVariables variables = new Variables();
    variables.setVariable(ProjectsUtil.VARIABLE_PROJECT_HOME, originalHome.toString());
    variables.setVariable(Defaults.VARIABLE_HOP_PROJECT_NAME, "original");
    variables.setVariable(Const.HOP_METADATA_FOLDER, originalHome + "/metadata");

    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setName("imported-db");
    databaseMeta.setDatabaseType("NONE");
    List<DatabaseMeta> connections = new ArrayList<>();
    connections.add(databaseMeta);
    TreeMap<String, String> connectionFileMap = new TreeMap<>();
    connectionFileMap.put("source.ktr", "imported-db");

    new HopImportDbConnections()
        .callExtensionPoint(
            LogChannel.GENERAL,
            variables,
            new Object[] {PROJECT_NAME, connections, connectionFileMap});

    assertEquals(
        originalHome.toString(), variables.getVariable(ProjectsUtil.VARIABLE_PROJECT_HOME));
    assertEquals("original", variables.getVariable(Defaults.VARIABLE_HOP_PROJECT_NAME));
    assertEquals(originalHome + "/metadata", variables.getVariable(Const.HOP_METADATA_FOLDER));
    assertTrue(Files.isRegularFile(targetHome.resolve("connections.csv")));
    assertTrue(
        Files.isRegularFile(
            targetHome.resolve("metadata").resolve("rdbms").resolve("imported-db.json")));
  }
}
