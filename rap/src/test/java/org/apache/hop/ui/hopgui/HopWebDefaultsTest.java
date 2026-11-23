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

package org.apache.hop.ui.hopgui;

import static org.apache.hop.ui.hopgui.HopWebDefaults.HOP_CONFIG_FOLDER;
import static org.apache.hop.ui.hopgui.HopWebDefaults.HOP_PLUGIN_BASE_FOLDERS;
import static org.apache.hop.ui.hopgui.HopWebDefaults.HOP_SHARED_JDBC_FOLDERS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HopWebDefaultsTest {

  private static final List<String> NAMES =
      List.of(HOP_CONFIG_FOLDER, HOP_PLUGIN_BASE_FOLDERS, HOP_SHARED_JDBC_FOLDERS);

  @TempDir Path catalinaBase;

  private final Map<String, String> saved = new HashMap<>();
  private final Map<String, String> environment = new HashMap<>();
  private Path webApp;

  @BeforeEach
  void setUp() throws Exception {
    for (String name : NAMES) {
      saved.put(name, System.getProperty(name));
      System.clearProperty(name);
    }
    webApp = Files.createDirectories(catalinaBase.resolve("webapps/ROOT"));
  }

  @AfterEach
  void tearDown() {
    for (String name : NAMES) {
      String value = saved.get(name);
      if (value == null) {
        System.clearProperty(name);
      } else {
        System.setProperty(name, value);
      }
    }
  }

  private List<String> apply() {
    return HopWebDefaults.apply(webApp.toString(), catalinaBase.toString(), environment::get);
  }

  @Test
  void usesTheFoldersOfTheTomcatLayout() throws Exception {
    Path config = Files.createDirectories(webApp.resolve("config"));
    Path plugins = Files.createDirectories(catalinaBase.resolve("plugins"));
    Path jdbc = Files.createDirectories(catalinaBase.resolve("jdbc-drivers"));

    List<String> applied = apply();

    assertEquals(config.toString(), System.getProperty(HOP_CONFIG_FOLDER));
    assertEquals(plugins.toString(), System.getProperty(HOP_PLUGIN_BASE_FOLDERS));
    assertEquals(jdbc.toString(), System.getProperty(HOP_SHARED_JDBC_FOLDERS));
    assertEquals(3, applied.size());
  }

  @Test
  void leavesMissingFoldersUnset() {
    assertTrue(apply().isEmpty());
    for (String name : NAMES) {
      assertNull(System.getProperty(name), name);
    }
  }

  @Test
  void systemPropertyWins() throws Exception {
    Files.createDirectories(catalinaBase.resolve("plugins"));
    System.setProperty(HOP_PLUGIN_BASE_FOLDERS, "/opt/my-plugins");
    environment.put(HOP_PLUGIN_BASE_FOLDERS, "/from/environment");

    apply();

    assertEquals("/opt/my-plugins", System.getProperty(HOP_PLUGIN_BASE_FOLDERS));
  }

  @Test
  void environmentVariableBeatsTheDefault() throws Exception {
    Files.createDirectories(catalinaBase.resolve("jdbc-drivers"));
    environment.put(HOP_SHARED_JDBC_FOLDERS, "/opt/drivers");

    apply();

    assertEquals("/opt/drivers", System.getProperty(HOP_SHARED_JDBC_FOLDERS));
  }

  @Test
  void blankSystemPropertyCountsAsUnset() throws Exception {
    // The image passes -DHOP_PLUGIN_BASE_FOLDERS="${HOP_PLUGIN_BASE_FOLDERS}", which is empty
    // when the variable is.
    Path plugins = Files.createDirectories(catalinaBase.resolve("plugins"));
    System.setProperty(HOP_PLUGIN_BASE_FOLDERS, "");

    apply();

    assertEquals(plugins.toString(), System.getProperty(HOP_PLUGIN_BASE_FOLDERS));
  }

  @Test
  void worksWithoutCatalinaBase() throws Exception {
    Path config = Files.createDirectories(webApp.resolve("config"));

    HopWebDefaults.apply(webApp.toString(), null, environment::get);

    assertEquals(config.toString(), System.getProperty(HOP_CONFIG_FOLDER));
    assertNull(System.getProperty(HOP_PLUGIN_BASE_FOLDERS));
  }
}
