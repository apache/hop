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

package org.apache.hop.resource;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ResourceUtil#assignNamedResourceDirectoryVariables}, the resolution of the
 * generated named-resource folder variables (DATA_PATH_n) that are created when a pipeline is
 * exported for remote execution. See issue #7209.
 */
class ResourceUtilTest {

  /**
   * Without a source/target folder mapping, every generated variable must default to the same
   * (resolved) folder as on the local machine. Leaving it unset caused unresolved
   * ${DATA_PATH_n}/file paths on the remote server (#7209).
   */
  @Test
  void defaultsToLocalFolderWhenNoMapping() throws Exception {
    IVariables variables = new Variables();
    Map<String, String> directoryMap = new LinkedHashMap<>();
    directoryMap.put("file:///data/in", "DATA_PATH_1");
    Map<String, String> variablesMap = new java.util.HashMap<>();

    ResourceUtil.assignNamedResourceDirectoryVariables(
        variables, directoryMap, null, null, variablesMap);

    assertEquals("file:///data/in", variablesMap.get("DATA_PATH_1"));
  }

  /** In the default case the referenced folder is resolved, including ${PROJECT_HOME}. */
  @Test
  void defaultResolvesProjectHomeVariable() throws Exception {
    IVariables variables = new Variables();
    variables.setVariable("PROJECT_HOME", "file:///home/user/project");

    Map<String, String> directoryMap = new LinkedHashMap<>();
    directoryMap.put("${PROJECT_HOME}/files", "DATA_PATH_1");
    Map<String, String> variablesMap = new java.util.HashMap<>();

    ResourceUtil.assignNamedResourceDirectoryVariables(
        variables, directoryMap, null, null, variablesMap);

    assertEquals("file:///home/user/project/files", variablesMap.get("DATA_PATH_1"));
  }

  /** Every generated folder variable gets a value, not just the first one. */
  @Test
  void defaultAssignsAllGeneratedVariables() throws Exception {
    IVariables variables = new Variables();
    variables.setVariable("PROJECT_HOME", "file:///home/user/project");

    Map<String, String> directoryMap = new LinkedHashMap<>();
    directoryMap.put("${PROJECT_HOME}/in", "DATA_PATH_1");
    directoryMap.put("file:///absolute/out", "DATA_PATH_2");
    Map<String, String> variablesMap = new java.util.HashMap<>();

    ResourceUtil.assignNamedResourceDirectoryVariables(
        variables, directoryMap, null, null, variablesMap);

    assertEquals("file:///home/user/project/in", variablesMap.get("DATA_PATH_1"));
    assertEquals("file:///absolute/out", variablesMap.get("DATA_PATH_2"));
  }

  /**
   * With a source (${PROJECT_HOME}) and target (/server) folder configured, the referenced folder
   * is mapped relative to the source folder onto the target folder on the server.
   */
  @Test
  void mapsProjectHomeSourceFolderOntoTargetFolder() throws Exception {
    IVariables variables = new Variables();
    variables.setVariable("PROJECT_HOME", "file:///home/user/project");

    Map<String, String> directoryMap = new LinkedHashMap<>();
    directoryMap.put("${PROJECT_HOME}/files", "DATA_PATH_1");
    Map<String, String> variablesMap = new java.util.HashMap<>();

    ResourceUtil.assignNamedResourceDirectoryVariables(
        variables, directoryMap, "${PROJECT_HOME}", "/server/", variablesMap);

    assertEquals("/server/files", variablesMap.get("DATA_PATH_1"));
  }
}
