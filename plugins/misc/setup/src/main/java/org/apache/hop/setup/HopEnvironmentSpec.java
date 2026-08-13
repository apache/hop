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

package org.apache.hop.setup;

import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Desired environment values and persistence targets. A {@code null} variable field means "leave
 * this variable unchanged". An empty string means "clear it".
 */
@Getter
@Setter
@NoArgsConstructor
public class HopEnvironmentSpec {

  private String configFolder;
  private String auditFolder;
  private String javaHome;
  private String options;
  private String jdbcFolders;

  private boolean writeUserEnv;
  private boolean writeShellRc;
  private String shellRcFile;
  private boolean writeScript;
  private String scriptFile;

  private boolean createFolders = true;
  private boolean copyExisting;
  private boolean dryRun;

  public Map<String, String> variables() {
    Map<String, String> map = new LinkedHashMap<>();
    put(map, HopSetupVariables.CONFIG_FOLDER, configFolder);
    put(map, HopSetupVariables.AUDIT_FOLDER, auditFolder);
    put(map, HopSetupVariables.JAVA_HOME, javaHome);
    put(map, HopSetupVariables.OPTIONS, options);
    put(map, HopSetupVariables.JDBC_FOLDERS, jdbcFolders);
    return map;
  }

  public boolean hasTarget() {
    return writeUserEnv || writeShellRc || writeScript;
  }

  private static void put(Map<String, String> map, String key, String value) {
    if (value != null) {
      map.put(key, value);
    }
  }
}
