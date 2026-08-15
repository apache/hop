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

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/** Current process environment / system properties for the five setup variables. */
@Getter
public class HopEnvironmentSnapshot {

  private final String configFolder;
  private final String auditFolder;
  private final String javaHome;
  private final String options;
  private final String jdbcFolders;
  private final boolean configFolderFromEnv;
  private final boolean auditFolderFromEnv;
  private final String wellKnownEnvFile;

  public HopEnvironmentSnapshot(
      String configFolder,
      String auditFolder,
      String javaHome,
      String options,
      String jdbcFolders,
      boolean configFolderFromEnv,
      boolean auditFolderFromEnv,
      String wellKnownEnvFile) {
    this.configFolder = configFolder;
    this.auditFolder = auditFolder;
    this.javaHome = javaHome;
    this.options = options;
    this.jdbcFolders = jdbcFolders;
    this.configFolderFromEnv = configFolderFromEnv;
    this.auditFolderFromEnv = auditFolderFromEnv;
    this.wellKnownEnvFile = wellKnownEnvFile;
  }

  public static HopEnvironmentSnapshot capture() {
    return capture(OsFamily.detect(), UserPaths.system());
  }

  public static HopEnvironmentSnapshot capture(OsFamily os, UserPaths paths) {
    String configEnv = firstNonBlank(System.getenv(HopSetupVariables.CONFIG_FOLDER), null);
    String auditEnv = firstNonBlank(System.getenv(HopSetupVariables.AUDIT_FOLDER), null);
    return new HopEnvironmentSnapshot(
        existingFolder(
            configEnv,
            System.getProperty(HopSetupVariables.CONFIG_FOLDER),
            HopEnvironmentDefaults.INSTALL_CONFIG_FOLDER),
        existingFolder(
            auditEnv,
            System.getProperty(HopSetupVariables.AUDIT_FOLDER),
            HopEnvironmentDefaults.INSTALL_AUDIT_FOLDER),
        firstNonBlank(
            System.getenv(HopSetupVariables.JAVA_HOME),
            System.getenv("JAVA_HOME"),
            System.getProperty("java.home")),
        firstNonBlank(System.getenv(HopSetupVariables.OPTIONS), HopSetupVariables.DEFAULT_OPTIONS),
        firstNonBlank(System.getenv(HopSetupVariables.JDBC_FOLDERS), ""),
        StringUtils.isNotBlank(configEnv),
        StringUtils.isNotBlank(auditEnv),
        HopEnvironmentDefaults.wellKnownEnvFile(os, paths));
  }

  public static boolean configFolderSetInEnvironment() {
    return StringUtils.isNotBlank(System.getenv(HopSetupVariables.CONFIG_FOLDER));
  }

  /**
   * Value to show as "existing": the process environment, else an explicit JVM property, else the
   * launcher relative default ({@code ./config} / {@code ./audit}). Do not use {@link
   * org.apache.hop.core.Const#HOP_CONFIG_FOLDER}, which expands {@code user.dir} to an absolute
   * path.
   */
  static String existingFolder(String env, String property, String installFallback) {
    if (StringUtils.isNotBlank(env)) {
      return env;
    }
    if (StringUtils.isNotBlank(property)) {
      return property;
    }
    return installFallback;
  }

  private static String firstNonBlank(String... values) {
    if (values == null) {
      return "";
    }
    for (String value : values) {
      if (StringUtils.isNotBlank(value)) {
        return value;
      }
    }
    return "";
  }
}
