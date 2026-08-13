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

import org.apache.commons.lang3.StringUtils;

/** Platform-specific recommended folders and persistence targets. */
public final class HopEnvironmentDefaults {

  /** Launcher fallback when {@code HOP_CONFIG_FOLDER} is unset ({@code <install>/config}). */
  public static final String INSTALL_CONFIG_FOLDER = "./config";

  /** Launcher fallback when {@code HOP_AUDIT_FOLDER} is unset ({@code <install>/audit}). */
  public static final String INSTALL_AUDIT_FOLDER = "./audit";

  private HopEnvironmentDefaults() {}

  public static String recommendedConfigFolder() {
    return recommendedConfigFolder(OsFamily.detect(), UserPaths.system());
  }

  public static String recommendedAuditFolder() {
    return recommendedAuditFolder(OsFamily.detect(), UserPaths.system());
  }

  public static String recommendedConfigFolder(OsFamily os, UserPaths paths) {
    if (os.isWindows()) {
      return paths.getHome().resolve(".hop").resolve("config").toString();
    }
    return paths.getXdgData().resolve("hop").toString();
  }

  public static String recommendedAuditFolder(OsFamily os, UserPaths paths) {
    if (os.isWindows()) {
      return paths.getHome().resolve(".hop").resolve("audit").toString();
    }
    return paths.getXdgState().resolve("hop").toString();
  }

  public static String wellKnownEnvFile(OsFamily os, UserPaths paths) {
    if (os.isWindows()) {
      return paths.getHome().resolve(".hop").resolve("hop-env.cmd").toString();
    }
    return paths.getXdgConfig().resolve("hop").resolve("hop-env.sh").toString();
  }

  public static String recommendedShellRcFile(UserPaths paths) {
    String shell = paths.getShell() == null ? "" : paths.getShell().replace('\\', '/');
    String name = shell.substring(shell.lastIndexOf('/') + 1);
    if ("zsh".equals(name)) {
      return paths.getHome().resolve(".zshrc").toString();
    }
    return paths.getHome().resolve(".bashrc").toString();
  }

  public static boolean supportsShellRc(UserPaths paths) {
    String shell = paths.getShell() == null ? "" : paths.getShell().replace('\\', '/');
    String name = shell.substring(shell.lastIndexOf('/') + 1);
    return StringUtils.isBlank(name)
        || "bash".equals(name)
        || "zsh".equals(name)
        || "sh".equals(name);
  }

  public static String recommendedOptions(String current) {
    return StringUtils.isNotBlank(current) ? current : HopSetupVariables.DEFAULT_OPTIONS;
  }

  public static String recommendedJavaHome(String hopJavaHome, String javaHome) {
    if (StringUtils.isNotBlank(hopJavaHome)) {
      return hopJavaHome;
    }
    return StringUtils.defaultString(javaHome);
  }
}
