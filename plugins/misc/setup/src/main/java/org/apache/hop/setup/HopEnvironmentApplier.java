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

import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.setup.persist.ConfigFolderSeeder;
import org.apache.hop.setup.persist.EnvScriptWriter;
import org.apache.hop.setup.persist.HopVfsFiles;
import org.apache.hop.setup.persist.IProcessRunner;
import org.apache.hop.setup.persist.ProjectRegistrationWriter;
import org.apache.hop.setup.persist.ShellRcWriter;
import org.apache.hop.setup.persist.WindowsUserEnvironmentWriter;

/** Applies a {@link HopEnvironmentSpec} to folders and the selected persistence targets. */
public class HopEnvironmentApplier {

  private final OsFamily os;
  private final UserPaths paths;
  private final ConfigFolderSeeder seeder;
  private final ProjectRegistrationWriter projects;
  private final WindowsUserEnvironmentWriter windowsWriter;
  private final ILogChannel log;

  public HopEnvironmentApplier() {
    this(
        OsFamily.detect(),
        UserPaths.system(),
        new ConfigFolderSeeder(),
        new ProjectRegistrationWriter(),
        new WindowsUserEnvironmentWriter(),
        new LogChannel("Hop setup"));
  }

  public HopEnvironmentApplier(
      OsFamily os,
      UserPaths paths,
      ConfigFolderSeeder seeder,
      IProcessRunner processRunner,
      ILogChannel log) {
    this(
        os,
        paths,
        seeder,
        new ProjectRegistrationWriter(),
        new WindowsUserEnvironmentWriter(processRunner),
        log);
  }

  public HopEnvironmentApplier(
      OsFamily os,
      UserPaths paths,
      ConfigFolderSeeder seeder,
      WindowsUserEnvironmentWriter windowsWriter,
      ILogChannel log) {
    this(os, paths, seeder, new ProjectRegistrationWriter(), windowsWriter, log);
  }

  public HopEnvironmentApplier(
      OsFamily os,
      UserPaths paths,
      ConfigFolderSeeder seeder,
      ProjectRegistrationWriter projects,
      WindowsUserEnvironmentWriter windowsWriter,
      ILogChannel log) {
    this.os = os;
    this.paths = paths;
    this.seeder = seeder;
    this.projects = projects == null ? new ProjectRegistrationWriter() : projects;
    this.windowsWriter = windowsWriter;
    this.log = log == null ? new LogChannel("Hop setup") : log;
  }

  public HopEnvironmentApplyResult apply(HopEnvironmentSpec spec) throws HopSetupException {
    resolveTargets(spec);
    Map<String, String> variables = spec.variables();
    if (variables.isEmpty()) {
      throw new HopSetupException(
          "No environment variables to write. Pass --defaults or set at least one variable.");
    }
    validate(spec, variables);

    HopEnvironmentApplyResult result = new HopEnvironmentApplyResult();
    result.setDryRun(spec.isDryRun());
    result.getVariablesWritten().addAll(variables.keySet());
    logBasic("Applying Hop environment variables: " + String.join(", ", variables.keySet()));

    seeder.seed(spec, result);
    projects.apply(spec, os, paths, result);

    if (spec.isWriteScript()) {
      writeScript(spec, variables, result);
    }
    if (spec.isWriteShellRc()) {
      writeShellRc(spec, variables, result);
    }
    if (spec.isWriteUserEnv()) {
      writeUserEnv(variables, result, spec.isDryRun());
    }

    result.addMessage("Restart Hop so the new environment variables take effect.");
    return result;
  }

  void resolveTargets(HopEnvironmentSpec spec) {
    if (!spec.hasTarget()) {
      if (os.isWindows()) {
        spec.setWriteUserEnv(true);
        spec.setWriteScript(true);
      } else {
        spec.setWriteShellRc(true);
        spec.setWriteScript(true);
      }
    }
    if (spec.isWriteScript() && StringUtils.isBlank(spec.getScriptFile())) {
      spec.setScriptFile(HopEnvironmentDefaults.wellKnownEnvFile(os, paths));
    }
    if (spec.isWriteShellRc() && StringUtils.isBlank(spec.getShellRcFile())) {
      spec.setShellRcFile(HopEnvironmentDefaults.recommendedShellRcFile(paths));
    }
  }

  private void validate(HopEnvironmentSpec spec, Map<String, String> variables)
      throws HopSetupException {
    if (spec.isWriteUserEnv() && !os.isWindows()) {
      throw new HopSetupException("Windows user environment can only be written on Windows");
    }
    if (spec.isWriteShellRc() && os.isWindows()) {
      throw new HopSetupException("A shell rc file cannot be written on Windows");
    }
    if (spec.isWriteShellRc() && !HopEnvironmentDefaults.supportsShellRc(paths)) {
      throw new HopSetupException(
          "The current shell is not bash or zsh. Write a hop-env.sh script instead.");
    }
    // Force quoting checks for every target that will receive the values.
    if (spec.isWriteShellRc() || (spec.isWriteScript() && !os.isWindows())) {
      ShellRcWriter.renderBlock(variables);
    }
    if (spec.isWriteScript()) {
      EnvScriptWriter.render(os, variables);
    }
    if (spec.isWriteUserEnv()) {
      windowsWriter.renderCommand(variables);
    }
  }

  private void writeScript(
      HopEnvironmentSpec spec, Map<String, String> variables, HopEnvironmentApplyResult result)
      throws HopSetupException {
    String content = EnvScriptWriter.render(os, variables);
    String path = spec.getScriptFile();
    result.getPlannedFiles().put(path, content);
    if (spec.isDryRun()) {
      result.addMessage("Would write script " + path);
      return;
    }
    HopVfsFiles.writeUtf8(path, content);
    result.addMessage("Wrote script " + path);
    logBasic("Wrote Hop environment script " + path);
  }

  private void writeShellRc(
      HopEnvironmentSpec spec, Map<String, String> variables, HopEnvironmentApplyResult result)
      throws HopSetupException {
    String path = spec.getShellRcFile();
    String existing = HopVfsFiles.exists(path) ? HopVfsFiles.readUtf8(path) : "";
    String next = ShellRcWriter.upsert(existing, variables);
    result.getPlannedFiles().put(path, next);
    if (spec.isDryRun()) {
      result.addMessage("Would update shell rc file " + path);
      return;
    }
    if (HopVfsFiles.exists(path)) {
      HopVfsFiles.writeUtf8(path + ".hop-setup.bak", existing);
    }
    HopVfsFiles.writeUtf8(path, next);
    result.addMessage("Updated shell rc file " + path);
    logBasic("Updated shell rc file " + path);
  }

  private void writeUserEnv(
      Map<String, String> variables, HopEnvironmentApplyResult result, boolean dryRun)
      throws HopSetupException {
    String command = windowsWriter.renderCommand(variables);
    result.getPlannedFiles().put("windows-user-env", command);
    if (dryRun) {
      result.addMessage("Would write Windows user environment variables");
      return;
    }
    windowsWriter.apply(variables);
    result.addMessage("Wrote Windows user environment variables");
    logBasic("Wrote Windows user environment variables");
  }

  private void logBasic(String message) {
    if (log != null && HopLogStore.isInitialized()) {
      log.logBasic(message);
    }
  }
}
