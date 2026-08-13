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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.setup.HopSetupException;

/**
 * Writes user-level Windows environment variables through PowerShell (no {@code setx}, no PATH
 * truncation).
 */
public class WindowsUserEnvironmentWriter {

  private final IProcessRunner processRunner;

  public WindowsUserEnvironmentWriter() {
    this(new SystemProcessRunner());
  }

  public WindowsUserEnvironmentWriter(IProcessRunner processRunner) {
    this.processRunner = processRunner;
  }

  public String renderCommand(Map<String, String> variables) throws HopSetupException {
    List<String> statements = new ArrayList<>();
    for (Map.Entry<String, String> entry : variables.entrySet()) {
      if (StringUtils.isEmpty(entry.getValue())) {
        statements.add(
            "[Environment]::SetEnvironmentVariable('" + entry.getKey() + "',$null,'User')");
      } else {
        String quoted = EnvValueEscaper.powershellSingleQuoted(entry.getKey(), entry.getValue());
        statements.add(
            "[Environment]::SetEnvironmentVariable('"
                + entry.getKey()
                + "',"
                + quoted
                + ",'User')");
      }
    }
    return String.join(";", statements);
  }

  public List<String> commandLine(Map<String, String> variables) throws HopSetupException {
    return List.of(
        "powershell.exe", "-NoProfile", "-NonInteractive", "-Command", renderCommand(variables));
  }

  public void apply(Map<String, String> variables) throws HopSetupException {
    if (variables.isEmpty()) {
      return;
    }
    List<String> command = commandLine(variables);
    try {
      int exit = processRunner.run(command);
      if (exit != 0) {
        throw new HopSetupException(
            "PowerShell exited with code " + exit + " while writing user environment variables");
      }
    } catch (HopSetupException e) {
      throw e;
    } catch (Exception e) {
      throw new HopSetupException("Unable to write Windows user environment variables", e);
    }
  }
}
