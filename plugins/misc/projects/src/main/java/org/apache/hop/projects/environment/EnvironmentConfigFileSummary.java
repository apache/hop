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

package org.apache.hop.projects.environment;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.DescribedVariablesConfigFile;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;

/**
 * Reads an environment configuration file for the environment dialog: its description, the
 * variables to show on hover, and a description-only save that leaves the rest of the file alone.
 */
public final class EnvironmentConfigFileSummary {
  private static final Class<?> PKG = EnvironmentConfigFileSummary.class;

  public static final int MAX_VARIABLES_IN_TOOLTIP = 40;
  public static final String MASKED_VALUE = "********";

  private static final Pattern SECRET_NAME =
      Pattern.compile("(?i).*(password|passwd|secret|token|credential).*");

  private enum State {
    OK,
    MISSING,
    UNREADABLE
  }

  private final String description;
  private final List<DescribedVariable> variables;
  private final State state;

  private EnvironmentConfigFileSummary(
      String description, List<DescribedVariable> variables, State state) {
    this.description = description;
    this.variables = variables;
    this.state = state;
  }

  /**
   * Read the configuration file at a resolved path.
   *
   * @param resolvedFilename absolute or local path with variables already resolved. Empty means the
   *     row has no file yet.
   * @return a summary. A missing or unreadable file does not throw.
   */
  public static EnvironmentConfigFileSummary read(String resolvedFilename) {
    if (StringUtils.isEmpty(resolvedFilename)) {
      return new EnvironmentConfigFileSummary(null, List.of(), State.MISSING);
    }
    try {
      if (!HopVfs.fileExists(resolvedFilename)) {
        return new EnvironmentConfigFileSummary(null, List.of(), State.MISSING);
      }
    } catch (Exception e) {
      return new EnvironmentConfigFileSummary(null, List.of(), State.UNREADABLE);
    }
    try {
      DescribedVariablesConfigFile file = new DescribedVariablesConfigFile(resolvedFilename);
      file.readFromFile();
      // Description first: getDescribedVariables() can replace the map with a nested "config".
      String description = file.getDescription();
      return new EnvironmentConfigFileSummary(description, copyVariables(file), State.OK);
    } catch (Exception e) {
      return new EnvironmentConfigFileSummary(null, List.of(), State.UNREADABLE);
    }
  }

  /**
   * @return the description stored in the file, or null when there is none or the file was not read
   */
  public String getDescription() {
    return description;
  }

  /**
   * Hover text for one configuration-file row.
   *
   * @param descriptionFromCell the description currently shown in the table, which may not be saved
   *     yet. The stored variable values are shown as written; they are not resolved.
   */
  public String tooltip(String descriptionFromCell) {
    List<String> lines = new ArrayList<>();
    String descriptionText = normalize(descriptionFromCell);
    if (!descriptionText.isEmpty()) {
      lines.add(descriptionText);
    }

    List<String> body = new ArrayList<>();
    if (state == State.MISSING) {
      body.add(message("LifecycleEnvironmentDialog.ConfigFile.ToolTip.Missing"));
    } else if (state == State.UNREADABLE) {
      body.add(message("LifecycleEnvironmentDialog.ConfigFile.ToolTip.Unreadable"));
    } else if (variables.isEmpty()) {
      body.add(message("LifecycleEnvironmentDialog.ConfigFile.ToolTip.NoVariables"));
    } else {
      int limit = Math.min(MAX_VARIABLES_IN_TOOLTIP, variables.size());
      for (int i = 0; i < limit; i++) {
        body.add(formatVariable(variables.get(i)));
      }
      if (variables.size() > MAX_VARIABLES_IN_TOOLTIP) {
        body.add(
            BaseMessages.getString(
                PKG,
                "LifecycleEnvironmentDialog.ConfigFile.ToolTip.More",
                Integer.toString(variables.size() - MAX_VARIABLES_IN_TOOLTIP)));
      }
    }

    if (!body.isEmpty()) {
      if (!lines.isEmpty()) {
        lines.add("");
      }
      lines.addAll(body);
    }
    return String.join("\n", lines);
  }

  /**
   * Write {@code cellDescription} into the file when it differs from what is already stored.
   *
   * <p>An unchanged description does not rewrite the file. A missing file is created only when the
   * description is not blank, with an empty variable list. A file that exists but cannot be read is
   * left alone; a non-blank new description is an error so the dialog can stay open.
   *
   * @return true when the file was written
   */
  public static boolean saveDescriptionIfChanged(String resolvedFilename, String cellDescription)
      throws HopException {
    if (StringUtils.isEmpty(resolvedFilename)) {
      return false;
    }
    String requested = normalize(cellDescription);
    boolean exists;
    try {
      exists = HopVfs.fileExists(resolvedFilename);
    } catch (Exception e) {
      if (requested.isEmpty()) {
        return false;
      }
      throw cannotRead(resolvedFilename, e);
    }

    DescribedVariablesConfigFile file = new DescribedVariablesConfigFile(resolvedFilename);
    if (exists) {
      try {
        file.readFromFile();
      } catch (Exception e) {
        if (requested.isEmpty()) {
          return false;
        }
        throw cannotRead(resolvedFilename, e);
      }
      // Compare without calling getDescribedVariables(), which rewrites the variable list.
      if (requested.equals(normalize(file.getDescription()))) {
        return false;
      }
      file.setDescription(requested);
      file.saveToFile();
      return true;
    }

    if (requested.isEmpty()) {
      return false;
    }
    file.setDescription(requested);
    file.setDescribedVariables(new ArrayList<>());
    file.saveToFile();
    return true;
  }

  private static HopException cannotRead(String resolvedFilename, Exception cause) {
    return new HopException(
        "Could not read configuration file '" + resolvedFilename + "' to update its description",
        cause);
  }

  private static List<DescribedVariable> copyVariables(DescribedVariablesConfigFile file) {
    List<DescribedVariable> copy = new ArrayList<>();
    for (DescribedVariable variable : file.getDescribedVariables()) {
      if (variable != null && StringUtils.isNotBlank(variable.getName())) {
        copy.add(new DescribedVariable(variable));
      }
    }
    return copy;
  }

  private static String formatVariable(DescribedVariable variable) {
    String value = maskValue(variable) ? MASKED_VALUE : Const.NVL(variable.getValue(), "");
    String line = variable.getName() + " = " + value;
    if (StringUtils.isNotBlank(variable.getDescription())) {
      line = line + " (" + variable.getDescription().trim() + ")";
    }
    return line;
  }

  private static boolean maskValue(DescribedVariable variable) {
    String name = variable.getName();
    if (name != null && SECRET_NAME.matcher(name).matches()) {
      return true;
    }
    String value = variable.getValue();
    return value != null && value.startsWith(Encr.PASSWORD_ENCRYPTED_PREFIX);
  }

  private static String message(String key) {
    return BaseMessages.getString(PKG, key);
  }

  private static String normalize(String value) {
    return value == null ? "" : value.trim();
  }
}
