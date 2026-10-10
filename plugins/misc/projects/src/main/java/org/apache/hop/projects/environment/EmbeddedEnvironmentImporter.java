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

package org.apache.hop.projects.environment;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.config.DescribedVariablesConfigFile;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;

/**
 * Builds embedded environment definitions from lifecycle environments already stored in the Hop
 * configuration. The value of each variable is copied in as its default. A later configuration file
 * replaces an earlier value for the same name.
 */
public final class EmbeddedEnvironmentImporter {

  private EmbeddedEnvironmentImporter() {}

  /**
   * Read the configuration files of each environment. A missing or unreadable file is recorded and
   * does not stop the others.
   *
   * @param environments lifecycle environments selected by the user
   * @param variables variable space used to resolve configuration file paths
   * @return one source per environment that has a name, in the given order
   */
  public static List<EnvironmentSource> read(
      List<LifecycleEnvironment> environments, IVariables variables) {
    List<EnvironmentSource> sources = new ArrayList<>();
    if (environments == null) {
      return sources;
    }
    for (LifecycleEnvironment environment : environments) {
      if (environment == null || StringUtils.isBlank(environment.getName())) {
        continue;
      }
      Map<String, VariableAssignment> byName = new LinkedHashMap<>();
      List<String> unreadable = new ArrayList<>();
      List<String> files = environment.getConfigurationFiles();
      if (files != null) {
        for (String filename : files) {
          readFile(filename, variables, byName, unreadable);
        }
      }
      sources.add(
          new EnvironmentSource(
              environment.getName().trim(),
              StringUtils.trimToNull(environment.getPurpose()),
              new ArrayList<>(byName.values()),
              unreadable));
    }
    return sources;
  }

  /**
   * @param name environment name
   * @param description environment description
   * @param assignments variables, including whether each one is a secret, and the default copied
   *     from the configuration file
   * @return a normalized embedded environment
   */
  public static EmbeddedEnvironment toEmbeddedEnvironment(
      String name, String description, List<VariableAssignment> assignments) {
    EmbeddedEnvironment environment = new EmbeddedEnvironment();
    environment.setName(name);
    environment.setDescription(description);
    if (assignments != null) {
      for (VariableAssignment assignment : assignments) {
        if (assignment == null || StringUtils.isBlank(assignment.getName())) {
          continue;
        }
        environment
            .getVariables()
            .add(
                new EmbeddedEnvironmentVariable(
                    assignment.getName(),
                    assignment.getDefaultValue(),
                    assignment.getDescription(),
                    assignment.isMandatory(),
                    assignment.isSecret()));
      }
    }
    EmbeddedEnvironmentValidator.normalize(environment);
    return environment;
  }

  private static void readFile(
      String filename,
      IVariables variables,
      Map<String, VariableAssignment> byName,
      List<String> unreadable) {
    if (StringUtils.isBlank(filename)) {
      return;
    }
    String resolved = variables == null ? filename : variables.resolve(filename);
    try (FileObject file = HopVfs.getFileObject(resolved)) {
      if (!file.exists()) {
        unreadable.add(filename);
        return;
      }
    } catch (Exception e) {
      unreadable.add(filename);
      return;
    }
    try {
      DescribedVariablesConfigFile configFile = new DescribedVariablesConfigFile(resolved);
      configFile.readFromFile();
      for (DescribedVariable variable : configFile.getDescribedVariables()) {
        addVariable(byName, variable);
      }
    } catch (Exception e) {
      unreadable.add(filename);
    }
  }

  private static void addVariable(
      Map<String, VariableAssignment> byName, DescribedVariable variable) {
    if (variable == null || StringUtils.isBlank(variable.getName())) {
      return;
    }
    String name = variable.getName().trim();
    boolean secret = EnvironmentConfigFileSummary.looksLikeSecret(name, variable.getValue());
    String value = StringUtils.trimToNull(variable.getValue());
    VariableAssignment existing = byName.get(name);
    if (existing == null) {
      byName.put(
          name,
          new VariableAssignment(
              name, StringUtils.trimToNull(variable.getDescription()), secret, false, value));
      return;
    }
    if (existing.description == null) {
      existing.description = StringUtils.trimToNull(variable.getDescription());
    }
    // Later files override, the same way configuration files are applied.
    if (value != null) {
      existing.defaultValue = value;
    }
    if (secret) {
      existing.secret = true;
    }
  }

  /** One lifecycle environment and the variables found in its configuration files. */
  @Getter
  public static final class EnvironmentSource {
    private final String name;
    private final String description;
    private final List<VariableAssignment> variables;
    private final List<String> unreadableFiles;

    public EnvironmentSource(
        String name,
        String description,
        List<VariableAssignment> variables,
        List<String> unreadableFiles) {
      this.name = name;
      this.description = description;
      this.variables = variables == null ? new ArrayList<>() : variables;
      this.unreadableFiles = unreadableFiles == null ? new ArrayList<>() : unreadableFiles;
    }
  }

  /**
   * One imported variable. {@code secret} and {@code mandatory} are suggestions until the user
   * confirms them. The description and default may be updated while files are merged. A later
   * non-blank value replaces the default. A blank value does not.
   */
  @Getter
  public static final class VariableAssignment {
    private final String name;
    private String description;
    private boolean secret;
    private boolean mandatory;
    private String defaultValue;

    public VariableAssignment(String name, String description, boolean secret) {
      this(name, description, secret, false, null);
    }

    public VariableAssignment(String name, String description, boolean secret, boolean mandatory) {
      this(name, description, secret, mandatory, null);
    }

    public VariableAssignment(
        String name, String description, boolean secret, boolean mandatory, String defaultValue) {
      this.name = name;
      this.description = description;
      this.secret = secret;
      this.mandatory = mandatory;
      this.defaultValue = StringUtils.trimToNull(defaultValue);
    }
  }
}
