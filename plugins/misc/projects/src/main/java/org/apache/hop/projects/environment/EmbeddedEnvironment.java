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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Lifecycle environment definition stored in {@code project-config.json}.
 *
 * <p>Holds the name, description, and variable defaults that are shared through version control.
 * Configuration files stay on the computer that runs Hop.
 */
@Getter
@Setter
@NoArgsConstructor
public class EmbeddedEnvironment {

  private String name;

  private String description;

  /**
   * Variables for this environment. A variable with {@code mandatory} or {@code secret} set is
   * written to the local configuration file. The others are applied as defaults only.
   */
  private List<EmbeddedEnvironmentVariable> variables = new ArrayList<>();

  /**
   * {@code mandatoryVariables} from project files written before mandatory was a flag on each
   * variable. Folded into {@code variables} by {@link #absorbLegacyVariables()}.
   */
  @JsonIgnore
  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  private List<EmbeddedEnvironmentVariable> legacyMandatoryVariables;

  /**
   * {@code secretVariables} from project files written before secret was a flag on each variable.
   * Folded into {@code variables} by {@link #absorbLegacyVariables()}.
   */
  @JsonIgnore
  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  private List<EmbeddedEnvironmentVariable> legacySecretVariables;

  /**
   * @return a deep copy
   */
  public EmbeddedEnvironment copy() {
    absorbLegacyVariables();
    EmbeddedEnvironment copy = new EmbeddedEnvironment();
    copy.name = name;
    copy.description = description;
    copy.variables = copyVariables(variables);
    return copy;
  }

  /**
   * Accept a project file that still stores mandatory variables in their own list. Each of those
   * variables is appended to {@code variables} with {@code mandatory} set.
   */
  @JsonProperty(value = "mandatoryVariables", access = JsonProperty.Access.WRITE_ONLY)
  public void setMandatoryVariables(List<EmbeddedEnvironmentVariable> legacyMandatoryVariables) {
    this.legacyMandatoryVariables = legacyMandatoryVariables;
  }

  /**
   * Accept a project file that still stores secrets in their own list. Each of those variables is
   * appended to {@code variables} with {@code secret} set.
   */
  @JsonProperty(value = "secretVariables", access = JsonProperty.Access.WRITE_ONLY)
  public void setSecretVariables(List<EmbeddedEnvironmentVariable> legacySecretVariables) {
    this.legacySecretVariables = legacySecretVariables;
  }

  /**
   * Move {@code mandatoryVariables} and {@code secretVariables} into {@code variables}. Safe to
   * call more than once. Mandatory entries are appended first.
   */
  public void absorbLegacyVariables() {
    absorb(legacyMandatoryVariables, false, true);
    legacyMandatoryVariables = null;
    absorb(legacySecretVariables, true, false);
    legacySecretVariables = null;
  }

  private void absorb(List<EmbeddedEnvironmentVariable> legacy, boolean secret, boolean mandatory) {
    if (legacy == null || legacy.isEmpty()) {
      return;
    }
    if (variables == null) {
      variables = new ArrayList<>();
    }
    for (EmbeddedEnvironmentVariable variable : legacy) {
      if (variable == null) {
        continue;
      }
      if (secret) {
        variable.setSecret(true);
      }
      if (mandatory) {
        variable.setMandatory(true);
      }
      variables.add(variable);
    }
  }

  private static List<EmbeddedEnvironmentVariable> copyVariables(
      List<EmbeddedEnvironmentVariable> source) {
    List<EmbeddedEnvironmentVariable> copy = new ArrayList<>();
    if (source == null) {
      return copy;
    }
    for (EmbeddedEnvironmentVariable variable : source) {
      if (variable == null) {
        continue;
      }
      copy.add(
          new EmbeddedEnvironmentVariable(
              variable.getName(),
              variable.getDefaultValue(),
              variable.getDescription(),
              variable.isMandatory(),
              variable.isSecret()));
    }
    return copy;
  }
}
