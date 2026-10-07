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
import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Lifecycle environment definition stored in {@code project-config.json}.
 *
 * <p>Holds the name, description, and placeholder variable defaults that are shared through version
 * control. Configuration files and real values stay on the computer that runs Hop.
 */
@Getter
@Setter
@NoArgsConstructor
public class EmbeddedEnvironment {

  private String name;

  private String description;

  /** Optional variables. Applied as defaults. Not written to a local configuration file. */
  private List<EmbeddedEnvironmentVariable> variables = new ArrayList<>();

  /** Values a person must fill in. Written to the local configuration file when one is created. */
  private List<EmbeddedEnvironmentVariable> mandatoryVariables = new ArrayList<>();

  /** Secrets. Written to the local configuration file when one is created. Placeholders only. */
  private List<EmbeddedEnvironmentVariable> secretVariables = new ArrayList<>();

  /**
   * @return a deep copy
   */
  public EmbeddedEnvironment copy() {
    EmbeddedEnvironment copy = new EmbeddedEnvironment();
    copy.name = name;
    copy.description = description;
    copy.variables = copyVariables(variables);
    copy.mandatoryVariables = copyVariables(mandatoryVariables);
    copy.secretVariables = copyVariables(secretVariables);
    return copy;
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
              variable.getName(), variable.getDefaultValue(), variable.getDescription()));
    }
    return copy;
  }
}
