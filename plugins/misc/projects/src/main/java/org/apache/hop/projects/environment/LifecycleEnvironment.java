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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.IAttributes;

/**
 * A project lifecycle environment describes the state of a project and its configuration.
 *
 * <p>Implements {@link IAttributes} so optional plugins (marketplace, resource checks, …) can store
 * namespaced group/key/value settings without hard-wiring fields into this class. Groups are
 * persisted under {@code attributesMap} in hop-config projects configuration.
 */
@Getter
@Setter
public class LifecycleEnvironment implements IAttributes {

  private String name;

  private String purpose;

  private String projectName;

  private String canvasText;

  /**
   * FORMAT locale for number, currency and date conversion when this environment is enabled
   * (language_COUNTRY, for example {@code nl_BE}). Empty inherits the installation regional
   * settings.
   */
  private String formatLocale;

  /**
   * Default timezone for date and timestamp conversion when this environment is enabled (IANA id,
   * for example {@code Europe/Brussels}). Empty inherits the JVM default.
   */
  private String timeZone;

  private List<String> configurationFiles;

  /** Group → (key → value); see {@link IAttributes}. */
  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  private Map<String, Map<String, String>> attributesMap;

  public LifecycleEnvironment() {
    configurationFiles = new ArrayList<>();
    attributesMap = new HashMap<>();
  }

  public LifecycleEnvironment(
      String name, String purpose, String projectName, List<String> configurationFiles) {
    this.name = name;
    this.purpose = purpose;
    this.projectName = projectName;
    this.configurationFiles = configurationFiles != null ? configurationFiles : new ArrayList<>();
    this.attributesMap = new HashMap<>();
  }

  public LifecycleEnvironment(LifecycleEnvironment env) {
    this.name = env.name;
    this.purpose = env.purpose;
    this.projectName = env.projectName;
    this.canvasText = env.canvasText;
    this.formatLocale = env.formatLocale;
    this.timeZone = env.timeZone;
    this.configurationFiles =
        env.configurationFiles != null
            ? new ArrayList<>(env.configurationFiles)
            : new ArrayList<>();
    this.attributesMap = deepCopyAttributes(env.attributesMap);
  }

  private static Map<String, Map<String, String>> deepCopyAttributes(
      Map<String, Map<String, String>> source) {
    Map<String, Map<String, String>> copy = new HashMap<>();
    if (source == null) {
      return copy;
    }
    for (Map.Entry<String, Map<String, String>> entry : source.entrySet()) {
      if (entry.getKey() == null) {
        continue;
      }
      copy.put(
          entry.getKey(),
          entry.getValue() == null ? new HashMap<>() : new HashMap<>(entry.getValue()));
    }
    return copy;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LifecycleEnvironment that = (LifecycleEnvironment) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  @Override
  public void setAttributesMap(Map<String, Map<String, String>> attributesMap) {
    this.attributesMap = attributesMap != null ? attributesMap : new HashMap<>();
  }

  @Override
  public Map<String, Map<String, String>> getAttributesMap() {
    if (attributesMap == null) {
      attributesMap = new HashMap<>();
    }
    return attributesMap;
  }

  @Override
  public void setAttributes(String groupName, Map<String, String> attributes) {
    if (groupName == null) {
      return;
    }
    getAttributesMap().put(groupName, attributes != null ? attributes : new HashMap<>());
  }

  @Override
  public void setAttribute(String groupName, String key, String value) {
    if (groupName == null || key == null) {
      return;
    }
    Map<String, String> attributes = getAttributes(groupName);
    if (attributes == null) {
      attributes = new HashMap<>();
      getAttributesMap().put(groupName, attributes);
    }
    attributes.put(key, value);
  }

  @Override
  public Map<String, String> getAttributes(String groupName) {
    return getAttributesMap().get(groupName);
  }

  @Override
  public String getAttribute(String groupName, String key) {
    Map<String, String> attributes = getAttributes(groupName);
    if (attributes == null) {
      return null;
    }
    return attributes.get(key);
  }
}
