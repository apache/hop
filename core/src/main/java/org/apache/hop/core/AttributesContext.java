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

package org.apache.hop.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

/**
 * Generic carrier for cross-plugin extension points.
 *
 * <p>Implements {@link IAttributes} so optional plugins can exchange namespaced group/key/value
 * data without depending on each other. Typical groups: {@code marketplace}, resource checks
 * ({@code resources}), etc.
 *
 * <p>Optional identity fields ({@link #projectName}, {@link #environmentName}, {@link #purpose},
 * {@link #projectHome}, {@link #configurationFiles}) describe the surrounding project/lifecycle
 * context when that is what is being activated; they are not required for every use of this class.
 */
@Getter
@Setter
public class AttributesContext implements IAttributes {

  private String projectName;
  private String environmentName;
  private String purpose;
  private String projectHome;
  private List<String> configurationFiles = new ArrayList<>();

  private Map<String, Map<String, String>> attributesMap = new HashMap<>();

  public AttributesContext() {
    // empty
  }

  /** Deep-copy attributes from any {@link IAttributes} source. */
  public AttributesContext(IAttributes source) {
    copyAttributesFrom(source);
  }

  /**
   * Replace this context's attributes map with a deep copy of {@code source}. Does not clear
   * identity fields.
   */
  public void copyAttributesFrom(IAttributes source) {
    attributesMap = deepCopy(source != null ? source.getAttributesMap() : null);
  }

  /** Copy this context's attributes into {@code target} (deep copy). */
  public void copyAttributesTo(IAttributes target) {
    if (target == null) {
      return;
    }
    target.setAttributesMap(deepCopy(attributesMap));
  }

  private static Map<String, Map<String, String>> deepCopy(
      Map<String, Map<String, String>> source) {
    Map<String, Map<String, String>> copy = new HashMap<>();
    if (source == null) {
      return copy;
    }
    for (Map.Entry<String, Map<String, String>> entry : source.entrySet()) {
      if (entry.getKey() == null) {
        continue;
      }
      Map<String, String> group =
          entry.getValue() == null ? new HashMap<>() : new HashMap<>(entry.getValue());
      copy.put(entry.getKey(), group);
    }
    return copy;
  }

  @Override
  public void setAttributesMap(Map<String, Map<String, String>> attributesMap) {
    this.attributesMap = attributesMap != null ? attributesMap : new HashMap<>();
  }

  @Override
  public Map<String, Map<String, String>> getAttributesMap() {
    return attributesMap;
  }

  @Override
  public void setAttributes(String groupName, Map<String, String> attributes) {
    if (groupName == null) {
      return;
    }
    attributesMap.put(groupName, attributes != null ? attributes : new HashMap<>());
  }

  @Override
  public void setAttribute(String groupName, String key, String value) {
    if (groupName == null || key == null) {
      return;
    }
    Map<String, String> attributes = attributesMap.get(groupName);
    if (attributes == null) {
      attributes = new HashMap<>();
      attributesMap.put(groupName, attributes);
    }
    attributes.put(key, value);
  }

  @Override
  public Map<String, String> getAttributes(String groupName) {
    return attributesMap.get(groupName);
  }

  @Override
  public String getAttribute(String groupName, String key) {
    Map<String, String> attributes = attributesMap.get(groupName);
    if (attributes == null) {
      return null;
    }
    return attributes.get(key);
  }

  public List<String> getConfigurationFiles() {
    return configurationFiles;
  }

  public void setConfigurationFiles(List<String> configurationFiles) {
    this.configurationFiles =
        configurationFiles != null ? new ArrayList<>(configurationFiles) : new ArrayList<>();
  }
}
