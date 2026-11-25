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

package org.apache.hop.lineage.hub;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * A lineage sink defined in metadata, as one run resolved it: the sink plugin and the settings it
 * is started with. Two runs that resolve a sink to the same settings share one sink; a run of
 * another project, or one after the sink was edited, can resolve the same name to other settings
 * and gets a sink of its own.
 *
 * @param name the name of the {@link org.apache.hop.lineage.metadata.LineageSink}
 * @param pluginId the id of the sink plugin
 * @param settings the variables the sink is initialized with
 */
public record LineageSinkTarget(String name, String pluginId, Map<String, String> settings) {

  public LineageSinkTarget {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(pluginId, "pluginId");
    settings = Map.copyOf(settings == null ? Map.of() : settings);
  }

  /**
   * The key events carry to reach this sink: the name, and a digest of the plugin and settings so
   * that the key changes when they do. The settings may hold a token, so they are not in the key.
   */
  public String key() {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(pluginId.getBytes(StandardCharsets.UTF_8));
      for (Map.Entry<String, String> setting : new TreeMap<>(settings).entrySet()) {
        digest.update((byte) 0);
        digest.update(setting.getKey().getBytes(StandardCharsets.UTF_8));
        digest.update((byte) 1);
        digest.update(String.valueOf(setting.getValue()).getBytes(StandardCharsets.UTF_8));
      }
      return name + "@" + HexFormat.of().formatHex(digest.digest(), 0, 8);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is not available", e);
    }
  }

  /** Without the settings: they may hold a token. */
  /** The name of the lineage sink a {@link #key()} is for. */
  public static String nameOf(String key) {
    if (key == null) {
      return null;
    }
    int at = key.lastIndexOf('@');
    return at < 0 ? key : key.substring(0, at);
  }

  @Override
  public String toString() {
    return "LineageSinkTarget{name=" + name + ", pluginId=" + pluginId + '}';
  }
}
