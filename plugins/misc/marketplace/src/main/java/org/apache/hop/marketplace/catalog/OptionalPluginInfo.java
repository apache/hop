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

package org.apache.hop.marketplace.catalog;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class OptionalPluginInfo {
  private String groupId;
  private String artifactId;
  private String version;
  private String name;
  private String category;
  private String description;
  private String installPath;

  /** Where this entry came from (e.g. {@code apache}, repository id, catalog URL). */
  private String source;

  /** Last modified time from remote repository (ISO-8601 or raw Nexus value), if known. */
  private String lastUpdated;

  /**
   * Minimum Hop version this plugin supports (e.g. {@code 2.18.1}). When set, discovery hides the
   * plugin if the running Hop version is older.
   */
  private String minHopVersion;

  /**
   * Optional maximum Hop version this plugin supports. When set, discovery hides the plugin if the
   * running Hop version is newer.
   */
  private String maxHopVersion;

  public OptionalPluginInfo() {
    // Jackson / default
  }

  public OptionalPluginInfo(
      String artifactId, String name, String category, String description, String installPath) {
    this.artifactId = artifactId;
    this.name = name;
    this.category = category;
    this.description = description;
    this.installPath = installPath;
    this.source = "apache";
  }
}
