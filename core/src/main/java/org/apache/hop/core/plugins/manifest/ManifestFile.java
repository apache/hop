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
package org.apache.hop.core.plugins.manifest;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * One file a plugin zip installs into a Hop home, or (with a {@code null} path) a dependency the
 * plugin declares but does not ship.
 *
 * <p>Jars carry their Maven coordinates when the build could resolve them, so two plugins that
 * install the same shared jar can be recognised as co-owners of one file.
 */
@Getter
@Setter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ManifestFile {

  /** Where a file lands relative to the Hop home, which decides who may delete it. */
  public enum Location {
    /** The plugin folder itself: its own jar, version.xml, dependencies.xml, icons. */
    PLUGIN,
    /** {@code <plugin folder>/lib/}: private to this plugin. */
    PRIVATE,
    /** {@code lib/core/}: on the system classpath, possibly shared with other plugins. */
    SHARED,
    /** {@code lib/jdbc/}: a JDBC driver visible to every database plugin. */
    JDBC,
    /** Anything else the zip drops into the Hop home, such as sample projects. */
    OTHER
  }

  /** Path relative to the Hop home, always with forward slashes. */
  private String path;

  private Location location;

  private String groupId;
  private String artifactId;
  private String version;
  private String classifier;

  /** Maven scope the build resolved this dependency with, when known. */
  private String scope;

  private Long size;
  private String sha256;

  public ManifestFile(String path, Location location) {
    this.path = path;
    this.location = location;
  }

  /** {@code groupId:artifactId} when the coordinates are known, else {@code null}. */
  @JsonIgnore
  public String getGa() {
    if (groupId == null || artifactId == null) {
      return null;
    }
    return groupId + ":" + artifactId;
  }

  @JsonIgnore
  public boolean isJar() {
    return path != null && path.endsWith(".jar");
  }
}
