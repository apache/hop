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

package org.apache.hop.marketplace.resolve;

import java.util.Objects;
import org.apache.hop.core.exception.HopException;

/** Simple GAV for Hop plugin zips. */
public record MavenCoordinates(String groupId, String artifactId, String version) {

  public MavenCoordinates {
    Objects.requireNonNull(groupId, "groupId");
    Objects.requireNonNull(artifactId, "artifactId");
    Objects.requireNonNull(version, "version");
  }

  /**
   * Parse {@code artifactId}, {@code artifactId:version}, or {@code groupId:artifactId:version}.
   */
  public static MavenCoordinates parse(String spec, String defaultGroupId, String defaultVersion)
      throws HopException {
    if (spec == null || spec.isBlank()) {
      throw new HopException("Plugin coordinate is empty");
    }
    String[] parts = spec.trim().split(":");
    return switch (parts.length) {
      case 1 -> new MavenCoordinates(defaultGroupId, parts[0], requireVersion(defaultVersion));
      case 2 -> new MavenCoordinates(defaultGroupId, parts[0], parts[1]);
      case 3 -> new MavenCoordinates(parts[0], parts[1], parts[2]);
      default ->
          throw new HopException(
              "Invalid coordinate '"
                  + spec
                  + "'. Use artifactId, artifactId:version, or groupId:artifactId:version");
    };
  }

  private static String requireVersion(String defaultVersion) throws HopException {
    if (defaultVersion == null || defaultVersion.isBlank()) {
      throw new HopException(
          "No version specified and no default Hop version available. Use artifactId:version");
    }
    return defaultVersion;
  }

  /** Maven repository relative path to the zip artifact (no leading slash). */
  public String zipRepositoryPath() {
    String groupPath = groupId.replace('.', '/');
    String fileName = artifactId + "-" + version + ".zip";
    return groupPath + "/" + artifactId + "/" + version + "/" + fileName;
  }

  public String gav() {
    return groupId + ":" + artifactId + ":" + version;
  }
}
