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

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

/**
 * Maven SNAPSHOT helpers. Nexus search often reports the <em>unique</em> version ({@code
 * 1.0.0-20260721.105615-1}) while repository layout stores files under the base folder ({@code
 * 1.0.0-SNAPSHOT/}).
 */
public final class SnapshotVersions {

  /** Unique timestamped SNAPSHOT: {@code base-yyyyMMdd.HHmmss-buildNumber}. */
  private static final Pattern UNIQUE_SNAPSHOT = Pattern.compile("^(.*)-(\\d{8}\\.\\d{6})-(\\d+)$");

  private SnapshotVersions() {}

  /** True for {@code *-SNAPSHOT} or unique timestamped SNAPSHOT versions. */
  public static boolean isSnapshot(String version) {
    if (StringUtils.isBlank(version)) {
      return false;
    }
    return version.endsWith("-SNAPSHOT") || isUniqueSnapshot(version);
  }

  /** True for {@code 1.0.0-20260721.105615-1} style unique versions. */
  public static boolean isUniqueSnapshot(String version) {
    return StringUtils.isNotBlank(version) && UNIQUE_SNAPSHOT.matcher(version.trim()).matches();
  }

  /**
   * Map unique SNAPSHOT versions to base {@code *-SNAPSHOT} used in Maven repository directories.
   * Leaves release and already-base SNAPSHOT versions unchanged.
   */
  public static String toBaseVersion(String version) {
    if (StringUtils.isBlank(version)) {
      return version;
    }
    String v = version.trim();
    if (v.endsWith("-SNAPSHOT")) {
      return v;
    }
    Matcher m = UNIQUE_SNAPSHOT.matcher(v);
    if (m.matches()) {
      return m.group(1) + "-SNAPSHOT";
    }
    return v;
  }

  /**
   * Prefer the version folder from a Maven asset path (e.g. {@code /g/a/0.4.0-SNAPSHOT/a-....zip}),
   * else {@link #toBaseVersion(String)}.
   */
  public static String toBaseVersion(String version, String mavenAssetPath) {
    String fromPath = versionFolderFromPath(mavenAssetPath);
    if (StringUtils.isNotBlank(fromPath)) {
      return fromPath;
    }
    return toBaseVersion(version);
  }

  /**
   * Extract the version directory from a repository-relative path such as {@code
   * org/apache/hop/hop-datavault/0.4.0-SNAPSHOT/hop-datavault-….zip}.
   */
  public static String versionFolderFromPath(String path) {
    if (StringUtils.isBlank(path)) {
      return null;
    }
    String p = path.trim();
    while (p.startsWith("/")) {
      p = p.substring(1);
    }
    while (p.endsWith("/")) {
      p = p.substring(0, p.length() - 1);
    }
    String[] parts = p.split("/");
    if (parts.length < 2) {
      return null;
    }
    // …/artifactId/version/filename
    return parts[parts.length - 2];
  }

  public static boolean containsSnapshotKeyword(String version) {
    return version != null && version.toUpperCase(Locale.ROOT).contains("SNAPSHOT");
  }
}
