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

import java.util.Collection;
import java.util.Comparator;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.util.VersionCompare;

/**
 * Plugin compatibility checks for marketplace discovery.
 *
 * <p>The ordering itself lives in {@link VersionCompare}, so the marketplace and everything else
 * that has to decide whether one version is newer than another agree on the answer. What is
 * specific to the marketplace, and stays here, is what a plugin's declared Hop bounds mean.
 */
public final class VersionCompat {

  private VersionCompat() {}

  /**
   * Whether {@code info} may be offered on the given running Hop version. Blank min/max means no
   * bound on that side. Missing hop version fails closed only when a bound is present.
   *
   * <p>{@code x.y.z-SNAPSHOT} is treated as the {@code x.y.z} line for these bounds so developers
   * on a SNAPSHOT build satisfy a {@code minHopVersion} of the same release (e.g. {@code
   * 2.19.0-SNAPSHOT} fulfills {@code minHopVersion: 2.19.0}). Artifact version ordering via {@link
   * #compare(String, String)} still ranks SNAPSHOT below the release.
   */
  public static boolean isCompatibleWithHop(OptionalPluginInfo info, String hopVersion) {
    if (info == null) {
      return false;
    }
    String min = info.getMinHopVersion();
    String max = info.getMaxHopVersion();
    if (StringUtils.isBlank(min) && StringUtils.isBlank(max)) {
      return true;
    }
    if (StringUtils.isBlank(hopVersion)) {
      return false;
    }
    // Compatibility uses the release line; SNAPSHOT is development of that line.
    String hopLine = stripSnapshotQualifier(hopVersion);
    if (StringUtils.isNotBlank(min) && compare(hopLine, stripSnapshotQualifier(min)) < 0) {
      return false;
    }
    if (StringUtils.isNotBlank(max) && compare(hopLine, stripSnapshotQualifier(max)) > 0) {
      return false;
    }
    return true;
  }

  /**
   * Strip a trailing {@code -SNAPSHOT} qualifier (case-insensitive). Used for Hop min/max
   * compatibility only.
   */
  static String stripSnapshotQualifier(String version) {
    return VersionCompare.stripSnapshotQualifier(version);
  }

  /**
   * @return negative if a &lt; b, zero if equal, positive if a &gt; b
   */
  public static int compare(String a, String b) {
    return VersionCompare.compare(a, b);
  }

  /** Highest version string in {@code versions}, or null if empty. */
  public static String latest(Collection<String> versions) {
    return VersionCompare.latest(versions);
  }

  /** Comparator for plugin artifact versions (newest first). */
  public static Comparator<String> newestFirst() {
    return VersionCompare.newestFirst();
  }
}
