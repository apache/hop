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
package org.apache.hop.core.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import org.apache.commons.lang3.StringUtils;

/**
 * Ordering for version strings.
 *
 * <p>Lightweight Maven-like ordering: numeric segments compare numerically and sort above
 * qualifiers, so {@code 1.0} is newer than {@code 1.0-alpha}, and {@code -SNAPSHOT} is a
 * pre-release of the version it names, so {@code 2.20.0-SNAPSHOT} is older than {@code 2.20.0}.
 *
 * <p>This lives in core because more than one part of Hop has to decide whether one version is
 * newer than another - the marketplace comparing a catalog against what is installed, the
 * notification system deciding whether a release is worth mentioning - and two implementations of
 * this would eventually disagree.
 */
public final class VersionCompare {

  private static final String SNAPSHOT_SUFFIX = "-snapshot";

  private VersionCompare() {
    // Utility class
  }

  /**
   * @param a The first version
   * @param b The second version
   * @return negative if a &lt; b, zero if equal, positive if a &gt; b
   */
  public static int compare(String a, String b) {
    List<Object> pa = parse(a);
    List<Object> pb = parse(b);
    int n = Math.max(pa.size(), pb.size());
    for (int i = 0; i < n; i++) {
      Object xa = i < pa.size() ? pa.get(i) : 0;
      Object xb = i < pb.size() ? pb.get(i) : 0;
      int c = comparePart(xa, xb);
      if (c != 0) {
        return c;
      }
    }
    return 0;
  }

  /**
   * @param versions The versions to choose from
   * @return The highest version, or null when there is none
   */
  public static String latest(Collection<String> versions) {
    if (versions == null || versions.isEmpty()) {
      return null;
    }
    return versions.stream()
        .filter(StringUtils::isNotBlank)
        .max(VersionCompare::compare)
        .orElse(null);
  }

  /**
   * @return A comparator ordering versions newest first
   */
  public static Comparator<String> newestFirst() {
    return (a, b) -> compare(b, a);
  }

  /**
   * @param version The version to test
   * @return Whether this names a snapshot rather than a published version
   */
  public static boolean isSnapshot(String version) {
    return StringUtils.isNotBlank(version)
        && version.trim().toLowerCase(Locale.ROOT).endsWith(SNAPSHOT_SUFFIX);
  }

  /**
   * Strip a trailing {@code -SNAPSHOT}, giving the release line a snapshot belongs to.
   *
   * @param version The version to strip
   * @return The version without its snapshot qualifier
   */
  public static String stripSnapshotQualifier(String version) {
    if (StringUtils.isBlank(version)) {
      return version;
    }
    String trimmed = version.trim();
    if (trimmed.toLowerCase(Locale.ROOT).endsWith(SNAPSHOT_SUFFIX)) {
      return trimmed.substring(0, trimmed.length() - SNAPSHOT_SUFFIX.length());
    }
    return trimmed;
  }

  private static int comparePart(Object a, Object b) {
    boolean aNum = a instanceof Integer;
    boolean bNum = b instanceof Integer;
    if (aNum && bNum) {
      return Integer.compare((Integer) a, (Integer) b);
    }
    if (aNum) {
      // A numeric segment outranks a qualifier: 1.0 is newer than 1.0-alpha.
      return 1;
    }
    if (bNum) {
      return -1;
    }
    String sa = String.valueOf(a).toLowerCase(Locale.ROOT);
    String sb = String.valueOf(b).toLowerCase(Locale.ROOT);
    int rankA = qualifierRank(sa);
    int rankB = qualifierRank(sb);
    if (rankA != rankB) {
      return Integer.compare(rankA, rankB);
    }
    return sa.compareTo(sb);
  }

  private static int qualifierRank(String qualifier) {
    if ("snapshot".equals(qualifier)) {
      return -1;
    }
    if ("final".equals(qualifier)
        || "ga".equals(qualifier)
        || "release".equals(qualifier)
        || qualifier.isEmpty()) {
      return 1;
    }
    return 0;
  }

  /**
   * Drop the {@code v} that release tags are conventionally written with.
   *
   * <p>Tagging a release {@code v3.0.0} is GitHub's own recommendation and the dominant convention
   * on it. Left in place, {@code v3} is not a number, and a qualifier sorts below every numeric
   * segment: {@code v3.0.0} would compare older than {@code 1.0.0}, and {@code v10} older than
   * {@code v9}. Only a {@code v} directly in front of a digit is dropped, so a version genuinely
   * named after a word - {@code vault-1.0} - is left alone.
   *
   * @param trimmed The version, already trimmed
   * @return The version without its {@code v} prefix
   */
  private static String stripVersionPrefix(String trimmed) {
    if (trimmed.length() > 1
        && (trimmed.charAt(0) == 'v' || trimmed.charAt(0) == 'V')
        && Character.isDigit(trimmed.charAt(1))) {
      return trimmed.substring(1);
    }
    return trimmed;
  }

  static List<Object> parse(String version) {
    List<Object> parts = new ArrayList<>();
    if (StringUtils.isBlank(version)) {
      return parts;
    }
    String trimmed = stripVersionPrefix(version.trim());
    StringBuilder buffer = new StringBuilder();
    for (int i = 0; i <= trimmed.length(); i++) {
      char ch = i < trimmed.length() ? trimmed.charAt(i) : '.';
      if (ch == '.' || ch == '-' || i == trimmed.length()) {
        if (buffer.length() > 0) {
          String token = buffer.toString();
          buffer.setLength(0);
          if (token.chars().allMatch(Character::isDigit)) {
            try {
              parts.add(Integer.parseInt(token));
            } catch (NumberFormatException e) {
              parts.add(token.toLowerCase(Locale.ROOT));
            }
          } else {
            parts.add(token.toLowerCase(Locale.ROOT));
          }
        }
      } else {
        buffer.append(ch);
      }
    }
    return parts;
  }
}
