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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import org.apache.commons.lang3.StringUtils;

/**
 * Hop / plugin version comparison and compatibility checks for marketplace discovery.
 *
 * <p>Lightweight Maven-like ordering: numeric segments, then qualifiers; {@code -SNAPSHOT} is
 * treated as a pre-release of the same base version.
 */
public final class VersionCompat {

  private VersionCompat() {}

  /**
   * Whether {@code info} may be offered on the given running Hop version. Blank min/max means no
   * bound on that side. Missing hop version fails closed only when a bound is present.
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
    if (StringUtils.isNotBlank(min) && compare(hopVersion, min) < 0) {
      return false;
    }
    if (StringUtils.isNotBlank(max) && compare(hopVersion, max) > 0) {
      return false;
    }
    return true;
  }

  /**
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

  /** Highest version string in {@code versions}, or null if empty. */
  public static String latest(Collection<String> versions) {
    if (versions == null || versions.isEmpty()) {
      return null;
    }
    return versions.stream()
        .filter(StringUtils::isNotBlank)
        .max(VersionCompat::compare)
        .orElse(null);
  }

  /** Comparator for plugin artifact versions (newest first). */
  public static Comparator<String> newestFirst() {
    return (a, b) -> compare(b, a);
  }

  private static int comparePart(Object a, Object b) {
    boolean aNum = a instanceof Integer;
    boolean bNum = b instanceof Integer;
    if (aNum && bNum) {
      return Integer.compare((Integer) a, (Integer) b);
    }
    if (aNum) {
      // numeric segment > qualifier (1.0 > 1.0-alpha)
      return 1;
    }
    if (bNum) {
      return -1;
    }
    String sa = String.valueOf(a).toLowerCase(Locale.ROOT);
    String sb = String.valueOf(b).toLowerCase(Locale.ROOT);
    // snapshot is older than release of same base when compared as qualifier after base
    int rankA = qualifierRank(sa);
    int rankB = qualifierRank(sb);
    if (rankA != rankB) {
      return Integer.compare(rankA, rankB);
    }
    return sa.compareTo(sb);
  }

  private static int qualifierRank(String q) {
    if ("snapshot".equals(q)) {
      return -1;
    }
    if ("final".equals(q) || "ga".equals(q) || "release".equals(q) || q.isEmpty()) {
      return 1;
    }
    return 0;
  }

  static List<Object> parse(String version) {
    List<Object> parts = new ArrayList<>();
    if (StringUtils.isBlank(version)) {
      return parts;
    }
    String v = version.trim();
    // Split on . - and treat SNAPSHOT specially
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i <= v.length(); i++) {
      char ch = i < v.length() ? v.charAt(i) : '.';
      if (ch == '.' || ch == '-' || i == v.length()) {
        if (buf.length() > 0) {
          String token = buf.toString();
          buf.setLength(0);
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
        buf.append(ch);
      }
    }
    return parts;
  }
}
