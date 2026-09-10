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
package org.apache.hop.imp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * Maps every observed spelling of a relational connection name onto one target name.
 *
 * <p>Kettle treated names as case-insensitive, so {@code Database}, {@code database} and {@code
 * DATABASE} are one group. A group collapses to a preferred spelling (shared.xml, then the most
 * frequent, then first seen). An optional mapper (a naming scheme) is then applied to that
 * canonical spelling. Distinct groups that land on the same target are recorded as collisions; they
 * still share the target name.
 */
@Getter
public final class ConnectionNameMap {

  private final Map<String, String> oldToNew = new LinkedHashMap<>();
  private final List<Collision> collisions = new ArrayList<>();

  public record Collision(String leftOriginal, String rightOriginal, String targetName) {}

  private ConnectionNameMap() {}

  public static ConnectionNameMap empty() {
    return new ConnectionNameMap();
  }

  /**
   * Names that cannot be rewritten: empty, table null markers, and values that contain a variable
   * expression.
   */
  public static boolean shouldSkip(String value) {
    if (StringUtils.isEmpty(value) || "<null>".equals(value)) {
      return true;
    }
    return value.contains("${");
  }

  /**
   * @param names every spelling found on connections and in pipeline/workflow references
   * @param preferredNames spellings that win inside a case-insensitive group (typically shared.xml)
   * @param mapper applied to the canonical original; {@code null} or identity leaves it as-is
   */
  public static ConnectionNameMap build(
      Collection<String> names, Collection<String> preferredNames, UnaryOperator<String> mapper) {
    Map<String, List<String>> groups = new LinkedHashMap<>();
    Map<String, Integer> frequency = new LinkedHashMap<>();
    if (names != null) {
      for (String name : names) {
        if (shouldSkip(name)) {
          continue;
        }
        String key = name.toLowerCase(Locale.ROOT);
        groups.computeIfAbsent(key, k -> new ArrayList<>()).add(name);
        frequency.merge(name, 1, Integer::sum);
      }
    }

    Map<String, String> preferredByLower = new LinkedHashMap<>();
    if (preferredNames != null) {
      for (String preferred : preferredNames) {
        if (shouldSkip(preferred)) {
          continue;
        }
        preferredByLower.putIfAbsent(preferred.toLowerCase(Locale.ROOT), preferred);
      }
    }

    UnaryOperator<String> effective = mapper != null ? mapper : UnaryOperator.identity();
    ConnectionNameMap result = new ConnectionNameMap();
    Map<String, String> targetOwner = new LinkedHashMap<>();

    for (Map.Entry<String, List<String>> group : groups.entrySet()) {
      String canonical =
          pickCanonical(group.getValue(), preferredByLower.get(group.getKey()), frequency);
      String target = canonical;
      try {
        String mapped = effective.apply(canonical);
        if (!shouldSkip(mapped)) {
          target = mapped;
        }
      } catch (Exception e) {
        // Keep the canonical original when the mapper fails.
      }

      String targetKey = target.toLowerCase(Locale.ROOT);
      String owner = targetOwner.putIfAbsent(targetKey, canonical);
      if (owner != null && !owner.equalsIgnoreCase(canonical)) {
        result.collisions.add(new Collision(owner, canonical, target));
      }

      Set<String> unique = new LinkedHashSet<>(group.getValue());
      for (String spelling : unique) {
        result.oldToNew.put(spelling, target);
      }
    }
    return result;
  }

  /**
   * Look up the target name for {@code name}. Exact match wins, then case-insensitive. Unknown
   * names (and skipped values) are returned unchanged.
   */
  public String targetFor(String name) {
    if (shouldSkip(name)) {
      return name;
    }
    String exact = oldToNew.get(name);
    if (exact != null) {
      return exact;
    }
    for (Map.Entry<String, String> entry : oldToNew.entrySet()) {
      if (entry.getKey().equalsIgnoreCase(name)) {
        return entry.getValue();
      }
    }
    return name;
  }

  public boolean isEmpty() {
    return oldToNew.isEmpty();
  }

  /** How many spellings actually change. */
  public int changedCount() {
    int count = 0;
    for (Map.Entry<String, String> entry : oldToNew.entrySet()) {
      if (!entry.getKey().equals(entry.getValue())) {
        count++;
      }
    }
    return count;
  }

  private static String pickCanonical(
      List<String> spellings, String preferred, Map<String, Integer> frequency) {
    if (preferred != null) {
      return preferred;
    }
    String best = spellings.get(0);
    int bestCount = frequency.getOrDefault(best, 0);
    Set<String> seen = new LinkedHashSet<>();
    for (String spelling : spellings) {
      if (!seen.add(spelling)) {
        continue;
      }
      int count = frequency.getOrDefault(spelling, 0);
      if (count > bestCount) {
        best = spelling;
        bestCount = count;
      }
    }
    return best;
  }
}
