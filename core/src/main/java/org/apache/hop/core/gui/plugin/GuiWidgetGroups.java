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

package org.apache.hop.core.gui.plugin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;

/** Partitions {@link GuiElements} into layout groups. SWT-free so it can be unit-tested in core. */
public final class GuiWidgetGroups {

  public static final String UNGROUPED_KEY = "";

  private GuiWidgetGroups() {}

  public static boolean hasGroups(List<GuiElements> children) {
    if (children == null) {
      return false;
    }
    for (GuiElements child : children) {
      if (include(child) && child.hasGroup()) {
        return true;
      }
    }
    return false;
  }

  /**
   * True when two included children declare different non-{@link GuiWidgetGroupType#NONE} types.
   * Mixed types fall back to {@link GuiWidgetGroupType#TABS}.
   */
  public static boolean hasMixedTypes(List<GuiElements> children) {
    GuiWidgetGroupType type = GuiWidgetGroupType.NONE;
    if (children == null) {
      return false;
    }
    for (GuiElements child : children) {
      if (!include(child) || child.getGroupType() == GuiWidgetGroupType.NONE) {
        continue;
      }
      if (type == GuiWidgetGroupType.NONE) {
        type = child.getGroupType();
      } else if (type != child.getGroupType()) {
        return true;
      }
    }
    return false;
  }

  public static GuiWidgetGroupType typeOf(List<GuiElements> children) {
    if (hasMixedTypes(children)) {
      return GuiWidgetGroupType.TABS;
    }
    GuiWidgetGroupType type = GuiWidgetGroupType.NONE;
    if (children != null) {
      for (GuiElements child : children) {
        if (!include(child) || child.getGroupType() == GuiWidgetGroupType.NONE) {
          continue;
        }
        type = child.getGroupType();
        break;
      }
    }
    return type == GuiWidgetGroupType.NONE ? GuiWidgetGroupType.TABS : type;
  }

  public static List<Bucket> from(List<GuiElements> children, String ungroupedLabel) {
    Map<String, Bucket> byKey = new LinkedHashMap<>();
    List<GuiElements> sorted = new ArrayList<>();
    if (children != null) {
      sorted.addAll(children);
    }
    Collections.sort(sorted);

    for (GuiElements child : sorted) {
      if (!include(child)) {
        continue;
      }
      String key = child.hasGroup() ? child.getGroup() : UNGROUPED_KEY;
      String label = child.hasGroup() ? child.getGroup() : ungroupedLabel;
      String order = child.hasGroup() ? Const.NVL(child.getGroupOrder(), "") : "0";
      Bucket bucket =
          byKey.computeIfAbsent(key, k -> new Bucket(key, label, order, child.getGroupImage()));
      if (StringUtils.isEmpty(bucket.getOrder()) && StringUtils.isNotEmpty(order)) {
        bucket.setOrder(order);
      }
      if (StringUtils.isEmpty(bucket.getImage()) && StringUtils.isNotEmpty(child.getGroupImage())) {
        bucket.setImage(child.getGroupImage());
      }
      bucket.getElements().add(child);
    }

    List<Bucket> buckets = new ArrayList<>(byKey.values());
    buckets.sort(
        Comparator.comparing((Bucket b) -> Const.NVL(b.getOrder(), ""))
            .thenComparing(b -> Const.NVL(b.getLabel(), "")));
    return buckets;
  }

  private static boolean include(GuiElements child) {
    return child != null && !child.isIgnored() && child.getId() != null;
  }

  @Getter
  @Setter
  public static final class Bucket {
    private final String key;
    private final String label;
    private String order;
    private String image;
    private final List<GuiElements> elements = new ArrayList<>();

    Bucket(String key, String label, String order, String image) {
      this.key = key;
      this.label = label;
      this.order = order;
      this.image = image;
    }
  }
}
