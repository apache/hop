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

package org.apache.hop.ui.hopgui.perspective.metadata;

import java.util.List;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataCategory;

/**
 * Presentation registry for metadata categories. The {@code core} layer only carries a stable
 * category id on each {@code @HopMetadata} type (see {@link HopMetadataCategory}); the human
 * readable label, the display order and the icon shown in the metadata perspective are resolved
 * here so that no presentation concern leaks into {@code core}.
 *
 * <p>Categories declared below are shown in declaration order. An unknown (but non-empty) category
 * id sorts after all known categories (alphabetically by id); the empty category id is the "Other"
 * bucket and always sorts last.
 */
public final class MetadataCategories {

  private static final Class<?> PKG = MetadataPerspective.class; // i18n

  /** The empty category id: types without a category land in the "Other" bucket. */
  public static final String OTHER = "";

  private static final String OTHER_IMAGE = "ui/images/metadata.svg";

  /** A known category: its id, the i18n key for its label and the icon shown in the tree. */
  private record CategoryInfo(String id, String labelKey, String image) {}

  /** Declaration order is the display order in the tree, the overview and the new-type menu. */
  private static final List<CategoryInfo> ORDERED =
      List.of(
          new CategoryInfo(
              HopMetadataCategory.CONNECTIONS,
              "MetadataPerspective.Category.Connections",
              "ui/images/database.svg"),
          new CategoryInfo(
              HopMetadataCategory.FILE_STORAGE,
              "MetadataPerspective.Category.FileStorage",
              "ui/images/location.svg"),
          new CategoryInfo(
              HopMetadataCategory.RUN_CONFIG,
              "MetadataPerspective.Category.RunConfig",
              "ui/images/gear.svg"),
          new CategoryInfo(
              HopMetadataCategory.SERVERS,
              "MetadataPerspective.Category.Servers",
              "ui/images/server.svg"),
          new CategoryInfo(
              HopMetadataCategory.EXECUTION,
              "MetadataPerspective.Category.Execution",
              "ui/images/analyzer.svg"),
          new CategoryInfo(
              HopMetadataCategory.LOGGING,
              "MetadataPerspective.Category.Logging",
              "ui/images/log.svg"),
          new CategoryInfo(
              HopMetadataCategory.TESTING,
              "MetadataPerspective.Category.Testing",
              "ui/images/catalog.svg"),
          new CategoryInfo(
              HopMetadataCategory.DATA_DEFINITION,
              "MetadataPerspective.Category.DataDefinition",
              "ui/images/partition_schema.svg"),
          new CategoryInfo(
              HopMetadataCategory.VARIABLES,
              "MetadataPerspective.Category.Variables",
              "ui/images/variable.svg"));

  private MetadataCategories() {
    // Utility class
  }

  /** Normalizes a (possibly null) category id to either a known/unknown id or {@link #OTHER}. */
  public static String normalize(String categoryId) {
    return Utils.isEmpty(categoryId) ? OTHER : categoryId;
  }

  private static CategoryInfo find(String categoryId) {
    for (CategoryInfo info : ORDERED) {
      if (info.id().equals(categoryId)) {
        return info;
      }
    }
    return null;
  }

  /**
   * Sort key for a category id. Known categories keep their declaration order; unknown non-empty
   * ids come next; the "Other" bucket is always last.
   */
  public static int orderOf(String categoryId) {
    String id = normalize(categoryId);
    if (OTHER.equals(id)) {
      return Integer.MAX_VALUE;
    }
    for (int i = 0; i < ORDERED.size(); i++) {
      if (ORDERED.get(i).id().equals(id)) {
        return i;
      }
    }
    // Unknown but non-empty category: after all known ones, before "Other".
    return ORDERED.size();
  }

  /** The translated label for a category id, suitable for display as a tree/overview heading. */
  public static String labelFor(String categoryId) {
    String id = normalize(categoryId);
    if (OTHER.equals(id)) {
      return BaseMessages.getString(PKG, "MetadataPerspective.Category.Other");
    }
    CategoryInfo info = find(id);
    if (info != null) {
      return BaseMessages.getString(PKG, info.labelKey());
    }
    // Unknown category id from a third-party plugin: show the raw id rather than nothing.
    return id;
  }

  /** The icon path for a category id, falling back to a generic metadata icon. */
  public static String imageFor(String categoryId) {
    CategoryInfo info = find(normalize(categoryId));
    return info != null ? info.image() : OTHER_IMAGE;
  }
}
