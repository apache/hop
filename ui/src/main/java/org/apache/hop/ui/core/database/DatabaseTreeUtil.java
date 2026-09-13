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

package org.apache.hop.ui.core.database;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.hop.ui.core.database.DatabaseTreeNode.Kind;
import org.apache.hop.ui.core.gui.GuiResource;
import org.eclipse.swt.graphics.Image;
import org.jspecify.annotations.Nullable;

/**
 * Utility class for sharing functions between the Database perspective and the Database Explorer
 * dialog.
 */
public class DatabaseTreeUtil {

  private DatabaseTreeUtil() {
    // Utility class
  }

  /**
   * Classify a schema object so views (and synonyms) can use a distinct tree icon.
   *
   * @param name object name
   * @param views view names in the same schema (any case)
   * @param synonyms synonym names in the same schema (any case)
   */
  public static Kind kindOf(String name, Collection<String> views, Collection<String> synonyms) {
    if (containsIgnoreCase(views, name)) {
      return Kind.VIEW;
    }
    if (containsIgnoreCase(synonyms, name)) {
      return Kind.SYNONYM;
    }
    return Kind.TABLE;
  }

  public static boolean containsIgnoreCase(
      @Nullable Collection<String> names, @Nullable String name) {
    if (names == null || name == null) {
      return false;
    }
    for (String candidate : names) {
      if (name.equalsIgnoreCase(candidate)) {
        return true;
      }
    }
    return false;
  }

  public static Collection<String> namesForSchema(
      Map<String, Collection<String>> map, String schemaName) {
    if (map == null || map.isEmpty()) {
      return List.of();
    }
    if (schemaName != null) {
      Collection<String> exact = map.get(schemaName);
      if (exact != null) {
        return exact;
      }
      for (Map.Entry<String, Collection<String>> entry : map.entrySet()) {
        if (schemaName.equalsIgnoreCase(entry.getKey()) && entry.getValue() != null) {
          return entry.getValue();
        }
      }
    }
    Collection<String> empty = map.get("");
    if (empty != null) {
      return empty;
    }
    Collection<String> missing = map.get(null);
    return missing != null ? missing : List.of();
  }

  public static Image imageFor(DatabaseTreeNode.Kind kind) {
    GuiResource resources = GuiResource.getInstance();
    return switch (kind) {
      case VIEW -> resources.getImageView();
      case SYNONYM -> resources.getImageSynonym();
      default -> resources.getImageTable();
    };
  }
}
