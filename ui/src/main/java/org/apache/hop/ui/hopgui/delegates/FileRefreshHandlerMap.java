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

package org.apache.hop.ui.hopgui.delegates;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;

/** Path → handlers. Several editors can watch the same file; the last close unwatches. */
final class FileRefreshHandlerMap {

  private final Map<String, List<IHopFileTypeHandler>> handlers = new HashMap<>();

  /**
   * @return {@code true} when this is the first handler for {@code key}
   */
  boolean add(String key, IHopFileTypeHandler handler) {
    if (key == null || handler == null) {
      return false;
    }
    List<IHopFileTypeHandler> list = handlers.computeIfAbsent(key, k -> new ArrayList<>());
    if (list.contains(handler)) {
      return false;
    }
    list.add(handler);
    return list.size() == 1;
  }

  /** Make {@code aliasKey} share the handler list of {@code existingKey}. */
  void alias(String existingKey, String aliasKey) {
    if (existingKey == null || aliasKey == null || existingKey.equals(aliasKey)) {
      return;
    }
    List<IHopFileTypeHandler> list = handlers.get(existingKey);
    if (list != null) {
      handlers.put(aliasKey, list);
    }
  }

  List<IHopFileTypeHandler> get(String key) {
    List<IHopFileTypeHandler> list = handlers.get(key);
    return list == null ? List.of() : List.copyOf(list);
  }

  /**
   * @return {@code true} when no handlers remain for {@code key} (the watch can be dropped)
   */
  boolean remove(String key, IHopFileTypeHandler handler) {
    if (key == null) {
      return true;
    }
    List<IHopFileTypeHandler> list = handlers.get(key);
    if (list == null) {
      return true;
    }
    list.remove(handler);
    if (list.isEmpty()) {
      removeKeysForList(list);
      return true;
    }
    return false;
  }

  /**
   * @return {@code true} when the path is no longer watched
   */
  boolean removeAll(String key) {
    List<IHopFileTypeHandler> list = handlers.get(key);
    if (list == null) {
      return true;
    }
    removeKeysForList(list);
    return true;
  }

  private void removeKeysForList(List<IHopFileTypeHandler> list) {
    handlers.entrySet().removeIf(entry -> entry.getValue() == list);
  }
}
