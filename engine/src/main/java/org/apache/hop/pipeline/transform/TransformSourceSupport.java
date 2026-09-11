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
package org.apache.hop.pipeline.transform;

import java.util.concurrent.ConcurrentHashMap;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;

/**
 * Helpers for {@link ITransformMeta#canStartWithoutInput()} used by Verify, the add-transform
 * dialog and the palette.
 */
public final class TransformSourceSupport {

  /** English search keyword added to pipeline-source create actions. */
  public static final String SEARCH_KEYWORD = "pipeline source";

  /** Check-result code for Verify comments on pipeline sources. */
  public static final String CHECK_CODE_PIPELINE_SOURCE = "CAN_START_WITHOUT_INPUT";

  private static final ConcurrentHashMap<String, Boolean> DEFAULT_SOURCE_CACHE =
      new ConcurrentHashMap<>();

  private TransformSourceSupport() {
    // utility
  }

  public static boolean isPipelineSource(ITransformMeta meta) {
    return meta != null && meta.canStartWithoutInput();
  }

  /**
   * Whether a newly dropped instance of this plugin (after {@code setDefault()}) can start without
   * incoming hops. Cached per plugin id.
   */
  public static boolean isPipelineSourceAtDefault(IPlugin plugin) {
    if (plugin == null || plugin.getIds() == null || plugin.getIds().length == 0) {
      return false;
    }
    String cacheKey = plugin.getIds()[0];
    return DEFAULT_SOURCE_CACHE.computeIfAbsent(cacheKey, id -> computeDefault(plugin));
  }

  public static void clearCache() {
    DEFAULT_SOURCE_CACHE.clear();
  }

  private static boolean computeDefault(IPlugin plugin) {
    try {
      ITransformMeta meta = PluginRegistry.getInstance().loadClass(plugin, ITransformMeta.class);
      if (meta == null) {
        return false;
      }
      meta.setDefault();
      return meta.canStartWithoutInput();
    } catch (Exception e) {
      return false;
    }
  }
}
