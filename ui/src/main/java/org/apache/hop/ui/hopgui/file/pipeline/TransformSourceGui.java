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
package org.apache.hop.ui.hopgui.file.pipeline;

import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transform.TransformSourceSupport;
import org.apache.hop.ui.hopgui.HopGui;

/**
 * Adds pipeline-source search keywords and tooltip text to transform create actions and palette
 * items.
 */
public final class TransformSourceGui {
  private static final Class<?> PKG = HopGui.class;

  private TransformSourceGui() {
    // utility
  }

  public static void addSearchKeywords(List<String> keywords, IPlugin plugin) {
    if (keywords == null || !TransformSourceSupport.isPipelineSourceAtDefault(plugin)) {
      return;
    }
    if (!keywords.contains(TransformSourceSupport.SEARCH_KEYWORD)) {
      keywords.add(TransformSourceSupport.SEARCH_KEYWORD);
    }
    String localized = BaseMessages.getString(PKG, "HopGuiPipelineGraph.PipelineSource.Keyword");
    if (StringUtils.isNotEmpty(localized) && !keywords.contains(localized)) {
      keywords.add(localized);
    }
  }

  public static void labelCreateAction(GuiAction action, IPlugin plugin) {
    if (action == null || !TransformSourceSupport.isPipelineSourceAtDefault(plugin)) {
      return;
    }
    addSearchKeywords(action.getKeywords(), plugin);
    String suffix = BaseMessages.getString(PKG, "HopGuiPipelineGraph.PipelineSource.TooltipSuffix");
    if (StringUtils.isEmpty(suffix)) {
      return;
    }
    if (StringUtils.isEmpty(action.getTooltip())) {
      action.setTooltip(suffix);
    } else if (!action.getTooltip().contains(suffix)) {
      action.setTooltip(action.getTooltip() + Const.CR + suffix);
    }
  }
}
