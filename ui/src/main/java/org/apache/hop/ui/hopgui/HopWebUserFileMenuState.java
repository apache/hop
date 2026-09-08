/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.hopgui;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.workflow.WorkflowMeta;

final class HopWebUserFileMenuState {

  private HopWebUserFileMenuState() {}

  static boolean canDownload(IHopFileTypeHandler handler, boolean saveAllowed) {
    return saveAllowed
        && handler != null
        && (handler.getSubject() instanceof PipelineMeta
            || handler.getSubject() instanceof WorkflowMeta);
  }

  static boolean shouldShowProjectExport(
      String projectHome, boolean pluginAvailable, boolean exportAllowed) {
    return StringUtils.isNotBlank(projectHome)
        && !Const.VAR_PROJECT_HOME.equals(projectHome)
        && pluginAvailable
        && exportAllowed;
  }

  static boolean shouldShowKettleImport(
      boolean pluginAvailable, boolean fileCreateAllowed, boolean metadataWriteAllowed) {
    return pluginAvailable && fileCreateAllowed && metadataWriteAllowed;
  }

  static boolean shouldShowSvgExport(
      boolean pipelineOpen, boolean workflowOpen, boolean exportAllowed) {
    return (pipelineOpen || workflowOpen) && exportAllowed;
  }
}
