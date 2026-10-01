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

package org.apache.hop.testing.xp;

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.testing.DataSet;
import org.apache.hop.testing.DataSetDefaults;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;

/**
 * Suggests a name, folder and base file name before the new data set dialog opens. The pipeline is
 * the one open in the explorer, including when the metadata perspective is active. A transform name
 * is included only when exactly one transform is selected; the create-from-transform action sets
 * the clicked transform itself before this runs.
 */
@ExtensionPoint(
    id = "HopGuiDataSetCreateBeforeDialog",
    extensionPointId = "HopGuiMetadataObjectCreateBeforeDialog",
    description = "Suggests a name, folder and filename for a new data set")
public class HopGuiDataSetCreateBeforeDialog implements IExtensionPoint<Object> {

  @Override
  public void callExtensionPoint(ILogChannel log, IVariables variables, Object object)
      throws HopException {
    if (!(object instanceof DataSet dataSet)) {
      return;
    }
    try {
      HopGui hopGui = HopGui.peekInstance();
      PipelineMeta pipelineMeta = activeExplorerPipeline(hopGui);
      IHopMetadataProvider metadataProvider = hopGui == null ? null : hopGui.getMetadataProvider();
      DataSetDefaults.apply(
          dataSet,
          pipelineMeta == null ? null : pipelineMeta.getFilename(),
          singleSelectedTransformName(pipelineMeta),
          variables,
          metadataProvider);
    } catch (Exception e) {
      log.logError("Error suggesting defaults for a new data set", e);
    }
  }

  /**
   * @return the transform name when exactly one transform is selected, otherwise null so several
   *     selected transforms are not collapsed into an arbitrary choice
   */
  static String singleSelectedTransformName(PipelineMeta pipelineMeta) {
    if (pipelineMeta == null) {
      return null;
    }
    List<TransformMeta> selected = pipelineMeta.getSelectedTransforms();
    if (selected == null || selected.size() != 1) {
      return null;
    }
    TransformMeta transform = selected.get(0);
    return transform == null ? null : transform.getName();
  }

  private static PipelineMeta activeExplorerPipeline(HopGui hopGui) {
    if (hopGui == null || hopGui.getPerspectiveManager() == null) {
      return null;
    }
    ExplorerPerspective explorer =
        hopGui.getPerspectiveManager().findPerspective(ExplorerPerspective.class);
    if (explorer == null) {
      return null;
    }
    IHopFileTypeHandler handler = explorer.getActiveFileTypeHandler();
    if (handler instanceof HopGuiPipelineGraph graph) {
      return graph.getPipelineMeta();
    }
    return null;
  }
}
