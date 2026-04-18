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

package org.apache.hop.ui.hopgui.file.pipeline.delegates;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.debug.PipelineDebugMeta;
import org.apache.hop.pipeline.debug.TransformDebugMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.pipeline.debug.PipelineDebugDialog;
import org.apache.hop.ui.pipeline.dialog.PipelineExecutionConfigurationDialog;
import org.eclipse.swt.SWT;

public class HopGuiPipelineRunDelegate {
  private static final Class<?> PKG = HopGui.class;

  private HopGuiPipelineGraph pipelineGraph;
  private HopGui hopGui;

  private PipelineExecutionConfiguration pipelineExecutionConfiguration;
  private PipelineExecutionConfiguration pipelinePreviewDebugExecutionConfiguration;

  /**
   * This contains a map between the name of a pipeline and the PipelineMeta object. If the pipeline
   * has no name it will be mapped under a number [1], [2] etc.
   */
  private List<PipelineMeta> pipelineMap;

  /** Remember preview and debug configuration per pipeline */
  private Map<PipelineMeta, PipelineDebugMeta> pipelinePreviewDebugMetaMap;

  /**
   * @param hopGui
   */
  public HopGuiPipelineRunDelegate(HopGui hopGui, HopGuiPipelineGraph pipelineGraph) {
    this.hopGui = hopGui;
    this.pipelineGraph = pipelineGraph;

    pipelineExecutionConfiguration = new PipelineExecutionConfiguration();
    pipelinePreviewDebugExecutionConfiguration = new PipelineExecutionConfiguration();

    pipelineMap = new ArrayList<>();
    pipelinePreviewDebugMetaMap = new HashMap<>();
  }

  public PipelineExecutionConfiguration executePipeline(
      final ILogChannel log,
      final PipelineMeta pipelineMeta,
      final boolean previewDebug,
      LogLevel logLevel)
      throws HopException {

    if (pipelineMeta == null) {
      return null;
    }

    // See if we need to ask for debugging information...
    //
    PipelineDebugMeta pipelineDebugMeta = null;
    PipelineExecutionConfiguration executionConfiguration = null;

    if (previewDebug) {
      executionConfiguration = getPipelinePreviewDebugExecutionConfiguration();
    } else {
      executionConfiguration = getPipelineExecutionConfiguration();
    }

    // Set MetaStore and safe mode information in both the exec config and the metadata
    //
    pipelineMeta.setMetadataProvider(hopGui.getMetadataProvider());

    if (previewDebug) {
      // Collect the first N rows from selected transforms; pause-on-breakpoint is configured per
      // transform in the dialog (TransformDebugMeta#setPausingOnBreakPoint).
      pipelineDebugMeta =
          pipelinePreviewDebugMetaMap.computeIfAbsent(pipelineMeta, PipelineDebugMeta::new);

      // Reset execution-state flag from any previous run so the finished listener
      // is not silently skipped when the same PipelineDebugMeta object is reused.
      pipelineDebugMeta.setStopClosePressed(false);

      List<TransformMeta> selectedTransforms = pipelineMeta.getSelectedTransforms();
      if (!Utils.isEmpty(selectedTransforms)) {
        pipelineDebugMeta.getTransformDebugMetaMap().clear();
        for (TransformMeta transformMeta : pipelineMeta.getSelectedTransforms()) {
          TransformDebugMeta transformDebugMeta = new TransformDebugMeta(transformMeta);
          transformDebugMeta.setRowCount(PropsUi.getInstance().getDefaultPreviewSize());
          transformDebugMeta.setReadingFirstRows(true);
          transformDebugMeta.setPausingOnBreakPoint(false);
          pipelineDebugMeta.getTransformDebugMetaMap().put(transformMeta, transformDebugMeta);
        }
      }
    }

    int debugAnswer = PipelineDebugDialog.DEBUG_CONFIG;

    if (previewDebug) {
      PipelineDebugDialog pipelineDebugDialog =
          new PipelineDebugDialog(
              hopGui.getShell(), pipelineGraph.getVariables(), pipelineDebugMeta);
      debugAnswer = pipelineDebugDialog.open();
      if (debugAnswer == PipelineDebugDialog.DEBUG_CANCEL) {
        return null;
      }
    }

    Map<String, String> variableMap = new HashMap<>();
    variableMap.putAll(executionConfiguration.getVariablesMap()); // the default

    executionConfiguration.setVariablesMap(variableMap);
    executionConfiguration.getUsedVariables(pipelineGraph.getVariables(), pipelineMeta);
    executionConfiguration.setLogLevel(logLevel);

    if (previewDebug) {
      // Make sure to re-set the default parameter values. They could have been changed since the
      // last execution.
      //
      for (String parameterName : pipelineMeta.listParameters()) {
        String defaultValue = pipelineMeta.getParameterDefault(parameterName);
        if (StringUtils.isNotEmpty(defaultValue)) {
          executionConfiguration.getParametersMap().put(parameterName, defaultValue);
        }
      }
    }

    boolean execConfigAnswer = true;

    if (debugAnswer == PipelineDebugDialog.DEBUG_CONFIG) {
      PipelineExecutionConfigurationDialog dialog =
          new PipelineExecutionConfigurationDialog(
              hopGui.getShell(), executionConfiguration, pipelineMeta);
      execConfigAnswer = dialog.open();
    }

    if (execConfigAnswer) {
      pipelineGraph.pipelineLogDelegate.addPipelineLog();
      pipelineGraph.pipelineGridDelegate.addPipelineGrid();

      // Set the run options
      pipelineMeta.setClearingLog(executionConfiguration.isClearingLog());

      ExtensionPointHandler.callExtensionPoint(
          log,
          pipelineGraph.getVariables(),
          HopExtensionPoint.HopGuiPipelineMetaExecutionStart.id,
          pipelineMeta);
      ExtensionPointHandler.callExtensionPoint(
          log,
          pipelineGraph.getVariables(),
          HopExtensionPoint.HopGuiPipelineExecutionConfiguration.id,
          executionConfiguration);

      if (previewDebug) {
        if (pipelineDebugMeta.getNrOfUsedTransforms() == 0) {
          MessageBox box = new MessageBox(hopGui.getShell(), SWT.ICON_WARNING | SWT.YES | SWT.NO);
          box.setText(
              BaseMessages.getString(
                  PKG, "HopGui.Dialog.Warning.NoPreviewOrDebugTransforms.Title"));
          box.setMessage(
              BaseMessages.getString(
                  PKG, "HopGui.Dialog.Warning.NoPreviewOrDebugTransforms.Message"));
          int answer = box.open();
          if (answer != SWT.YES) {
            return null;
          }
        }
        pipelineGraph.debug(executionConfiguration, pipelineDebugMeta);
      } else {
        pipelineGraph.start(executionConfiguration);
      }
    }
    return executionConfiguration;
  }

  /**
   * Gets pipelineGraph
   *
   * @return value of pipelineGraph
   */
  public HopGuiPipelineGraph getPipelineGraph() {
    return pipelineGraph;
  }

  /**
   * @param pipelineGraph The pipelineGraph to set
   */
  public void setPipelineGraph(HopGuiPipelineGraph pipelineGraph) {
    this.pipelineGraph = pipelineGraph;
  }

  /**
   * Gets hopGui
   *
   * @return value of hopGui
   */
  public HopGui getHopGui() {
    return hopGui;
  }

  /**
   * @param hopGui The hopGui to set
   */
  public void setHopGui(HopGui hopGui) {
    this.hopGui = hopGui;
  }

  /**
   * Gets pipelineExecutionConfiguration
   *
   * @return value of pipelineExecutionConfiguration
   */
  public PipelineExecutionConfiguration getPipelineExecutionConfiguration() {
    return pipelineExecutionConfiguration;
  }

  /**
   * @param pipelineExecutionConfiguration The pipelineExecutionConfiguration to set
   */
  public void setPipelineExecutionConfiguration(
      PipelineExecutionConfiguration pipelineExecutionConfiguration) {
    this.pipelineExecutionConfiguration = pipelineExecutionConfiguration;
  }

  /** Execution settings for pipeline preview and debug runs. */
  public PipelineExecutionConfiguration getPipelinePreviewDebugExecutionConfiguration() {
    return pipelinePreviewDebugExecutionConfiguration;
  }

  /**
   * @param pipelinePreviewDebugExecutionConfiguration The preview and debug execution configuration
   */
  public void setPipelinePreviewDebugExecutionConfiguration(
      PipelineExecutionConfiguration pipelinePreviewDebugExecutionConfiguration) {
    this.pipelinePreviewDebugExecutionConfiguration = pipelinePreviewDebugExecutionConfiguration;
  }

  /**
   * Gets pipelineMap
   *
   * @return value of pipelineMap
   */
  public List<PipelineMeta> getPipelineMap() {
    return pipelineMap;
  }

  /**
   * @param pipelineMap The pipelineMap to set
   */
  public void setPipelineMap(List<PipelineMeta> pipelineMap) {
    this.pipelineMap = pipelineMap;
  }

  /** Preview and debug metadata remembered per pipeline. */
  public Map<PipelineMeta, PipelineDebugMeta> getPipelinePreviewDebugMetaMap() {
    return pipelinePreviewDebugMetaMap;
  }

  /**
   * @param pipelinePreviewDebugMetaMap The preview and debug meta map to set
   */
  public void setPipelinePreviewDebugMetaMap(
      Map<PipelineMeta, PipelineDebugMeta> pipelinePreviewDebugMetaMap) {
    this.pipelinePreviewDebugMetaMap = pipelinePreviewDebugMetaMap;
  }
}
