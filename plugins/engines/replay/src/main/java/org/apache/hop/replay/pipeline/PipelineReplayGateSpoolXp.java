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

package org.apache.hop.replay.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.replay.ReplayDefaults;
import org.apache.hop.replay.ReplayGate;
import org.apache.hop.replay.ReplayGateUtil;

@ExtensionPoint(
    id = "PipelineReplayGateSpoolXp",
    description = "Spool rows for transforms marked with a Replay Gate",
    extensionPointId = "PipelineStartThreads")
public class PipelineReplayGateSpoolXp implements IExtensionPoint<IPipelineEngine<PipelineMeta>> {

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, IPipelineEngine<PipelineMeta> pipeline)
      throws HopException {
    if (!ReplayGateUtil.isRunningUnderReplay(pipeline)) {
      return;
    }

    PipelineMeta pipelineMeta = pipeline.getPipelineMeta();
    if (pipelineMeta == null) {
      return;
    }

    Map<String, String> replayGroup =
        pipelineMeta.getAttributesMap().get(ReplayDefaults.REPLAY_GATE_GROUP);
    if (replayGroup == null || replayGroup.isEmpty()) {
      return;
    }

    List<ReplayGateSpoolRowListener> listeners = new ArrayList<>();

    for (TransformMeta transformMeta : pipelineMeta.getTransforms()) {
      String transformName = transformMeta.getName();
      ReplayGate gate = ReplayGateUtil.getTransformReplayGate(replayGroup, transformName);
      if (gate != null && gate.isEnabled()) {
        List<IEngineComponent> copies = pipeline.getComponentCopies(transformName);
        for (IEngineComponent copy : copies) {
          ReplayGateSpoolRowListener listener =
              new ReplayGateSpoolRowListener(
                  copy, gate, pipelineMeta.getName(), variables, copies.size(), log);
          try {
            IRowMeta fields = pipelineMeta.getTransformFields(variables, transformName);
            if (fields != null) {
              listener.setSavedRowMeta(fields);
            }
          } catch (Exception e) {
            // Non-critical: metadata will be obtained on the first written row
          }
          copy.addRowListener(listener);
          listeners.add(listener);
          log.logBasic(
              "Replay Gate active on transform ["
                  + transformName
                  + "] (copy "
                  + copy.getCopyNr()
                  + "), spooling rows to "
                  + ReplayGateUtil.getTransformSpoolPath(
                      variables, gate, pipelineMeta.getName(), transformName));
        }
      }
    }

    if (!listeners.isEmpty()) {
      pipeline.addExecutionFinishedListener(
          p -> {
            boolean pipelineHasErrors = p.getErrors() > 0;
            for (ReplayGateSpoolRowListener listener : listeners) {
              boolean hasErrors = pipelineHasErrors || listener.getComponent().getErrors() > 0;
              listener.close(hasErrors);
            }
          });
    }
  }
}
