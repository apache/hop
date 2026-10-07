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

package org.apache.hop.ai.engine;

import org.apache.hop.core.gui.IUndo;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.shared.ISnapshotUndoSupport;

/** Keeps the graph's undo history right when a batch of proposals is rolled back. */
public final class AiProposalUndo {

  private AiProposalUndo() {}

  /**
   * After the pipeline or workflow was put back as it was before a failed batch, the graph's undo
   * history must start from that state again, not from the half-applied one. Nothing was pushed
   * yet: proposals before the last are chained to it.
   */
  public static void forgetPartialChange(HopGui hopGui, IUndo meta) {
    if (hopGui == null || meta == null) {
      return;
    }
    IHopFileTypeHandler handler = hopGui.getActiveFileTypeHandler();
    if (handler instanceof ISnapshotUndoSupport support && support.isUndoMeta(meta)) {
      support.recordAfterChange(true);
    }
  }
}
