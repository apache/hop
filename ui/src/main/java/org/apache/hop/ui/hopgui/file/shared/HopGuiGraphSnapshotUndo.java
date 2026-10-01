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
package org.apache.hop.ui.hopgui.file.shared;

import java.util.function.Supplier;
import org.apache.hop.base.AbstractMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.undo.XmlSnapshotUndo;
import org.apache.hop.core.undo.XmlSnapshotUndo.ContentRestorer;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;

/**
 * Graph-owned gzip XML undo/redo stacks, ported from hop-data-vault {@code ModelGraphSnapshotUndo}.
 */
public class HopGuiGraphSnapshotUndo<M extends AbstractMeta> {

  private static final Class<?> PKG = HopGui.class;

  private final HopGui hopGui;
  private final XmlSnapshotUndo<M> engine;
  private final Supplier<M> metaSupplier;
  private final Supplier<String> filenameSupplier;
  private final Runnable afterRestore;

  private byte[] lastSnapshot;
  private byte[] lastSavedSnapshot;
  private boolean positionChangeUndoMarked;

  public HopGuiGraphSnapshotUndo(
      HopGui hopGui,
      Class<M> type,
      String xmlRootTag,
      ContentRestorer<M> restorer,
      Supplier<M> metaSupplier,
      Supplier<String> filenameSupplier,
      Runnable afterRestore) {
    this.hopGui = hopGui;
    this.engine =
        new XmlSnapshotUndo<>(type, xmlRootTag, restorer, () -> PropsUi.getInstance().getMaxUndo());
    this.metaSupplier = metaSupplier;
    this.filenameSupplier = filenameSupplier;
    this.afterRestore = afterRestore;
  }

  public void initialize() {
    lastSnapshot = captureQuiet();
    lastSavedSnapshot = lastSnapshot;
  }

  public void rememberSavedSnapshot() {
    lastSavedSnapshot = captureQuiet();
    lastSnapshot = lastSavedSnapshot;
  }

  public void refreshLastSnapshot() {
    if (engine.isApplyingSnapshot()) {
      return;
    }
    lastSnapshot = captureQuiet();
  }

  public boolean canUndo() {
    return engine.canUndo();
  }

  public boolean canRedo() {
    return engine.canRedo();
  }

  public boolean isApplyingSnapshot() {
    return engine.isApplyingSnapshot();
  }

  public void markUndoPoint() {
    M model = metaSupplier.get();
    if (model == null || engine.isApplyingSnapshot()) {
      return;
    }
    try {
      engine.markChange(model, metadataProvider());
    } catch (HopException e) {
      showRecordError(e);
    }
  }

  public byte[] captureUndoSnapshot() {
    return captureQuiet();
  }

  public void commitDialogUndo(byte[] beforeChange) {
    if (beforeChange == null || engine.isApplyingSnapshot()) {
      return;
    }
    byte[] after = captureQuiet();
    if (after == null || XmlSnapshotUndo.sameXmlContent(beforeChange, after)) {
      return;
    }
    engine.pushSnapshot(beforeChange);
    lastSnapshot = after;
  }

  /**
   * Compatibility hook for leftover {@code addUndo*} calls that fire after the mutation. Pushes
   * {@code lastSnapshot} (the pre-change document) unless {@code nextAlso} indicates a chained
   * follow-up of the same user action.
   */
  public void recordAfterChange(boolean nextAlso) {
    if (engine.isApplyingSnapshot()) {
      return;
    }
    if (!nextAlso && lastSnapshot != null) {
      engine.pushSnapshot(lastSnapshot);
    }
    lastSnapshot = captureQuiet();
  }

  public void markPositionUndoPoint() {
    if (!positionChangeUndoMarked) {
      markUndoPoint();
      positionChangeUndoMarked = true;
    }
  }

  public void resetPositionUndoMark() {
    positionChangeUndoMarked = false;
  }

  public void undo() {
    apply(true);
  }

  public void redo() {
    apply(false);
  }

  private void apply(boolean isUndo) {
    M model = metaSupplier.get();
    if (model == null) {
      return;
    }
    try {
      boolean applied =
          isUndo
              ? engine.undo(model, metadataProvider(), filenameSupplier.get())
              : engine.redo(model, metadataProvider(), filenameSupplier.get());
      if (!applied) {
        return;
      }
      lastSnapshot = captureQuiet();
      applyDirtyFlag(model);
      if (afterRestore != null) {
        afterRestore.run();
      }
    } catch (HopException e) {
      showApplyError(e);
    }
  }

  private void applyDirtyFlag(M model) {
    if (lastSavedSnapshot != null
        && XmlSnapshotUndo.sameXmlContent(lastSnapshot, lastSavedSnapshot)) {
      model.clearChanged();
    } else {
      model.setChanged();
    }
  }

  private byte[] captureQuiet() {
    M model = metaSupplier.get();
    if (model == null || engine.isApplyingSnapshot()) {
      return null;
    }
    try {
      return engine.captureSnapshot(model, metadataProvider());
    } catch (HopException e) {
      showRecordError(e);
      return null;
    }
  }

  private IHopMetadataProvider metadataProvider() {
    return hopGui.getMetadataProvider();
  }

  private void showRecordError(Exception e) {
    new ErrorDialog(
        hopGui.getShell(),
        BaseMessages.getString(PKG, "HopGui.Undo.Error.Record.Title"),
        BaseMessages.getString(PKG, "HopGui.Undo.Error.Record.Message"),
        e);
  }

  private void showApplyError(Exception e) {
    new ErrorDialog(
        hopGui.getShell(),
        BaseMessages.getString(PKG, "HopGui.Undo.Error.Apply.Title"),
        BaseMessages.getString(PKG, "HopGui.Undo.Error.Apply.Message"),
        e);
  }
}
