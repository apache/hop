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

package org.apache.hop.ui.hopgui.file.delegates;

import java.util.List;
import org.apache.hop.base.AbstractMeta;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.security.Permission;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.security.HopSecurityUi;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.dialog.NotePadDialog;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.shared.ISnapshotUndoSupport;

public class HopGuiNotePadDelegate {
  private static final Class<?> PKG = HopGui.class;

  private HopGui hopGui;
  private IHopFileTypeHandler handler;
  private PropsUi props;

  public HopGuiNotePadDelegate(HopGui hopGui, IHopFileTypeHandler handler) {
    this.hopGui = hopGui;
    this.handler = handler;
    this.props = PropsUi.getInstance();
  }

  /** Default note width in canvas units (300 × native zoom). */
  public static int defaultNoteWidth() {
    return (int) Math.round(ConstUi.NOTE_DEFAULT_WIDTH * PropsUi.getNativeZoomFactor());
  }

  public void deleteNotes(AbstractMeta meta, List<NotePadMeta> notes) {
    if (Utils.isEmpty(notes)) {
      return; // Nothing to do
    }
    if (!HopSecurityUi.check(Permission.FILE_EDIT)) {
      return;
    }
    markUndo(meta);
    for (NotePadMeta notePadMeta : notes) {
      int idx = meta.indexOfNote(notePadMeta);
      if (idx >= 0) {
        meta.removeNote(idx);
      }
    }
    handler.updateGui();
  }

  public void deleteNote(AbstractMeta meta, NotePadMeta notePadMeta) {
    if (!HopSecurityUi.check(Permission.FILE_EDIT)) {
      return;
    }
    int idx = meta.indexOfNote(notePadMeta);
    if (idx >= 0) {
      markUndo(meta);
      meta.removeNote(idx);
    }
    handler.updateGui();
  }

  private void markUndo(AbstractMeta meta) {
    if (handler instanceof ISnapshotUndoSupport support && support.isUndoMeta(meta)) {
      support.markUndoPoint();
    }
  }

  private byte[] captureUndo(AbstractMeta meta) {
    if (handler instanceof ISnapshotUndoSupport support && support.isUndoMeta(meta)) {
      return support.captureUndoSnapshot();
    }
    return null;
  }

  private void commitUndo(AbstractMeta meta, byte[] beforeSnapshot) {
    if (handler instanceof ISnapshotUndoSupport support && support.isUndoMeta(meta)) {
      support.commitDialogUndo(beforeSnapshot);
    }
  }

  public void newNote(IVariables variables, AbstractMeta meta, int x, int y) {
    if (!HopSecurityUi.check(Permission.FILE_EDIT)) {
      return;
    }
    String title = BaseMessages.getString(PKG, "PipelineGraph.Dialog.NoteEditor.Title");
    NotePadDialog dialog =
        new NotePadDialog(variables, hopGui.getShell(), title, meta.getFilename());
    byte[] beforeSnapshot = captureUndo(meta);
    NotePadMeta note = dialog.open();
    if (note != null) {
      NotePadMeta newNote =
          new NotePadMeta(
              note.getNote(),
              x,
              y,
              ConstUi.NOTE_MIN_SIZE,
              ConstUi.NOTE_MIN_SIZE,
              note.getFontName(),
              note.getFontSize(),
              note.isFontBold(),
              note.isFontItalic(),
              note.getFontColorRed(),
              note.getFontColorGreen(),
              note.getFontColorBlue(),
              note.getBackGroundColorRed(),
              note.getBackGroundColorGreen(),
              note.getBackGroundColorBlue(),
              note.getBorderColorRed(),
              note.getBorderColorGreen(),
              note.getBorderColorBlue());
      newNote.setMarkdown(note.isMarkdown());
      newNote.setNoteType(note.getNoteType());
      // Apply grid snapping; default width is readable for Markdown wrapping
      PropsUi.setSize(newNote, defaultNoteWidth(), ConstUi.NOTE_MIN_SIZE);
      meta.addNote(newNote);
      commitUndo(meta, beforeSnapshot);
      handler.updateGui();
    }
  }
}
